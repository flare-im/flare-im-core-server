//! # 进程启动（Composition Root）
//!
//! 解析监听地址、调用 [`super::wiring::initialize`] 装配依赖，并注册 **gRPC + 运行时**。

use flare_im_core_contracts::service_names::CAPABILITY;
use flare_server_core::error::Result;
use tracing::info;

use super::extensions::CapabilityExtensions;
use super::process_config::CapabilityServiceConfig;
use super::runtime_context::ApplicationContext;
use super::wiring;

/// 应用启动器（仅进程生命周期与传输层）。
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run_from_env() -> Result<()> {
        Self::run_from_env_with(CapabilityExtensions::default()).await
    }

    /// [`Self::run_from_env`] with what the binary adds, such as a plugin service router.
    pub async fn run_from_env_with(extensions: CapabilityExtensions) -> Result<()> {
        let app_config = flare_im_core_service_kit::load_app_config_from_env();
        let _tracing = flare_im_core_service_kit::tracing::init_tracing_from_config(Some(
            app_config.logging(),
        ))?;

        let config = capability_config_from_env(app_config)?;
        info!(
            runtime_config_file = ?config.runtime_config_file,
            "Starting Flare Capability service"
        );

        Self::run_with(config, extensions).await
    }

    pub async fn run(config: CapabilityServiceConfig) -> Result<()> {
        Self::run_with(config, CapabilityExtensions::default()).await
    }

    pub async fn run_with(
        config: CapabilityServiceConfig,
        extensions: CapabilityExtensions,
    ) -> Result<()> {
        let app_config = flare_im_core_service_kit::load_app_config_from_env();
        let cap_service = app_config.capability_service();
        let runtime_plan = flare_im_core_service_kit::build_service_runtime_plan(
            app_config,
            &cap_service.runtime,
            CAPABILITY,
            "CAPABILITY",
            50110,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        let context = wiring::initialize(config, extensions).await?;
        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, runtime_plan).await
    }

    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_core_service_kit::ImServiceRuntimePlan,
    ) -> Result<()> {
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_core_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());
        info!(
            address = %address,
            port = %address.port(),
            "Starting Flare Capability gRPC (ExtensionPlugin + CapabilityService)..."
        );

        let address_clone = address;
        let capability_grpc = context.capability_grpc;
        let extension_router = context.extension_router;
        let extension_verifier = context.extension_verifier;
        let plugin_resource_bridge = context.plugin_resource_bridge;
        let tenant_projection = context.tenant_projection;
        let client_build_stats = context.client_build_stats;
        let tenant_notice = context.tenant_notice;
        let capability_admin = context.capability_admin;
        let hook_retention_background = context.hook_retention_background;
        let entity_lifecycle_worker = context.entity_lifecycle_worker;
        let tenant_notice_relay = context.tenant_notice_relay;
        let plugin_reconciler = context.plugin_reconciler;

        let mut runtime = runtime_plan.service_runtime();
        for task in context.plugin_message_events {
            runtime = runtime.add_task(Box::new(task));
        }
        if let Some((pool, publisher)) = context.audit_delivery {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-audit-delivery",
                move |shutdown| async move {
                    publisher
                        .run_durable(pool, async {
                            let _ = shutdown.await;
                        })
                        .await;
                    Ok(())
                },
            );
        }
        // 插件实体生命周期通知：至少一次调用插件的 entityLifecycle.unbound，停机时结束本轮。
        if let Some(worker) = entity_lifecycle_worker {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-entity-lifecycle",
                move |shutdown| async move {
                    worker
                        .run(async {
                            let _ = shutdown.await;
                        })
                        .await;
                    Ok(())
                },
            );
        }
        // 插件运行时收敛：比较期望和已确认状态，向插件进程下发，停机时结束本轮。
        if let Some(reconciler) = plugin_reconciler {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-plugin-reconcile",
                move |shutdown| async move {
                    reconciler
                        .run(async {
                            let _ = shutdown.await;
                        })
                        .await;
                    Ok(())
                },
            );
        }
        // 租户通知 outbox：至少一次发到通知通道，多副本并发时通道按版本去重。
        if let Some(relay) = tenant_notice_relay {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-tenant-notice-relay",
                move |shutdown| async move {
                    relay
                        .run(async {
                            let _ = shutdown.await;
                        })
                        .await;
                    Ok(())
                },
            );
        }
        // Hook 执行证据的保留期清理；写入在 Ingest。
        if let Some(background) = hook_retention_background {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-hook-execution-retention",
                move |shutdown_rx| async move {
                    crate::infrastructure::hook_retention::run_retention(
                        background.repository,
                        background.metrics,
                        background.task,
                        shutdown_rx,
                    )
                    .await;
                    Ok(())
                },
            );
        }
        let runtime = runtime
            .add_spawn_with_shutdown("flare-capability-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                // ExtensionPlugin.Call: the Cell-signed `x-flare-ctx` is the only identity. Anonymous
                // admission answers the health check alone; without a key set the router is served
                // bare and answers nothing but the health check (caller metadata is never read).
                let (extension_plugin_signed, extension_plugin_unverified) =
                    crate::interface::grpc::extensions::extension_plugin_services(
                        extension_router,
                        extension_verifier,
                    );

                let capability_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        // Administer carries plugin packages: up to 32 MiB of raw bytes.
                        flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer::new(
                            capability_grpc,
                        )
                        .max_decoding_message_size(48 * 1024 * 1024)
                        .max_encoding_message_size(48 * 1024 * 1024),
                    );

                let capability_admin_service = ContextLayer::new().allow_missing().layer(
                    flare_grpc_proto::capability::capability_admin_service_server::CapabilityAdminServiceServer::new(
                        capability_admin,
                    ),
                );

                let tenant_projection_service = tenant_projection.map(|server| {
                    ContextLayer::new().allow_missing().layer(
                        flare_grpc_proto::control::tenant_projection_server::TenantProjectionServer::new(
                            server,
                        ),
                    )
                });

                let client_build_stats_service = client_build_stats.map(|server| {
                    ContextLayer::new().allow_missing().layer(
                        flare_grpc_proto::control::client_build_stats_server::ClientBuildStatsServer::new(
                            server,
                        ),
                    )
                });

                info!(
                    tenant_projection = tenant_projection_service.is_some(),
                    client_build_stats = client_build_stats_service.is_some(),
                    "ExtensionPlugin (router) + CapabilityService registered"
                );
                let server = Server::builder()
                    .layer(grpc_metrics)
                    .add_optional_service(extension_plugin_signed)
                    .add_optional_service(extension_plugin_unverified)
                    .add_service(capability_service)
                    .add_service(capability_admin_service)
                    .add_optional_service(plugin_resource_bridge.map(|bridge| flare_grpc_proto::capability::plugin_host_bridge_server::PluginHostBridgeServer::new(bridge)
                        .max_decoding_message_size(16384).max_encoding_message_size(66560)))
                    .add_optional_service(tenant_projection_service)
                    .add_optional_service(client_build_stats_service)
                    .add_service(
                        flare_grpc_proto::capability::tenant_notice_service_server::TenantNoticeServiceServer::new(
                            tenant_notice,
                        ),
                    );

                server
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| {
                        // tonic 的 transport error 把真因（AddrInUse / 权限 / TLS 等）
                        // 藏在 source() 链里，只 `{}` 打顶层就只剩一句
                        // "transport error"——服务崩溃重启 83 次却查不出为什么。
                        let mut detail = e.to_string();
                        let mut cause: Option<&(dyn std::error::Error + 'static)> =
                            std::error::Error::source(&e);
                        while let Some(c) = cause {
                            detail.push_str(&format!(" <- {c}"));
                            cause = c.source();
                        }
                        format!("gRPC server error: {detail} (address={address_clone})").into()
                    })
            });

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core_service_kit::discovery::register_runtime_service_only(
                        &service_name,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}

/// Capability 的 PostgreSQL 连接串：`DATABASE_URL`（非空）优先，其次是配置档里的 postgres 档
/// （`[services.capability].postgres`，没有则 `media`）。两者都没有时报错、服务不启动：以前回落到
/// 内置的带口令开发库连接串，部署漏配时会悄悄连向一个带默认口令的库（与「不带默认库口令」的
/// 决定相悖）。错误里不带任何连接串。
fn resolve_database_url(
    env_database_url: Option<String>,
    profile_url: Option<&str>,
) -> Result<String> {
    env_database_url
        .filter(|url| !url.trim().is_empty())
        .or_else(|| {
            profile_url
                .filter(|url| !url.trim().is_empty())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::ConfigurationError,
                "flare-capability needs a PostgreSQL connection: set DATABASE_URL, or a postgres profile for the capability service ([services.capability].postgres, or [postgres.media])",
            )
            .build_error()
        })
}

fn capability_config_from_env(
    app_config: &flare_im_core_service_kit::FlareAppConfig,
) -> Result<CapabilityServiceConfig> {
    let cap_service = app_config.capability_service();
    let postgres_profile = cap_service
        .postgres
        .as_deref()
        .and_then(|name| app_config.postgres_profile(name))
        .or_else(|| app_config.postgres_profile("media"));
    let database_url = Some(resolve_database_url(
        std::env::var("DATABASE_URL").ok(),
        postgres_profile.map(|p| p.url.as_str()),
    )?);
    let postgres_max_connections = std::env::var("CAPABILITY_POSTGRES_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.max_connections))
        .unwrap_or(10);
    let postgres_min_connections = std::env::var("CAPABILITY_POSTGRES_MIN_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.min_connections))
        .unwrap_or(2);
    let postgres_acquire_timeout_seconds =
        std::env::var("CAPABILITY_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|p| p.acquire_timeout_seconds))
            .unwrap_or(10);
    let postgres_idle_timeout_seconds = std::env::var("CAPABILITY_POSTGRES_IDLE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.idle_timeout_seconds))
        .unwrap_or(300);
    let postgres_max_lifetime_seconds = std::env::var("CAPABILITY_POSTGRES_MAX_LIFETIME_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.max_lifetime_seconds))
        .unwrap_or(1800);

    let runtime_config_file = std::env::var("CAPABILITY_RUNTIME_CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/services/capability.toml")));
    // The tenant notice channel lives next to the gateway kick channel `{namespace}:kick`.
    let token_store = app_config.redis_profile("token_store");
    let notice_redis_url = std::env::var("FLARE_CAPABILITY_NOTICE_REDIS_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| token_store.map(|profile| profile.url.clone()));
    let notice_publishers = crate::domain::model::NoticePublishers::parse(
        &std::env::var("FLARE_TENANT_NOTICE_PUBLISHERS").unwrap_or_default(),
    )
    .map_err(|detail| {
        flare_server_core::error::FlareError::system(format!(
            "invalid FLARE_TENANT_NOTICE_PUBLISHERS: {detail}"
        ))
    })?;
    let notice_channel = flare_im_core_service_kit::tenant_notice::notice_channel(
        token_store
            .and_then(|profile| profile.namespace.as_deref())
            .unwrap_or("flare"),
    );

    Ok(CapabilityServiceConfig {
        runtime_config_file,
        database_url,
        postgres_max_connections,
        postgres_min_connections,
        postgres_acquire_timeout_seconds,
        postgres_idle_timeout_seconds,
        postgres_max_lifetime_seconds,
        notice_redis_url,
        notice_channel,
        notice_publishers,
    })
}

#[cfg(test)]
mod database_url_tests {
    use super::resolve_database_url;

    const ENV_URL: &str = "postgres://w14h-env@db:5432/core";
    const PROFILE_URL: &str = "postgres://w14h-profile@db:5432/core";

    /// 既没有 `DATABASE_URL` 也没有 postgres 档（或都为空）时报配置错误，不回落到内置的带口令
    /// 连接串；错误里不带任何连接串。
    #[test]
    fn a_missing_database_url_fails_closed() {
        for (env, profile) in [
            (None, None),
            (Some(String::new()), None),
            (Some("  ".to_string()), Some("")),
        ] {
            let error = resolve_database_url(env.clone(), profile).unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(printed.contains("DATABASE_URL"), "{printed}");
            assert!(!printed.contains("postgres://"), "{printed}");
            assert!(!printed.contains("postgresql://"), "{printed}");
            assert!(!printed.contains("flare123"), "{printed}");
        }
    }

    /// 显式给出的连接串照用：`DATABASE_URL` 优先于配置档，空的 `DATABASE_URL` 不遮住配置档。
    #[test]
    fn an_explicit_database_url_is_used() {
        assert_eq!(
            resolve_database_url(Some(ENV_URL.into()), Some(PROFILE_URL)).unwrap(),
            ENV_URL
        );
        assert_eq!(
            resolve_database_url(None, Some(PROFILE_URL)).unwrap(),
            PROFILE_URL
        );
        assert_eq!(
            resolve_database_url(Some(String::new()), Some(PROFILE_URL)).unwrap(),
            PROFILE_URL
        );
    }
}
