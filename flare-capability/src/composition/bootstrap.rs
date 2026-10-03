//! # 进程启动（Composition Root）
//!
//! 解析监听地址、调用 [`super::wiring::initialize`] 装配依赖，并注册 **gRPC + 运行时**。

use flare_im_contracts::service_names::CAPABILITY;
use flare_server_core::error::Result;
use tracing::info;

use crate::domain::model::ExecutionMode;

use super::process_config::CapabilityServiceConfig;
use super::runtime_context::ApplicationContext;
use super::wiring;

/// 应用启动器（仅进程生命周期与传输层）。
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run_from_env() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let _tracing =
            flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()))?;

        let config = capability_config_from_env(app_config)?;
        info!(
            config_file = ?config.config_file,
            runtime_config_file = ?config.runtime_config_file,
            tenant_id = ?config.tenant_id,
            refresh_interval_secs = config.refresh_interval_secs,
            "Starting Flare Capability service"
        );

        Self::run(config).await
    }

    pub async fn run(config: CapabilityServiceConfig) -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let cap_service = app_config.capability_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &cap_service.runtime,
            CAPABILITY,
            "CAPABILITY",
            50110,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        let context = wiring::initialize(config).await?;
        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, runtime_plan).await
    }

    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
    ) -> Result<()> {
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());
        info!(
            address = %address,
            port = %address.port(),
            "Starting Flare Capability gRPC (HookPlugin + ExtensionPlugin + CapabilityService)..."
        );

        let address_clone = address;
        let im_hook_plugin = context.im_hook_plugin;
        let capability_grpc = context.capability_grpc;
        let extension_router = context.extension_router;
        let tenant_projection = context.tenant_projection;
        let capability_admin = context.capability_admin;
        let hook_config_listener = context.hook_config_listener;

        let mut runtime = runtime_plan.service_runtime();
        // Hook 配置失效通知的监听任务（独立 PostgreSQL 连接）随进程停机停掉、关掉连接。
        if let Some(listener) = hook_config_listener {
            runtime = runtime.add_spawn_with_shutdown(
                "flare-capability-hook-config-listener",
                move |shutdown_rx| async move {
                    let _ = shutdown_rx.await;
                    listener.shutdown().await;
                    Ok(())
                },
            );
        }
        let runtime = runtime
            .add_spawn_with_shutdown("flare-capability-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                // HookPlugin 不经旧元数据上下文：每个 Call 在服务内验 `x-flare-ctx`，无签名或
                // 验不过一律 UNAUTHENTICATED（见 ImHookPluginServer）。
                let hook_plugin_service =
                    flare_grpc_proto::capability::hook_plugin_server::HookPluginServer::new(
                        im_hook_plugin,
                    );

                let extension_plugin_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::extension_plugin_server::ExtensionPluginServer::new(
                            extension_router,
                        ),
                    );

                let capability_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer::new(
                            capability_grpc,
                        ),
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

                info!(
                    tenant_projection = tenant_projection_service.is_some(),
                    "HookPlugin + ExtensionPlugin (router) + CapabilityService registered"
                );
                let server = Server::builder()
                    .layer(grpc_metrics)
                    .add_service(hook_plugin_service)
                    .add_service(extension_plugin_service)
                    .add_service(capability_service)
                    .add_service(capability_admin_service)
                    .add_optional_service(tenant_projection_service);

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
                    flare_im_service_kit::discovery::register_runtime_service_only(
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

fn default_config_center(explicit: Option<String>, static_registry: bool) -> Option<String> {
    match explicit {
        Some(endpoint) => Some(endpoint),
        None if static_registry => None,
        None => Some("consul://localhost:28500".to_string()),
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
    app_config: &flare_im_service_kit::FlareAppConfig,
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

    // Config center is Consul (see deploy/docker-compose.yml: Consul serves both service
    // registry and config center); there is no etcd in the deployment. Defaulting to the
    // stale etcd endpoint caused the config watcher to fail every 60s with "tcp connect error".
    // Explicit static mode has no Consul at all: only an explicitly configured config
    // center is used, otherwise the watcher polls a nonexistent localhost agent forever.
    let explicit_endpoint = std::env::var("CONFIG_CENTER_ENDPOINT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let config_center_endpoint =
        default_config_center(explicit_endpoint, app_config.uses_static_registry());

    let tenant_id = std::env::var("TENANT_ID").ok();
    let config_file = std::env::var("CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/hooks.toml")));
    let runtime_config_file = std::env::var("CAPABILITY_RUNTIME_CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/services/capability.toml")));

    Ok(CapabilityServiceConfig {
        config_file,
        runtime_config_file,
        database_url,
        postgres_max_connections,
        postgres_min_connections,
        postgres_acquire_timeout_seconds,
        postgres_idle_timeout_seconds,
        postgres_max_lifetime_seconds,
        config_center_endpoint,
        tenant_id,
        execution_mode: ExecutionMode::Sequential,
        refresh_interval_secs: 60,
    })
}

#[cfg(test)]
mod config_center_tests {
    use super::default_config_center;

    #[test]
    fn static_registry_uses_only_an_explicit_config_center() {
        assert_eq!(default_config_center(None, true), None);
        assert_eq!(
            default_config_center(Some("consul://consul:8500".into()), true).as_deref(),
            Some("consul://consul:8500")
        );
        assert_eq!(
            default_config_center(None, false).as_deref(),
            Some("consul://localhost:28500")
        );
    }
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
