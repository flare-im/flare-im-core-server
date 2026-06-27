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
        flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()));

        let config = capability_config_from_env(app_config);
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
        info!(
            address = %address,
            port = %address.port(),
            "Starting Flare Capability gRPC (HookPlugin + ExtensionPlugin + CapabilityService)..."
        );

        let address_clone = address;
        let im_hook_plugin = context.im_hook_plugin;
        let capability_grpc = context.capability_grpc;
        let extension_router = context.extension_router;

        let runtime = runtime_plan
            .service_runtime()
            .add_spawn_with_shutdown("flare-capability-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let hook_plugin_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::hook_plugin_server::HookPluginServer::new(
                            im_hook_plugin,
                        ),
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

                info!("HookPlugin + ExtensionPlugin (router) + CapabilityService registered");
                let server = Server::builder()
                    .add_service(hook_plugin_service)
                    .add_service(extension_plugin_service)
                    .add_service(capability_service);

                server
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
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

fn capability_config_from_env(
    app_config: &flare_im_service_kit::FlareAppConfig,
) -> CapabilityServiceConfig {
    let cap_service = app_config.capability_service();
    let postgres_profile = cap_service
        .postgres
        .as_deref()
        .and_then(|name| app_config.postgres_profile(name))
        .or_else(|| app_config.postgres_profile("media"));
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| postgres_profile.map(|p| p.url.clone()))
        .or_else(|| Some("postgresql://flare:flare123@localhost:25432/flare2".to_string()));
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

    let config_center_endpoint = std::env::var("CONFIG_CENTER_ENDPOINT")
        .ok()
        .or_else(|| Some("etcd://localhost:22379".to_string()));

    let tenant_id = std::env::var("TENANT_ID").ok();
    let config_file = std::env::var("CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/hooks.toml")));
    let runtime_config_file = std::env::var("CAPABILITY_RUNTIME_CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/services/capability.toml")));

    CapabilityServiceConfig {
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
    }
}
