//! # 进程启动（Composition Root）
//!
//! 解析监听地址、调用 [`super::wiring::initialize`] 装配依赖，并注册 **gRPC + 运行时**。

use flare_im_contracts::service_names::CAPABILITY;
use flare_server_core::error::Result;
use tracing::info;

use super::process_config::CapabilityServiceConfig;
use super::runtime_context::ApplicationContext;
use super::wiring;

/// 应用启动器（仅进程生命周期与传输层）。
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
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
