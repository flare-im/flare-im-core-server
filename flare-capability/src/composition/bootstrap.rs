//! # 进程启动（Composition Root）
//!
//! 解析监听地址、调用 [`super::wiring::initialize`] 装配依赖，并注册 **gRPC + 运行时**（与领域逻辑分离）。

use std::net::SocketAddr;

use anyhow::{Context, Result};
use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::CAPABILITY;
use tracing::info;

use crate::interface::grpc::StromSfuExtensionPluginServer;

use super::process_config::CapabilityServiceConfig;
use super::runtime_context::ApplicationContext;
use super::wiring;

/// 应用启动器（仅进程生命周期与传输层）。
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点。
    pub async fn run(config: CapabilityServiceConfig) -> Result<()> {
        use flare_im_core::load_config;

        let app_config = load_config(Some("config"));

        let address: SocketAddr = format!(
            "{}:{}",
            app_config.base().server.address,
            app_config.base().server.port
        )
        .parse()
        .context("invalid flare-capability server address")?;
        info!(address = %address, "Server address parsed successfully");

        let context = wiring::initialize(config).await?;

        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, address).await
    }

    async fn run_with_context(context: ApplicationContext, address: SocketAddr) -> Result<()> {
        use tonic::transport::Server;

        info!(
            address = %address,
            port = %address.port(),
            "Starting Flare Capability gRPC (HookPlugin + ExtensionPlugin + CapabilityService)..."
        );

        let address_clone = address;
        let im_hook_plugin = context.im_hook_plugin;
        let capability_grpc = context.capability_grpc;
        let plugin_routes = context.plugin_routes;
        let strom_sfu_rtc = context.strom_sfu_rtc;

        let runtime = ServiceRuntime::new(CAPABILITY)
            .with_address(address)
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
            .add_spawn_with_shutdown("flare-capability-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let hook_plugin_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::hook_plugin_server::HookPluginServer::new(
                            im_hook_plugin,
                        ),
                    );

                let extension_plugin =
                    StromSfuExtensionPluginServer::new(strom_sfu_rtc, plugin_routes);
                let extension_plugin_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::extension_plugin_server::ExtensionPluginServer::new(
                            extension_plugin,
                        ),
                    );

                let capability_service = ContextLayer::new()
                    .allow_missing()
                    .layer(
                        flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer::new(
                            capability_grpc,
                        ),
                    );

                info!("HookPlugin + ExtensionPlugin (SFU / strom when configured) + CapabilityService registered");
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

        let runtime =
            flare_im_core::health::attach_runtime_health_checks(runtime, CAPABILITY);

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only(CAPABILITY, addr, None)
                        .await
                })
            })
            .await
    }
}
