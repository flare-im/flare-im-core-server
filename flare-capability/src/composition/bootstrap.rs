//! # 进程启动（Composition Root）
//!
//! 解析监听地址、调用 [`super::wiring::initialize`] 装配依赖，并注册 **gRPC + 运行时**（与领域逻辑分离）。
//!
//! ## 插件装配钩子
//!
//! [`ApplicationBootstrap::run_with_rtc_plugins`] 在 wiring 完成后、gRPC 启动前调用调用方提供的
//! `wire_plugins(PluginContext)`，由外层 binary 按编译 `feature` 把 RTC / 媒体后端等插件 crate
//! 通过 `set_rtc_backend` / `register_extension_operations` / `PluginRouteBook::upsert` 挂入；
//! 核心代码对具体实现一无所知。

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::CAPABILITY;
use tracing::info;

use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
use crate::infrastructure::config::CapabilityRuntimeConfig;

use super::process_config::CapabilityServiceConfig;
use super::runtime_context::ApplicationContext;
use super::wiring;

/// 插件装配上下文：暴露挂入 RTC / Extension operations 所需的最小句柄集。
///
/// 仍在核心内定义（只依赖核心类型），避免任何实现细节泄漏。
///
/// 字段均为 [`Clone`] 且内部共享 `Arc`：可 **按值** 传入异步 `wire` 闭包，避免生命周期与
/// `FnOnce(&mut …)` 的高阶 trait 问题。
#[derive(Clone)]
pub struct PluginContext {
    pub registry: CapabilityExtensionRegistry,
    pub plugin_routes: Arc<PluginRouteBook>,
    pub runtime: Arc<CapabilityRuntimeConfig>,
}

/// 应用启动器（仅进程生命周期与传输层）。
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点（**不含**任何 RTC 后端）。
    ///
    /// 外层 binary 如果要默认挂入 RTC 后端，请改用 [`Self::run_with_rtc_plugins`]。
    pub async fn run(config: CapabilityServiceConfig) -> Result<()> {
        Self::run_with_rtc_plugins(config, |_ctx| async move { Ok::<(), anyhow::Error>(()) }).await
    }

    /// 运行应用，并在 wiring 完成后调用 `wire_plugins` 挂入 RTC / Extension 实现。
    pub async fn run_with_rtc_plugins<F, Fut>(
        config: CapabilityServiceConfig,
        wire_plugins: F,
    ) -> Result<()>
    where
        F: FnOnce(PluginContext) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
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

        let plugin_ctx = PluginContext {
            registry: context.capability_registry.clone(),
            plugin_routes: Arc::clone(&context.plugin_routes),
            runtime: Arc::clone(&context.capability_runtime),
        };
        wire_plugins(plugin_ctx).await?;

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
        let _plugin_routes = context.plugin_routes;
        let extension_router = context.extension_router;

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

        let runtime = flare_im_core::health::attach_runtime_health_checks(runtime, CAPABILITY);

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
