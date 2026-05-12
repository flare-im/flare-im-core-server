//! 应用启动：gRPC PushService 监听

use std::net::SocketAddr;

use anyhow::{Context, Result};
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::PUSH_PROXY;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::load_config;

        let app_config = load_config(Some("config"));
        let listen =
            std::env::var("PUSH_PROXY_LISTEN").unwrap_or_else(|_| "0.0.0.0:50090".to_string());
        let address: SocketAddr = listen
            .parse()
            .context("PUSH_PROXY_LISTEN must be host:port (e.g. 0.0.0.0:50090)")?;
        info!(address = %address, "Push Proxy listen address");

        let context = wire::initialize(app_config).await?;
        Self::run_with_context(context, address).await
    }

    async fn run_with_context(context: ApplicationContext, address: SocketAddr) -> Result<()> {
        use flare_grpc_proto::push::push_service_server::PushServiceServer;
        use tonic::transport::Server;

        let handler = context.handler.clone();
        let address_clone = address;

        let runtime = flare_im_core::health::attach_runtime_health_checks(
            ServiceRuntime::new(PUSH_PROXY)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
                .add_spawn_with_shutdown("push-proxy-grpc", move |shutdown_rx| async move {
                    use flare_server_core::middleware::ContextLayer;

                    let svc = ContextLayer::new()
                        .allow_missing()
                        .layer(PushServiceServer::new(handler));

                    Server::builder()
                        .add_service(svc)
                        .serve_with_shutdown(address_clone, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| format!("gRPC server error: {}", e).into())
                }),
            PUSH_PROXY,
        );

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only(PUSH_PROXY, addr, None)
                        .await
                })
            })
            .await
    }
}
