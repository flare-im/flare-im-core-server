//! 应用启动器 - 负责依赖注入和服务启动

use std::net::SocketAddr;

use anyhow::{Context as AnyhowContext, Result};
use tracing::{error, info};

use crate::service::wire::{self, ApplicationContext};
use flare_im_core::service_names::SIGNALING_ROUTE;
use flare_server_core::runtime::ServiceRuntime;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        let app_config = load_config(Some("./config"));
        let service_config = app_config.signaling_route_service();

        info!("Parsing server address...");
        let address: SocketAddr =
            ServiceHelper::parse_server_addr(app_config, &service_config.runtime, SIGNALING_ROUTE)
                .with_context(|| "invalid signaling route server address")?;
        info!(address = %address, "Server address parsed successfully");

        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(context: ApplicationContext, address: SocketAddr) -> Result<()> {
        use flare_proto::signaling::router::router_upstream_service_server::RouterUpstreamServiceServer;
        use tonic::transport::Server;

        let upstream_handler = context.upstream_handler;

        info!(
            address = %address,
            port = %address.port(),
            "Starting Router gRPC service (upstream only; downstream push lives in flare-push-worker)"
        );

        let address_clone = address;
        let runtime = ServiceRuntime::new(SIGNALING_ROUTE, address)
            .add_spawn_with_shutdown("router-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let upstream_service = ContextLayer::new()
                    .allow_missing()
                    .layer(RouterUpstreamServiceServer::new(upstream_handler));

                Server::builder()
                    .add_service(upstream_service)
                    .serve_with_shutdown(address_clone, async move {
                        info!(
                            address = %address_clone,
                            port = %address_clone.port(),
                            "✅ Router gRPC upstream is listening"
                        );

                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {
                                tracing::info!("shutdown signal received (Ctrl+C)");
                            }
                            _ = shutdown_rx => {
                                tracing::info!("shutdown signal received (service registration failed)");
                            }
                        }
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            });

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    match flare_im_core::discovery::register_service_only(
                        SIGNALING_ROUTE,
                        addr,
                        None,
                    )
                    .await
                    {
                        Ok(Some(registry)) => {
                            info!("✅ Service registered: {}", SIGNALING_ROUTE);
                            Ok(Some(registry))
                        }
                        Ok(None) => {
                            info!("Service discovery not configured, skipping registration");
                            Ok(None)
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                "❌ Service registration failed"
                            );
                            Err(format!("Service registration failed: {}", e).into())
                        }
                    }
                })
            })
            .await
    }
}
