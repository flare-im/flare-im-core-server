//! 应用启动器 - 负责依赖注入和服务启动

use std::net::SocketAddr;

use anyhow::{Context, Result};
use tracing::{error, info};

use flare_server_core::runtime::ServiceRuntime;

use super::wire;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        // 加载应用配置
        let app_config = load_config(Some("config"));
        let service_config = app_config.media_service();

        info!("Parsing server address...");
        let address: SocketAddr =
            ServiceHelper::parse_server_addr(app_config, &service_config.runtime, "flare-media")
                .context("invalid media server address")?;
        info!(address = %address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: wire::ApplicationContext,
        address: SocketAddr,
    ) -> Result<()> {
        use flare_proto::media::media_service_server::MediaServiceServer;
        use tonic::transport::Server;

        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Media gRPC service..."
        );

        // 使用 ServiceRuntime 管理服务生命周期
        let address_clone = address;
        let runtime = ServiceRuntime::new("media", address)
            .add_spawn_with_shutdown("media-grpc", move |shutdown_rx| async move {
                // 使用 ContextLayer 包裹 Service
                use flare_server_core::middleware::ContextLayer;

                let media_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MediaServiceServer::new(handler));

                Server::builder()
                    .add_service(media_service)
                    .serve_with_shutdown(address_clone, async move {
                        info!(
                            address = %address_clone,
                            port = %address_clone.port(),
                            "✅ Media gRPC service is listening"
                        );

                        // 同时监听 Ctrl+C 和关闭通道
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

        // 运行服务（带服务注册）
        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    // 注册服务（使用常量）
                    use flare_im_core::service_names::MEDIA;
                    match flare_im_core::discovery::register_service_only(MEDIA, addr, None).await {
                        Ok(Some(registry)) => {
                            info!("✅ Service registered: {}", MEDIA);
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
