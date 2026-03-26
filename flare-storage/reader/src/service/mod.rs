use std::net::SocketAddr;

use anyhow::{Context, Result};
use flare_im_core::service_names::STORAGE_READER;
use tracing::{error, info};

use flare_server_core::runtime::ServiceRuntime;

mod wire;

pub use wire::{ApplicationContext, MessageStorageType};

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        // 加载应用配置
        let app_config = load_config(Some("config"));
        let service_config = app_config.storage_reader_service();

        info!("Parsing server address...");
        let address: SocketAddr = ServiceHelper::parse_server_addr(
            app_config,
            &service_config.runtime,
            STORAGE_READER,
        )
        .context("invalid storage reader server address")?;
        info!(address = %address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(context: ApplicationContext<MessageStorageType>, address: SocketAddr) -> Result<()> {
        use flare_proto::storage::storage_reader_service_server::StorageReaderServiceServer;
        use tonic::transport::Server;

        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Storage Reader gRPC service..."
        );

        // 使用 ServiceRuntime 管理服务生命周期
        let address_clone = address;
        let runtime = ServiceRuntime::new(STORAGE_READER, address)
            .add_spawn_with_shutdown("storage-reader-grpc", move |shutdown_rx| async move {
                // 使用 ContextLayer 包裹 Service
                use flare_server_core::middleware::ContextLayer;
                
                let storage_reader_service = ContextLayer::new()
                    .allow_missing()
                    .layer(StorageReaderServiceServer::new(handler));
                
                Server::builder()
                    .add_service(storage_reader_service)
                    .serve_with_shutdown(address_clone, async move {
                        info!(
                            address = %address_clone,
                            port = %address_clone.port(),
                            "✅ Storage Reader gRPC service is listening"
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
                    match flare_im_core::discovery::register_service_only(
                        STORAGE_READER,
                        addr,
                        None,
                    )
                    .await
                    {
                        Ok(Some(registry)) => {
                            info!("✅ Service registered: {}", STORAGE_READER);
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
