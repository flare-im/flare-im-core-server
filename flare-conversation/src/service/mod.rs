use std::net::SocketAddr;

use anyhow::{Context, Result};
use tracing::{error, info};

use flare_server_core::runtime::ServiceRuntime;

mod wire;

pub use wire::ApplicationContext;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        // 加载应用配置
        let app_config = load_config(Some("config"));
        let service_config = app_config.conversation_service();

        info!("Parsing server address...");
        let address: SocketAddr = ServiceHelper::parse_server_addr(
            app_config,
            &service_config.runtime,
            "flare-conversation",
        )
        .context("invalid conversation server address")?;
        info!(address = %address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(mut context: ApplicationContext, address: SocketAddr) -> Result<()> {
        use flare_proto::conversation::conversation_manage_service_server::ConversationManageServiceServer;
        use flare_proto::conversation::conversation_read_service_server::ConversationReadServiceServer;
        use tonic::transport::Server;

        let handler = context.handler.clone();

        // 若已配置 ReadReceipt Kafka 消费者，则后台启动
        if let Some(consumer) = context.read_receipt_consumer.take() {
            tokio::spawn(async move {
                if let Err(e) = consumer.run().await {
                    tracing::error!(error = %e, "ReadReceipt consumer loop exited");
                }
            });
        }

        // 若已配置 ConversationEnsure Kafka 消费者（Orchestrator 异步会话创建），则后台启动
        if let Some(consumer) = context.conversation_ensure_consumer.take() {
            tokio::spawn(async move {
                if let Err(e) = consumer.run().await {
                    tracing::error!(error = %e, "ConversationEnsure consumer loop exited");
                }
            });
        }

        info!(
            address = %address,
            port = %address.port(),
            "Starting Conversation gRPC service..."
        );

        // 使用 ServiceRuntime 管理服务生命周期
        let address_clone = address;
        let runtime = ServiceRuntime::new("conversation", address)
            .add_spawn_with_shutdown("conversation-grpc", move |shutdown_rx| async move {
                // 使用 ContextLayer 直接包裹 Service
                use flare_server_core::middleware::ContextLayer;

                let read_svc = ContextLayer::new()
                    .allow_missing()
                    .layer(ConversationReadServiceServer::new(handler.clone()));
                let manage_svc = ContextLayer::new()
                    .allow_missing()
                    .layer(ConversationManageServiceServer::new(handler));

                Server::builder()
                    .add_service(read_svc)
                    .add_service(manage_svc)
                    .serve_with_shutdown(address_clone, async move {
                        info!(
                            address = %address_clone,
                            port = %address_clone.port(),
                            "✅ Conversation gRPC service is listening"
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
                    use flare_im_core::service_names::CONVERSATION;
                    match flare_im_core::discovery::register_service_only(CONVERSATION, addr, None)
                        .await
                    {
                        Ok(Some(registry)) => {
                            info!("✅ Service registered: {}", CONVERSATION);
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
