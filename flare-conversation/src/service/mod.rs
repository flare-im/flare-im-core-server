use std::net::SocketAddr;

use anyhow::Context;
use flare_im_core::service_names::CONVERSATION;
use tracing::info;

use flare_core_runtime::ServiceRuntime;

mod wire;

pub use wire::ApplicationContext;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> anyhow::Result<()> {
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
        .map_err(|e| anyhow::anyhow!("invalid conversation server address: {}", e))?;
        info!(address = %address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config)
            .await
            .context("wire::initialize failed")?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: ApplicationContext,
        address: SocketAddr,
    ) -> anyhow::Result<()> {
        use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageServiceServer;
        use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadServiceServer;
        use tonic::transport::Server;

        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Conversation gRPC service..."
        );

        // 使用 ServiceRuntime 管理服务生命周期
        let address_clone = address;
        let mut runtime = flare_im_core::health::attach_runtime_health_checks(
            ServiceRuntime::new(CONVERSATION)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
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
                        .serve_with_shutdown(address_clone, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("gRPC server error: {}", e),
                            ))
                        })
                }),
            CONVERSATION,
        );

        // 将 ReadReceipt JetStream 消费者纳入 Runtime 管理
        if let Some(consumer) = context.read_receipt_consumer {
            runtime = runtime.add_spawn("read-receipt-consumer", async move {
                consumer
                    .run()
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("read-receipt-consumer error: {}", e),
                        ))
                    })
            });
        }

        // 将 ConversationEnsure JetStream 消费者纳入 Runtime 管理
        if let Some(consumer) = context.conversation_ensure_consumer {
            runtime = runtime.add_spawn("conversation-ensure-consumer", async move {
                consumer
                    .run()
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("conversation-ensure-consumer error: {}", e),
                        ))
                    })
            });
        }

        // 运行服务（带服务注册）
        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only(
                        CONVERSATION,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
            .map_err(|e| anyhow::anyhow!("runtime error: {}", e))
    }
}
