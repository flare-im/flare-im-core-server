use flare_im_contracts::service_names::CONVERSATION;
use flare_server_core::error::AnyhowContext;
use tracing::info;

mod wire;

pub use wire::ApplicationContext;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> flare_server_core::error::Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.conversation_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            CONVERSATION,
            "CONVERSATION",
            50090,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config)
            .await
            .context("wire::initialize failed")?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, runtime_plan).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
    ) -> flare_server_core::error::Result<()> {
        use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageServiceServer;
        use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Conversation gRPC service..."
        );

        let address_clone = address;
        let mut runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "conversation-grpc",
            move |shutdown_rx| async move {
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
                        Box::new(std::io::Error::other(format!("gRPC server error: {}", e)))
                    })
            },
        );

        // 将 ReadReceipt JetStream 消费者纳入 Runtime 管理
        if let Some(consumer) = context.read_receipt_consumer {
            runtime = runtime.add_spawn("read-receipt-consumer", async move {
                consumer
                    .run()
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::other(format!(
                            "read-receipt-consumer error: {}",
                            e
                        )))
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
                        Box::new(std::io::Error::other(format!(
                            "conversation-ensure-consumer error: {}",
                            e
                        )))
                    })
            });
        }

        // 运行服务（带服务注册）
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
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("runtime error: {}", e))
            })
    }
}
