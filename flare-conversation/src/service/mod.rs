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

        // 读接口的身份边界：带 x-flare-ctx 就验签并以之为准，FLARE_REQUIRE_SIGNED_READ_CTX
        // 打开时拒绝未签名调用。
        let read_layer =
            flare_im_service_kit::signed_read::signed_read_layer_from_env(CONVERSATION)?;

        // 运行服务
        Self::run_with_context(context, runtime_plan, read_layer).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        read_layer: flare_server_core::SignedFirstContextLayer,
    ) -> flare_server_core::error::Result<()> {
        use flare_grpc_proto::conversation::conversation_admin_service_server::ConversationAdminServiceServer;
        use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());
        let handler = context.handler.clone();
        let admin_handler = context.admin_handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Conversation gRPC service..."
        );

        let address_clone = address;
        let mut runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "conversation-grpc",
            move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let read_svc =
                    crate::interface::grpc::signed_read_service(handler.clone(), &read_layer);
                let read_receipt_svc = crate::interface::grpc::signed_read_receipt_service(
                    handler.clone(),
                    &read_layer,
                );
                // 会话管理仍按调用方元数据（PurgeConversationHistory 除外，逐 RPC 验签）：Social
                // Bridge 以服务签名代成员调用，「服务代用户」的表达定下之前不能套用读接口的冲突规则。
                let manage_svc = ContextLayer::new()
                    .allow_missing()
                    .layer(ConversationManageServiceServer::new(handler));
                // 管理员只读不读 ContextLayer 构造的上下文，只认转发的 Staff 上下文；包一层只为统一请求 ID。
                let admin_svc = ContextLayer::new()
                    .allow_missing()
                    .layer(ConversationAdminServiceServer::new(admin_handler));

                Server::builder()
                    .layer(grpc_metrics)
                    .add_service(read_svc)
                    .add_service(read_receipt_svc)
                    .add_service(manage_svc)
                    .add_service(admin_svc)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::other(format!("gRPC server error: {}", e)))
                    })
            },
        );

        // 将 ReadReceipt JetStream 消费者纳入 Runtime 管理；停机信号到达即退出循环，
        // 不再等运行时的任务停机超时。
        if let Some(consumer) = context.read_receipt_consumer {
            runtime = runtime.add_spawn_with_shutdown(
                "read-receipt-consumer",
                |shutdown_rx| async move {
                    consumer
                        .run_until(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(std::io::Error::other(format!(
                                "read-receipt-consumer error: {}",
                                e
                            )))
                        })
                },
            );
        }

        // 将 ConversationEnsure JetStream 消费者纳入 Runtime 管理
        if let Some(consumer) = context.conversation_ensure_consumer {
            runtime = runtime.add_spawn_with_shutdown(
                "conversation-ensure-consumer",
                |shutdown_rx| async move {
                    consumer
                        .run_until(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            Box::new(std::io::Error::other(format!(
                                "conversation-ensure-consumer error: {}",
                                e
                            )))
                        })
                },
            );
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
