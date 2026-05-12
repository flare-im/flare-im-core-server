//! 应用启动器 - 负责依赖注入和服务启动

use std::net::SocketAddr;

use anyhow::{Context, Result};
use tracing::info;

use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::ORCHESTRATOR;
use flare_server_core::mq::NatsConsumerConfig;

use super::wire;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        // 初始化 OpenTelemetry 追踪
        #[cfg(feature = "tracing")]
        {
            let otlp_endpoint = std::env::var("OTLP_ENDPOINT").ok();
            if let Err(e) =
                flare_im_core::tracing::init_tracing(ORCHESTRATOR, otlp_endpoint.as_deref())
            {
                tracing::error!(error = %e, "Failed to initialize OpenTelemetry tracing");
            } else {
                info!("✅ OpenTelemetry tracing initialized");
            }
        }

        // 加载应用配置
        let app_config = load_config(Some("config"));
        let service_config = app_config.orchestrator_service();

        info!("Parsing server address...");
        let address: SocketAddr =
            ServiceHelper::parse_server_addr(app_config, &service_config.runtime, ORCHESTRATOR)
                .map_err(|e| anyhow::anyhow!("invalid orchestrator server address: {}", e))?;
        info!(address = %address, "Server address parsed successfully");

        let context = wire::initialize(app_config)
            .await
            .context("wire::initialize failed")?;

        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: wire::ApplicationContext,
        address: SocketAddr,
    ) -> Result<()> {
        use flare_grpc_proto::message::message_action_service_server::MessageActionServiceServer;
        use flare_grpc_proto::message::message_send_service_server::MessageSendServiceServer;
        use tonic::transport::Server;

        let send_grpc = context.message_send_grpc.clone();
        let action_grpc = context.message_action_grpc.clone();

        let consumer_config = context.consumer_config.clone();
        let main_queue_dispatcher = context.main_queue_dispatcher.clone();
        let orchestrator_mq_config = context.config.clone();

        let topics = main_queue_dispatcher.topics();
        if topics.is_empty() {
            anyhow::bail!("orchestrator: no JetStream topics registered on main queue dispatcher");
        }

        info!(
            topics = ?topics,
            group = %orchestrator_mq_config.consumer_group(),
            backend = %orchestrator_mq_config.mq_backend,
            "Starting Message Orchestrator MQ consumer (TOPIC_MESSAGE_MAIN)..."
        );

        let mq_tasks = match orchestrator_mq_config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                orchestrator_mq_config.as_ref(),
                consumer_config,
                main_queue_dispatcher,
                "orchestrator-main-queue-consumer",
            )
            .map_err(|e| anyhow::anyhow!("create orchestrator kafka consumers: {}", e))?,
            "nats" | "jetstream" => flare_server_core::mq::nats::build_nats_consumer_tasks(
                orchestrator_mq_config.as_ref(),
                consumer_config,
                main_queue_dispatcher,
                "orchestrator-main-queue-consumer",
            )
            .await
            .map_err(|e| anyhow::anyhow!("create orchestrator nats consumers: {}", e))?,
            other => anyhow::bail!("unsupported mq backend: {}", other),
        };

        info!(
            address = %address,
            port = %address.port(),
            "Starting Message Orchestrator gRPC service..."
        );

        let address_clone = address;
        let mut service_runtime = ServiceRuntime::new(ORCHESTRATOR)
            .with_address(address)
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
            .add_spawn_with_shutdown("orchestrator-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let send_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MessageSendServiceServer::new(send_grpc));
                let action_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MessageActionServiceServer::new(action_grpc));

                Server::builder()
                    .add_service(send_service)
                    .add_service(action_service)
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
            });

        for task in mq_tasks {
            service_runtime = service_runtime.add_task(Box::new(task));
        }

        let runtime =
            flare_im_core::health::attach_runtime_health_checks(service_runtime, ORCHESTRATOR);

        let config = context.config.clone();
        let mut metadata: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        if let Some(server_id) = config.server_id.as_ref() {
            metadata.insert("server_id".to_string(), server_id.to_string());
        }

        let svid = config.svid.as_deref().unwrap_or("svid.im");
        metadata.insert("svid".to_string(), svid.to_string());

        let metadata_clone = Some(metadata);

        runtime
            .run_with_registration(move |addr| {
                let metadata = metadata_clone.clone();
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only_with_metadata(
                        ORCHESTRATOR,
                        addr,
                        None,
                        metadata,
                    )
                    .await
                })
            })
            .await
            .map_err(|e| anyhow::anyhow!("runtime error: {}", e))
    }
}
