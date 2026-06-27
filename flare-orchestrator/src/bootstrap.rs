//! 应用启动器 - 负责依赖注入和服务启动

use flare_server_core::error::{AnyhowContext, Result};
use tracing::info;

use flare_im_contracts::service_names::ORCHESTRATOR;
use flare_server_core::mq::NatsConsumerConfig;

use super::wire;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.orchestrator_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            ORCHESTRATOR,
            "MESSAGE_ORCHESTRATOR",
            50181,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        let context = wire::initialize(app_config)
            .await
            .context("wire::initialize failed")?;

        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, runtime_plan).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: wire::ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
    ) -> Result<()> {
        use flare_grpc_proto::message::message_action_service_server::MessageActionServiceServer;
        use flare_grpc_proto::message::message_event_service_server::MessageEventServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let action_grpc = context.message_action_grpc.clone();
        let event_execute_grpc = context.message_event_execute_grpc.clone();

        let consumer_config = context.consumer_config.clone();
        let main_queue_dispatcher = context.main_queue_dispatcher.clone();
        let failure_publishers = context.failure_publishers.clone();
        let retry_forwarder_dispatcher = context.retry_forwarder_dispatcher.clone();
        let orchestrator_mq_config = context.config.clone();
        let topics = main_queue_dispatcher.topics();
        if topics.is_empty() {
            return Err(flare_server_core::error::FlareError::system(
                "orchestrator: no JetStream topics registered on main queue dispatcher",
            ));
        }

        info!(
            topics = ?topics,
            group = %orchestrator_mq_config.consumer_group(),
            backend = %orchestrator_mq_config.mq_backend,
            "Starting Message Orchestrator MQ consumer (TOPIC_MESSAGE_MAIN)..."
        );

        let mut mq_tasks = match orchestrator_mq_config.mq_backend.as_str() {
            "kafka" => {
                flare_server_core::mq::kafka::build_kafka_consumer_tasks_with_failure_publishers(
                    orchestrator_mq_config.as_ref(),
                    consumer_config.clone(),
                    main_queue_dispatcher,
                    "orchestrator-main-queue-consumer",
                    failure_publishers.clone(),
                )
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create orchestrator kafka consumers: {}",
                        e
                    ))
                })?
            }
            "nats" => {
                flare_server_core::mq::nats::build_nats_consumer_tasks_with_failure_publishers(
                    orchestrator_mq_config.as_ref(),
                    consumer_config.clone(),
                    main_queue_dispatcher,
                    "orchestrator-main-queue-consumer",
                    failure_publishers.clone(),
                )
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create orchestrator nats consumers: {}",
                        e
                    ))
                })?
            }
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        if orchestrator_mq_config.mq_backend.as_str() == "kafka"
            && let Some(dispatcher) = retry_forwarder_dispatcher
        {
            let retry_tasks =
                flare_server_core::mq::kafka::build_kafka_consumer_tasks_with_failure_publishers(
                    orchestrator_mq_config.as_ref(),
                    consumer_config.with_ordered(false).with_batch_size(1),
                    dispatcher,
                    "orchestrator-retry-forwarder",
                    failure_publishers,
                )
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create orchestrator kafka retry-forwarder consumers: {}",
                        e
                    ))
                })?;
            mq_tasks.extend(retry_tasks);
        }

        info!(
            address = %address,
            port = %address.port(),
            "Starting Message Orchestrator gRPC service..."
        );

        let address_clone = address;
        let mut service_runtime = runtime_plan
            .service_runtime()
            .add_spawn_with_shutdown("orchestrator-grpc", move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let action_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MessageActionServiceServer::new(action_grpc));
                let event_execute_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MessageEventServiceServer::new(event_execute_grpc));

                Server::builder()
                    .add_service(action_service)
                    .add_service(event_execute_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        tracing::error!(
                            %address_clone,
                            error = %e,
                            "orchestrator gRPC listen/serve failed (常见原因: 端口已被占用 AddrInUse)"
                        );
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::AddrInUse,
                            format!(
                                "gRPC server error on {address_clone}: {e} \
                                 (若端口被占用，请先 stop_server.sh 或释放该端口)"
                            ),
                        ))
                    })
            });

        if context.config.metrics.enabled {
            let metrics_config = context.config.metrics.clone();
            service_runtime = service_runtime.add_spawn_with_shutdown(
                "orchestrator-metrics",
                move |shutdown_rx| async move {
                    flare_im_service_kit::metrics::serve_prometheus_metrics(
                        metrics_config,
                        shutdown_rx,
                    )
                    .await
                },
            );
        }

        let user_sync_compensation_worker = context.user_sync_compensation_worker.clone();
        service_runtime = service_runtime.add_spawn_with_shutdown(
            "user-sync-compensation-worker",
            move |mut shutdown_rx| async move {
                let mut interval = tokio::time::interval(user_sync_compensation_worker.interval());
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            tracing::info!("user_sync compensation worker shutting down");
                            break;
                        }
                        _ = interval.tick() => {
                            if let Err(error) = user_sync_compensation_worker.replay_once().await {
                                tracing::warn!(error = %error, "user_sync compensation worker tick failed");
                            }
                        }
                    }
                }
                Ok(())
            },
        );

        for task in mq_tasks {
            service_runtime = service_runtime.add_task(Box::new(task));
        }

        let config = context.config.clone();
        let mut metadata: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        if let Some(server_id) = config.server_id.as_ref() {
            metadata.insert("server_id".to_string(), server_id.to_string());
        }

        let svid = config.svid.as_deref().unwrap_or("svid.im");
        metadata.insert("svid".to_string(), svid.to_string());

        let metadata_clone = Some(metadata);

        service_runtime
            .run_with_registration(move |addr| {
                let metadata = metadata_clone.clone();
                let service_name = service_name.clone();
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only_with_metadata(
                        &service_name,
                        addr,
                        None,
                        metadata,
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
