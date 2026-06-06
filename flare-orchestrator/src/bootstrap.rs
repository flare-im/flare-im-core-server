//! 应用启动器 - 负责依赖注入和服务启动

use std::{net::SocketAddr, time::Duration};

use flare_server_core::error::{AnyhowContext, Result};
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
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "invalid orchestrator server address: {}",
                        e
                    ))
                })?;
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
        let failure_publishers = context.failure_publishers.clone();
        let retry_forwarder_dispatcher = context.retry_forwarder_dispatcher.clone();
        let orchestrator_mq_config = context.config.clone();
        let wal_replay_handler = context.wal_replay_handler.clone();
        let wal_replay_enabled = orchestrator_mq_config.wal_replay_enabled;
        let wal_replay_interval_ms = orchestrator_mq_config.wal_replay_interval_ms.max(100);
        let wal_replay_error_backoff_ms =
            orchestrator_mq_config.wal_replay_error_backoff_ms.max(100);
        let wal_replay_batch_limit = orchestrator_mq_config.wal_replay_batch_limit;
        let wal_replay_claim_lease_ms = orchestrator_mq_config.wal_replay_claim_lease_ms.max(1000);

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
            "nats" | "jetstream" => {
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
                    flare_im_core::metrics::serve_prometheus_metrics(metrics_config, shutdown_rx)
                        .await
                },
            );
        }

        if wal_replay_enabled && wal_replay_batch_limit > 0 {
            service_runtime = service_runtime.add_spawn_with_shutdown(
                "orchestrator-wal-replay",
                move |mut shutdown_rx| async move {
                    let mut ticker =
                        tokio::time::interval(Duration::from_millis(wal_replay_interval_ms));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    tracing::info!(
                        interval_ms = wal_replay_interval_ms,
                        error_backoff_ms = wal_replay_error_backoff_ms,
                        batch_limit = wal_replay_batch_limit,
                        claim_lease_ms = wal_replay_claim_lease_ms,
                        "Starting orchestrator WAL replay loop"
                    );

                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                tracing::info!("Stopping orchestrator WAL replay loop");
                                break;
                            }
                            _ = ticker.tick() => {
                                match wal_replay_handler.replay_once(wal_replay_batch_limit).await {
                                    Ok(report) => {
                                        if report.scanned > 0 {
                                            tracing::info!(
                                                scanned = report.scanned,
                                                replayed = report.replayed,
                                                failed = report.failed,
                                                skipped = report.skipped,
                                                "WAL replay cycle completed"
                                            );
                                        } else {
                                            tracing::trace!("WAL replay cycle completed with no pending messages");
                                        }
                                    }
                                    Err(error) => {
                                        tracing::error!(
                                            error = %error,
                                            backoff_ms = wal_replay_error_backoff_ms,
                                            "WAL replay cycle failed; backing off"
                                        );

                                        let backoff = tokio::time::sleep(Duration::from_millis(
                                            wal_replay_error_backoff_ms,
                                        ));
                                        tokio::pin!(backoff);
                                        tokio::select! {
                                            _ = &mut shutdown_rx => {
                                                tracing::info!("Stopping orchestrator WAL replay loop");
                                                break;
                                            }
                                            _ = &mut backoff => {}
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                },
            );
        } else {
            tracing::info!(
                enabled = wal_replay_enabled,
                batch_limit = wal_replay_batch_limit,
                "Orchestrator WAL replay loop disabled"
            );
        }

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
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("runtime error: {}", e))
            })
    }
}
