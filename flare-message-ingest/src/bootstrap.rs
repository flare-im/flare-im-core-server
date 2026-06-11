//! 应用启动器 - 负责依赖注入和服务启动

use std::time::Duration;

use flare_im_contracts::service_names::MESSAGE_INGEST;
use flare_server_core::error::{AnyhowContext, Result};
use tracing::info;

use super::wire;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        Self::run_with_shutdown_signals(Vec::new()).await
    }

    pub async fn run_with_shutdown_signals(
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.message_ingest_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            MESSAGE_INGEST,
            "MESSAGE_INGEST",
            50182,
        )?;

        let context = wire::initialize(app_config)
            .await
            .context("wire::initialize failed")?;

        info!(address = %runtime_plan.address, "Message ingest bootstrap created successfully");

        Self::run_with_context(context, runtime_plan, signals).await
    }

    async fn run_with_context(
        context: wire::ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        use flare_grpc_proto::message::message_send_service_server::MessageSendServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let send_grpc = context.message_send_grpc.clone();
        let config = context.config.clone();
        let wal_replay_handler = context.wal_replay_handler.clone();
        let wal_replay_enabled = config.wal_replay_enabled;
        let wal_replay_interval_ms = config.wal_replay_interval_ms.max(100);
        let wal_replay_error_backoff_ms = config.wal_replay_error_backoff_ms.max(100);
        let wal_replay_batch_limit = config.wal_replay_batch_limit;
        let wal_replay_claim_lease_ms = config.wal_replay_claim_lease_ms.max(1000);

        info!(
            address = %address,
            port = %address.port(),
            "Starting Message Ingest gRPC service"
        );

        let address_clone = address;
        let mut service_runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "message-ingest-grpc",
            move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let send_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MessageSendServiceServer::new(send_grpc));

                Server::builder()
                    .add_service(send_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        tracing::error!(
                            %address_clone,
                            error = %e,
                            "message ingest gRPC listen/serve failed"
                        );
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::AddrInUse,
                            format!("gRPC server error on {address_clone}: {e}"),
                        ))
                    })
            },
        );

        if context.config.metrics.enabled {
            let metrics_config = context.config.metrics.clone();
            service_runtime = service_runtime.add_spawn_with_shutdown(
                "message-ingest-metrics",
                move |shutdown_rx| async move {
                    flare_im_service_kit::metrics::serve_prometheus_metrics(
                        metrics_config,
                        shutdown_rx,
                    )
                    .await
                },
            );
        }

        if wal_replay_enabled && wal_replay_batch_limit > 0 {
            service_runtime = service_runtime.add_spawn_with_shutdown(
                "message-ingest-wal-replay",
                move |mut shutdown_rx| async move {
                    let mut ticker =
                        tokio::time::interval(Duration::from_millis(wal_replay_interval_ms));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    tracing::info!(
                        interval_ms = wal_replay_interval_ms,
                        error_backoff_ms = wal_replay_error_backoff_ms,
                        batch_limit = wal_replay_batch_limit,
                        claim_lease_ms = wal_replay_claim_lease_ms,
                        "Starting message ingest WAL replay loop"
                    );

                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                tracing::info!("Stopping message ingest WAL replay loop");
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
                                                tracing::info!("Stopping message ingest WAL replay loop");
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
                "Message ingest WAL replay loop disabled"
            );
        }

        let mut metadata: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        if let Some(server_id) = config.server_id.as_ref() {
            metadata.insert("server_id".to_string(), server_id.to_string());
        }

        let svid = config.svid.as_deref().unwrap_or("svid.im");
        metadata.insert("svid".to_string(), svid.to_string());

        let metadata_clone = Some(metadata);

        service_runtime
            .run_with_registration_and_signals(
                move |addr| {
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
                },
                signals,
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("runtime error: {}", e))
            })
    }
}
