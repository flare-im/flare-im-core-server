use flare_im_contracts::service_names::PUSH_WORKER;
use flare_server_core::error::Result;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        // Decide on the effective broker configuration before any consumer connects.
        let mode = require_delivery_provenance(&crate::config::PushWorkerConfig::from_app_config(
            app_config,
        )?)?;
        let service_config = app_config.push_worker_service();
        let runtime = flare_im_service_kit::build_background_service_runtime(
            app_config,
            &service_config.runtime,
            PUSH_WORKER,
        )?;
        let ctx = wire::initialize(app_config, mode).await?;
        Self::run_with_runtime(ctx, runtime).await
    }

    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        require_delivery_provenance(&context.config)?;
        Self::run_with_runtime(
            context,
            flare_im_service_kit::background_service_runtime(PUSH_WORKER),
        )
        .await
    }

    async fn run_with_runtime(
        context: ApplicationContext,
        mut runtime: flare_core_runtime::ServiceRuntime,
    ) -> Result<()> {
        info!("Starting Push Worker (push-online/push-offline) via ServiceRuntime...");

        let usage = context.usage;
        runtime = runtime.add_spawn_with_shutdown("push-worker-usage", move |shutdown_rx| {
            usage.run(shutdown_rx)
        });

        if context.config.metrics.enabled {
            let metrics_config = context.config.metrics.clone();
            runtime = runtime.add_spawn_with_shutdown(
                "push-worker-metrics",
                move |shutdown_rx| async move {
                    flare_im_service_kit::metrics::serve_prometheus_metrics(
                        metrics_config,
                        shutdown_rx,
                    )
                    .await
                },
            );
        }

        let tasks = match context.config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-delivery-consumer",
            )
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-worker kafka consumers: {}",
                    e
                ))
            })?,
            // 接通死信:处理失败/毒消息投到独立死信流 flare.im.dlq.push-delivery,不再无限重投。
            "nats" => flare_server_core::mq::nats::build_nats_consumer_tasks_with_dlq(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-delivery-consumer",
                "flare.im.dlq.push-delivery",
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-worker nats consumers: {}",
                    e
                ))
            })?,
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        runtime
            .run()
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}

const PROVENANCE_ENV: &str = "PUSH_WORKER_EVENT_PROVENANCE_MODE";

/// Refuse to consume until the deployment states how forged broker records are
/// excluded; see `flare_im_service_kit::push_delivery`. Runs before any consumer
/// or DLQ publisher exists, so a refused start leaves the broker backlog intact.
fn require_delivery_provenance(
    config: &crate::config::PushWorkerConfig,
) -> Result<flare_im_service_kit::push_delivery::PushDeliveryMode> {
    flare_im_service_kit::push_delivery::require_push_delivery(
        "Push Worker",
        PROVENANCE_ENV,
        std::env::var(PROVENANCE_ENV).ok().as_deref(),
        flare_im_service_kit::push_delivery::BrokerEndpoint {
            backend: &config.mq_backend,
            nats_url: &config.jetstream_url,
        },
    )
}

#[cfg(test)]
mod provenance_gate_tests {
    use super::*;
    use flare_server_core::error::ErrorCode;

    /// The default deployment keeps Push paused; the refusal happens before
    /// wiring, i.e. before any broker consumer or DLQ publisher exists.
    #[tokio::test]
    async fn bootstrap_refuses_without_a_provenance_mode() {
        if std::env::var(PROVENANCE_ENV).is_ok() {
            return;
        }
        let error = ApplicationBootstrap::run().await.unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert!(error.to_string().contains("Push Worker is paused"));
    }
}
