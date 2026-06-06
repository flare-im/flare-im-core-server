use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::PUSH_WORKER;
use flare_server_core::error::Result;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        use flare_im_core::load_config;

        let app_config = load_config(Some("./config"));
        let ctx = wire::initialize(app_config).await?;
        Self::run_with_context(ctx).await
    }

    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        info!("Starting Push Worker (push-online/push-offline) via ServiceRuntime...");

        let mut runtime = ServiceRuntime::mq_consumer()
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown);

        if context.config.metrics.enabled {
            let metrics_config = context.config.metrics.clone();
            runtime = runtime.add_spawn_with_shutdown(
                "push-worker-metrics",
                move |shutdown_rx| async move {
                    flare_im_core::metrics::serve_prometheus_metrics(metrics_config, shutdown_rx)
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
            "nats" | "jetstream" => flare_server_core::mq::nats::build_nats_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-delivery-consumer",
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

        flare_im_core::health::attach_runtime_health_checks(runtime, PUSH_WORKER)
            .run()
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}
