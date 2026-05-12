use anyhow::Result;
use flare_core_runtime::ServiceRuntime;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_im_core::service_names::PUSH_SERVER;

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        use flare_im_core::load_config;

        let app_config = load_config(Some("./config"));
        let ctx = wire::initialize(app_config).await?;
        Self::run_with_context(ctx).await
    }

    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        info!(
            "Starting Push Server (push-request -> push-online/push-offline) via ServiceRuntime..."
        );

        let mut runtime = ServiceRuntime::mq_consumer()
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown);

        let tasks = match context.config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
            )
            .map_err(|e| anyhow::anyhow!("create push-server kafka consumers: {}", e))?,
            "nats" | "jetstream" => flare_server_core::mq::nats::build_nats_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
            )
            .await
            .map_err(|e| anyhow::anyhow!("create push-server nats consumers: {}", e))?,
            other => anyhow::bail!("unsupported mq backend: {}", other),
        };

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        flare_im_core::health::attach_runtime_health_checks(runtime, PUSH_SERVER)
            .run()
            .await
    }
}
