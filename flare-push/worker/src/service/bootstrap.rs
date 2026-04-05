use anyhow::Result;
use flare_server_core::kafka::KafkaMessageFetcher;
use flare_server_core::mq::consumer::{ConsumerRuntimeTask, MqConsumerTask};
use flare_core_runtime::ServiceRuntime;
use flare_im_core::service_names::PUSH_WORKER;
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

        let topics = context.dispatcher.topics();
        if topics.is_empty() {
            anyhow::bail!("push-worker: no Kafka topics registered on dispatcher");
        }

        let fetcher = KafkaMessageFetcher::new_with_consumer_group(
            context.config.as_ref(),
            topics,
            context
                .consumer_config
                .kafka_consumer_group_override
                .as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("create kafka fetcher error: {}", e))?;

        let consumer = ConsumerRuntimeTask::from_parts(
            context.consumer_config,
            context.dispatcher.clone(),
            fetcher,
        );

        let task = MqConsumerTask::new("push-delivery-consumer", Box::new(consumer));

        flare_im_core::health::attach_runtime_health_checks(
            ServiceRuntime::mq_consumer()
                .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
                .add_task(Box::new(task)),
            PUSH_WORKER,
        )
            .run()
            .await
    }
}
