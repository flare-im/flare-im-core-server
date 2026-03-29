use anyhow::Result;
use flare_server_core::kafka::KafkaMessageFetcher;
use flare_server_core::mq::consumer::ConsumerRuntimeTask;
use flare_server_core::runtime::ServiceRuntime;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_im_core::service_names::PUSH_WORKER;

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

        let task = ConsumerRuntimeTask::from_parts(
            context.consumer_config,
            context.dispatcher.clone(),
            fetcher,
        );

        ServiceRuntime::new_consumer_only(PUSH_WORKER)
            .add_mq_consumer_runtime("push-delivery-consumer", task)
            .run()
            .await
    }
}
