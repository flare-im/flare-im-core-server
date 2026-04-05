use std::sync::Arc;

use anyhow::Result;
use flare_im_core::event::types::types;
use flare_im_core::Ctx;
use flare_server_core::eventbus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::kafka::KafkaProducerBuilder;

use crate::config::PushWorkerConfig;

pub struct DlqPublisher {
    event_publisher: Arc<MqEventBus>,
    dlq_topic: String,
}

impl DlqPublisher {
    pub fn new(config: Arc<PushWorkerConfig>) -> Result<Self> {
        let producer = KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to build kafka producer: {}", e))?;

        Ok(Self {
            event_publisher: MqEventBus::new(Arc::new(producer)),
            dlq_topic: config.push_dlq_topic.clone(),
        })
    }

    pub async fn publish(&self, ctx: &Ctx, key: Option<&str>, payload: Vec<u8>) -> Result<()> {
        let partition_key = key.unwrap_or_default();
        let envelope = EventEnvelope::new(types::SYSTEM, partition_key, 0, payload)
            .with_source("flare-push-worker-dlq");

        self.event_publisher
            .publish(ctx, &self.dlq_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }
}
