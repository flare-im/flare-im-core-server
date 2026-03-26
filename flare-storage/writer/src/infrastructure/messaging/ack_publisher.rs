use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use flare_server_core::context::Ctx;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::to_vec;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::events::AckEvent;
use crate::domain::repository::AckPublisher;

pub struct MqAckPublisher {
    producer: Arc<FutureProducer>,
    config: Arc<StorageWriterConfig>,
    topic: String,
}

impl MqAckPublisher {
    pub fn new(
        producer: Arc<FutureProducer>,
        config: Arc<StorageWriterConfig>,
        topic: String,
    ) -> Self {
        Self {
            producer,
            config,
            topic,
        }
    }
}

impl AckPublisher for MqAckPublisher {
    #[instrument(skip(self, event), fields(message_id = %event.message_id, conversation_id = %event.conversation_id))]
    async fn publish(&self, ctx: &Ctx, event: AckEvent<'_>) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let payload = to_vec(&event)?;

        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(event.conversation_id);

        self.producer
            .send(record, Duration::from_millis(self.config.kafka_timeout_ms))
            .await
            .map_err(|(err, _)| anyhow!("failed to publish ACK: {err}"))?;

        Ok(())
    }
}
