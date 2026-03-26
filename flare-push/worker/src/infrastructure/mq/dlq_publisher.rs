use std::sync::Arc;

use anyhow::Result;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::producer::Producer;

use crate::config::PushWorkerConfig;

pub struct DlqPublisher {
    producer: Arc<dyn Producer>,
    config: Arc<PushWorkerConfig>,
}

impl DlqPublisher {
    pub fn new(config: Arc<PushWorkerConfig>) -> Result<Self> {
        let producer = KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to build kafka producer: {}", e))?;
        Ok(Self {
            producer: Arc::new(producer),
            config,
        })
    }

    pub async fn publish(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.producer
            .send(ctx, &self.config.push_dlq_topic, key, payload, None)
            .await
            .map_err(|e| anyhow::anyhow!("mq send failed: {}", e))
    }
}

