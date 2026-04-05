use std::sync::Arc;

use anyhow::Result;
use flare_proto::common::PushTaskEnvelope;
use flare_im_core::Ctx;
use flare_server_core::mq::producer::Producer;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use prost::Message as _;

use crate::config::PushServerConfig;

pub struct PushServerMqPublisher {
    producer: Arc<dyn Producer>,
    config: Arc<PushServerConfig>,
}

impl PushServerMqPublisher {
    pub fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let producer = Arc::new(
            KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to build kafka producer: {}", e))?,
        );

        Ok(Self {
            producer,
            config,
        })
    }

    pub async fn publish_online_task(
        &self,
        ctx: &Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope
        let _task = PushTaskEnvelope::decode(payload.as_slice())
            .map_err(|e| anyhow::anyhow!("decode PushTaskEnvelope failed: {}", e))?;

        self.producer
            .send(ctx, &self.config.push_online_topic, key, payload, None)
            .await
            .map_err(|e| anyhow::anyhow!("mq publish failed: {}", e))
    }

    pub async fn publish_offline_task(
        &self,
        ctx: &Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope
        let _task = PushTaskEnvelope::decode(payload.as_slice())
            .map_err(|e| anyhow::anyhow!("decode PushTaskEnvelope failed: {}", e))?;

        self.producer
            .send(ctx, &self.config.push_offline_topic, key, payload, None)
            .await
            .map_err(|e| anyhow::anyhow!("mq publish failed: {}", e))
    }

    pub async fn publish_dlq(&self, ctx: &Ctx, key: Option<&str>, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send(ctx, &self.config.push_dlq_topic, key, payload, None)
            .await
            .map_err(|e| anyhow::anyhow!("mq publish failed: {}", e))
    }
}
