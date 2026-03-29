use std::sync::Arc;

use anyhow::Result;
use flare_im_core::event::types::types;
use flare_proto::common::PushTaskEnvelope;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use prost::Message as _;

use crate::config::PushServerConfig;

pub struct PushServerMqPublisher {
    event_publisher: Arc<MqEventBus>,
    config: Arc<PushServerConfig>,
}

impl PushServerMqPublisher {
    pub fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let producer = KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to build kafka producer: {}", e))?;

        Ok(Self {
            event_publisher: MqEventBus::new(Arc::new(producer)),
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

        let partition_key = key.unwrap_or_default();
        let envelope = EventEnvelope::new(
            types::SYSTEM,
            partition_key,
            0,
            payload, // 直接使用原始 payload
        )
        .with_source("flare-push-server");

        self.event_publisher
            .publish(ctx, &self.config.push_online_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
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

        let partition_key = key.unwrap_or_default();
        let envelope = EventEnvelope::new(types::SYSTEM, partition_key, 0, payload)
            .with_source("flare-push-server");

        self.event_publisher
            .publish(ctx, &self.config.push_offline_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }

    pub async fn publish_dlq(&self, ctx: &Ctx, key: Option<&str>, payload: Vec<u8>) -> Result<()> {
        let partition_key = key.unwrap_or_default();
        let envelope = EventEnvelope::new(types::SYSTEM, partition_key, 0, payload)
            .with_source("flare-push-server-dlq");

        self.event_publisher
            .publish(ctx, &self.config.push_dlq_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }
}
