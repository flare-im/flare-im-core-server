use std::sync::Arc;

use anyhow::Result;
use flare_im_core::event::types::types;
use flare_proto::common::PushTaskEnvelope;
use flare_server_core::event_bus::EventEnvelope;
use flare_server_core::event_bus::EventPublisher;
use flare_server_core::event_bus::MqEventBus;
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

    fn build_task_event_payload(
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let task = PushTaskEnvelope::decode(payload.as_slice())
            .map_err(|e| anyhow::anyhow!("decode PushTaskEnvelope failed: {}", e))?;
        let envelope = EventEnvelope::new(
            types::SYSTEM,
            key.unwrap_or_default(),
            0,
            task.encode_to_vec(),
        );
        envelope
            .to_json_bytes()
            .map_err(|e| anyhow::anyhow!("encode EventEnvelope failed: {}", e))
    }

    pub async fn publish_online_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        let payload = Self::build_task_event_payload(key, payload)?;
        let envelope = serde_json::from_slice::<EventEnvelope>(payload.as_slice())
            .map_err(|e| anyhow::anyhow!("decode EventEnvelope failed: {}", e))?;
        self.event_publisher
            .publish(ctx, &self.config.push_online_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }

    pub async fn publish_offline_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        let payload = Self::build_task_event_payload(key, payload)?;
        let envelope = serde_json::from_slice::<EventEnvelope>(payload.as_slice())
            .map_err(|e| anyhow::anyhow!("decode EventEnvelope failed: {}", e))?;
        self.event_publisher
            .publish(ctx, &self.config.push_offline_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }

    pub async fn publish_dlq(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        let envelope = EventEnvelope::new(
            types::SYSTEM,
            key.unwrap_or_default(),
            0,
            payload,
        );
        self.event_publisher
            .publish(ctx, &self.config.push_dlq_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }
}

