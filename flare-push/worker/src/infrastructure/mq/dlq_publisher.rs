use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::event::types::types;
use flare_server_core::error::Result;
use flare_server_core::eventbus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;

use crate::config::PushWorkerConfig;

pub struct DlqPublisher {
    event_publisher: Arc<MqEventBus>,
    dlq_topic: String,
}

impl DlqPublisher {
    pub async fn new(config: Arc<PushWorkerConfig>) -> Result<Self> {
        let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
            "kafka" => Arc::new(KafkaProducerBuilder::new().build(config.as_ref()).map_err(
                |e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to build kafka producer: {}",
                        e
                    ))
                },
            )?),
            "nats" => Arc::new(
                NatsProducerBuilder::new()
                    .build(config.as_ref())
                    .await
                    .map_err(|e| {
                        flare_server_core::error::FlareError::system(format!(
                            "failed to build jetstream producer: {}",
                            e
                        ))
                    })?,
            ),
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        Ok(Self {
            event_publisher: MqEventBus::new(producer),
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
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("event publish failed: {}", e))
            })
    }
}
