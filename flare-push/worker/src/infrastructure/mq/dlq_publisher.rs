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

    #[cfg(test)]
    fn from_producer(producer: Arc<dyn Producer>, dlq_topic: impl Into<String>) -> Self {
        Self {
            event_publisher: MqEventBus::new(producer),
            dlq_topic: dlq_topic.into(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::EventBusEnvelope;
    use flare_server_core::Context;
    use flare_server_core::mq::producer::{ProducerError, ProducerMessage};
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct CapturedSend {
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
    }

    #[derive(Default)]
    struct CapturingProducer {
        sends: Mutex<Vec<CapturedSend>>,
    }

    #[async_trait::async_trait]
    impl Producer for CapturingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            payload: Vec<u8>,
            _headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            self.sends
                .lock()
                .expect("captured sends lock")
                .push(CapturedSend {
                    topic: topic.to_string(),
                    key: key.map(ToString::to_string),
                    payload,
                });
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "capturing-producer"
        }
    }

    #[tokio::test]
    async fn publish_wraps_payload_with_dlq_topic_key_and_source() {
        let producer = Arc::new(CapturingProducer::default());
        let publisher = DlqPublisher::from_producer(producer.clone(), "flare.im.dlq.push");
        let ctx = Arc::new(Context::with_request_id("req-dlq"));
        let original_payload = b"poison-push-task".to_vec();

        publisher
            .publish(&ctx, Some("conversation-a"), original_payload.clone())
            .await
            .expect("publish dlq");

        let sends = producer.sends.lock().expect("captured sends lock");
        assert_eq!(sends.len(), 1);
        let sent = &sends[0];
        assert_eq!(sent.topic, "flare.im.dlq.push");
        assert_eq!(sent.key.as_deref(), Some("conversation-a"));

        let envelope =
            EventBusEnvelope::decode(sent.payload.as_slice()).expect("event bus envelope");
        assert_eq!(envelope.partition_key, "conversation-a");
        assert_eq!(envelope.source, "flare-push-worker-dlq");
        assert_eq!(envelope.payload, original_payload);
    }
}
