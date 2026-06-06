use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::Result;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;
use prost::Message as _;

use crate::config::PushServerConfig;

pub struct PushServerMqPublisher {
    producer: Arc<dyn Producer>,
    config: Arc<PushServerConfig>,
}

impl PushServerMqPublisher {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
            "kafka" => Arc::new(KafkaProducerBuilder::new().build(config.as_ref()).map_err(
                |e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to build kafka producer: {}",
                        e
                    ))
                },
            )?),
            "nats" | "jetstream" => Arc::new(
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

        Ok(Self { producer, config })
    }

    pub async fn publish_online_task(
        &self,
        ctx: &Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope
        let _task = PushTaskEnvelope::decode(payload.as_slice()).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "decode PushTaskEnvelope failed: {}",
                e
            ))
        })?;

        self.producer
            .send(ctx, &self.config.push_online_topic, key, payload, None)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }

    pub async fn publish_offline_task(
        &self,
        ctx: &Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        // 验证 payload 为有效的 PushTaskEnvelope
        let _task = PushTaskEnvelope::decode(payload.as_slice()).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "decode PushTaskEnvelope failed: {}",
                e
            ))
        })?;

        self.producer
            .send(ctx, &self.config.push_offline_topic, key, payload, None)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }

    pub async fn publish_dlq(&self, ctx: &Ctx, key: Option<&str>, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send(ctx, &self.config.push_dlq_topic, key, payload, None)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }
}
