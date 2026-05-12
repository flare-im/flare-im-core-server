//! 将 Push 请求写入 MQ：push-request topic

use std::sync::Arc;

use anyhow::Result;
use flare_grpc_proto::push::{PushCustomRequest, PushMessageRequest, PushNotificationRequest};
use flare_im_core::Ctx;
use flare_im_core::event::types::types;
use flare_proto::common::PushTaskEnvelope;
use flare_server_core::eventbus::EventEnvelope;
use flare_server_core::eventbus::EventPublisher;
use flare_server_core::eventbus::MqEventBus;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;
use prost::Message as _;
use tracing::instrument;

use crate::config::PushProxyConfig;

/// Push Proxy 使用的 MQ 发布器：push-request（消息/通知）
pub struct PushProxyMqPublisher {
    config: Arc<PushProxyConfig>,
    event_publisher: Arc<MqEventBus>,
}

impl PushProxyMqPublisher {
    pub async fn new(config: Arc<PushProxyConfig>) -> Result<Self> {
        let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
            "kafka" => Arc::new(
                KafkaProducerBuilder::new()
                    .build(config.as_ref())
                    .map_err(|e| anyhow::anyhow!("failed to build kafka producer: {}", e))?,
            ),
            "nats" | "jetstream" => Arc::new(
                NatsProducerBuilder::new()
                    .build(config.as_ref())
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to build jetstream producer: {}", e))?,
            ),
            other => anyhow::bail!("unsupported mq backend: {}", other),
        };
        Ok(Self {
            config,
            event_publisher: MqEventBus::new(producer),
        })
    }

    /// 将 PushMessageRequest 写入 push-request topic（设计文档：PushProxy 只负责入队缓冲）
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_message(&self, ctx: &Ctx, req: &PushMessageRequest) -> Result<()> {
        let key = req.user_ids.first().map(String::as_str);
        let envelope = EventEnvelope::new(
            types::MESSAGE,
            key.unwrap_or_default(),
            0,
            req.encode_to_vec(),
        );
        self.event_publisher
            .publish(ctx, &self.config.push_request_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))?;
        Ok(())
    }

    /// 将 PushNotificationRequest 写入 push-request topic（当前直接复用 proto 请求体作为载荷）
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_notification(
        &self,
        ctx: &Ctx,
        req: &PushNotificationRequest,
    ) -> Result<()> {
        let key = req.user_ids.first().map(String::as_str);
        let envelope = EventEnvelope::new(
            types::NOTIFICATION,
            key.unwrap_or_default(),
            0,
            req.encode_to_vec(),
        );
        self.event_publisher
            .publish(ctx, &self.config.push_request_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))?;
        Ok(())
    }

    /// 将 PushCustomRequest 写入 push-request topic
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_custom(&self, ctx: &Ctx, req: &PushCustomRequest) -> Result<()> {
        let key = req.user_ids.first().map(String::as_str);
        let envelope = EventEnvelope::new(
            types::CUSTOM,
            key.unwrap_or_default(),
            0,
            req.encode_to_vec(),
        );
        self.event_publisher
            .publish(ctx, &self.config.push_request_topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))?;
        Ok(())
    }

    /// 发布 PushTaskEnvelope 到在线/离线推送 topic
    #[instrument(skip(self, ctx, task))]
    pub async fn publish_push_task(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        is_online: bool,
    ) -> Result<()> {
        let topic = if is_online {
            &self.config.push_online_topic
        } else {
            &self.config.push_offline_topic
        };

        let envelope = EventEnvelope::new(types::SYSTEM, &task.user_id, 0, task.encode_to_vec())
            .with_source("flare-push-proxy");

        self.event_publisher
            .publish(ctx, topic, &envelope)
            .await
            .map_err(|e| anyhow::anyhow!("event publish failed: {}", e))
    }
}
