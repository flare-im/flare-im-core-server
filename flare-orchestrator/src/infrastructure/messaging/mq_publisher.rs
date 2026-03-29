//! 编排侧 Kafka 发布:基于 [flare_server_core::event_bus::MqEventBus] / [EventPublisher],
//! Topic 与 [flare_im_core::constants::topics] 对齐。

use std::pin::Pin;
use std::sync::Arc;

use flare_im_core::constants::topics::{
    TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_CREATED as TOPIC_MESSAGE_STORAGE,
    TOPIC_MESSAGE_EVENTS, TOPIC_MESSAGE_MAIN, TOPIC_PUSH_MESSAGES,
};
use flare_im_core::event::event_type_str_from_proto_event;
use flare_im_core::event::EVENT_TYPE_OPERATION_CONVERSATION_ENSURE;
use flare_im_core::event::types::types;
use flare_proto::common::Event;
use flare_proto::push::PushMessageRequest;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use prost::Message as _;

use crate::config::MessageOrchestratorConfig;
use crate::domain::repository::MessageEventPublisher;
use crate::error::{FlareError, Result};

const MAIN_KIND_MESSAGE: u8 = 1;
const MAIN_KIND_EVENT: u8 = 2;

/// MQ 发布器:发布消息和事件到对应的 topic
pub struct MqMessagePublisher {
    bus: Arc<MqEventBus>,
}

fn encode_main_payload(kind: u8, first: &[u8], second: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + first.len() + 4 + second.len());
    payload.push(kind);
    payload.extend_from_slice(&(first.len() as u32).to_be_bytes());
    payload.extend_from_slice(first);
    payload.extend_from_slice(&(second.len() as u32).to_be_bytes());
    payload.extend_from_slice(second);
    payload
}

impl MqMessagePublisher {
    pub fn new(config: Arc<MessageOrchestratorConfig>) -> Result<Arc<Self>> {
        let producer = KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| FlareError::system(format!("Kafka producer: {}", e)))?;
        let bus = MqEventBus::new(Arc::new(producer));
        Ok(Arc::new(Self { bus }))
    }

    /// 发布消息到存储 topic
    pub async fn publish_storage_message(
        &self,
        ctx: &Ctx,
        message: &flare_proto::common::Message,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        let payload = message.encode_to_vec();
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %message.conversation_id,
                "Message too large, reject publish"
            );
            return Err(FlareError::system("Message too large"));
        }

        let envelope = EventEnvelope::new(
            types::MESSAGE,
            &message.conversation_id,
            message.seq as u64,
            payload,
        )
        .with_source("flare-orchestrator");

        self.bus
            .publish(ctx, TOPIC_MESSAGE_STORAGE, &envelope)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    /// 发布领域事件到事件 topic
    pub async fn publish_domain_event(
        &self,
        ctx: &Ctx,
        event: &flare_proto::common::Event,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        let event_type = match event_type_str_from_proto_event(event) {
            Some(s) => s.to_string(),
            None => {
                tracing::warn!(conversation_id = %event.conversation_id, "reject unsupported event type");
                return Err(FlareError::system("unsupported event type"));
            }
        };

        let payload = event.encode_to_vec();
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                "Event too large, reject publish"
            );
            return Err(FlareError::system("Event too large"));
        }

        let envelope = EventEnvelope::new(
            event_type,
            &event.conversation_id,
            event.seq as u64,
            payload,
        )
        .with_source("flare-orchestrator");

        self.bus
            .publish(ctx, TOPIC_MESSAGE_EVENTS, &envelope)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    /// 发布会话 ensure 事件
    pub async fn publish_conversation_ensure(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        _tenant_id: &str,
        conversation_type: &str,
        business_type: &str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Result<()> {
        #[derive(serde::Serialize)]
        struct EnsurePayload {
            conversation_type: String,
            business_type: String,
            participants: Vec<String>,
            channel_id: String,
        }

        let payload = EnsurePayload {
            conversation_type: conversation_type.to_string(),
            business_type: business_type.to_string(),
            participants,
            channel_id: stored_channel_id,
        };

        let json_bytes =
            serde_json::to_vec(&payload).map_err(|e| FlareError::system(e.to_string()))?;

        let envelope = EventEnvelope::new(
            EVENT_TYPE_OPERATION_CONVERSATION_ENSURE,
            conversation_id,
            0,
            json_bytes,
        )
        .with_source("flare-orchestrator");

        self.bus
            .publish(ctx, TOPIC_CONVERSATION_ENSURE, &envelope)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn publish_main_message(
        &self,
        ctx: &Ctx,
        message: &flare_proto::common::Message,
        push: &flare_proto::push::PushMessageRequest,
    ) -> Result<()> {
        let payload = encode_main_payload(
            MAIN_KIND_MESSAGE,
            &message.encode_to_vec(),
            &push.encode_to_vec(),
        );
        let envelope = EventEnvelope::new(
            types::MESSAGE,
            &message.conversation_id,
            message.seq as u64,
            payload,
        )
        .with_source("flare-orchestrator");

        self.bus
            .publish(ctx, TOPIC_MESSAGE_MAIN, &envelope)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    async fn publish_main_event(
        &self,
        ctx: &Ctx,
        event: &flare_proto::common::Event,
    ) -> Result<()> {
        let event_type = match event_type_str_from_proto_event(event) {
            Some(s) => s.to_string(),
            None => {
                tracing::warn!(conversation_id = %event.conversation_id, "reject unsupported event type");
                return Err(FlareError::system("unsupported event type"));
            }
        };
        let payload = encode_main_payload(MAIN_KIND_EVENT, &event.encode_to_vec(), &[]);
        let envelope = EventEnvelope::new(
            event_type,
            &event.conversation_id,
            event.seq as u64,
            payload,
        )
        .with_source("flare-orchestrator");

        self.bus
            .publish(ctx, TOPIC_MESSAGE_MAIN, &envelope)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }
}

impl MessageEventPublisher for MqMessagePublisher {
    fn publish_storage<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let msg = match payload.with_context(ctx).to_message() {
            Some(m) => m,
            None => {
                return Box::pin(async {
                    Err(FlareError::system("storage payload has no message"))
                });
            }
        };
        Box::pin(async move { self.publish_storage_message(ctx, &msg).await })
    }

    fn publish_event<'a>(
        &'a self,
        ctx: &'a Ctx,
        mut event: Event,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if event
                .request_id
                .as_ref()
                .map(|v| v.is_empty())
                .unwrap_or(true)
            {
                event.request_id = Some(ctx.request_id().to_string());
            }
            self.publish_main_event(ctx, &event).await
        })
    }

    fn publish_push<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        // 临时消息等仅推送场景走 push topic。
        Box::pin(async move {
            let message = match payload.message.as_ref() {
                Some(m) => m,
                None => return Err(FlareError::system("push payload has no message")),
            };
            let envelope = EventEnvelope::new(
                types::MESSAGE,
                &message.conversation_id,
                message.seq as u64,
                payload.encode_to_vec(),
            )
            .with_source("flare-orchestrator");
            self.bus
                .publish(ctx, TOPIC_PUSH_MESSAGES, &envelope)
                .await
                .map_err(|e| FlareError::system(e.to_string()))
        })
    }

    fn publish_both<'a>(
        &'a self,
        ctx: &'a Ctx,
        storage_payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
        push_payload: flare_proto::push::PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let msg = match storage_payload.with_context(ctx).to_message() {
            Some(m) => m,
            None => {
                return Box::pin(async {
                    Err(FlareError::system("storage payload has no message"))
                });
            }
        };
        Box::pin(async move { self.publish_main_message(ctx, &msg, &push_payload).await })
    }
}

/// 领域 [MessageEventPublisher] 外壳，并暴露 `publish_conversation_ensure`。
pub struct OrchestratorPublisher(pub Arc<MqMessagePublisher>);

impl std::fmt::Debug for OrchestratorPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OrchestratorPublisher").finish()
    }
}

impl MessageEventPublisher for OrchestratorPublisher {
    fn publish_storage<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_storage(ctx, payload)
    }

    fn publish_event<'a>(
        &'a self,
        ctx: &'a Ctx,
        event: Event,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_event(ctx, event)
    }

    fn publish_push<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_push(ctx, payload)
    }

    fn publish_both<'a>(
        &'a self,
        ctx: &'a Ctx,
        storage_payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
        push_payload: PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_both(ctx, storage_payload, push_payload)
    }
}

impl OrchestratorPublisher {
    pub async fn publish_conversation_ensure(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        conversation_type: &str,
        business_type: &str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Result<()> {
        let ctx = Ctx::default();
        self.0
            .publish_conversation_ensure(
                &ctx,
                conversation_id,
                tenant_id,
                conversation_type,
                business_type,
                participants,
                stored_channel_id,
            )
            .await
    }
}
