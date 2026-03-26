//! 编排侧 Kafka 发布：基于 [flare_server_core::event_bus::MqEventBus] / [EventPublisher]，
//! Topic 与 [flare_im_core::constants::topics] 对齐。

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flare_im_core::abstractions::topics::{
    event_type_str_from_proto_event, message_to_topic_event_envelope, topic_event_envelope_from_event,
    to_event_envelope, EVENT_TYPE_CONVERSATION_ENSURE,
};
use flare_im_core::constants::topics::{
    TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_CREATED, TOPIC_MESSAGE_EVENTS, TOPIC_PUSH_MESSAGES,
    TOPIC_PUSH_NOTIFICATIONS,
};
use flare_im_core::event::types::types;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{CustomEvent, Event, EventType, TopicEventEnvelope};
use flare_proto::push::{Notification, PushMessageRequest, PushNotificationRequest};
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use prost::Message as ProstMessage;
use tokio::sync::Mutex;

use crate::config::MessageOrchestratorConfig;
use crate::domain::repository::MessageEventPublisher;
use crate::error::{FlareError, Result};

fn tenant_id_from_message(msg: &flare_proto::common::Message, config_default: &str) -> String {
    msg.extra
        .get("x-tenant-id")
        .or_else(|| msg.extra.get("tenant_id"))
        .cloned()
        .unwrap_or_else(|| config_default.to_string())
}

fn flush_ctx() -> Ctx {
    Ctx::default()
}

fn partition_key_push(req: &PushMessageRequest) -> String {
    req.user_ids
        .first()
        .cloned()
        .or_else(|| req.message.as_ref().map(|m| m.conversation_id.clone()))
        .unwrap_or_default()
}

/// 通知类推送：走 [TOPIC_PUSH_NOTIFICATIONS]，载荷为 [PushNotificationRequest]。
fn push_message_to_notification_request(req: &PushMessageRequest) -> Option<PushNotificationRequest> {
    let opt = req.options.as_ref()?;
    if !(opt.require_online && !opt.persist_if_offline) {
        return None;
    }
    let msg = req.message.as_ref()?;
    let title = msg
        .offline_push_info
        .as_ref()
        .map(|i| i.title.clone())
        .unwrap_or_default();
    let body = msg
        .offline_push_info
        .as_ref()
        .filter(|i| !i.body.is_empty())
        .map(|i| i.body.clone())
        .unwrap_or_else(|| String::from_utf8_lossy(&msg.content).into_owned());
    let notification = Notification {
        title,
        body,
        data: Default::default(),
        metadata: Default::default(),
        click_action: String::new(),
        icon: String::new(),
        sound: String::new(),
    };
    Some(PushNotificationRequest {
        user_ids: req.user_ids.clone(),
        notification: Some(notification),
        options: req.options.clone(),
        metadata: req.metadata.clone(),
    })
}

/// MQ 发布器：缓冲 + 周期 flush，与 Push Proxy 一致使用 JSON [EventEnvelope] 入 Kafka。
pub struct MqMessagePublisher {
    bus: Arc<MqEventBus>,
    config: Arc<MessageOrchestratorConfig>,
    storage_buffer: Arc<Mutex<Vec<flare_proto::common::Message>>>,
    event_buffer: Arc<Mutex<Vec<Event>>>,
    push_buffer: Arc<Mutex<Vec<PushMessageRequest>>>,
    storage_last_flush: Arc<Mutex<std::time::Instant>>,
    event_last_flush: Arc<Mutex<std::time::Instant>>,
    push_last_flush: Arc<Mutex<std::time::Instant>>,
}

impl MqMessagePublisher {
    pub fn new(config: Arc<MessageOrchestratorConfig>) -> Result<Arc<Self>> {
        let producer = KafkaProducerBuilder::new()
            .build(config.as_ref())
            .map_err(|e| FlareError::system(format!("Kafka producer: {}", e)))?;
        let bus = MqEventBus::new(Arc::new(producer));
        let now = std::time::Instant::now();
        let publisher = Arc::new(Self {
            bus,
            config: config.clone(),
            storage_buffer: Arc::new(Mutex::new(Vec::new())),
            event_buffer: Arc::new(Mutex::new(Vec::new())),
            push_buffer: Arc::new(Mutex::new(Vec::new())),
            storage_last_flush: Arc::new(Mutex::new(now)),
            event_last_flush: Arc::new(Mutex::new(now)),
            push_last_flush: Arc::new(Mutex::new(now)),
        });

        let publisher_clone = Arc::clone(&publisher);
        let flush_interval = Duration::from_millis(config.kafka_flush_interval_ms);
        tokio::spawn(async move {
            publisher_clone.auto_flush_loop(flush_interval).await;
        });

        Ok(publisher)
    }

    async fn auto_flush_loop(self: Arc<Self>, flush_interval: Duration) {
        let mut interval = tokio::time::interval(flush_interval);
        loop {
            interval.tick().await;

            let storage_messages = {
                let mut buffer = self.storage_buffer.lock().await;
                let last_flush = self.storage_last_flush.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;
                if should_flush && !buffer.is_empty() {
                    let messages = buffer.drain(..).collect();
                    drop(buffer);
                    Some(messages)
                } else {
                    None
                }
            };
            if let Some(messages) = storage_messages {
                if let Err(e) = self.publish_storage_batch(messages).await {
                    tracing::error!(error = %e, "flush storage batch failed");
                }
                *self.storage_last_flush.lock().await = std::time::Instant::now();
            }

            let events = {
                let mut buffer = self.event_buffer.lock().await;
                let last_flush = self.event_last_flush.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;
                if should_flush && !buffer.is_empty() {
                    let ev = buffer.drain(..).collect();
                    drop(buffer);
                    Some(ev)
                } else {
                    None
                }
            };
            if let Some(ev) = events {
                if let Err(e) = self.publish_event_batch(ev).await {
                    tracing::error!(error = %e, "flush event batch failed");
                }
                *self.event_last_flush.lock().await = std::time::Instant::now();
            }

            let push_messages = {
                let mut buffer = self.push_buffer.lock().await;
                let last_flush = self.push_last_flush.lock().await;
                let should_flush = buffer.len() >= self.config.kafka_batch_size
                    || last_flush.elapsed() >= flush_interval;
                if should_flush && !buffer.is_empty() {
                    let messages = buffer.drain(..).collect();
                    drop(buffer);
                    Some(messages)
                } else {
                    None
                }
            };
            if let Some(messages) = push_messages {
                if let Err(e) = self.publish_push_batch(messages).await {
                    tracing::error!(error = %e, "flush push batch failed");
                }
                *self.push_last_flush.lock().await = std::time::Instant::now();
            }
        }
    }

    async fn publish_topic_envelope(
        &self,
        ctx: &Ctx,
        topic: &str,
        envelope: &TopicEventEnvelope,
    ) -> Result<()> {
        let ev = to_event_envelope(envelope);
        self.bus
            .publish(ctx, topic, &ev)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    /// 供 [crate::infrastructure::messaging::event_bus_adapter::OrchestratorEventBusAdapter] 写入 [TOPIC_MESSAGE_EVENTS]
    pub async fn publish_topic_event_envelope_to_events_topic(
        &self,
        ctx: &Ctx,
        envelope: &TopicEventEnvelope,
    ) -> Result<()> {
        self.publish_topic_envelope(ctx, TOPIC_MESSAGE_EVENTS, envelope)
            .await
    }

    async fn publish_storage_batch(&self, payloads: Vec<flare_proto::common::Message>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let batch_len = payloads.len();
        let ctx = flush_ctx();
        let config_default = self.config.default_tenant_id.as_deref().unwrap_or("default");
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        for payload in payloads {
            let tenant_id = tenant_id_from_message(&payload, config_default);
            let seq = payload.seq as u64;
            let te = message_to_topic_event_envelope(&payload, tenant_id.as_str(), seq);
            let encoded = ProstMessage::encode_to_vec(&te);
            if encoded.len() > MAX_MESSAGE_SIZE {
                tracing::error!(
                    payload_size = encoded.len(),
                    conversation_id = %payload.conversation_id,
                    "TopicEventEnvelope too large, skip"
                );
                continue;
            }
            self.publish_topic_envelope(&ctx, TOPIC_MESSAGE_CREATED, &te).await?;
        }
        tracing::info!(
            topic = %TOPIC_MESSAGE_CREATED,
            batch_size = batch_len,
            "Published storage batch"
        );
        Ok(())
    }

    async fn publish_event_batch(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let batch_len = events.len();
        let ctx = flush_ctx();
        let tenant_id = self.config.default_tenant_id.as_deref().unwrap_or("default");
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        for event in events {
            let event_type = match event_type_str_from_proto_event(&event) {
                Some(s) => s.to_string(),
                None => {
                    tracing::warn!(conversation_id = %event.conversation_id, "skip unsupported event type");
                    continue;
                }
            };
            let te = topic_event_envelope_from_event(
                event.conversation_id.clone(),
                Some(event.clone()),
                tenant_id,
                event_type,
                event.seq as u64,
                event.request_id.clone().unwrap_or_default(),
            );
            let buf = ProstMessage::encode_to_vec(&te);
            if buf.len() > MAX_MESSAGE_SIZE {
                tracing::error!(payload_size = buf.len(), "TopicEventEnvelope too large, skip");
                continue;
            }
            self.publish_topic_envelope(&ctx, TOPIC_MESSAGE_EVENTS, &te).await?;
        }
        tracing::info!(topic = %TOPIC_MESSAGE_EVENTS, batch_size = batch_len, "Published event batch");
        Ok(())
    }

    async fn publish_push_batch(&self, payloads: Vec<PushMessageRequest>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let batch_len = payloads.len();
        let ctx = flush_ctx();
        for req in payloads {
            self.publish_push_one(&ctx, &req).await?;
        }
        tracing::info!(
            topics = %format!("{}/{}", TOPIC_PUSH_MESSAGES, TOPIC_PUSH_NOTIFICATIONS),
            count = batch_len,
            "Published push batch"
        );
        Ok(())
    }

    async fn publish_push_one(&self, ctx: &Ctx, req: &PushMessageRequest) -> Result<()> {
        let key = partition_key_push(req);
        if let Some(pnr) = push_message_to_notification_request(req) {
            let env = EventEnvelope::new(types::NOTIFICATION, &key, 0, pnr.encode_to_vec());
            return self
                .bus
                .publish(ctx, TOPIC_PUSH_NOTIFICATIONS, &env)
                .await
                .map_err(|e| FlareError::system(e.to_string()));
        }
        let env = EventEnvelope::new(types::MESSAGE, &key, 0, req.encode_to_vec());
        self.bus
            .publish(ctx, TOPIC_PUSH_MESSAGES, &env)
            .await
            .map_err(|e| FlareError::system(e.to_string()))
    }

    pub async fn flush(&self) -> Result<()> {
        let storage_messages = {
            let mut buffer = self.storage_buffer.lock().await;
            if !buffer.is_empty() {
                Some(buffer.drain(..).collect())
            } else {
                None
            }
        };
        if let Some(messages) = storage_messages {
            self.publish_storage_batch(messages).await?;
        }

        let events = {
            let mut buffer = self.event_buffer.lock().await;
            if !buffer.is_empty() {
                Some(buffer.drain(..).collect())
            } else {
                None
            }
        };
        if let Some(ev) = events {
            self.publish_event_batch(ev).await?;
        }

        let push_messages = {
            let mut buffer = self.push_buffer.lock().await;
            if !buffer.is_empty() {
                Some(buffer.drain(..).collect())
            } else {
                None
            }
        };
        if let Some(messages) = push_messages {
            self.publish_push_batch(messages).await?;
        }

        let now = std::time::Instant::now();
        *self.storage_last_flush.lock().await = now;
        *self.event_last_flush.lock().await = now;
        *self.push_last_flush.lock().await = now;
        Ok(())
    }

    /// 会话 ensure（异步创建）：发往 [TOPIC_CONVERSATION_ENSURE]
    pub async fn publish_conversation_ensure(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        conversation_type: &str,
        business_type: &str,
        participants: Vec<String>,
    ) -> Result<()> {
        #[derive(serde::Serialize)]
        struct EnsurePayload {
            conversation_type: String,
            business_type: String,
            participants: Vec<String>,
        }
        let payload = EnsurePayload {
            conversation_type: conversation_type.to_string(),
            business_type: business_type.to_string(),
            participants,
        };
        let json_bytes = serde_json::to_vec(&payload).map_err(|e| FlareError::system(e.to_string()))?;
        let event = Event {
            conversation_id: conversation_id.to_string(),
            seq: 0,
            r#type: EventType::EventCustom as i32,
            created_at: None,
            event_id: format!("{}:conv_ensure:0", conversation_id),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Custom(CustomEvent {
                namespace: String::new(),
                name: EVENT_TYPE_CONVERSATION_ENSURE.to_string(),
                version: String::new(),
                payload: json_bytes,
                metadata: std::collections::HashMap::new(),
            })),
        };
        let envelope = TopicEventEnvelope {
            conversation_id: conversation_id.to_string(),
            event: Some(event),
            tenant_id: tenant_id.to_string(),
            event_type: EVENT_TYPE_CONVERSATION_ENSURE.to_string(),
            seq: 0,
            request_id: String::new(),
        };
        let ctx = flush_ctx();
        self.publish_topic_envelope(&ctx, TOPIC_CONVERSATION_ENSURE, &envelope).await?;
        tracing::debug!(topic = %TOPIC_CONVERSATION_ENSURE, conversation_id = %conversation_id, "Published conversation.ensure");
        Ok(())
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
            None => return Box::pin(async { Err(FlareError::system("storage payload has no message")) }),
        };
        Box::pin(async move {
            let should_flush = {
                let mut buffer = self.storage_buffer.lock().await;
                buffer.push(msg);
                buffer.len() >= self.config.kafka_batch_size
            };
            if should_flush {
                let messages: Vec<flare_proto::common::Message> = {
                    let mut buffer = self.storage_buffer.lock().await;
                    buffer.drain(..).collect()
                };
                self.publish_storage_batch(messages).await?;
                *self.storage_last_flush.lock().await = std::time::Instant::now();
            }
            Ok(())
        })
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
            self.publish_event_batch(vec![event]).await?;
            *self.event_last_flush.lock().await = std::time::Instant::now();
            Ok(())
        })
    }

    fn publish_push<'a>(
        &'a self,
        _ctx: &'a Ctx,
        payload: PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let should_flush = {
                let mut buffer = self.push_buffer.lock().await;
                buffer.push(payload);
                buffer.len() >= self.config.kafka_batch_size
            };
            if should_flush {
                let messages: Vec<PushMessageRequest> = {
                    let mut buffer = self.push_buffer.lock().await;
                    buffer.drain(..).collect()
                };
                self.publish_push_batch(messages).await?;
            }
            Ok(())
        })
    }

    fn publish_both<'a>(
        &'a self,
        ctx: &'a Ctx,
        storage_payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
        push_payload: PushMessageRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let msg = match storage_payload.with_context(ctx).to_message() {
            Some(m) => m,
            None => return Box::pin(async { Err(FlareError::system("storage payload has no message")) }),
        };
        Box::pin(async move {
            let (storage_should_flush, push_should_flush) = {
                let mut storage_buffer = self.storage_buffer.lock().await;
                let mut push_buffer = self.push_buffer.lock().await;
                storage_buffer.push(msg);
                push_buffer.push(push_payload);
                (
                    storage_buffer.len() >= self.config.kafka_batch_size,
                    push_buffer.len() >= self.config.kafka_batch_size,
                )
            };
            if storage_should_flush || push_should_flush {
                self.flush().await?;
            }
            Ok(())
        })
    }
}
