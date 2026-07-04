//! 推送仓储实现：基于 [flare_server_core::mq::Producer] trait，
//! 将消息和事件发布到 TOPIC_MESSAGE_MAIN 或 TOPIC_PUSH_MESSAGES/TOPIC_PUSH_EVENTS。
//!
//! ## 架构
//! - 依赖抽象的 `Producer` trait，而非具体的 MQ 实现
//! - 实现领域层的 `PushRepository` trait
//! - 持久化消息/事件通过 `Producer::send()` 发布到 TOPIC_MESSAGE_MAIN
//! - 仅推送消息/事件通过 `Producer::send()` 发布到 TOPIC_PUSH_MESSAGES/TOPIC_PUSH_EVENTS

use std::collections::HashMap;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::constants::headers::{
    DELIVERY_MODE_PING, DELIVERY_MODE_PING_WITH_INLINE, HEADER_DELIVERY_MODE,
    HEADER_INLINE_EVENTS_TRUNCATED,
};
use flare_im_contracts::constants::topics::{
    TOPIC_MESSAGE_CREATED, TOPIC_MESSAGE_EVENTS, TOPIC_MESSAGE_MAIN, TOPIC_PUSH_ENVELOPE,
    TOPIC_PUSH_EVENTS, TOPIC_PUSH_MESSAGES,
};
use flare_im_contracts::event::{
    mq_envelope_for_main_queue_event_with_headers, mq_envelope_for_main_queue_message_with_headers,
};
use flare_im_contracts::utils::{
    TimelineMetadata, context_to_mq_metadata, current_millis, embed_timeline_in_extra_map,
    normalize_tenant_id,
};
use flare_proto::common::{Event, EventType, Message, MqEnvelope, PushEnvelope};
use flare_server_core::mq::producer::{Producer, ProducerError};
use prost::Message as _;

use crate::repository::PushRepository;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

/// 推送仓储实现：基于 Producer trait 发布消息和事件
///
/// ## 设计
/// - 持有 `Arc<dyn Producer>` 而非具体实现，符合依赖倒置原则
/// - 所有发布操作通过 `Producer::send()` 完成
pub struct MqPushRepository {
    producer: Arc<dyn Producer>,
}

impl MqPushRepository {
    /// 从已有的 Producer 创建推送仓储
    ///
    /// # 参数
    /// - `producer`: 生产者实例（如 JetStreamProducer、NatsProducer）
    pub fn new(producer: Arc<dyn Producer>) -> Arc<Self> {
        Arc::new(Self { producer })
    }

    /// 将 ProducerError 转换为 FlareError
    fn map_producer_error(e: ProducerError) -> flare_server_core::error::FlareError {
        e.into_flare_error()
    }

    /// 从 Ctx 构造跨队列 headers，供外层 MQ 与内层 MqEnvelope 同时携带。
    fn build_headers_from_ctx(ctx: &Ctx) -> HashMap<String, String> {
        let mut headers = context_to_mq_metadata(ctx);
        headers
            .entry("x-tenant-id".to_string())
            .or_insert_with(|| normalize_tenant_id(ctx.tenant_id().unwrap_or("0")));
        headers
    }

    fn build_timeline_headers(ctx: &Ctx, emit_ts: Option<i64>) -> HashMap<String, String> {
        let ingestion_ts = current_millis();
        let mut headers = Self::build_headers_from_ctx(ctx);
        embed_timeline_in_extra_map(
            &mut headers,
            &TimelineMetadata {
                emit_ts,
                ingestion_ts,
                ..TimelineMetadata::default()
            },
        );
        headers
    }

    fn message_headers(ctx: &Ctx, message: &Message) -> HashMap<String, String> {
        Self::build_timeline_headers(ctx, (message.created_at > 0).then_some(message.created_at))
    }

    fn event_headers(ctx: &Ctx, event: &Event) -> HashMap<String, String> {
        Self::build_timeline_headers(ctx, (event.created_at > 0).then_some(event.created_at))
    }

    fn finalize_mq_headers(mq: &mut MqEnvelope) {
        mq.headers
            .entry("x-envelope-id".to_string())
            .or_insert_with(|| mq.envelope_id.clone());
        mq.headers
            .entry("x-produced-at-ms".to_string())
            .or_insert_with(|| mq.produced_at.to_string());
        mq.headers
            .entry("x-conversation-id".to_string())
            .or_insert_with(|| mq.conversation_id.clone());
        mq.headers
            .entry("x-conversation-seq".to_string())
            .or_insert_with(|| mq.seq.to_string());
    }

    fn envelope_headers(ctx: &Ctx, envelope: &mut PushEnvelope) -> HashMap<String, String> {
        if envelope.envelope_id.trim().is_empty() {
            envelope.envelope_id = uuid::Uuid::new_v4().to_string();
        }
        let mut headers = Self::build_headers_from_ctx(ctx);
        for (key, value) in envelope.headers.drain() {
            headers.insert(key, value);
        }
        if envelope.tenant_id.trim().is_empty() {
            envelope.tenant_id = headers
                .get("x-tenant-id")
                .cloned()
                .unwrap_or_else(|| "0".to_string());
        }
        if envelope.trace_id.trim().is_empty()
            && let Some(trace_id) = headers.get("x-trace-id")
        {
            envelope.trace_id = trace_id.clone();
        }
        headers
            .entry("x-envelope-id".to_string())
            .or_insert_with(|| envelope.envelope_id.clone());
        headers
            .entry("x-produced-at-ms".to_string())
            .or_insert_with(|| envelope.created_at.to_string());
        envelope.headers = headers.clone();
        headers
    }

    fn message_event(message: &Message, include_payload: bool) -> Event {
        Event {
            conversation_id: message.conversation_id.clone(),
            conversation_seq: message.conversation_seq,
            r#type: EventType::EventMessage as i32,
            created_at: message.created_at,
            event_id: if message.server_id.trim().is_empty() {
                let kind = if include_payload {
                    "message-inline"
                } else {
                    "message-ping"
                };
                format!(
                    "{kind}:{}:{}",
                    message.conversation_id, message.conversation_seq
                )
            } else if include_payload {
                format!("message-inline:{}", message.server_id)
            } else {
                format!("message-ping:{}", message.server_id)
            },
            request_id: None,
            payload: include_payload
                .then(|| flare_proto::common::event::Payload::Message(message.clone())),
        }
    }

    fn message_ping_event(message: &Message) -> Event {
        Self::message_event(message, false)
    }

    fn message_inline_event(message: &Message) -> Event {
        Self::message_event(message, true)
    }

    pub async fn push_only_message_ping(
        &self,
        ctx: &Ctx,
        message: &Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
        large_conversation: bool,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        let event = Self::message_ping_event(message);
        let mut mq = mq_envelope_for_main_queue_event_with_headers(
            &event,
            recipient_user_ids,
            Self::event_headers(ctx, &event),
        );
        mq.push_only = true;
        mq.large_conversation = large_conversation;
        mq.headers
            .insert("push-only".to_string(), "true".to_string());
        mq.headers.insert(
            HEADER_DELIVERY_MODE.to_string(),
            DELIVERY_MODE_PING.to_string(),
        );
        mq.headers.insert(
            HEADER_INLINE_EVENTS_TRUNCATED.to_string(),
            "true".to_string(),
        );
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject push-only message ping"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "message ping payload too large",
            )
            .param("size", payload.len().to_string())
            .param("max_size", MAX_MESSAGE_SIZE.to_string())
            .build_error());
        }

        self.producer
            .send(
                ctx,
                TOPIC_PUSH_EVENTS,
                Some(&conversation_id),
                payload,
                Some(mq.headers.clone()),
            )
            .await
            .map_err(Self::map_producer_error)
    }

    pub async fn push_only_message_inline_event(
        &self,
        ctx: &Ctx,
        message: &Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        let event = Self::message_inline_event(message);
        let mut mq = mq_envelope_for_main_queue_event_with_headers(
            &event,
            recipient_user_ids,
            Self::event_headers(ctx, &event),
        );
        mq.push_only = true;
        mq.headers
            .insert("push-only".to_string(), "true".to_string());
        mq.headers.insert(
            HEADER_DELIVERY_MODE.to_string(),
            DELIVERY_MODE_PING_WITH_INLINE.to_string(),
        );
        mq.headers.insert(
            HEADER_INLINE_EVENTS_TRUNCATED.to_string(),
            "false".to_string(),
        );
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject push-only message inline event"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "message inline event payload too large",
            )
            .param("size", payload.len().to_string())
            .param("max_size", MAX_MESSAGE_SIZE.to_string())
            .build_error());
        }

        self.producer
            .send(
                ctx,
                TOPIC_PUSH_EVENTS,
                Some(&conversation_id),
                payload,
                Some(mq.headers.clone()),
            )
            .await
            .map_err(Self::map_producer_error)
    }
}

impl PushRepository for MqPushRepository {
    /// 推送完整消息到 MQ（持久化 + 推送）
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 3. 生成 envelope_id 和 produced_at_ms
    /// 4. 发布到 TOPIC_MESSAGE_MAIN（由 Storage Writer 消费并持久化）
    async fn publish_message(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
        large_conversation: bool,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope
        let mut mq = mq_envelope_for_main_queue_message_with_headers(
            &message,
            recipient_user_ids,
            Self::message_headers(ctx, &message),
            large_conversation,
        );
        Self::finalize_mq_headers(&mut mq);
        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject publish"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "message payload too large",
            )
            .param("size", payload.len().to_string())
            .param("max_size", MAX_MESSAGE_SIZE.to_string())
            .build_error());
        }

        let headers = Some(mq.headers.clone());

        // 发布到 TOPIC_MESSAGE_MAIN，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_MESSAGE_MAIN,
                Some(&conversation_id),
                payload,
                headers,
            )
            .await
            .map_err(Self::map_producer_error)
    }

    /// 推送领域事件到 MQ（持久化 + 推送）
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 3. 生成 envelope_id 和 produced_at_ms
    /// 4. 发布到 TOPIC_MESSAGE_MAIN（由 Storage Writer 消费并持久化）
    async fn publish_event(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope
        let mut mq = mq_envelope_for_main_queue_event_with_headers(
            &event,
            recipient_user_ids,
            Self::event_headers(ctx, &event),
        );
        Self::finalize_mq_headers(&mut mq);
        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject publish"
            );
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "event payload too large")
                    .param("size", payload.len().to_string())
                    .param("max_size", MAX_MESSAGE_SIZE.to_string())
                    .build_error(),
            );
        }

        let headers = Some(mq.headers.clone());

        // 发布到 TOPIC_MESSAGE_MAIN，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_MESSAGE_MAIN,
                Some(&conversation_id),
                payload,
                headers,
            )
            .await
            .map_err(Self::map_producer_error)
    }

    /// 仅推送消息（不持久化）
    ///
    /// 用于临时消息（TYPING、SYSTEM_EVENT）：
    /// - 只推送给在线用户
    /// - 不经过 WAL
    /// - 不持久化到数据库
    /// - 离线用户直接丢弃
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 设置 envelope.push_only = true（标记为仅推送）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 TOPIC_PUSH_MESSAGES（由 Push Service 直接消费并推送，不经过 Storage Writer）
    async fn push_only_message(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope
        let mut mq = mq_envelope_for_main_queue_message_with_headers(
            &message,
            recipient_user_ids,
            Self::message_headers(ctx, &message),
            false,
        );

        // 标记为仅推送（不持久化）
        mq.push_only = true;
        mq.headers
            .insert("push-only".to_string(), "true".to_string());
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject push-only"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "message payload too large",
            )
            .param("size", payload.len().to_string())
            .param("max_size", MAX_MESSAGE_SIZE.to_string())
            .build_error());
        }

        let headers = mq.headers.clone();

        // 发布到 TOPIC_PUSH_MESSAGES，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_PUSH_MESSAGES,
                Some(&conversation_id),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }
    /// 仅推送事件（不持久化）
    ///
    /// 用于临时事件（如正在输入、在线状态等）：
    /// - 只推送给在线用户
    /// - 不经过 WAL
    /// - 不持久化到数据库
    /// - 离线用户直接丢弃
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 设置 envelope.push_only = true（标记为仅推送）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 TOPIC_PUSH_EVENTS（由 Push Service 直接消费并推送，不经过 Storage Writer）
    async fn push_only_event(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope
        let mut mq = mq_envelope_for_main_queue_event_with_headers(
            &event,
            recipient_user_ids,
            Self::event_headers(ctx, &event),
        );

        // 标记为仅推送（不持久化）
        mq.push_only = true;
        mq.headers
            .insert("push-only".to_string(), "true".to_string());
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject push-only"
            );
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "event payload too large")
                    .param("size", payload.len().to_string())
                    .param("max_size", MAX_MESSAGE_SIZE.to_string())
                    .build_error(),
            );
        }

        let headers = mq.headers.clone();

        // 发布到 TOPIC_PUSH_EVENTS，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_PUSH_EVENTS,
                Some(&conversation_id),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }
    /// 仅保存消息（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 消息已读回执（只需保存状态，不需要推送）
    /// - 消息撤回记录（只需保存记录，不需要推送）
    /// - 系统内部操作（只需持久化，不需要推送）
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 设置 envelope.persistence_only = true（标记为仅持久化）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 TOPIC_MESSAGE_CREATED（由 Storage Writer 消费并持久化，不推送给用户）
    async fn persistence_only_message(
        &self,
        ctx: &Ctx,
        message: Message,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope（不需要接收者列表）
        let mut mq = mq_envelope_for_main_queue_message_with_headers(
            &message,
            Vec::new(),
            Self::message_headers(ctx, &message),
            false,
        );

        // 标记为仅持久化（不推送）
        mq.persistence_only = true;
        mq.headers
            .insert("persistence-only".to_string(), "true".to_string());
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject persistence-only"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "message payload too large",
            )
            .param("size", payload.len().to_string())
            .param("max_size", MAX_MESSAGE_SIZE.to_string())
            .build_error());
        }

        let headers = mq.headers.clone();

        // 发布到 TOPIC_MESSAGE_CREATED，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_MESSAGE_CREATED,
                Some(&conversation_id),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }

    /// 仅保存事件（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 事件记录（只需保存记录，不需要推送）
    /// - 审计日志（只需持久化，不需要推送）
    /// - 系统内部事件（只需保存，不需要推送）
    ///
    /// Infrastructure 实现层负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 设置 envelope.persistence_only = true（标记为仅持久化）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 TOPIC_MESSAGE_EVENTS（由 Storage Writer 消费并持久化，不推送给用户）
    async fn persistence_only_event(
        &self,
        ctx: &Ctx,
        event: Event,
        conversation_id: String,
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope（不需要接收者列表）
        let mut mq = mq_envelope_for_main_queue_event_with_headers(
            &event,
            Vec::new(),
            Self::event_headers(ctx, &event),
        );

        // 标记为仅持久化（不推送）
        mq.persistence_only = true;
        mq.headers
            .insert("persistence-only".to_string(), "true".to_string());
        Self::finalize_mq_headers(&mut mq);

        let payload = mq.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                conversation_id = %conversation_id,
                "MqEnvelope too large, reject persistence-only"
            );
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "event payload too large")
                    .param("size", payload.len().to_string())
                    .param("max_size", MAX_MESSAGE_SIZE.to_string())
                    .build_error(),
            );
        }

        let headers = mq.headers.clone();

        // 发布到 TOPIC_MESSAGE_EVENTS，使用 conversation_id 作为分区键
        self.producer
            .send(
                ctx,
                TOPIC_MESSAGE_EVENTS,
                Some(&conversation_id),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }

    /// 发布统一推送信封（ACK、通知、CustomData、系统消息）
    ///
    /// 用于不需要存储的推送场景：
    /// - ACK 推送（消息确认）
    /// - 通知推送（系统通知）
    /// - CustomData 推送（自定义数据）
    /// - 系统消息推送（系统公告）
    ///
    /// Infrastructure 实现层负责：
    /// 1. 将 PushEnvelope 序列化
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers（如果未设置）
    /// 3. 发布到 TOPIC_PUSH_ENVELOPE（由 Push Server 消费并执行推送）
    async fn publish_push_envelope(&self, ctx: &Ctx, mut envelope: PushEnvelope) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        let headers = Self::envelope_headers(ctx, &mut envelope);

        // 序列化 PushEnvelope
        let payload = envelope.encode_to_vec();

        // 校验消息大小
        if payload.len() > MAX_MESSAGE_SIZE {
            tracing::error!(
                payload_size = payload.len(),
                envelope_id = %envelope.envelope_id,
                "PushEnvelope too large, reject publish"
            );
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "push envelope too large")
                    .param("size", payload.len().to_string())
                    .param("max_size", MAX_MESSAGE_SIZE.to_string())
                    .build_error(),
            );
        }

        // 使用 envelope_id 作为分区键
        let partition_key = envelope.envelope_id.clone();

        // 发布到 TOPIC_PUSH_ENVELOPE
        self.producer
            .send(
                ctx,
                TOPIC_PUSH_ENVELOPE,
                Some(&partition_key),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::constants::topics::TOPIC_MESSAGE_MAIN;
    use flare_proto::common::MqEnvelope;
    use flare_server_core::Context;
    use flare_server_core::mq::producer::ProducerMessage;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct CapturedSend {
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
        headers: Option<HashMap<String, String>>,
    }

    #[derive(Default)]
    struct CapturingProducer {
        send: Mutex<Option<CapturedSend>>,
    }

    #[async_trait::async_trait]
    impl Producer for CapturingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            payload: Vec<u8>,
            headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            *self.send.lock().expect("capture producer poisoned") = Some(CapturedSend {
                topic: topic.to_string(),
                key: key.map(ToString::to_string),
                payload,
                headers,
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
    async fn publish_message_carries_context_and_timeline_in_outer_and_inner_headers() {
        let producer = Arc::new(CapturingProducer::default());
        let repo = MqPushRepository::new(producer.clone());
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-message-publish-test")
                .with_trace_id("trace-message-publish-test")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 42,
            created_at: 1_700_000_000_000,
            ..Message::default()
        };

        repo.publish_message(
            &ctx,
            message,
            vec!["receiver-a".to_string()],
            "conversation-a".to_string(),
            false,
        )
        .await
        .expect("publish should succeed");

        let captured = producer
            .send
            .lock()
            .expect("capture producer poisoned")
            .clone()
            .expect("send should be captured");
        assert_eq!(captured.topic, TOPIC_MESSAGE_MAIN);
        assert_eq!(captured.key.as_deref(), Some("conversation-a"));

        let outer_headers = captured.headers.expect("outer headers should be set");
        assert_eq!(
            outer_headers.get("x-tenant-id").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(
            outer_headers.get("x-trace-id").map(String::as_str),
            Some("trace-message-publish-test")
        );
        assert!(outer_headers.contains_key("timeline"));
        assert!(outer_headers.contains_key("x-envelope-id"));

        let envelope = MqEnvelope::decode(captured.payload.as_slice())
            .expect("payload should decode as MqEnvelope");
        assert_eq!(envelope.conversation_id, "conversation-a");
        assert_eq!(envelope.seq, 42);
        assert!(!envelope.large_conversation);
        assert_eq!(
            envelope.headers.get("x-tenant-id").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(
            envelope.headers.get("timeline"),
            outer_headers.get("timeline")
        );
        assert_eq!(
            envelope.headers.get("x-envelope-id"),
            outer_headers.get("x-envelope-id")
        );
    }

    #[tokio::test]
    async fn publish_message_marks_large_conversation_without_materialized_recipients() {
        let producer = Arc::new(CapturingProducer::default());
        let repo = MqPushRepository::new(producer.clone());
        let ctx: Ctx = Arc::new(Context::default().with_tenant_id("tenant-a"));
        let message = Message {
            server_id: "message-large".to_string(),
            conversation_id: "conversation-large".to_string(),
            conversation_seq: 88,
            ..Message::default()
        };

        repo.publish_message(
            &ctx,
            message,
            Vec::new(),
            "conversation-large".to_string(),
            true,
        )
        .await
        .expect("publish should succeed");

        let captured = producer
            .send
            .lock()
            .expect("capture producer poisoned")
            .clone()
            .expect("send should be captured");
        let envelope = MqEnvelope::decode(captured.payload.as_slice())
            .expect("payload should decode as MqEnvelope");
        assert!(envelope.large_conversation);
        assert!(envelope.recipient_user_ids.is_empty());
    }

    #[tokio::test]
    async fn push_only_message_ping_marks_large_conversation_without_inline_payload() {
        let producer = Arc::new(CapturingProducer::default());
        let repo = MqPushRepository::new(producer.clone());
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-large-ping")
                .with_trace_id("trace-large-ping")
                .with_tenant_id("tenant-a"),
        );
        let message = Message {
            server_id: "message-large".to_string(),
            conversation_id: "conversation-large".to_string(),
            conversation_seq: 99,
            created_at: 1_700_000_000_099,
            ..Message::default()
        };

        repo.push_only_message_ping(
            &ctx,
            &message,
            Vec::new(),
            "conversation-large".to_string(),
            true,
        )
        .await
        .expect("push-only ping should publish");

        let captured = producer
            .send
            .lock()
            .expect("capture producer poisoned")
            .clone()
            .expect("send should be captured");
        assert_eq!(captured.topic, TOPIC_PUSH_EVENTS);
        assert_eq!(captured.key.as_deref(), Some("conversation-large"));

        let outer_headers = captured.headers.expect("outer headers should be set");
        assert_eq!(
            outer_headers.get(HEADER_DELIVERY_MODE).map(String::as_str),
            Some(DELIVERY_MODE_PING)
        );
        assert_eq!(
            outer_headers
                .get(HEADER_INLINE_EVENTS_TRUNCATED)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            outer_headers.get("push-only").map(String::as_str),
            Some("true")
        );

        let envelope = MqEnvelope::decode(captured.payload.as_slice())
            .expect("payload should decode as MqEnvelope");
        assert!(envelope.push_only);
        assert!(envelope.large_conversation);
        assert!(envelope.recipient_user_ids.is_empty());
        assert_eq!(
            envelope
                .headers
                .get(HEADER_DELIVERY_MODE)
                .map(String::as_str),
            Some(DELIVERY_MODE_PING)
        );
        assert_eq!(
            envelope
                .headers
                .get(HEADER_INLINE_EVENTS_TRUNCATED)
                .map(String::as_str),
            Some("true")
        );
        match envelope.payload.expect("event payload should exist") {
            flare_proto::common::mq_envelope::Payload::Event(event) => {
                assert_eq!(event.conversation_id, "conversation-large");
                assert_eq!(event.conversation_seq, 99);
                assert_eq!(event.r#type, EventType::EventMessage as i32);
                assert_eq!(event.event_id, "message-ping:message-large");
                assert!(
                    event.payload.is_none(),
                    "ping must not inline message bytes"
                );
            }
            other => panic!("expected event payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn push_only_message_inline_event_carries_typed_message_payload() {
        let producer = Arc::new(CapturingProducer::default());
        let repo = MqPushRepository::new(producer.clone());
        let ctx: Ctx = Arc::new(Context::default().with_tenant_id("tenant-a"));
        let message = Message {
            server_id: "message-small".to_string(),
            conversation_id: "conversation-small".to_string(),
            conversation_seq: 7,
            created_at: 1_700_000_000_007,
            ..Message::default()
        };

        repo.push_only_message_inline_event(
            &ctx,
            &message,
            vec!["receiver-a".to_string()],
            "conversation-small".to_string(),
        )
        .await
        .expect("push-only inline event should publish");

        let captured = producer
            .send
            .lock()
            .expect("capture producer poisoned")
            .clone()
            .expect("send should be captured");
        assert_eq!(captured.topic, TOPIC_PUSH_EVENTS);
        assert_eq!(captured.key.as_deref(), Some("conversation-small"));

        let envelope = MqEnvelope::decode(captured.payload.as_slice())
            .expect("payload should decode as MqEnvelope");
        assert!(envelope.push_only);
        assert!(!envelope.large_conversation);
        assert_eq!(envelope.recipient_user_ids, vec!["receiver-a".to_string()]);
        assert_eq!(
            envelope
                .headers
                .get(HEADER_DELIVERY_MODE)
                .map(String::as_str),
            Some(DELIVERY_MODE_PING_WITH_INLINE)
        );
        assert_eq!(
            envelope
                .headers
                .get(HEADER_INLINE_EVENTS_TRUNCATED)
                .map(String::as_str),
            Some("false")
        );
        match envelope.payload.expect("event payload should exist") {
            flare_proto::common::mq_envelope::Payload::Event(event) => {
                assert_eq!(event.event_id, "message-inline:message-small");
                match event.payload.expect("inline message should exist") {
                    flare_proto::common::event::Payload::Message(inline) => {
                        assert_eq!(inline.server_id, "message-small");
                        assert_eq!(inline.conversation_id, "conversation-small");
                        assert_eq!(inline.conversation_seq, 7);
                    }
                    other => panic!("expected inline message payload, got {other:?}"),
                }
            }
            other => panic!("expected event payload, got {other:?}"),
        }
    }

    #[test]
    fn transient_producer_errors_remain_retryable() {
        let errors = [
            ProducerError::Connection("nats disconnected".to_string()),
            ProducerError::Timeout("publish ack timeout".to_string()),
            ProducerError::Send("broker unavailable".to_string()),
            ProducerError::Batch("batch publish failed".to_string()),
        ];

        for error in errors {
            let mapped = MqPushRepository::map_producer_error(error);
            assert_eq!(mapped.code(), Some(ErrorCode::ServiceUnavailable));
            assert!(mapped.is_retryable());
        }
    }

    #[test]
    fn non_transient_producer_errors_remain_non_retryable() {
        let serialization = MqPushRepository::map_producer_error(ProducerError::Serialization(
            "bad payload".into(),
        ));
        assert_eq!(serialization.code(), Some(ErrorCode::SerializationError));
        assert!(!serialization.is_retryable());

        let configuration = MqPushRepository::map_producer_error(ProducerError::Configuration(
            "missing broker url".into(),
        ));
        assert_eq!(configuration.code(), Some(ErrorCode::ConfigurationError));
        assert!(!configuration.is_retryable());
    }
}
