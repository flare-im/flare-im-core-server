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

use flare_im_core::Ctx;
use flare_im_core::constants::topics::{
    TOPIC_MESSAGE_CREATED, TOPIC_MESSAGE_EVENTS, TOPIC_MESSAGE_MAIN, TOPIC_PUSH_ENVELOPE,
    TOPIC_PUSH_EVENTS, TOPIC_PUSH_MESSAGES,
};
use flare_im_core::event::{mq_envelope_for_main_queue_event, mq_envelope_for_main_queue_message};
use flare_proto::common::{Event, Message, PushEnvelope};
use flare_server_core::mq::producer::{Producer, ProducerError};
use prost::Message as _;

use crate::domain::repository::PushRepository;
use crate::error::{ErrorBuilder, ErrorCode, Result, to_system_err_with};

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
    fn map_producer_error(e: ProducerError) -> crate::error::FlareError {
        to_system_err_with(e, "producer_error")
    }

    /// 从 Ctx 构造 headers
    fn build_headers_from_ctx(_ctx: &Ctx) -> Option<HashMap<String, String>> {
        Some(HashMap::new())
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
    ) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

        // 构造 MqEnvelope
        let mq: flare_proto::MqEnvelope =
            mq_envelope_for_main_queue_message(&message, recipient_user_ids);
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

        // 从 Ctx 构造 headers
        let headers = Self::build_headers_from_ctx(ctx);

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
        let mq = mq_envelope_for_main_queue_event(&event, recipient_user_ids);
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

        // 从 Ctx 构造 headers
        let headers = Self::build_headers_from_ctx(ctx);

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
        let mut mq = mq_envelope_for_main_queue_message(&message, recipient_user_ids);

        // 标记为仅推送（不持久化）
        mq.push_only = true;

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

        // 从 Ctx 构造 headers
        let mut headers = Self::build_headers_from_ctx(ctx).unwrap_or_default();
        headers.insert("push-only".to_string(), "true".to_string());

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
        let mut mq = mq_envelope_for_main_queue_event(&event, recipient_user_ids);

        // 标记为仅推送（不持久化）
        mq.push_only = true;

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

        // 从 Ctx 构造 headers
        let mut headers = Self::build_headers_from_ctx(ctx).unwrap_or_default();
        headers.insert("push-only".to_string(), "true".to_string());

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
        let mut mq = mq_envelope_for_main_queue_message(&message, Vec::new());

        // 标记为仅持久化（不推送）
        mq.persistence_only = true;

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

        // 从 Ctx 构造 headers
        let mut headers = Self::build_headers_from_ctx(ctx).unwrap_or_default();
        headers.insert("persistence-only".to_string(), "true".to_string());

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
        let mut mq = mq_envelope_for_main_queue_event(&event, Vec::new());

        // 标记为仅持久化（不推送）
        mq.persistence_only = true;

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

        // 从 Ctx 构造 headers
        let mut headers = Self::build_headers_from_ctx(ctx).unwrap_or_default();
        headers.insert("persistence-only".to_string(), "true".to_string());

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
    async fn publish_push_envelope(&self, ctx: &Ctx, envelope: PushEnvelope) -> Result<()> {
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

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

        // 从 Ctx 构造 headers，并添加 envelope 中的 headers
        let headers = Self::build_headers_from_ctx(ctx).unwrap_or_default();

        // 使用 envelope_id 作为分区键
        let partition_key = &envelope.envelope_id;

        // 发布到 TOPIC_PUSH_ENVELOPE
        self.producer
            .send(
                ctx,
                TOPIC_PUSH_ENVELOPE,
                Some(partition_key),
                payload,
                Some(headers),
            )
            .await
            .map_err(Self::map_producer_error)
    }
}
