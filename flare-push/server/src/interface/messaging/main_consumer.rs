//! 推送主队列消费者 - 处理 TOPIC_MESSAGE_MAIN 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_MAIN 中的 MqEnvelope 消息
//! 2. 根据 MqPayloadKind 分发到不同的处理逻辑
//! 3. 处理 push_only 和 persistence_only 标记
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - Payload 分发：根据 payload_kind 分发到 Message 或 Event 处理逻辑

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::access_gateway::{PushEventRequest, PushMessageRequest};
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

/// 推送主队列消费者处理器
///
/// 处理 `TOPIC_MESSAGE_MAIN` 中的 MqEnvelope 消息，根据 payload_kind 分发处理
pub struct PushMainHandler {
    /// 推送路由处理器（应用层）
    route_handler: Arc<PushRouterHandler>,
    /// MQ 发布器（用于发送 DLQ）
    publisher: Arc<PushServerMqPublisher>,
}

impl PushMainHandler {
    /// 创建新的推送主队列消费者处理器
    ///
    /// # 参数
    /// - `route_handler`: 推送路由处理器
    /// - `publisher`: MQ 发布器
    ///
    /// # 返回
    /// - `Self`: 推送主队列消费者处理器实例
    pub fn new(
        route_handler: Arc<PushRouterHandler>,
        publisher: Arc<PushServerMqPublisher>,
    ) -> Self {
        Self {
            route_handler,
            publisher,
        }
    }

    /// 处理 Message payload
    async fn handle_message_payload(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: &MqEnvelope,
        original_payload: Vec<u8>,
    ) -> Result<MessageResult, ConsumerError> {
        // 检查 persistence_only 标记
        if envelope.persistence_only {
            tracing::trace!(
                envelope_id = %envelope.envelope_id,
                "Message marked as persistence_only, skipping push"
            );
            return Ok(MessageResult::Ack);
        }

        let proto_message = match &envelope.payload {
            Some(mq_envelope::Payload::Message(m)) => m,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Message payload missing or wrong variant"
                );
                return Err(ConsumerError::Deserialization(
                    "Message payload missing or wrong variant".to_string(),
                ));
            }
        };

        // 构建 PushMessageRequest
        let req = PushMessageRequest {
            user_ids: envelope.recipient_user_ids.clone(),
            messages: vec![proto_message.clone()],
            options: None,
        };

        // 调用 Application 层
        match self.route_handler.handle_message(ctx, req).await {
            Ok(()) => Ok(MessageResult::Ack),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    envelope_id = %envelope.envelope_id,
                    "Failed to handle message payload, sending to DLQ"
                );
                // 发送到 DLQ
                if let Err(dlq_err) = self
                    .publisher
                    .publish_dlq(ctx, Some(&envelope.conversation_id), original_payload)
                    .await
                {
                    tracing::error!(error = %dlq_err, "Failed to send message to DLQ");
                }
                Ok(MessageResult::Ack)
            }
        }
    }

    /// 处理 Event payload
    async fn handle_event_payload(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: &MqEnvelope,
        original_payload: Vec<u8>,
    ) -> Result<MessageResult, ConsumerError> {
        // 检查 recipient_user_ids
        if envelope.recipient_user_ids.is_empty() {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                "Event MqEnvelope without recipients, skipping push"
            );
            return Ok(MessageResult::Ack);
        }

        let proto_event = match &envelope.payload {
            Some(mq_envelope::Payload::Event(e)) => e,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Event payload missing or wrong variant"
                );
                return Err(ConsumerError::Deserialization(
                    "Event payload missing or wrong variant".to_string(),
                ));
            }
        };

        // 构建 PushEventRequest
        let req = PushEventRequest {
            user_ids: envelope.recipient_user_ids.clone(),
            events: vec![proto_event.clone()],
            options: None,
        };

        // 调用 Application 层
        match self.route_handler.handle_event(ctx, req).await {
            Ok(()) => Ok(MessageResult::Ack),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    envelope_id = %envelope.envelope_id,
                    "Failed to handle event payload, sending to DLQ"
                );
                // 发送到 DLQ
                if let Err(dlq_err) = self
                    .publisher
                    .publish_dlq(ctx, Some(&envelope.conversation_id), original_payload)
                    .await
                {
                    tracing::error!(error = %dlq_err, "Failed to send message to DLQ");
                }
                Ok(MessageResult::Ack)
            }
        }
    }
}

#[async_trait]
impl MessageHandler for PushMainHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 根据 payload_kind 分发到不同的处理逻辑
    /// 3. 处理 push_only 和 persistence_only 标记
    /// 4. 失败时发送到 DLQ
    ///
    /// # 参数
    /// - `message`: MQ 消息
    ///
    /// # 返回
    /// - `Ok(MessageResult::Ack)`: 处理成功
    /// - `Err(ConsumerError)`: 处理失败
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        // 1. 反序列化 MqEnvelope
        let envelope = message.decode_protobuf::<MqEnvelope>().map_err(|e| {
            tracing::error!(
                error = %e,
                topic = %message.context.topic,
                "Failed to deserialize MqEnvelope"
            );
            ConsumerError::Deserialization(format!("Failed to deserialize MqEnvelope: {}", e))
        })?;

        tracing::trace!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            push_only = envelope.push_only,
            persistence_only = envelope.persistence_only,
            "Processing MqEnvelope from TOPIC_MESSAGE_MAIN"
        );

        // 2. 从 headers 中重建上下文
        let ctx = &message.context.ctx;

        // 3. 根据 payload_kind 分发
        match MqPayloadKind::try_from(envelope.payload_kind) {
            Ok(MqPayloadKind::Message) => {
                self.handle_message_payload(ctx, &envelope, message.payload.clone())
                    .await
            }
            Ok(MqPayloadKind::Event) => {
                self.handle_event_payload(ctx, &envelope, message.payload.clone())
                    .await
            }
            _ => {
                tracing::warn!(
                    envelope_id = %envelope.envelope_id,
                    payload_kind = ?envelope.payload_kind,
                    "Unknown payload_kind, skipping"
                );
                Ok(MessageResult::Ack)
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "push-main-handler"
    }
}
