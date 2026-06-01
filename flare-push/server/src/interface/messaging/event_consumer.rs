//! 推送事件消费者 - 处理 TOPIC_PUSH_EVENTS 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_EVENTS 中的 MqEnvelope 消息
//! 2. 反序列化 MqEnvelope 并验证 payload_kind 为 EVENT
//! 3. 调用 PushRouterHandler 处理推送路由
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 错误处理：失败时发送到 DLQ

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::access_gateway::PushEventRequest;
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use flare_im_core::error::FlareError;

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

fn retry_or_dlq_result(error: &FlareError) -> Option<MessageResult> {
    error.is_retryable().then_some(MessageResult::Nack)
}

/// 推送事件消费者处理器
///
/// 处理 `TOPIC_PUSH_EVENTS` 中的 MqEnvelope 消息，负责事件推送
pub struct PushEventHandler {
    /// 推送路由处理器（应用层）
    route_handler: Arc<PushRouterHandler>,
    /// MQ 发布器（用于发送 DLQ）
    publisher: Arc<PushServerMqPublisher>,
}

impl PushEventHandler {
    /// 创建新的推送事件消费者处理器
    ///
    /// # 参数
    /// - `route_handler`: 推送路由处理器
    /// - `publisher`: MQ 发布器
    ///
    /// # 返回
    /// - `Self`: 推送事件消费者处理器实例
    pub fn new(
        route_handler: Arc<PushRouterHandler>,
        publisher: Arc<PushServerMqPublisher>,
    ) -> Self {
        Self {
            route_handler,
            publisher,
        }
    }
}

#[async_trait]
impl MessageHandler for PushEventHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 验证 payload_kind 为 EVENT
    /// 3. 从 payload 提取 Event
    /// 4. 从 headers 中重建上下文
    /// 5. 调用 PushRouterHandler.handle_event()
    /// 6. 失败时发送到 DLQ
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
            "Processing MqEnvelope from TOPIC_PUSH_EVENTS"
        );

        // 2. 验证 payload_kind
        if envelope.payload_kind != MqPayloadKind::Event as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected EVENT, sending to DLQ"
            );
            // 发送到 DLQ
            let ctx = &message.context.ctx;
            if let Err(e) = self
                .publisher
                .publish_dlq(
                    ctx,
                    Some(&envelope.conversation_id),
                    message.payload.clone(),
                )
                .await
            {
                tracing::error!(error = %e, "Failed to send message to DLQ");
            }
            return Ok(MessageResult::Ack);
        }

        // 3. 从 payload oneof 提取 Event
        let proto_event = match &envelope.payload {
            Some(mq_envelope::Payload::Event(e)) => e,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Event payload missing or wrong variant, sending to DLQ"
                );
                let ctx = &message.context.ctx;
                if let Err(e) = self
                    .publisher
                    .publish_dlq(
                        ctx,
                        Some(&envelope.conversation_id),
                        message.payload.clone(),
                    )
                    .await
                {
                    tracing::error!(error = %e, "Failed to send message to DLQ");
                }
                return Ok(MessageResult::Ack);
            }
        };

        // 4. 从 headers 中重建上下文
        let ctx = &message.context.ctx;

        // 5. 构建 PushEventRequest
        let req = PushEventRequest {
            user_ids: envelope.recipient_user_ids.clone(),
            events: vec![proto_event.clone()],
            options: None,
        };

        // 6. 调用 Application 层
        match self.route_handler.handle_event(ctx, req).await {
            Ok(()) => {
                tracing::trace!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Successfully processed MqEnvelope"
                );
                Ok(MessageResult::Ack)
            }
            Err(e) => {
                if let Some(result) = retry_or_dlq_result(&e) {
                    tracing::warn!(
                        error = %e,
                        topic = %message.context.topic,
                        partition = message.context.partition,
                        offset = message.context.offset,
                        "Retryable failure while processing MqEnvelope, NACKing for redelivery"
                    );
                    return Ok(result);
                }

                tracing::error!(
                    error = %e,
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    "Failed to process MqEnvelope, sending to DLQ"
                );
                // 发送到 DLQ
                if let Err(dlq_err) = self
                    .publisher
                    .publish_dlq(
                        ctx,
                        Some(&envelope.conversation_id),
                        message.payload.clone(),
                    )
                    .await
                {
                    tracing::error!(error = %dlq_err, "Failed to send message to DLQ");
                }
                Ok(MessageResult::Ack)
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "push-event-handler"
    }
}

#[cfg(test)]
mod tests {
    use flare_im_core::error::{ErrorBuilder, ErrorCode};

    use super::*;

    #[test]
    fn retryable_error_requests_nack() {
        let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "mq unavailable").build_error();

        assert!(matches!(
            retry_or_dlq_result(&err),
            Some(MessageResult::Nack)
        ));
    }

    #[test]
    fn non_retryable_error_uses_dlq_path() {
        let err = ErrorBuilder::new(ErrorCode::MessageSendFailed, "bad payload").build_error();

        assert!(retry_or_dlq_result(&err).is_none());
    }
}
