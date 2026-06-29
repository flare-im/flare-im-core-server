//! 推送消息消费者 - 处理 TOPIC_PUSH_MESSAGES 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_MESSAGES 中的 MqEnvelope 消息
//! 2. 反序列化 MqEnvelope 并验证 payload_kind 为 MESSAGE
//! 3. 调用 PushRouterHandler 处理消息推送
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 错误处理：失败时发送到 DLQ

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::access_gateway::PushMessageRequest;
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use flare_server_core::error::FlareError;

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

fn retry_or_dlq_result(_error: &FlareError) -> Option<MessageResult> {
    None
}

/// 推送消息消费者处理器
///
/// 处理 `TOPIC_PUSH_MESSAGES` 中的 MqEnvelope 消息，负责消息推送
pub struct PushMessageHandler {
    /// 推送路由处理器（应用层）
    route_handler: Arc<PushRouterHandler>,
    /// MQ 发布器（用于发送 DLQ）
    publisher: Arc<PushServerMqPublisher>,
}

impl PushMessageHandler {
    /// 创建新的推送消息消费者处理器
    ///
    /// # 参数
    /// - `route_handler`: 推送路由处理器
    /// - `publisher`: MQ 发布器
    ///
    /// # 返回
    /// - `Self`: 推送消息消费者处理器实例
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
impl MessageHandler for PushMessageHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 验证 payload_kind 为 MESSAGE
    /// 3. 从 payload 提取 Message
    /// 4. 从 headers 中重建上下文
    /// 5. 调用 PushRouterHandler.handle_message()
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
        let envelope = match message.decode_protobuf::<MqEnvelope>() {
            Ok(envelope) => envelope,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    topic = %message.context.topic,
                    "Failed to deserialize MqEnvelope, sending raw payload to DLQ"
                );
                if let Err(dlq_err) = self
                    .publisher
                    .publish_dlq(
                        &message.context.ctx,
                        message.context.key.as_deref(),
                        message.payload.clone(),
                    )
                    .await
                {
                    return Err(ConsumerError::DeadLetter(dlq_err.to_string()));
                }
                return Ok(MessageResult::Ack);
            }
        };

        tracing::trace!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            "Processing MqEnvelope from TOPIC_PUSH_MESSAGES"
        );

        // 2. 验证 payload_kind
        if envelope.payload_kind != MqPayloadKind::Message as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected MESSAGE, sending to DLQ"
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

        // 3. 从 payload oneof 提取 Message
        let proto_message = match &envelope.payload {
            Some(mq_envelope::Payload::Message(m)) => m,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Message payload missing or wrong variant, sending to DLQ"
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

        // 5. 构建 PushMessageRequest
        let req = PushMessageRequest {
            user_ids: envelope.recipient_user_ids.clone(),
            messages: vec![proto_message.clone()],
            options: None,
        };

        // 6. 调用 Application 层
        match self.route_handler.handle_message(ctx, req).await {
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
        "push-message-handler"
    }
}

#[cfg(test)]
mod tests {
    use flare_server_core::error::{ErrorBuilder, ErrorCode};

    use super::*;

    #[test]
    fn retryable_error_uses_dlq_path() {
        let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "mq unavailable").build_error();

        assert!(retry_or_dlq_result(&err).is_none());
    }

    #[test]
    fn non_retryable_error_uses_dlq_path() {
        let err = ErrorBuilder::new(ErrorCode::MessageSendFailed, "bad payload").build_error();

        assert!(retry_or_dlq_result(&err).is_none());
    }
}
