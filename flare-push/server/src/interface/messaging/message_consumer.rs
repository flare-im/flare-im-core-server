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
use prost::Message as _;
use tracing::instrument;

use flare_server_core::error::FlareError;

use super::acknowledge_after_dlq;
use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

fn retry_or_dlq_result(_error: &FlareError) -> Option<MessageResult> {
    None
}

/// 一条 `TOPIC_PUSH_MESSAGES` 记录的解码结果。
#[derive(Debug)]
pub struct DecodedPushMessage {
    /// 信封元数据；其 `payload` 已移入 `request.messages`。
    pub envelope: MqEnvelope,
    /// 交给 `PushRouterHandler` 的推送请求。
    pub request: PushMessageRequest,
}

/// 记录不符合 `TOPIC_PUSH_MESSAGES` 线格式的原因；此类记录进入 DLQ。
#[derive(Debug)]
pub enum PushMessageRecordError {
    /// 字节不是 protobuf `MqEnvelope`。
    Undecodable(String),
    /// `payload_kind` 不是 MESSAGE。
    UnexpectedPayloadKind {
        envelope_id: String,
        conversation_id: String,
        payload_kind: i32,
    },
    /// `payload` 缺失或不是 `Message`。
    MissingMessage {
        envelope_id: String,
        conversation_id: String,
    },
}

impl PushMessageRecordError {
    /// DLQ 分区键：信封可解码时用会话 ID，否则沿用原记录键。
    fn dlq_key<'a>(&'a self, record_key: Option<&'a str>) -> Option<&'a str> {
        match self {
            Self::Undecodable(_) => record_key,
            Self::UnexpectedPayloadKind {
                conversation_id, ..
            }
            | Self::MissingMessage {
                conversation_id, ..
            } => Some(conversation_id),
        }
    }
}

/// 按 `TOPIC_PUSH_MESSAGES` 线格式解码一条记录。
///
/// 线格式为 protobuf `MqEnvelope`：`payload_kind = MESSAGE`，载荷为 `Message`，接收者取
/// `recipient_user_ids`。Push Proxy 与 message pipeline 的 push-only 记录都按此发布。
pub fn decode_push_message_record(
    payload: &[u8],
) -> Result<DecodedPushMessage, PushMessageRecordError> {
    let mut envelope = MqEnvelope::decode(payload)
        .map_err(|error| PushMessageRecordError::Undecodable(error.to_string()))?;
    if envelope.payload_kind != MqPayloadKind::Message as i32 {
        return Err(PushMessageRecordError::UnexpectedPayloadKind {
            envelope_id: envelope.envelope_id,
            conversation_id: envelope.conversation_id,
            payload_kind: envelope.payload_kind,
        });
    }
    let Some(mq_envelope::Payload::Message(message)) = envelope.payload.take() else {
        return Err(PushMessageRecordError::MissingMessage {
            envelope_id: envelope.envelope_id,
            conversation_id: envelope.conversation_id,
        });
    };
    let request = PushMessageRequest {
        user_ids: envelope.recipient_user_ids.clone(),
        messages: vec![message],
        options: None,
    };
    Ok(DecodedPushMessage { envelope, request })
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
        let ctx = &message.context.ctx;

        // 1-3. 解码 MqEnvelope、校验 payload_kind 并取出 Message
        let DecodedPushMessage { envelope, request } =
            match decode_push_message_record(&message.payload) {
                Ok(decoded) => decoded,
                Err(error) => {
                    tracing::error!(
                        error = ?error,
                        topic = %message.context.topic,
                        "Push message record rejected, sending to DLQ"
                    );
                    return acknowledge_after_dlq(
                        self.publisher
                            .publish_dlq(
                                ctx,
                                error.dlq_key(message.context.key.as_deref()),
                                message.payload.clone(),
                            )
                            .await,
                    );
                }
            };

        tracing::trace!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            "Processing MqEnvelope from TOPIC_PUSH_MESSAGES"
        );

        // 4-6. 上下文来自 MQ 记录头；调用 Application 层
        match self.route_handler.handle_message(ctx, request).await {
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
                acknowledge_after_dlq(
                    self.publisher
                        .publish_dlq(
                            ctx,
                            Some(&envelope.conversation_id),
                            message.payload.clone(),
                        )
                        .await,
                )
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

    fn envelope(kind: MqPayloadKind, payload: Option<mq_envelope::Payload>) -> MqEnvelope {
        MqEnvelope {
            envelope_id: "env-1".to_string(),
            recipient_user_ids: vec!["user-1".to_string()],
            conversation_id: "conv-1".to_string(),
            seq: 3,
            payload_kind: kind as i32,
            payload,
            ..MqEnvelope::default()
        }
    }

    #[test]
    fn message_envelope_decodes_to_push_request() {
        let message = flare_proto::common::Message {
            server_id: "srv-1".to_string(),
            conversation_id: "conv-1".to_string(),
            ..Default::default()
        };
        let bytes = envelope(
            MqPayloadKind::Message,
            Some(mq_envelope::Payload::Message(message.clone())),
        )
        .encode_to_vec();

        let decoded = decode_push_message_record(&bytes).unwrap();

        assert_eq!(decoded.request.user_ids, vec!["user-1".to_string()]);
        assert_eq!(decoded.request.messages, vec![message]);
        assert_eq!(decoded.envelope.conversation_id, "conv-1");
        assert!(decoded.envelope.payload.is_none());
    }

    #[test]
    fn non_message_records_are_rejected_with_dlq_key() {
        let event = envelope(
            MqPayloadKind::Event,
            Some(mq_envelope::Payload::Event(Default::default())),
        );
        let missing = envelope(MqPayloadKind::Message, None);
        for bytes in [event.encode_to_vec(), missing.encode_to_vec()] {
            let error = decode_push_message_record(&bytes).unwrap_err();
            assert_eq!(error.dlq_key(Some("record-key")), Some("conv-1"));
        }

        let error = decode_push_message_record(&[0xff, 0xff]).unwrap_err();
        assert!(matches!(error, PushMessageRecordError::Undecodable(_)));
        assert_eq!(error.dlq_key(Some("record-key")), Some("record-key"));
    }

    #[test]
    fn non_retryable_error_uses_dlq_path() {
        let err = ErrorBuilder::new(ErrorCode::MessageSendFailed, "bad payload").build_error();

        assert!(retry_or_dlq_result(&err).is_none());
    }
}
