//! 统一推送消费者
//!
//! 消费 TOPIC_PUSH_ENVELOPE，处理统一推送信封（ACK、通知、CustomData、系统消息）。
//!
//! ## 职责
//! 1. 从 MQ 消费 PushEnvelope
//! 2. 解析推送目标（全量/用户/设备）
//! 3. 调用 PushDispatcher 执行推送
//! 4. 处理推送结果（重试、DLQ）

use std::sync::Arc;

use async_trait::async_trait;
use flare_proto::PushEnvelope;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use prost::Message as _;
use tracing::{debug, instrument};

use crate::application::handlers::PushRouterHandler;

/// 统一推送消费者
///
/// ## 设计
/// - 实现 `MessageHandler` trait，由 MQ Consumer 调用
/// - 持有 `PushRouterHandler` 引用，负责实际推送逻辑
/// - 支持上下文传播（从 headers 提取 trace_id/tenant_id）
pub struct PushHandler {
    route_handler: Arc<PushRouterHandler>,
}

impl PushHandler {
    /// 创建推送消费者
    pub fn new(route_handler: Arc<PushRouterHandler>) -> Self {
        Self { route_handler }
    }

    /// 被 NAK 过的信封（`retry_count > 0`）这次处理成功时记一行 INFO：broker 重投后下游已恢复。
    /// 以前只看得出它没进 DLQ。
    fn note_redelivered_success(message: &Message, envelope_id: &str) {
        let retry_count = message.context.retry_count;
        if retry_count > 0 {
            tracing::info!(
                retry_count,
                envelope_id,
                topic = %message.context.topic,
                "PushEnvelope handled after broker redelivery"
            );
        }
    }

    /// 解析 PushEnvelope
    fn parse_envelope(payload: &[u8]) -> std::result::Result<PushEnvelope, ConsumerError> {
        PushEnvelope::decode(payload).map_err(|e| {
            ConsumerError::Deserialization(format!("Failed to decode PushEnvelope: {}", e))
        })
    }
}

#[async_trait]
impl MessageHandler for PushHandler {
    /// 处理推送信封消息
    ///
    /// ## 流程
    /// 1. 解析 PushEnvelope
    /// 2. 提取上下文
    /// 3. 根据 payload_kind 调用对应的处理方法
    /// 4. 处理推送结果
    #[instrument(skip(self, message), fields(topic = %message.context.topic))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        // 1. 解析 PushEnvelope
        let envelope = match Self::parse_envelope(&message.payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    topic = %message.context.topic,
                    "Failed to decode PushEnvelope, terminating poison message"
                );
                return Ok(MessageResult::DeadLetter);
            }
        };

        debug!(
            envelope_id = %envelope.envelope_id,
            payload_kind = ?envelope.payload_kind,
            target_type = ?envelope.target_type,
            "Received PushEnvelope"
        );

        let envelope_id = envelope.envelope_id.clone();
        match self
            .route_handler
            .handle_push_envelope(&message.context.ctx, envelope)
            .await
        {
            Ok(()) => {
                Self::note_redelivered_success(&message, &envelope_id);
                Ok(MessageResult::Ack)
            }
            Err(err) if err.is_retryable() => {
                tracing::warn!(
                    error = %err,
                    topic = %message.context.topic,
                    "Retryable PushEnvelope handling failure, nacking for broker redelivery"
                );
                Ok(MessageResult::Nack)
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    topic = %message.context.topic,
                    "Non-retryable PushEnvelope handling failure"
                );
                Ok(MessageResult::DeadLetter)
            }
        }
    }

    /// 返回消费者名称（用于监控）
    fn name(&self) -> &str {
        "push-handler"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::{AckPayload, PushPayloadKind, PushTargetType};

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// 被 NAK 过的信封重投后处理成功时记一行 INFO；首投成功不记。
    #[test]
    fn a_redelivered_envelope_that_succeeds_is_logged() {
        use flare_server_core::mq::consumer::{ContentType, MessageContext};

        let captured = CapturedLogs::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let text = || String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();

        let mut message = Message::new(
            Vec::new(),
            ContentType::Protobuf,
            MessageContext::new(
                Arc::new(flare_server_core::context::Context::with_request_id(
                    "req-1",
                )),
                "flare.im.push.envelope".to_string(),
            ),
        );
        PushHandler::note_redelivered_success(&message, "env-first");
        assert!(!text().contains("after broker redelivery"), "{}", text());

        message.context.retry_count = 3;
        PushHandler::note_redelivered_success(&message, "env-again");
        let logs = text();
        assert!(
            logs.contains("PushEnvelope handled after broker redelivery"),
            "{logs}"
        );
        assert!(logs.contains("retry_count=3"), "{logs}");
        assert!(logs.contains("envelope_id=\"env-again\""), "{logs}");
    }

    #[test]
    fn test_parse_envelope() {
        let envelope = PushEnvelope {
            envelope_id: "test-123".to_string(),
            tenant_id: "tenant-1".to_string(),
            trace_id: "trace-123".to_string(),
            created_at: 1234567890,
            target_type: PushTargetType::Users as i32,
            target_user_ids: vec!["user-1".to_string()],
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Ack as i32,
            options: None,
            payload: Some(flare_proto::push_envelope::Payload::Ack(AckPayload {
                message_id: "msg-123".to_string(),
                conversation_id: "conv-123".to_string(),
                seq: 100,
                ack_type: "received".to_string(),
                ack_at: 1234567890,
            })),
            headers: std::collections::HashMap::new(),
        };

        let payload = prost::Message::encode_to_vec(&envelope);
        let parsed = PushHandler::parse_envelope(&payload).unwrap();

        assert_eq!(parsed.envelope_id, "test-123");
        assert_eq!(parsed.tenant_id, "tenant-1");
    }
}
