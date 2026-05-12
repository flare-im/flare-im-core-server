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
use flare_im_core::Ctx;
use flare_proto::common::PushEnvelope;
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
    #[allow(dead_code)]
    route_handler: Arc<PushRouterHandler>,
}

impl PushHandler {
    /// 创建推送消费者
    pub fn new(route_handler: Arc<PushRouterHandler>) -> Self {
        Self { route_handler }
    }

    /// 从 MQ headers 提取上下文
    fn extract_ctx_from_headers(headers: &std::collections::HashMap<String, String>) -> Ctx {
        let trace_id = headers
            .get("x-trace-id")
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let _tenant_id = headers.get("x-tenant-id").cloned();

        Arc::new(flare_server_core::context::Context::with_request_id(
            trace_id,
        ))
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
        let envelope = Self::parse_envelope(&message.payload)?;

        debug!(
            envelope_id = %envelope.envelope_id,
            payload_kind = ?envelope.payload_kind,
            target_type = ?envelope.target_type,
            "Received PushEnvelope"
        );

        // 2. 提取上下文
        let ctx = Self::extract_ctx_from_headers(&message.context.headers);

        // 3. 根据 payload_kind 处理
        // TODO: 实现具体的推送逻辑
        let _ = (ctx, envelope);

        Ok(MessageResult::Ack)
    }

    /// 返回消费者名称（用于监控）
    fn name(&self) -> &str {
        "push-handler"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{AckPayload, PushPayloadKind, PushTargetType};

    #[test]
    fn test_parse_envelope() {
        let envelope = PushEnvelope {
            envelope_id: "test-123".to_string(),
            tenant_id: "tenant-1".to_string(),
            trace_id: "trace-123".to_string(),
            created_at_ms: 1234567890,
            target_type: PushTargetType::Users as i32,
            target_user_ids: vec!["user-1".to_string()],
            target_device_ids: Vec::new(),
            payload_kind: PushPayloadKind::Ack as i32,
            options: None,
            payload: Some(flare_proto::common::push_envelope::Payload::Ack(
                AckPayload {
                    message_id: "msg-123".to_string(),
                    conversation_id: "conv-123".to_string(),
                    seq: 100,
                    ack_type: "received".to_string(),
                    ack_at_ms: 1234567890,
                },
            )),
            headers: std::collections::HashMap::new(),
        };

        let payload = prost::Message::encode_to_vec(&envelope);
        let parsed = PushHandler::parse_envelope(&payload).unwrap();

        assert_eq!(parsed.envelope_id, "test-123");
        assert_eq!(parsed.tenant_id, "tenant-1");
    }
}
