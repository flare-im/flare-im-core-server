//! 客户端 ACK 上行处理：显式已读回执进入标准 IM 事件流。
//!
//! PushAck 与普通 Conversation delivery ACK 仅记录/跳过；ReadAck / Batch.read_acks 转换为
//! `EVENT_READ_RECEIPT`，由 orchestrator fanout，并由 conversation 的 ReadReceipt consumer
//! 统一更新已读游标。这样避免 route 维护第二套会话写路径。

use std::sync::Arc;

use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;
use flare_proto::common::{
    ConversationAck, Event, EventType, PushAck, ReadAck, ReadReceiptEvent,
    event::Payload as EventPayload,
};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::domain::repository::NoopRouteRepository;
use crate::infrastructure::forwarder::{MessageForwarder, svid};
use flare_server_core::error::{Result, require_user_id};

/// 客户端 ACK 转发器：显式会话已读 → ReadReceiptEvent；PushAck / delivery ACK → 占位日志。
pub struct AckToPushProxyForwarder {
    message_forwarder: Arc<MessageForwarder>,
}

impl AckToPushProxyForwarder {
    pub fn new(message_forwarder: Arc<MessageForwarder>) -> Arc<Self> {
        Arc::new(Self { message_forwarder })
    }

    /// 处理客户端上行 ACK。
    pub async fn forward_client_ack(&self, ctx: &Context, ack: Ack) -> Result<()> {
        match ack.payload {
            Some(AckPayload::Conversation(conversation_ack)) => {
                self.log_conversation_ack_skip(ctx, &conversation_ack);
            }
            Some(AckPayload::Read(read_ack)) => {
                self.apply_read_ack(ctx, &read_ack).await?;
            }
            Some(AckPayload::Batch(batch)) => {
                for push in batch.push_acks {
                    self.log_push_ack_skip(ctx, &push);
                }
                for conversation_ack in batch.conversation_acks {
                    self.log_conversation_ack_skip(ctx, &conversation_ack);
                }
                for read_ack in batch.read_acks {
                    if let Err(error) = self.apply_read_ack(ctx, &read_ack).await {
                        warn!(
                            request_id = %ctx.request_id(),
                            conversation_id = %read_ack.conversation_id,
                            %error,
                            "batch read ack mark read failed"
                        );
                    }
                }
            }
            Some(AckPayload::Push(push_ack)) => {
                self.log_push_ack_skip(ctx, &push_ack);
            }
            _ => {
                debug!(
                    request_id = %ctx.request_id(),
                    ack_payload = ack_payload_name(ack.payload.as_ref()),
                    "skip client ack: unsupported payload for conversation read"
                );
            }
        }
        Ok(())
    }

    fn log_push_ack_skip(&self, ctx: &Context, push_ack: &PushAck) {
        debug!(
            request_id = %ctx.request_id(),
            user_id = ctx.user_id().unwrap_or_default(),
            window_id = %push_ack.window_id,
            ack_seq = push_ack.ack_seq,
            "skip push ack forward: PushAck RPC removed"
        );
    }

    fn log_conversation_ack_skip(&self, ctx: &Context, ack: &ConversationAck) {
        debug!(
            request_id = %ctx.request_id(),
            user_id = ctx.user_id().unwrap_or_default(),
            conversation_id = %ack.conversation_id,
            delivered_seq = ack.last_delivered_seq,
            "skip conversation ack forward: delivery ack does not update read position"
        );
    }

    async fn apply_read_ack(&self, ctx: &Context, ack: &ReadAck) -> Result<()> {
        let conversation_id = ack.conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(());
        }

        let Some(read_seq) = read_seq_from_read_ack(ack) else {
            debug!(
                request_id = %ctx.request_id(),
                user_id = ctx.user_id().unwrap_or_default(),
                conversation_id = %conversation_id,
                "skip read ack: empty read seq"
            );
            return Ok(());
        };

        let user_id = require_user_id(ctx)?;

        let event = build_read_receipt_event(ctx, conversation_id, &user_id, read_seq);
        self.message_forwarder
            .forward_event(ctx, svid::IM, &event, Arc::new(NoopRouteRepository))
            .await
            .map_err(|error| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    format!("read receipt event forwarding failed: {error}"),
                )
                .build_error()
            })?;

        info!(
            request_id = %ctx.request_id(),
            user_id = %user_id,
            conversation_id = %conversation_id,
            read_seq,
            event_id = %event.event_id,
            "client read ack routed as read receipt event"
        );
        Ok(())
    }
}

fn ack_payload_name(payload: Option<&AckPayload>) -> &'static str {
    match payload {
        Some(AckPayload::Send(_)) => "send",
        Some(AckPayload::Event(_)) => "event",
        Some(AckPayload::Push(_)) => "push",
        Some(AckPayload::Conversation(_)) => "conversation",
        Some(AckPayload::Read(_)) => "read",
        Some(AckPayload::Batch(_)) => "batch",
        None => "none",
    }
}

fn read_seq_from_read_ack(ack: &ReadAck) -> Option<u64> {
    if ack.read_seq == 0 {
        return None;
    }
    Some(ack.read_seq)
}

fn build_read_receipt_event(
    ctx: &Context,
    conversation_id: &str,
    user_id: &str,
    read_seq: u64,
) -> Event {
    let now_ms = chrono::Utc::now().timestamp_millis();
    Event {
        conversation_id: conversation_id.to_string(),
        conversation_seq: 0,
        r#type: EventType::EventReadReceipt as i32,
        created_at: now_ms,
        event_id: Uuid::new_v4().to_string(),
        request_id: Some(ctx.request_id().to_string()),
        payload: Some(EventPayload::Read(ReadReceiptEvent {
            conversation_id: conversation_id.to_string(),
            read_seq,
            user_id: user_id.to_string(),
            message_ids: Vec::new(),
            read_at: Some(now_ms),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_ack(read_seq: u64) -> flare_proto::common::ReadAck {
        flare_proto::common::ReadAck {
            conversation_id: "c1".to_string(),
            read_seq,
            device_id: Some("device-1".to_string()),
            ack_id: Some("ack-read-1".to_string()),
        }
    }

    #[test]
    fn typed_read_ack_uses_read_seq() {
        assert_eq!(read_seq_from_read_ack(&read_ack(99)), Some(99));
    }

    #[test]
    fn typed_read_ack_with_zero_seq_is_ignored() {
        assert_eq!(read_seq_from_read_ack(&read_ack(0)), None);
    }

    #[test]
    fn builds_canonical_read_receipt_event() {
        let ctx = Context::with_request_id("req-read-1");
        let event = build_read_receipt_event(&ctx, "c1", "reader", 42);

        assert_eq!(event.conversation_id, "c1");
        assert_eq!(event.r#type, EventType::EventReadReceipt as i32);
        assert_eq!(event.request_id.as_deref(), Some("req-read-1"));

        let Some(flare_proto::common::event::Payload::Read(read)) = event.payload else {
            panic!("expected read receipt payload");
        };
        assert_eq!(read.conversation_id, "c1");
        assert_eq!(read.user_id, "reader");
        assert_eq!(read.read_seq, 42);
        assert!(read.read_at.is_some());
    }
}
