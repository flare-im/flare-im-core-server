//! 操作事件派发器实现：Kafka Event + Push 一次派发

use std::sync::Arc;

use flare_proto::common::{Message as ProtoMessage, MessageStatus};
use flare_proto::push::{PushMessageRequest, PushOptions};
use flare_server_core::context::Ctx;

use crate::domain::event::MessageOperationDomainEvent;
use crate::domain::repository::MessageEventPublisher;
use crate::domain::service::operation_event_dispatcher::OperationEventDispatcher;
use crate::error::{to_system_err_with, Result};

/// 操作事件派发器实现：持有 MessageEventPublisher，派发时先写 Kafka Event 再写 Push
pub struct OperationEventDispatcherImpl {
    publisher: Arc<dyn MessageEventPublisher>,
}

impl OperationEventDispatcherImpl {
    pub fn new(publisher: Arc<dyn MessageEventPublisher>) -> Self {
        Self { publisher }
    }
}

impl OperationEventDispatcher for OperationEventDispatcherImpl {
    async fn dispatch(
        &self,
        ctx: &Ctx,
        proto_event: flare_proto::common::Event,
        domain_event: MessageOperationDomainEvent,
    ) -> Result<()> {
        self.publisher
            .publish_event(ctx, proto_event)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "publish_event failed in dispatch");
                to_system_err_with(e, "Failed to publish operation event to Kafka")
            })?;

        if let Some(push_req) = build_push_from_domain_event(&domain_event) {
            self.publisher
                .publish_push(ctx, push_req)
                .await
                .map_err(|e| to_system_err_with(e, "Failed to publish push for operation"))?;
        }

        Ok(())
    }

    async fn dispatch_event_only(&self, ctx: &Ctx, proto_event: flare_proto::common::Event) -> Result<()> {
        self.publisher
            .publish_event(ctx, proto_event)
            .await
            .map_err(|e| to_system_err_with(e, "Failed to publish operation event to Kafka"))
    }
}

/// 从领域事件基类填充 ProtoMessage 的公共字段
fn base_to_proto_message(base: &crate::domain::event::MessageOperationEvent) -> ProtoMessage {
    let mut m = ProtoMessage::default();
    m.server_id = base.message_id.clone();
    m.conversation_id = base.conversation_id.clone();
    m.sender_id = base.operator_id.clone();
    m.status = MessageStatus::Sent as i32;
    m
}

/// 统一构建 Push 请求壳，避免重复
fn push_request(message: ProtoMessage, persist_if_offline: bool, priority: i32) -> PushMessageRequest {
    PushMessageRequest {
        user_ids: vec![],
        message: Some(message),
        options: Some(PushOptions { persist_if_offline, priority, ..Default::default() }),
        metadata: Default::default(),
    }
}

fn build_push_from_domain_event(event: &MessageOperationDomainEvent) -> Option<PushMessageRequest> {
    use MessageOperationDomainEvent::*;

    let push = match event {
        Recalled(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.status = MessageStatus::Recalled as i32;
            push_request(proto_msg, true, 1)
        }
        Edited(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("current_edit_version".to_string(), e.edit_version.to_string());
            proto_msg.extra.insert("last_edited_at".to_string(), chrono::Utc::now().to_rfc3339());
            push_request(proto_msg, true, 1)
        }
        Deleted(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("deleted_at".to_string(), chrono::Utc::now().to_rfc3339());
            proto_msg.extra.insert("delete_type".to_string(), e.delete_type.clone());
            proto_msg.extra.insert("is_deleted".to_string(), "true".to_string());
            push_request(proto_msg, true, 1)
        }
        Read(_) | Favorited(_) | Unfavorited(_) => return None,
        ReactionAdded(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("reaction_emoji".to_string(), e.emoji.clone());
            proto_msg.extra.insert("reaction_action".to_string(), "added".to_string());
            proto_msg.extra.insert("reaction_operator".to_string(), e.base.operator_id.clone());
            push_request(proto_msg, false, 0)
        }
        ReactionRemoved(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("reaction_emoji".to_string(), e.emoji.clone());
            proto_msg.extra.insert("reaction_action".to_string(), "removed".to_string());
            proto_msg.extra.insert("reaction_operator".to_string(), e.base.operator_id.clone());
            push_request(proto_msg, false, 0)
        }
        Pinned(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("operation".to_string(), "pinned".to_string());
            proto_msg.extra.insert("pinned_by".to_string(), e.base.operator_id.clone());
            push_request(proto_msg, false, 1)
        }
        Unpinned(e) => {
            let mut proto_msg = base_to_proto_message(&e.base);
            proto_msg.extra.insert("operation".to_string(), "unpinned".to_string());
            proto_msg.extra.insert("unpinned_by".to_string(), e.base.operator_id.clone());
            push_request(proto_msg, false, 1)
        }
    };
    Some(push)
}
