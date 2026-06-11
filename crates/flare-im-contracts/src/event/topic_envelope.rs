//! IM topic event constants and shared envelope builders.
//!
//! Topic 名请使用 `crate::constants::topics`。

use std::collections::HashMap;

use flare_proto::common::EventType;

/// 异步会话创建事件类型；MQ 载荷使用 protobuf `MqEnvelope(EventCustom)`。
pub const EVENT_TYPE_OPERATION_CONVERSATION_ENSURE: &str = "operation.conversation_ensure";
pub const EVENT_TYPE_CONVERSATION_ENSURE: &str = "conversation.ensure";
pub const EVENT_TYPE_MESSAGE_CREATED: &str = "message.created";
pub const EVENT_TYPE_OPERATION_RECALLED: &str = "operation.recalled";
pub const EVENT_TYPE_OPERATION_EDITED: &str = "operation.edited";
pub const EVENT_TYPE_OPERATION_DELETED: &str = "operation.deleted";
pub const EVENT_TYPE_OPERATION_READ_RECEIPT: &str = "operation.read_receipt";
pub const EVENT_TYPE_OPERATION_REACTION: &str = "operation.reaction";
pub const EVENT_TYPE_OPERATION_PIN: &str = "operation.pin";
pub const EVENT_TYPE_OPERATION_UNPIN: &str = "operation.unpin";
pub const EVENT_TYPE_OPERATION_MARK: &str = "operation.mark";
pub const EVENT_TYPE_OPERATION_UNMARK: &str = "operation.unmark";

pub const CONVERSATION_UPDATE_TYPE_UNREAD: &str = "unread";
pub const CONVERSATION_UPDATE_TYPE_SUMMARY: &str = "summary";
pub const CONVERSATION_UPDATE_TYPE_REMOVE: &str = "remove";

pub fn event_type_str_from_proto_event(event: &flare_proto::common::Event) -> Option<&'static str> {
    match EventType::try_from(event.r#type).ok()? {
        EventType::EventMessageRecall => Some(EVENT_TYPE_OPERATION_RECALLED),
        EventType::EventMessageEdit => Some(EVENT_TYPE_OPERATION_EDITED),
        EventType::EventMessageDelete => Some(EVENT_TYPE_OPERATION_DELETED),
        EventType::EventReadReceipt => Some(EVENT_TYPE_OPERATION_READ_RECEIPT),
        EventType::EventReaction => Some(EVENT_TYPE_OPERATION_REACTION),
        EventType::EventPin => Some(EVENT_TYPE_OPERATION_PIN),
        EventType::EventUnpin => Some(EVENT_TYPE_OPERATION_UNPIN),
        EventType::EventMark => Some(EVENT_TYPE_OPERATION_MARK),
        EventType::EventUnmark => Some(EVENT_TYPE_OPERATION_UNMARK),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn conversation_update_envelope(
    user_id: impl Into<String>,
    conversation_id: impl Into<String>,
    tenant_id: impl Into<String>,
    update_type: impl Into<String>,
    max_seq: u64,
    last_read_seq: u64,
    summary_snapshot: Vec<u8>,
    metadata: HashMap<String, String>,
    updated_at_ms: i64,
) -> flare_proto::common::ConversationUpdateEnvelope {
    flare_proto::common::ConversationUpdateEnvelope {
        user_id: user_id.into(),
        conversation_id: conversation_id.into(),
        tenant_id: tenant_id.into(),
        update_type: update_type.into(),
        max_seq,
        last_read_seq,
        summary_snapshot,
        attributes: metadata,
        updated_at: updated_at_ms,
    }
}
