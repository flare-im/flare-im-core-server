//! 写侧领域事件模型（与 proto 解耦）

use std::collections::HashMap;

/// 事件类型（与 common/event.proto EventType 语义对齐）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EventType {
    Unspecified = 0,
    Message = 1,
    MessageRecall = 2,
    MessageEdit = 3,
    MessageDelete = 4,
    ReadReceipt = 5,
    Typing = 6,
    ConversationUpdate = 7,
    ConversationDelete = 8,
    Presence = 9,
    CallSignal = 10,
    Reaction = 11,
    Pin = 12,
    Unpin = 13,
    Mark = 14,
    Unmark = 15,
    Custom = 99,
}

/// 写侧领域事件
#[derive(Debug, Clone)]
pub struct Event {
    pub tenant_id: String,
    pub conversation_id: String,
    pub seq: u64,
    pub r#type: EventType,
    pub created_at: Option<prost_types::Timestamp>,
    pub operator_id: String,
    pub event_seq: Option<u64>,
    pub request_id: Option<String>,
    pub payload: Option<EventPayload>,
}

#[derive(Debug, Clone)]
pub enum EventPayload {
    Recall(RecallPayload),
    Edit(EditPayload),
    Delete(DeletePayload),
    Read(ReadPayload),
    Reaction(ReactionPayload),
    Pin(PinPayload),
    Unpin(UnpinPayload),
    Mark(MarkPayload),
    Unmark(UnmarkPayload),
    Message(super::Message),
    Custom(CustomPayload),
    Other,
}

#[derive(Debug, Clone)]
pub struct RecallPayload {
    pub server_msg_id: String,
    pub reason: String,
    pub time_limit_seconds: Option<i32>,
    pub allow_admin_recall: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct EditPayload {
    pub server_msg_id: String,
    pub new_content: Vec<u8>,
    pub edit_version: i32,
    pub reason: String,
    pub show_edited_mark: bool,
}

#[derive(Debug, Clone)]
pub struct DeletePayload {
    pub server_msg_id: String,
    pub delete_type: Option<i32>,
    pub scope: Option<i32>,
    pub target_user_id: Option<String>,
    pub reason: Option<String>,
    pub notify_others: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ReadPayload {
    pub conversation_id: String,
    pub read_seq: u64,
    pub user_id: String,
    pub message_ids: Vec<String>,
    pub read_at: Option<prost_types::Timestamp>,
    pub burn_after_read: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ReactionPayload {
    pub server_msg_id: String,
    pub user_id: String,
    pub emoji: String,
    pub action: i32, // ReactionAction
}

#[derive(Debug, Clone)]
pub struct PinPayload {
    pub server_msg_id: String,
    pub pinned_by: String,
    pub reason: Option<String>,
    pub expire_at: Option<prost_types::Timestamp>,
}

#[derive(Debug, Clone)]
pub struct UnpinPayload {
    pub server_msg_id: String,
}

#[derive(Debug, Clone)]
pub struct MarkPayload {
    pub server_msg_id: String,
    pub user_id: String,
    pub mark_type: i32,
    pub color: String,
}

#[derive(Debug, Clone)]
pub struct UnmarkPayload {
    pub server_msg_id: String,
    pub user_id: String,
    pub mark_type: i32,
}

#[derive(Debug, Clone)]
pub struct CustomPayload {
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub payload: Vec<u8>,
    pub metadata: HashMap<String, String>,
}
