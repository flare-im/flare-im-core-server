//! 读侧领域事件模型（与 proto 解耦）

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

/// 读侧领域事件（用于 query_events / query_message_events）
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
    /// 序列化后的 payload（如 Message 或各操作 Payload 的 bytes），按需解码
    pub payload_bytes: Option<Vec<u8>>,
}
