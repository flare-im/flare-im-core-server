use flare_proto::Message;
use flare_proto::common::Event;
use flare_proto::common::SnapshotConversationRow;

/// 从快照条目中取全局水位（多会话取 max last_conversation_seq）。
pub fn snapshot_global_seq(items: &[SnapshotConversationRow]) -> i64 {
    items
        .iter()
        .map(|i| i.last_conversation_seq as i64)
        .max()
        .unwrap_or(0)
}

pub fn max_seq_from_proto_messages(messages: &[Message]) -> i64 {
    messages
        .iter()
        .map(|m| m.conversation_seq as i64)
        .max()
        .unwrap_or(0)
}

/// 事件列表最大会话 seq（`Event.conversation_seq`）。
pub fn max_seq_from_events(events: &[Event]) -> i64 {
    events
        .iter()
        .map(|e| e.conversation_seq as i64)
        .max()
        .unwrap_or(0)
}
