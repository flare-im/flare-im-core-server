//! Proto 与领域模型互转（**仅用于 interface 层**，application/domain/infrastructure 不依赖 proto）
//! Message 与 proto 互转统一使用 flare_im_contracts::message

use crate::domain::model::{
    EditHistoryEntry, Event, EventType, MarkEntry, ReactionItem, ReadListEntry, VisibilityStatus,
};
use chrono::{DateTime, Utc};
use flare_im_contracts::utils::{millis_to_timestamp, timestamp_to_millis};
use flare_proto::common::{EventType as ProtoEventType, MessageContent};
use prost::Message as ProstMessage;

/// DateTime<Utc> -> Timestamp
pub fn datetime_to_timestamp(dt: Option<DateTime<Utc>>) -> Option<prost_types::Timestamp> {
    dt.map(|dt| prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

/// Timestamp -> DateTime<Utc>
pub fn timestamp_to_datetime(ts: Option<prost_types::Timestamp>) -> Option<DateTime<Utc>> {
    ts.and_then(|ts| DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
}

fn datetime_to_millis(dt: Option<DateTime<Utc>>) -> i64 {
    dt.map(|dt| dt.timestamp_millis()).unwrap_or_default()
}

fn millis_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    if ms <= 0 {
        return None;
    }
    DateTime::from_timestamp_millis(ms)
}

/// 统一从 flare-im-core 重导出
pub use flare_im_contracts::message::{message_from_proto, message_into_proto, message_to_proto};

/// 从 proto Event 转为领域 Event（整条 event 序列化存于 payload_bytes；operator_id 由 metadata 注入，此处填空）
pub fn event_from_proto(p: &flare_proto::common::Event) -> Event {
    let r#type = event_type_from_proto_i32(p.r#type);
    let payload_bytes = p.encode_to_vec();
    Event {
        tenant_id: String::new(),
        conversation_id: p.conversation_id.clone(),
        seq: p.conversation_seq,
        r#type,
        created_at: millis_to_timestamp(p.created_at),
        operator_id: String::new(),
        event_seq: None,
        request_id: p.request_id.clone(),
        payload_bytes: Some(payload_bytes),
    }
}

/// 从领域 Event 转为 proto Event（用于 gRPC 响应；从 payload_bytes 反序列化整条 Event）
pub fn event_to_proto(e: &Event) -> Option<flare_proto::common::Event> {
    e.payload_bytes
        .as_ref()
        .and_then(|b| flare_proto::common::Event::decode(b.as_slice()).ok())
}

/// 领域 Event 转 proto；解码失败时返回空 Event 占位（proto 无 operator_id）
pub fn event_to_proto_or_default(e: &Event) -> flare_proto::common::Event {
    event_to_proto(e).unwrap_or_else(|| flare_proto::common::Event {
        conversation_id: e.conversation_id.clone(),
        conversation_seq: e.seq,
        r#type: event_type_to_proto_i32(e.r#type),
        created_at: e
            .created_at
            .as_ref()
            .and_then(timestamp_to_millis)
            .unwrap_or_default(),
        event_id: format!("{}:{}", e.conversation_id, e.seq),
        request_id: e.request_id.clone(),
        ..Default::default()
    })
}

pub fn event_type_from_proto_i32(v: i32) -> EventType {
    match ProtoEventType::try_from(v) {
        Ok(ProtoEventType::EventMessage) => EventType::Message,
        Ok(ProtoEventType::EventMessageRecall) => EventType::MessageRecall,
        Ok(ProtoEventType::EventMessageEdit) => EventType::MessageEdit,
        Ok(ProtoEventType::EventMessageDelete) => EventType::MessageDelete,
        Ok(ProtoEventType::EventReadReceipt) => EventType::ReadReceipt,
        Ok(ProtoEventType::EventConversationUpdate) => EventType::ConversationUpdate,
        Ok(ProtoEventType::EventConversationDelete) => EventType::ConversationDelete,
        Ok(ProtoEventType::EventReaction) => EventType::Reaction,
        Ok(ProtoEventType::EventPin) => EventType::Pin,
        Ok(ProtoEventType::EventUnpin) => EventType::Unpin,
        Ok(ProtoEventType::EventMark) => EventType::Mark,
        Ok(ProtoEventType::EventUnmark) => EventType::Unmark,
        Ok(ProtoEventType::EventMessageRetentionScheduled) => EventType::MessageBurnScheduled,
        Ok(ProtoEventType::EventMessageRetentionExpired) => EventType::MessageBurned,
        Ok(ProtoEventType::EventMessageRetentionPurged) => EventType::MessageHardDeleted,
        Ok(ProtoEventType::EventCustom) => EventType::Custom,
        _ => EventType::Unspecified,
    }
}

pub fn event_type_to_proto_i32(v: EventType) -> i32 {
    match v {
        EventType::Message => ProtoEventType::EventMessage as i32,
        EventType::MessageRecall => ProtoEventType::EventMessageRecall as i32,
        EventType::MessageEdit => ProtoEventType::EventMessageEdit as i32,
        EventType::MessageDelete => ProtoEventType::EventMessageDelete as i32,
        EventType::ReadReceipt => ProtoEventType::EventReadReceipt as i32,
        EventType::ConversationUpdate => ProtoEventType::EventConversationUpdate as i32,
        EventType::ConversationDelete => ProtoEventType::EventConversationDelete as i32,
        EventType::Reaction => ProtoEventType::EventReaction as i32,
        EventType::Pin => ProtoEventType::EventPin as i32,
        EventType::Unpin => ProtoEventType::EventUnpin as i32,
        EventType::Mark => ProtoEventType::EventMark as i32,
        EventType::Unmark => ProtoEventType::EventUnmark as i32,
        EventType::MessageBurnScheduled => ProtoEventType::EventMessageRetentionScheduled as i32,
        EventType::MessageBurned => ProtoEventType::EventMessageRetentionExpired as i32,
        EventType::MessageHardDeleted => ProtoEventType::EventMessageRetentionPurged as i32,
        EventType::Custom => ProtoEventType::EventCustom as i32,
        EventType::Typing
        | EventType::Presence
        | EventType::CallSignal
        | EventType::Unspecified => ProtoEventType::Unspecified as i32,
    }
}

// ---------- Storage DTOs -> Proto（gRPC 响应） ----------

/// 领域编辑历史条目 -> proto MessageEditHistoryEntry
pub fn edit_history_entry_to_proto(
    e: &EditHistoryEntry,
) -> flare_grpc_proto::storage::MessageEditHistoryEntry {
    let content = MessageContent::decode(e.content_bytes.as_slice()).ok();
    flare_grpc_proto::storage::MessageEditHistoryEntry {
        edit_version: e.edit_version,
        content,
        edited_at: datetime_to_timestamp(e.edited_at),
        editor_id: e.editor_id.clone(),
        reason: e.reason.clone().unwrap_or_default(),
        show_edited_mark: e.show_edited_mark,
    }
}

/// 领域已读条目 -> proto MessageReadListEntry
pub fn read_list_entry_to_proto(
    e: &ReadListEntry,
) -> flare_grpc_proto::storage::MessageReadListEntry {
    flare_grpc_proto::storage::MessageReadListEntry {
        user_id: e.user_id.clone(),
        read_at: datetime_to_timestamp(e.read_at),
        burned_at: datetime_to_timestamp(e.burned_at),
    }
}

/// 领域标记条目 -> proto MessageMarkEntry
pub fn mark_entry_to_proto(e: &MarkEntry) -> flare_grpc_proto::storage::MessageMarkEntry {
    flare_grpc_proto::storage::MessageMarkEntry {
        user_id: e.user_id.clone(),
        mark_type: e.mark_type,
        color: e.color.as_deref().unwrap_or("").to_string(),
        marked_at: datetime_to_timestamp(e.marked_at),
    }
}

/// 领域反应条目 -> proto MessageReactionItem
pub fn reaction_item_to_proto(r: &ReactionItem) -> flare_grpc_proto::storage::MessageReactionItem {
    flare_grpc_proto::storage::MessageReactionItem {
        emoji: r.emoji.clone(),
        user_ids: r.user_ids.clone(),
        count: r.count,
        last_updated: datetime_to_timestamp(r.last_updated),
    }
}

// ---------- Proto -> 领域（用于 MessageUpdate 等从外部入参） ----------

pub fn visibility_status_from_proto(v: i32) -> VisibilityStatus {
    match flare_grpc_proto::VisibilityStatus::try_from(v) {
        Ok(flare_grpc_proto::VisibilityStatus::Visible) => VisibilityStatus::Visible,
        Ok(flare_grpc_proto::VisibilityStatus::Hidden) => VisibilityStatus::Hidden,
        Ok(flare_grpc_proto::VisibilityStatus::Deleted) => VisibilityStatus::Deleted,
        _ => VisibilityStatus::Visible,
    }
}

pub fn visibility_status_to_proto(v: VisibilityStatus) -> i32 {
    v as i32
}

/// 从 proto MessageReadRecord 转为领域 ReadListEntry
pub fn read_list_entry_from_proto(p: &flare_proto::common::MessageReadRecord) -> ReadListEntry {
    ReadListEntry {
        user_id: p.user_id.clone(),
        read_at: millis_to_datetime(p.read_at),
        burned_at: p.retention_expired_at.and_then(millis_to_datetime),
    }
}

/// 从 proto Reaction 转为领域 ReactionItem
pub fn reaction_item_from_proto(p: &flare_proto::common::Reaction) -> ReactionItem {
    ReactionItem {
        emoji: p.emoji.clone(),
        user_ids: p.user_ids.clone(),
        count: p.count,
        last_updated: millis_to_datetime(p.updated_at),
    }
}

/// 领域 ReadListEntry -> proto MessageReadRecord（供 repository 写 DB 使用）
pub fn read_list_entry_to_common_proto(
    e: &ReadListEntry,
) -> flare_proto::common::MessageReadRecord {
    flare_proto::common::MessageReadRecord {
        user_id: e.user_id.clone(),
        read_at: datetime_to_millis(e.read_at),
        retention_expired_at: e.burned_at.map(|dt| dt.timestamp_millis()),
    }
}

/// 领域 ReactionItem -> proto Reaction（供 repository 写 DB 使用）
pub fn reaction_item_to_common_proto(r: &ReactionItem) -> flare_proto::common::Reaction {
    flare_proto::common::Reaction {
        emoji: r.emoji.clone(),
        user_ids: r.user_ids.clone(),
        count: r.count,
        updated_at: datetime_to_millis(r.last_updated),
        created_at: 0,
    }
}

/// Proto FilterExpression -> 领域 FilterExpression
pub fn filter_expression_from_proto(
    p: &flare_proto::common::FilterExpression,
) -> crate::domain::model::FilterExpression {
    crate::domain::model::FilterExpression {
        field: p.field.clone(),
        operator: format!(
            "{:?}",
            flare_proto::common::FilterOperator::try_from(p.op)
                .unwrap_or(flare_proto::common::FilterOperator::Eq)
        ),
        value: p.values.join(","),
    }
}

/// 领域 FilterExpression -> Proto FilterExpression
pub fn filter_expression_to_proto(
    d: &crate::domain::model::FilterExpression,
) -> flare_proto::common::FilterExpression {
    flare_proto::common::FilterExpression {
        field: d.field.clone(),
        op: flare_proto::common::FilterOperator::Eq as i32, // 默认使用 Eq，实际应根据 operator 字符串解析
        values: if d.value.is_empty() {
            vec![]
        } else {
            vec![d.value.clone()]
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_to_proto_i32_uses_proto_database_values() {
        assert_eq!(
            event_type_to_proto_i32(EventType::Reaction),
            ProtoEventType::EventReaction as i32
        );
        assert_eq!(
            event_type_to_proto_i32(EventType::MessageBurnScheduled),
            ProtoEventType::EventMessageRetentionScheduled as i32
        );
    }
}
