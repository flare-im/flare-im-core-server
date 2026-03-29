//! 将领域命令转换为 proto Event，用于事件流发布（与 common/event.proto 对齐）

use chrono::Utc;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{
    Event, EventType, MarkEvent as ProtoMarkEvent, MessageDeleteEvent as ProtoMessageDeleteEvent,
    MessageEditEvent as ProtoMessageEditEvent, MessageRecallEvent as ProtoMessageRecallEvent,
    PinEvent as ProtoPinEvent, ReactionEvent as ProtoReactionEvent,
    ReadReceiptEvent as ProtoReadReceiptEvent, UnmarkEvent as ProtoUnmarkEvent,
    UnpinEvent as ProtoUnpinEvent,
};
use prost_types::Timestamp;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteScope, DeleteType, EditMessageCommand,
    MarkMessageCommand, PinMessageCommand, ReadMessageCommand, RecallMessageCommand,
    RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
};

fn to_timestamp(dt: chrono::DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// 从领域命令构建 proto Event（用于写入 Kafka 操作 topic，由 storage writer 消费）
pub struct EventBuilder;

impl EventBuilder {
    pub fn recall(cmd: &RecallMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMessageRecall as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Recall(ProtoMessageRecallEvent {
                server_msg_id: cmd.base.message_id.clone(),
                reason: cmd.reason.clone().unwrap_or_default(),
                time_limit_seconds: cmd.time_limit_seconds,
                allow_admin_recall: Some(cmd.allow_admin_override),
            })),
        }
    }

    pub fn edit(cmd: &EditMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMessageEdit as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Edit(ProtoMessageEditEvent {
                server_msg_id: cmd.base.message_id.clone(),
                new_content: cmd.new_content.clone(),
                edit_version: 0, // 由 storage 持久化时确定
                reason: cmd.reason.clone().unwrap_or_default(),
                show_edited_mark: true,
            })),
        }
    }

    /// 为单条消息构建删除事件（批量删除时对每条 message_id 调用一次并递增 seq）
    pub fn delete_one(server_msg_id: &str, cmd: &DeleteMessageCommand, seq: u64) -> Event {
        let delete_type = match cmd.delete_type {
            DeleteType::Hard => 2, // DELETE_TYPE_HARD
            DeleteType::Soft => 1, // DELETE_TYPE_SOFT
        };
        let delete_scope = match cmd.delete_scope {
            DeleteScope::UserPrivate => 1,        // DELETE_SCOPE_USER_PRIVATE
            DeleteScope::ConversationGlobal => 2, // DELETE_SCOPE_CONVERSATION_GLOBAL
        };
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMessageDelete as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Delete(ProtoMessageDeleteEvent {
                server_msg_id: server_msg_id.to_string(),
                delete_type: Some(delete_type),
                reason: cmd.reason.clone(),
                notify_others: Some(cmd.notify_others),
                scope: Some(delete_scope),
                target_user_id: cmd.target_user_id.clone(),
            })),
        }
    }

    pub fn read(cmd: &ReadMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReadReceipt as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Read(ProtoReadReceiptEvent {
                conversation_id: cmd.base.conversation_id.clone(),
                read_seq: 0,
                user_id: cmd.base.operator_id.clone(),
                message_ids: cmd.message_ids.clone(),
                read_at: Some(to_timestamp(cmd.read_at.unwrap_or_else(Utc::now))),
                burn_after_read: Some(cmd.burn_after_read),
            })),
        }
    }

    pub fn reaction_add(cmd: &AddReactionCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReaction as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Reaction(ProtoReactionEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                emoji: cmd.emoji.clone(),
                action: 1, // REACTION_ACTION_ADD
            })),
        }
    }

    pub fn reaction_remove(cmd: &RemoveReactionCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReaction as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Reaction(ProtoReactionEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                emoji: cmd.emoji.clone(),
                action: 2, // REACTION_ACTION_REMOVE
            })),
        }
    }

    pub fn pin(cmd: &PinMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventPin as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Pin(ProtoPinEvent {
                server_msg_id: cmd.base.message_id.clone(),
                pinned_by: cmd.base.operator_id.clone(),
                reason: cmd.reason.clone(),
                expire_at: cmd.expire_at.map(|dt| to_timestamp(dt)),
            })),
        }
    }

    pub fn unpin(cmd: &UnpinMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventUnpin as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Unpin(ProtoUnpinEvent {
                server_msg_id: cmd.base.message_id.clone(),
            })),
        }
    }

    pub fn mark(cmd: &MarkMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMark as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Mark(ProtoMarkEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                mark_type: cmd.mark_type,
                color: String::new(),
            })),
        }
    }

    pub fn unmark(cmd: &UnmarkMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventUnmark as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Unmark(ProtoUnmarkEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.user_id.clone(),
                mark_type: cmd.mark_type.unwrap_or(0),
            })),
        }
    }
}
