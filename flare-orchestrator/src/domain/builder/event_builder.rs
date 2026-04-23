//! 事件构建器
//!
//! 提供统一的事件构建能力，支持所有事件类型。
//!
//! ## 设计
//! - 函数式构建：简单事件直接使用函数
//! - Builder 模式：复杂事件使用 Builder
//! - 命令转换：从领域命令构建事件（用于 Kafka 发布）
//! - 类型安全：编译时检查必填字段

use chrono::Utc;
use flare_proto::common::{
    CustomEvent, Event, EventType, MarkEvent, MessageDeleteEvent, MessageEditEvent,
    MessageRecallEvent, PinEvent, ReactionEvent, ReadReceiptEvent, TypingEvent, UnmarkEvent,
    UnpinEvent, event,
};
use prost_types::Timestamp;
use uuid::Uuid;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteScope, DeleteType, EditMessageCommand,
    MarkMessageCommand, PinMessageCommand, ReadMessageCommand, RecallMessageCommand,
    RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
};

// =============================================================================
// 工具函数
// =============================================================================

/// 将 chrono DateTime 转换为 prost Timestamp
fn to_timestamp(dt: chrono::DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

// =============================================================================
// 函数式构建（简单事件）
// =============================================================================

/// 构建撤回事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被撤回消息的服务端 ID
/// - `reason`: 撤回原因（可选）
///
/// # 示例
/// ```rust
/// let event = build_recall_event("conv-123", "msg-456", Some("发错了"));
/// ```
pub fn build_recall_event(
    conversation_id: &str,
    server_msg_id: &str,
    reason: Option<&str>,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0, // 由服务分配
        r#type: EventType::EventMessageRecall as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Recall(MessageRecallEvent {
            server_msg_id: server_msg_id.to_string(),
            reason: reason.unwrap_or_default().to_string(),
            time_limit_seconds: None,
            allow_admin_recall: None,
        })),
    }
}

/// 构建编辑事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被编辑消息的服务端 ID
/// - `new_content`: 新内容（序列化后的字节）
/// - `edit_version`: 编辑版本号（递增）
///
/// # 示例
/// ```rust
/// let event = build_edit_event("conv-123", "msg-456", new_content, 1);
/// ```
pub fn build_edit_event(
    conversation_id: &str,
    server_msg_id: &str,
    new_content: Vec<u8>,
    edit_version: i32,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventMessageEdit as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Edit(MessageEditEvent {
            server_msg_id: server_msg_id.to_string(),
            new_content,
            edit_version,
            reason: String::new(),
            show_edited_mark: true,
        })),
    }
}

/// 构建删除事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被删除消息的服务端 ID
/// - `delete_type`: 删除类型（软删/硬删）
///
/// # 示例
/// ```rust
/// let event = build_delete_event("conv-123", "msg-456", DeleteType::Soft);
/// ```
pub fn build_delete_event(
    conversation_id: &str,
    server_msg_id: &str,
    delete_type: flare_proto::common::DeleteType,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventMessageDelete as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Delete(MessageDeleteEvent {
            server_msg_id: server_msg_id.to_string(),
            delete_type: Some(delete_type as i32),
            reason: None,
            notify_others: None,
            scope: None,
            target_user_id: None,
        })),
    }
}

/// 构建已读回执事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `user_id`: 已读用户 ID
/// - `read_seq`: 已读到的消息 seq
///
/// # 示例
/// ```rust
/// let event = build_read_receipt_event("conv-123", "user-456", 100);
/// ```
pub fn build_read_receipt_event(conversation_id: &str, user_id: &str, read_seq: u64) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventReadReceipt as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Read(ReadReceiptEvent {
            conversation_id: conversation_id.to_string(),
            read_seq,
            user_id: user_id.to_string(),
            message_ids: Vec::new(),
            read_at: None,
            burn_after_read: None,
        })),
    }
}

/// 构建表情反应事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被反应的消息服务端 ID
/// - `user_id`: 操作用户 ID
/// - `emoji`: 表情标识
/// - `action`: 操作类型（添加/移除）
///
/// # 示例
/// ```rust
/// let event = build_reaction_event("conv-123", "msg-456", "user-789", "👍", ReactionAction::Add);
/// ```
pub fn build_reaction_event(
    conversation_id: &str,
    server_msg_id: &str,
    user_id: &str,
    emoji: &str,
    action: flare_proto::common::ReactionAction,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventReaction as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Reaction(ReactionEvent {
            server_msg_id: server_msg_id.to_string(),
            user_id: user_id.to_string(),
            emoji: emoji.to_string(),
            action: action as i32,
        })),
    }
}

/// 构建置顶事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被置顶的消息服务端 ID
/// - `pinned_by`: 置顶操作者 user_id
///
/// # 示例
/// ```rust
/// let event = build_pin_event("conv-123", "msg-456", "user-789");
/// ```
pub fn build_pin_event(conversation_id: &str, server_msg_id: &str, pinned_by: &str) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventPin as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Pin(PinEvent {
            server_msg_id: server_msg_id.to_string(),
            pinned_by: pinned_by.to_string(),
            reason: None,
            expire_at: None,
        })),
    }
}

/// 构建取消置顶事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被取消置顶的消息服务端 ID
///
/// # 示例
/// ```rust
/// let event = build_unpin_event("conv-123", "msg-456");
/// ```
pub fn build_unpin_event(conversation_id: &str, server_msg_id: &str) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventUnpin as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Unpin(UnpinEvent {
            server_msg_id: server_msg_id.to_string(),
        })),
    }
}

/// 构建标记事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被标记的消息服务端 ID
/// - `user_id`: 操作用户 ID
/// - `mark_type`: 标记类型
///
/// # 示例
/// ```rust
/// let event = build_mark_event("conv-123", "msg-456", "user-789", MarkType::Important);
/// ```
pub fn build_mark_event(
    conversation_id: &str,
    server_msg_id: &str,
    user_id: &str,
    mark_type: flare_proto::common::MarkType,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventMark as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Mark(MarkEvent {
            server_msg_id: server_msg_id.to_string(),
            user_id: user_id.to_string(),
            mark_type: mark_type as i32,
            color: String::new(),
        })),
    }
}

/// 构建取消标记事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `server_msg_id`: 被取消标记的消息服务端 ID
/// - `user_id`: 操作用户 ID
/// - `mark_type`: 标记类型
///
/// # 示例
/// ```rust
/// let event = build_unmark_event("conv-123", "msg-456", "user-789", MarkType::Important);
/// ```
pub fn build_unmark_event(
    conversation_id: &str,
    server_msg_id: &str,
    user_id: &str,
    mark_type: flare_proto::common::MarkType,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventUnmark as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Unmark(UnmarkEvent {
            server_msg_id: server_msg_id.to_string(),
            user_id: user_id.to_string(),
            mark_type: mark_type as i32,
        })),
    }
}

/// 构建正在输入事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `user_id`: 正在输入的用户 ID
/// - `typing`: 是否正在输入
///
/// # 示例
/// ```rust
/// let event = build_typing_event("conv-123", "user-456", true);
/// ```
pub fn build_typing_event(conversation_id: &str, user_id: &str, typing: bool) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventTyping as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Typing(TypingEvent {
            conversation_id: conversation_id.to_string(),
            user_id: user_id.to_string(),
            typing,
        })),
    }
}

/// 构建自定义事件
///
/// # 参数
/// - `conversation_id`: 会话 ID
/// - `namespace`: 业务命名空间
/// - `name`: 事件名
/// - `payload`: 业务载荷
///
/// # 示例
/// ```rust
/// let event = build_custom_event("conv-123", "myapp", "custom_action", vec![1, 2, 3]);
/// ```
pub fn build_custom_event(
    conversation_id: &str,
    namespace: &str,
    name: &str,
    payload: Vec<u8>,
) -> Event {
    Event {
        conversation_id: conversation_id.to_string(),
        seq: 0,
        r#type: EventType::EventCustom as i32,
        created_at: Some(Timestamp::from(std::time::SystemTime::now())),
        event_id: Uuid::new_v4().to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(event::Payload::Custom(CustomEvent {
            namespace: namespace.to_string(),
            name: name.to_string(),
            version: "1.0".to_string(),
            payload,
            metadata: std::collections::HashMap::new(),
        })),
    }
}

// =============================================================================
// Builder 模式（复杂事件）
// =============================================================================

/// 事件构建器（Builder 模式）
///
/// 用于构建复杂事件，支持流式 API。
///
/// # 示例
/// ```rust
/// let event = EventBuilder::new(EventType::EventMessageRecall)
///     .conversation_id("conv-123")
///     .with_recall_payload("msg-456", Some("发错了"))
///     .build();
/// ```
pub struct EventBuilder {
    conversation_id: String,
    event_type: EventType,
    event_id: String,
    request_id: Option<String>,
    payload: Option<event::Payload>,
}

impl EventBuilder {
    /// 创建新的事件构建器
    pub fn new(event_type: EventType) -> Self {
        Self {
            conversation_id: String::new(),
            event_type,
            event_id: Uuid::new_v4().to_string(),
            request_id: None,
            payload: None,
        }
    }

    /// 设置会话 ID
    pub fn conversation_id(mut self, conversation_id: impl Into<String>) -> Self {
        self.conversation_id = conversation_id.into();
        self
    }

    /// 设置事件 ID（可选，默认自动生成）
    pub fn event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = event_id.into();
        self
    }

    /// 设置请求 ID（可选）
    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// 设置撤回事件载荷
    pub fn with_recall_payload(mut self, server_msg_id: &str, reason: Option<&str>) -> Self {
        self.payload = Some(event::Payload::Recall(MessageRecallEvent {
            server_msg_id: server_msg_id.to_string(),
            reason: reason.unwrap_or_default().to_string(),
            time_limit_seconds: None,
            allow_admin_recall: None,
        }));
        self
    }

    /// 设置编辑事件载荷
    pub fn with_edit_payload(
        mut self,
        server_msg_id: &str,
        new_content: Vec<u8>,
        edit_version: i32,
    ) -> Self {
        self.payload = Some(event::Payload::Edit(MessageEditEvent {
            server_msg_id: server_msg_id.to_string(),
            new_content,
            edit_version,
            reason: String::new(),
            show_edited_mark: true,
        }));
        self
    }

    /// 设置删除事件载荷
    pub fn with_delete_payload(
        mut self,
        server_msg_id: &str,
        delete_type: flare_proto::common::DeleteType,
    ) -> Self {
        self.payload = Some(event::Payload::Delete(MessageDeleteEvent {
            server_msg_id: server_msg_id.to_string(),
            delete_type: Some(delete_type as i32),
            reason: None,
            notify_others: None,
            scope: None,
            target_user_id: None,
        }));
        self
    }

    /// 设置已读回执事件载荷
    pub fn with_read_receipt_payload(mut self, user_id: &str, read_seq: u64) -> Self {
        self.payload = Some(event::Payload::Read(ReadReceiptEvent {
            conversation_id: self.conversation_id.clone(),
            read_seq,
            user_id: user_id.to_string(),
            message_ids: Vec::new(),
            read_at: None,
            burn_after_read: None,
        }));
        self
    }

    /// 设置表情反应事件载荷
    pub fn with_reaction_payload(
        mut self,
        server_msg_id: &str,
        user_id: &str,
        emoji: &str,
        action: flare_proto::common::ReactionAction,
    ) -> Self {
        self.payload = Some(event::Payload::Reaction(ReactionEvent {
            server_msg_id: server_msg_id.to_string(),
            user_id: user_id.to_string(),
            emoji: emoji.to_string(),
            action: action as i32,
        }));
        self
    }

    /// 构建事件
    pub fn build(self) -> Event {
        Event {
            conversation_id: self.conversation_id,
            seq: 0, // 由服务分配
            r#type: self.event_type as i32,
            created_at: Some(Timestamp::from(std::time::SystemTime::now())),
            event_id: self.event_id,
            event_seq: None,
            request_id: self.request_id,
            payload: self.payload,
        }
    }

    // =========================================================================
    // 命令转换方法（从领域命令构建事件）
    // =========================================================================

    /// 从撤回命令构建事件
    pub fn recall(cmd: &RecallMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMessageRecall as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Recall(MessageRecallEvent {
                server_msg_id: cmd.base.message_id.clone(),
                reason: cmd.reason.clone().unwrap_or_default(),
                time_limit_seconds: cmd.time_limit_seconds,
                allow_admin_recall: Some(cmd.allow_admin_override),
            })),
        }
    }

    /// 从编辑命令构建事件
    pub fn edit(cmd: &EditMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMessageEdit as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Edit(MessageEditEvent {
                server_msg_id: cmd.base.message_id.clone(),
                new_content: cmd.new_content.clone(),
                edit_version: 0, // 由 storage 持久化时确定
                reason: cmd.reason.clone().unwrap_or_default(),
                show_edited_mark: true,
            })),
        }
    }

    /// 从删除命令构建单条消息删除事件（批量删除时对每条 message_id 调用一次并递增 seq）
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
            payload: Some(event::Payload::Delete(MessageDeleteEvent {
                server_msg_id: server_msg_id.to_string(),
                delete_type: Some(delete_type),
                reason: cmd.reason.clone(),
                notify_others: Some(cmd.notify_others),
                scope: Some(delete_scope),
                target_user_id: cmd.target_user_id.clone(),
            })),
        }
    }

    /// 从已读命令构建事件
    pub fn read(cmd: &ReadMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReadReceipt as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Read(ReadReceiptEvent {
                conversation_id: cmd.base.conversation_id.clone(),
                read_seq: 0,
                user_id: cmd.base.operator_id.clone(),
                message_ids: cmd.message_ids.clone(),
                read_at: Some(to_timestamp(cmd.read_at.unwrap_or_else(Utc::now))),
                burn_after_read: Some(cmd.burn_after_read),
            })),
        }
    }

    /// 从添加表情命令构建事件
    pub fn reaction_add(cmd: &AddReactionCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReaction as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Reaction(ReactionEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                emoji: cmd.emoji.clone(),
                action: 1, // REACTION_ACTION_ADD
            })),
        }
    }

    /// 从移除表情命令构建事件
    pub fn reaction_remove(cmd: &RemoveReactionCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventReaction as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Reaction(ReactionEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                emoji: cmd.emoji.clone(),
                action: 2, // REACTION_ACTION_REMOVE
            })),
        }
    }

    /// 从置顶命令构建事件
    pub fn pin(cmd: &PinMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventPin as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Pin(PinEvent {
                server_msg_id: cmd.base.message_id.clone(),
                pinned_by: cmd.base.operator_id.clone(),
                reason: cmd.reason.clone(),
                expire_at: cmd.expire_at.map(|dt| to_timestamp(dt)),
            })),
        }
    }

    /// 从取消置顶命令构建事件
    pub fn unpin(cmd: &UnpinMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventUnpin as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Unpin(UnpinEvent {
                server_msg_id: cmd.base.message_id.clone(),
            })),
        }
    }

    /// 从标记命令构建事件
    pub fn mark(cmd: &MarkMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventMark as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Mark(MarkEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.base.operator_id.clone(),
                mark_type: cmd.mark_type,
                color: String::new(),
            })),
        }
    }

    /// 从取消标记命令构建事件
    pub fn unmark(cmd: &UnmarkMessageCommand, seq: u64) -> Event {
        Event {
            conversation_id: cmd.base.conversation_id.clone(),
            seq,
            r#type: EventType::EventUnmark as i32,
            created_at: Some(to_timestamp(Utc::now())),
            event_id: format!("{}:{}", cmd.base.conversation_id, seq),
            event_seq: None,
            request_id: None,
            payload: Some(event::Payload::Unmark(UnmarkEvent {
                server_msg_id: cmd.base.message_id.clone(),
                user_id: cmd.user_id.clone(),
                mark_type: cmd.mark_type.unwrap_or(0),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_recall_event() {
        let event = build_recall_event("conv-123", "msg-456", Some("发错了"));
        assert_eq!(event.conversation_id, "conv-123");
        assert_eq!(event.r#type, EventType::EventMessageRecall as i32);
        assert!(!event.event_id.is_empty());
    }

    #[test]
    fn test_event_builder() {
        let event = EventBuilder::new(EventType::EventMessageRecall)
            .conversation_id("conv-123")
            .with_recall_payload("msg-456", Some("发错了"))
            .build();

        assert_eq!(event.conversation_id, "conv-123");
        assert_eq!(event.r#type, EventType::EventMessageRecall as i32);
    }

    #[test]
    fn test_build_read_receipt_event() {
        let event = build_read_receipt_event("conv-123", "user-456", 100);
        assert_eq!(event.conversation_id, "conv-123");
        assert_eq!(event.r#type, EventType::EventReadReceipt as i32);
    }
}
