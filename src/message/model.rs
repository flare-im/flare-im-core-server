//! 统一消息领域模型（与 common/message.proto 严格对齐）

use std::collections::HashMap;

use flare_proto::common::{
    ContentVisibility, MessageContent, MessageRetentionLifecycle, MessageRetentionPolicy,
    MessageRetentionState, OfflinePushInfo, RetentionMode, RetentionTrigger,
};
use prost_types::Timestamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionTransitionError {
    InvalidExpireAfterSeconds,
    RetentionDisabled,
    AlreadyTerminal(MessageRetentionLifecycle),
    RetentionNotDue { expire_at: i64, now: i64 },
}

impl std::fmt::Display for RetentionTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidExpireAfterSeconds => write!(f, "expire_after_seconds must be positive"),
            Self::RetentionDisabled => write!(f, "retention is not enabled for this message"),
            Self::AlreadyTerminal(lifecycle) => {
                write!(f, "retention lifecycle is terminal: {lifecycle:?}")
            }
            Self::RetentionNotDue { expire_at, now } => {
                write!(
                    f,
                    "message retention is not due: expire_at={expire_at}, now={now}"
                )
            }
        }
    }
}

impl std::error::Error for RetentionTransitionError {}

/// 消息领域模型（与 common/message.proto Message 一一对应）
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub server_id: String,
    pub conversation_id: String,
    pub client_msg_id: String,
    pub sender_id: String,
    pub source: i32,
    pub conversation_seq: u64,
    pub timestamp: Option<Timestamp>,
    pub conversation_type: i32,
    pub message_type: i32,
    pub message_seq: Option<u64>,
    /// 会话频道 ID：单聊=对方 user_id，群聊=群 ID，频道/话题=对应 ID
    pub channel_id: String,
    pub sender_name: String,
    pub sender_avatar: String,
    pub content: Option<MessageContent>,
    pub status: i32,
    pub retention_policy: Option<MessageRetentionPolicy>,
    pub retention_state: Option<MessageRetentionState>,
    pub offline_push_info: Option<OfflinePushInfo>,
    pub extra: HashMap<String, String>,
    pub extensions: HashMap<String, Vec<u8>>,
}

impl Message {
    /// 单聊时对方 user_id（= channel_id）；群聊/频道返回空。proto 无 receiver_id，以此替代。
    pub fn single_chat_receiver(&self) -> &str {
        if self.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            self.channel_id.as_str()
        } else {
            ""
        }
    }

    pub fn retention_lifecycle(&self) -> MessageRetentionLifecycle {
        self.retention_state
            .as_ref()
            .and_then(|state| MessageRetentionLifecycle::try_from(state.lifecycle).ok())
            .unwrap_or(MessageRetentionLifecycle::None)
    }

    pub fn content_visibility(&self) -> ContentVisibility {
        self.retention_state
            .as_ref()
            .and_then(|state| ContentVisibility::try_from(state.content_visibility).ok())
            .unwrap_or(ContentVisibility::Available)
    }

    pub fn enable_retention_after_read(
        &mut self,
        expire_after_seconds: i64,
        visibility_after_expiration: ContentVisibility,
    ) -> Result<(), RetentionTransitionError> {
        if expire_after_seconds <= 0 {
            return Err(RetentionTransitionError::InvalidExpireAfterSeconds);
        }
        if self.retention_lifecycle().is_terminal() {
            return Err(RetentionTransitionError::AlreadyTerminal(
                self.retention_lifecycle(),
            ));
        }
        self.retention_policy = Some(MessageRetentionPolicy {
            mode: RetentionMode::AfterRead as i32,
            trigger: RetentionTrigger::AfterRead as i32,
            expire_after_seconds: Some(expire_after_seconds),
            expire_at: None,
            visibility_after_expiration: visibility_after_expiration as i32,
            attributes: HashMap::new(),
        });
        self.retention_state = Some(MessageRetentionState {
            lifecycle: MessageRetentionLifecycle::Active as i32,
            content_visibility: ContentVisibility::Available as i32,
            first_triggered_at: None,
            expire_at: None,
            expired_at: None,
            purged_at: None,
            triggered_by_user_id: None,
        });
        Ok(())
    }

    /// Marks the first real read and schedules retention expiration with server time.
    /// Repeated ReadAck is idempotent and returns `Ok(false)`.
    pub fn mark_read_for_retention(
        &mut self,
        reader_id: &str,
        now_seconds: i64,
    ) -> Result<bool, RetentionTransitionError> {
        if !self.retention_enabled() {
            return Ok(false);
        }
        match self.retention_lifecycle() {
            MessageRetentionLifecycle::None => {
                self.ensure_retention_state().lifecycle = MessageRetentionLifecycle::Active as i32;
            }
            MessageRetentionLifecycle::Scheduled
            | MessageRetentionLifecycle::Expired
            | MessageRetentionLifecycle::Purged => return Ok(false),
            MessageRetentionLifecycle::Active | MessageRetentionLifecycle::Unspecified => {}
        }
        let state = self.ensure_retention_state();
        state.first_triggered_at = Some(now_seconds);
        state.triggered_by_user_id = Some(reader_id.to_string());
        self.schedule_retention_expiration(now_seconds)
    }

    pub fn schedule_retention_expiration(
        &mut self,
        now_seconds: i64,
    ) -> Result<bool, RetentionTransitionError> {
        let policy = self
            .retention_policy
            .as_ref()
            .ok_or(RetentionTransitionError::RetentionDisabled)?;
        let after = policy
            .expire_after_seconds
            .filter(|seconds| *seconds > 0)
            .ok_or(RetentionTransitionError::InvalidExpireAfterSeconds)?;
        if self.retention_lifecycle().is_terminal() {
            return Ok(false);
        }
        if self
            .retention_state
            .as_ref()
            .and_then(|state| state.expire_at.as_ref())
            .is_some()
        {
            self.ensure_retention_state().lifecycle = MessageRetentionLifecycle::Scheduled as i32;
            return Ok(false);
        }
        let expire_at = now_seconds.saturating_add(after);
        let state = self.ensure_retention_state();
        state.lifecycle = MessageRetentionLifecycle::Scheduled as i32;
        state.expire_at = Some(expire_at);
        Ok(true)
    }

    pub fn expire_retained_content(
        &mut self,
        now_seconds: i64,
    ) -> Result<bool, RetentionTransitionError> {
        if !self.retention_enabled() {
            return Err(RetentionTransitionError::RetentionDisabled);
        }
        match self.retention_lifecycle() {
            MessageRetentionLifecycle::Expired | MessageRetentionLifecycle::Purged => {
                return Ok(false);
            }
            MessageRetentionLifecycle::Scheduled => {}
            lifecycle => return Err(RetentionTransitionError::AlreadyTerminal(lifecycle)),
        }
        if let Some(expire_at) = self
            .retention_state
            .as_ref()
            .and_then(|state| state.expire_at.as_ref())
            .copied()
            && now_seconds < expire_at
        {
            return Err(RetentionTransitionError::RetentionNotDue {
                expire_at,
                now: now_seconds,
            });
        }
        let visibility_after_expiration = self
            .retention_policy
            .as_ref()
            .and_then(|policy| ContentVisibility::try_from(policy.visibility_after_expiration).ok())
            .unwrap_or(ContentVisibility::Redacted);
        let state = self.ensure_retention_state();
        state.lifecycle = MessageRetentionLifecycle::Expired as i32;
        state.content_visibility = visibility_after_expiration as i32;
        state.expired_at = Some(now_seconds);
        if matches!(
            visibility_after_expiration,
            ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged
        ) {
            self.content = None;
        }
        Ok(true)
    }

    pub fn purge_retained_content(
        &mut self,
        now_seconds: i64,
    ) -> Result<bool, RetentionTransitionError> {
        if !self.retention_enabled() {
            return Err(RetentionTransitionError::RetentionDisabled);
        }
        if self.retention_lifecycle() == MessageRetentionLifecycle::Purged {
            return Ok(false);
        }
        let state = self.ensure_retention_state();
        state.lifecycle = MessageRetentionLifecycle::Purged as i32;
        state.content_visibility = ContentVisibility::Purged as i32;
        state.purged_at = Some(now_seconds);
        self.content = None;
        self.extensions.clear();
        Ok(true)
    }

    pub fn can_read(&self) -> bool {
        !matches!(
            self.content_visibility(),
            ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged
        )
    }

    pub fn can_edit(&self) -> bool {
        !self.retention_lifecycle().is_terminal()
    }

    pub fn can_forward(&self) -> bool {
        self.can_read()
    }

    pub fn can_quote(&self) -> bool {
        self.can_read()
    }

    fn retention_enabled(&self) -> bool {
        self.retention_policy
            .as_ref()
            .and_then(|policy| RetentionMode::try_from(policy.mode).ok())
            .is_some_and(|mode| mode != RetentionMode::None)
    }

    fn ensure_retention_state(&mut self) -> &mut MessageRetentionState {
        self.retention_state.get_or_insert(MessageRetentionState {
            lifecycle: MessageRetentionLifecycle::None as i32,
            content_visibility: ContentVisibility::Available as i32,
            first_triggered_at: None,
            expire_at: None,
            expired_at: None,
            purged_at: None,
            triggered_by_user_id: None,
        })
    }
}

trait RetentionLifecycleExt {
    fn is_terminal(&self) -> bool;
}

impl RetentionLifecycleExt for MessageRetentionLifecycle {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            MessageRetentionLifecycle::Expired | MessageRetentionLifecycle::Purged
        )
    }
}

/// 附件（业务解析 content 或 extensions 时使用，proto Message 无此字段）
#[derive(Debug, Clone, Default)]
pub struct Attachment {
    pub url: String,
    pub name: Option<String>,
    pub content_type: Option<String>,
    pub size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{TextContent, message_content::Content};

    fn plain_message() -> Message {
        Message {
            server_id: "m1".to_string(),
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "hello".to_string(),
                    mentions: Vec::new(),
                })),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn ordinary_message_is_not_affected_by_retention_ack() {
        let mut message = plain_message();

        assert!(!message.mark_read_for_retention("u1", 100).unwrap());
        assert_eq!(
            message.retention_lifecycle(),
            MessageRetentionLifecycle::None
        );
        assert!(message.content.is_some());
    }

    #[test]
    fn enable_retention_sets_active_policy() {
        let mut message = plain_message();

        message
            .enable_retention_after_read(30, ContentVisibility::Redacted)
            .unwrap();

        let policy = message.retention_policy.as_ref().unwrap();
        assert_eq!(policy.mode, RetentionMode::AfterRead as i32);
        assert_eq!(policy.expire_after_seconds, Some(30));
        assert_eq!(
            message.retention_lifecycle(),
            MessageRetentionLifecycle::Active
        );
    }

    #[test]
    fn first_read_schedules_retention_expiration() {
        let mut message = plain_message();
        message
            .enable_retention_after_read(30, ContentVisibility::Redacted)
            .unwrap();

        assert!(message.mark_read_for_retention("u1", 100).unwrap());

        let state = message.retention_state.unwrap();
        assert_eq!(state.lifecycle, MessageRetentionLifecycle::Scheduled as i32);
        assert_eq!(state.expire_at, Some(130));
        assert_eq!(state.triggered_by_user_id.as_deref(), Some("u1"));
    }

    #[test]
    fn repeated_read_ack_is_idempotent() {
        let mut message = plain_message();
        message
            .enable_retention_after_read(30, ContentVisibility::Redacted)
            .unwrap();

        assert!(message.mark_read_for_retention("u1", 100).unwrap());
        assert!(!message.mark_read_for_retention("u1", 101).unwrap());

        let state = message.retention_state.unwrap();
        assert_eq!(state.expire_at, Some(130));
    }

    #[test]
    fn expiration_redacts_content() {
        let mut message = plain_message();
        message
            .enable_retention_after_read(30, ContentVisibility::Redacted)
            .unwrap();
        message.mark_read_for_retention("u1", 100).unwrap();

        assert!(message.expire_retained_content(131).unwrap());

        assert_eq!(
            message.retention_lifecycle(),
            MessageRetentionLifecycle::Expired
        );
        assert!(message.content.is_none());
        assert!(!message.can_read());
    }
}
