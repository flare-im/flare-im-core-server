//! 统一消息领域模型（与 common/message.proto 严格对齐）

use std::collections::HashMap;

/// 阅后即焚 FSM 状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum BurnStatus {
    None = 0,
    Init = 1,
    Read = 2,
    BurnPending = 3,
    Burned = 4,
    HardDeleted = 5,
}

impl BurnStatus {
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::Init,
            2 => Self::Read,
            3 => Self::BurnPending,
            4 => Self::Burned,
            5 => Self::HardDeleted,
            _ => Self::None,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Burned | Self::HardDeleted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BurnTransitionError {
    InvalidAfterReadSeconds,
    BurnDisabled,
    AlreadyTerminal(BurnStatus),
    BurnNotDue { burn_at: i64, now: i64 },
}

impl std::fmt::Display for BurnTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAfterReadSeconds => write!(f, "burn_after_read_seconds must be positive"),
            Self::BurnDisabled => write!(f, "burn is not enabled for this message"),
            Self::AlreadyTerminal(status) => write!(f, "burn state is terminal: {status:?}"),
            Self::BurnNotDue { burn_at, now } => {
                write!(f, "message burn is not due: burn_at={burn_at}, now={now}")
            }
        }
    }
}

impl std::error::Error for BurnTransitionError {}

/// 消息领域模型（与 common/message.proto Message 一一对应）
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub server_id: String,
    pub conversation_id: String,
    pub client_msg_id: String,
    pub sender_id: String,
    pub source: i32,
    pub seq: u64,
    pub timestamp: Option<prost_types::Timestamp>,
    pub conversation_type: i32,
    pub message_type: i32,
    /// 会话频道 ID：单聊=对方 user_id，群聊=群 ID，频道/话题=对应 ID
    pub channel_id: String,
    pub sender_name: String,
    pub sender_avatar: String,
    pub content: Vec<u8>,
    pub status: i32,
    pub burn_enabled: bool,
    pub burn_after_read_seconds: Option<i64>,
    pub burn_status: i32,
    pub first_read_at: Option<i64>,
    pub burn_at: Option<i64>,
    pub burned_at: Option<i64>,
    pub offline_push_info: Option<flare_proto::common::OfflinePushInfo>,
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

    pub fn burn_status(&self) -> BurnStatus {
        BurnStatus::from_i32(self.burn_status)
    }

    pub fn enable_burn(&mut self, after_read_seconds: i64) -> Result<(), BurnTransitionError> {
        if after_read_seconds <= 0 {
            return Err(BurnTransitionError::InvalidAfterReadSeconds);
        }
        if self.burn_status().is_terminal() {
            return Err(BurnTransitionError::AlreadyTerminal(self.burn_status()));
        }
        self.burn_enabled = true;
        self.burn_after_read_seconds = Some(after_read_seconds);
        self.burn_status = BurnStatus::Init.as_i32();
        self.first_read_at = None;
        self.burn_at = None;
        self.burned_at = None;
        Ok(())
    }

    /// Marks the first real read and schedules burn with server time.
    /// Repeated ReadAck is idempotent and returns `Ok(false)`.
    pub fn mark_read_for_burn(
        &mut self,
        reader_id: &str,
        now: i64,
    ) -> Result<bool, BurnTransitionError> {
        let _ = reader_id; // per-user burn state is reserved for the next storage model.
        if !self.burn_enabled {
            return Ok(false);
        }
        match self.burn_status() {
            BurnStatus::None => {
                self.burn_status = BurnStatus::Init.as_i32();
            }
            BurnStatus::BurnPending | BurnStatus::Burned | BurnStatus::HardDeleted => {
                return Ok(false);
            }
            BurnStatus::Read => {
                return self.schedule_burn(now);
            }
            BurnStatus::Init => {}
        }
        self.first_read_at = Some(now);
        self.burn_status = BurnStatus::Read.as_i32();
        self.schedule_burn(now)
    }

    pub fn schedule_burn(&mut self, now: i64) -> Result<bool, BurnTransitionError> {
        if !self.burn_enabled {
            return Err(BurnTransitionError::BurnDisabled);
        }
        if self.burn_status().is_terminal() {
            return Ok(false);
        }
        if self.burn_at.is_some() {
            self.burn_status = BurnStatus::BurnPending.as_i32();
            return Ok(false);
        }
        let after = self
            .burn_after_read_seconds
            .filter(|seconds| *seconds > 0)
            .ok_or(BurnTransitionError::InvalidAfterReadSeconds)?;
        if self.first_read_at.is_none() {
            self.first_read_at = Some(now);
        }
        self.burn_at = Some(now.saturating_add(after));
        self.burn_status = BurnStatus::BurnPending.as_i32();
        Ok(true)
    }

    pub fn burn(&mut self, now: i64) -> Result<bool, BurnTransitionError> {
        if !self.burn_enabled {
            return Err(BurnTransitionError::BurnDisabled);
        }
        match self.burn_status() {
            BurnStatus::Burned | BurnStatus::HardDeleted => return Ok(false),
            BurnStatus::BurnPending => {}
            status => return Err(BurnTransitionError::AlreadyTerminal(status)),
        }
        if let Some(burn_at) = self.burn_at
            && now < burn_at
        {
            return Err(BurnTransitionError::BurnNotDue { burn_at, now });
        }
        self.burned_at = Some(now);
        self.burn_status = BurnStatus::Burned.as_i32();
        self.content.clear();
        Ok(true)
    }

    pub fn hard_delete(&mut self, now: i64) -> Result<bool, BurnTransitionError> {
        let _ = now;
        if !self.burn_enabled {
            return Err(BurnTransitionError::BurnDisabled);
        }
        if self.burn_status() == BurnStatus::HardDeleted {
            return Ok(false);
        }
        self.burn_status = BurnStatus::HardDeleted.as_i32();
        self.content.clear();
        self.extensions.clear();
        Ok(true)
    }

    pub fn can_read(&self) -> bool {
        !self.burn_status().is_terminal()
    }

    pub fn can_edit(&self) -> bool {
        !self.burn_status().is_terminal()
    }

    pub fn can_forward(&self) -> bool {
        !self.burn_status().is_terminal()
    }

    pub fn can_quote(&self) -> bool {
        !self.burn_status().is_terminal()
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

    fn plain_message() -> Message {
        Message {
            server_id: "m1".to_string(),
            content: b"hello".to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn ordinary_message_is_not_affected_by_burn_ack() {
        let mut message = plain_message();

        assert!(!message.mark_read_for_burn("u1", 100).unwrap());
        assert_eq!(message.burn_status(), BurnStatus::None);
        assert_eq!(message.content, b"hello");
    }

    #[test]
    fn enable_burn_sets_initial_state() {
        let mut message = plain_message();

        message.enable_burn(30).unwrap();

        assert!(message.burn_enabled);
        assert_eq!(message.burn_after_read_seconds, Some(30));
        assert_eq!(message.burn_status(), BurnStatus::Init);
        assert_eq!(message.burn_at, None);
    }

    #[test]
    fn first_read_schedules_server_authoritative_burn_at() {
        let mut message = plain_message();
        message.enable_burn(30).unwrap();

        assert!(message.mark_read_for_burn("u2", 1000).unwrap());

        assert_eq!(message.first_read_at, Some(1000));
        assert_eq!(message.burn_at, Some(1030));
        assert_eq!(message.burn_status(), BurnStatus::BurnPending);
    }

    #[test]
    fn repeated_read_ack_is_idempotent() {
        let mut message = plain_message();
        message.enable_burn(30).unwrap();

        assert!(message.mark_read_for_burn("u2", 1000).unwrap());
        assert!(!message.mark_read_for_burn("u2", 1010).unwrap());

        assert_eq!(message.first_read_at, Some(1000));
        assert_eq!(message.burn_at, Some(1030));
        assert_eq!(message.burn_status(), BurnStatus::BurnPending);
    }

    #[test]
    fn burn_due_message_hides_content_and_blocks_mutations() {
        let mut message = plain_message();
        message.enable_burn(30).unwrap();
        message.mark_read_for_burn("u2", 1000).unwrap();

        assert!(message.burn(1030).unwrap());

        assert_eq!(message.burn_status(), BurnStatus::Burned);
        assert_eq!(message.burned_at, Some(1030));
        assert!(message.content.is_empty());
        assert!(!message.can_read());
        assert!(!message.can_edit());
        assert!(!message.can_forward());
        assert!(!message.can_quote());
    }

    #[test]
    fn burn_before_due_is_rejected() {
        let mut message = plain_message();
        message.enable_burn(30).unwrap();
        message.mark_read_for_burn("u2", 1000).unwrap();

        assert!(matches!(
            message.burn(1029),
            Err(BurnTransitionError::BurnNotDue {
                burn_at: 1030,
                now: 1029
            })
        ));
    }
}
