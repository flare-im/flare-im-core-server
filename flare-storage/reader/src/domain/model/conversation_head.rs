//! 会话消息链顶端（用于同步水位，避免全量 bootstrap）

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct ConversationMessageHead {
    pub max_seq: i64,
    pub last_message_id: String,
    pub last_at: Option<DateTime<Utc>>,
}
