//! 会话消息链顶端（用于同步水位，避免全量 bootstrap）

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct ConversationMessageHead {
    /// 会话 seq 高水位 = GREATEST(messages.max_seq, events.max_seq)。
    /// 消息与事件共用同一会话 seq 计数器，水位必须覆盖两者。
    pub max_seq: i64,
    /// 最后一条**消息**的 server_id（会话只有事件时为空）。
    pub last_message_id: String,
    pub last_at: Option<DateTime<Utc>>,
}
