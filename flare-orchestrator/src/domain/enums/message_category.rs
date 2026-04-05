//! 消息类别枚举
//!
//! 用于决定消息的处理策略

/// 消息类别（用于决定处理策略）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageCategory {
    /// 临时消息（TYPING、SYSTEM_EVENT）：只推送，不持久化，不经过 WAL
    Temporary,
    /// 通知消息（NOTIFICATION）：根据 persistent 标志决定是否持久化，但都推送
    Notification,
    /// 操作消息（OPERATION）：根据操作类型决定同步/异步处理
    Operation,
    /// 普通消息：推送+持久化+WAL
    Normal,
}

impl MessageCategory {
    /// 是否需要持久化
    pub fn needs_persistence(&self) -> bool {
        match self {
            MessageCategory::Temporary => false,
            MessageCategory::Notification => false, // 由 persistent 标志决定
            MessageCategory::Operation => true,
            MessageCategory::Normal => true,
        }
    }

    /// 是否需要写入 WAL
    pub fn needs_wal(&self) -> bool {
        match self {
            MessageCategory::Temporary => false,
            MessageCategory::Notification => false, // 由 persistent 标志决定
            MessageCategory::Operation => true,
            MessageCategory::Normal => true,
        }
    }

    /// 是否为临时消息
    pub fn is_temporary(&self) -> bool {
        matches!(self, MessageCategory::Temporary)
    }

    /// 是否为操作消息
    pub fn is_operation(&self) -> bool {
        matches!(self, MessageCategory::Operation)
    }

    /// 是否为通知消息
    pub fn is_notification(&self) -> bool {
        matches!(self, MessageCategory::Notification)
    }
}
