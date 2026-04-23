//! 持久化模式枚举
//!
//! 用于控制消息和事件的持久化行为

/// 持久化模式
///
/// 用于控制消息和事件的持久化行为
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceMode {
    /// 自动模式：根据消息/事件类型自动判断
    /// - 临时消息/事件（TYPING、SYSTEM_EVENT、PRESENCE 等）：仅推送
    /// - 普通消息/事件：持久化 + 推送
    Auto,

    /// 强制持久化：无论消息/事件类型如何，都进行持久化 + 推送
    /// 用于特殊场景，如重要通知、系统公告等
    ForcePersistence,

    /// 强制仅推送：无论消息/事件类型如何，都仅推送不持久化
    /// 用于临时消息、实时状态等
    ForcePushOnly,
}

impl Default for PersistenceMode {
    fn default() -> Self {
        PersistenceMode::Auto
    }
}

impl PersistenceMode {
    /// 判断是否应该仅推送（不持久化）
    ///
    /// # 参数
    /// - `is_temporary`: 消息/事件是否为临时类型（由 MessageProfile 或 EventType 判断）
    ///
    /// # 返回
    /// - `true`: 仅推送，不持久化
    /// - `false`: 持久化 + 推送
    pub fn should_push_only(&self, is_temporary: bool) -> bool {
        match self {
            PersistenceMode::Auto => is_temporary,
            PersistenceMode::ForcePersistence => false,
            PersistenceMode::ForcePushOnly => true,
        }
    }
}
