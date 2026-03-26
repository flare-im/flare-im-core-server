//! 事件类型常量定义
//!
//! 定义 IM 系统中六大类事件类型,用于构建 EventEnvelope
//! 具体操作类型由事件 payload 中的字段定义

/// 事件类型常量
pub mod types {
    /// 消息事件
    pub const MESSAGE: &str = "message";
    
    /// 事件
    pub const EVENT: &str = "event";
    
    /// ACK
    pub const ACK: &str = "ack";
    
    /// 通知
    pub const NOTIFICATION: &str = "notification";
    
    /// 自定义数据
    pub const CUSTOM: &str = "custom";
    
    /// 系统消息
    pub const SYSTEM: &str = "system";
}

/// 辅助函数：判断事件类型

/// 检查是否为消息事件
pub fn is_message_event(event_type: &str) -> bool {
    event_type == types::MESSAGE
}

/// 检查是否为操作事件
pub fn is_event(event_type: &str) -> bool {
    event_type == types::EVENT
}

/// 检查是否为 ACK 事件
pub fn is_ack_event(event_type: &str) -> bool {
    event_type == types::ACK
}

/// 检查是否为通知事件
pub fn is_notification_event(event_type: &str) -> bool {
    event_type == types::NOTIFICATION
}

/// 检查是否为自定义数据事件
pub fn is_custom_event(event_type: &str) -> bool {
    event_type == types::CUSTOM
}

/// 检查是否为系统事件
pub fn is_system_event(event_type: &str) -> bool {
    event_type == types::SYSTEM
}



