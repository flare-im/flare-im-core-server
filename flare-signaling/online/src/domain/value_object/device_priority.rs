//! DevicePriority 值对象
//!
//! 设备优先级，用于多设备推送策略
//!
//! 优先级定义（参考微信、飞书）：
//! - Critical(4): 关键优先级，强制推送（忽略免打扰）
//! - High(3): 高优先级，用户当前正在使用的设备
//! - Normal(2): 普通优先级（默认）
//! - Low(1): 低优先级，后台/离线设备
//! - Unspecified(0): 未指定

use serde::{Deserialize, Serialize};
use std::fmt;

/// 设备优先级值对象
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum DevicePriority {
    Unspecified = 0,
    Low = 1,
    #[default]
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl DevicePriority {
    /// 从 i32 创建优先级
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Unspecified,
            1 => Self::Low,
            2 => Self::Normal,
            3 => Self::High,
            4 => Self::Critical,
            _ => Self::Normal, // 默认值
        }
    }

    /// 转换为 i32
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }

    /// 判断是否为高优先级（High 或 Critical）
    pub fn is_high_priority(&self) -> bool {
        matches!(self, Self::High | Self::Critical)
    }

    /// 判断是否应该接收推送（排除 Low）
    pub fn should_receive_push(&self) -> bool {
        !matches!(self, Self::Low)
    }

    /// 判断是否忽略免打扰
    pub fn ignores_do_not_disturb(&self) -> bool {
        matches!(self, Self::Critical)
    }
}

impl fmt::Display for DevicePriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unspecified => write!(f, "Unspecified"),
            Self::Low => write!(f, "Low"),
            Self::Normal => write!(f, "Normal"),
            Self::High => write!(f, "High"),
            Self::Critical => write!(f, "Critical"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(DevicePriority::Critical > DevicePriority::High);
        assert!(DevicePriority::High > DevicePriority::Normal);
        assert!(DevicePriority::Normal > DevicePriority::Low);
    }

    #[test]
    fn test_priority_conversion() {
        assert_eq!(DevicePriority::from_i32(3), DevicePriority::High);
        assert_eq!(DevicePriority::High.as_i32(), 3);
    }

    #[test]
    fn test_priority_methods() {
        assert!(DevicePriority::Critical.is_high_priority());
        assert!(!DevicePriority::Low.should_receive_push());
        assert!(DevicePriority::Critical.ignores_do_not_disturb());
    }
}
