//! 推送策略枚举
//!
//! 定义不同的推送目标选择策略

use serde::{Deserialize, Serialize};

/// 推送策略枚举
///
/// 定义如何选择推送目标设备
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PushStrategy {
    /// 所有在线设备
    AllDevices,
    /// 最优单设备（优先级+质量）
    BestDevice,
    /// 活跃设备（排除 Low 优先级）
    ActiveDevices,
    /// 主设备（优先级最高）
    PrimaryDevice,
}

impl PushStrategy {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "all_devices" | "all" => Some(PushStrategy::AllDevices),
            "best_device" | "best" => Some(PushStrategy::BestDevice),
            "active_devices" | "active" => Some(PushStrategy::ActiveDevices),
            "primary_device" | "primary" => Some(PushStrategy::PrimaryDevice),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            PushStrategy::AllDevices => "all_devices",
            PushStrategy::BestDevice => "best_device",
            PushStrategy::ActiveDevices => "active_devices",
            PushStrategy::PrimaryDevice => "primary_device",
        }
    }
}

impl Default for PushStrategy {
    fn default() -> Self {
        PushStrategy::BestDevice
    }
}

impl std::fmt::Display for PushStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushStrategy::AllDevices => write!(f, "AllDevices"),
            PushStrategy::BestDevice => write!(f, "BestDevice"),
            PushStrategy::ActiveDevices => write!(f, "ActiveDevices"),
            PushStrategy::PrimaryDevice => write!(f, "PrimaryDevice"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            PushStrategy::from_str("all_devices"),
            Some(PushStrategy::AllDevices)
        );
        assert_eq!(
            PushStrategy::from_str("all"),
            Some(PushStrategy::AllDevices)
        );
        assert_eq!(
            PushStrategy::from_str("best_device"),
            Some(PushStrategy::BestDevice)
        );
        assert_eq!(
            PushStrategy::from_str("best"),
            Some(PushStrategy::BestDevice)
        );
        assert_eq!(
            PushStrategy::from_str("active_devices"),
            Some(PushStrategy::ActiveDevices)
        );
        assert_eq!(
            PushStrategy::from_str("active"),
            Some(PushStrategy::ActiveDevices)
        );
        assert_eq!(
            PushStrategy::from_str("primary_device"),
            Some(PushStrategy::PrimaryDevice)
        );
        assert_eq!(
            PushStrategy::from_str("primary"),
            Some(PushStrategy::PrimaryDevice)
        );
        assert_eq!(PushStrategy::from_str("invalid"), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(PushStrategy::AllDevices.as_str(), "all_devices");
        assert_eq!(PushStrategy::BestDevice.as_str(), "best_device");
        assert_eq!(PushStrategy::ActiveDevices.as_str(), "active_devices");
        assert_eq!(PushStrategy::PrimaryDevice.as_str(), "primary_device");
    }

    #[test]
    fn test_default() {
        assert_eq!(PushStrategy::default(), PushStrategy::BestDevice);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", PushStrategy::AllDevices), "AllDevices");
        assert_eq!(format!("{}", PushStrategy::BestDevice), "BestDevice");
        assert_eq!(format!("{}", PushStrategy::ActiveDevices), "ActiveDevices");
        assert_eq!(format!("{}", PushStrategy::PrimaryDevice), "PrimaryDevice");
    }
}
