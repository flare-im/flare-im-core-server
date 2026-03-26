//! 路由目标实体
//!
//! RouteTarget 表示设备路由信息，用于推送目标选择

use serde::{Deserialize, Serialize};

/// 路由目标实体
///
/// 包含设备的完整路由信息，用于推送和消息路由
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteTarget {
    /// 用户 ID
    user_id: String,
    /// 设备 ID
    device_id: String,
    /// 设备平台（ios/android/web/pc）
    device_platform: String,
    /// 设备所在 Gateway ID
    gateway_id: String,
    /// Gateway 所在 Server ID
    server_id: String,
    /// 设备优先级
    priority: DevicePriority,
    /// 质量分 0–100
    quality_score: f64,
}

impl RouteTarget {
    /// 创建新的路由目标
    pub fn new(
        user_id: String,
        device_id: String,
        device_platform: String,
        gateway_id: String,
        server_id: String,
        priority: DevicePriority,
        quality_score: f64,
    ) -> Self {
        Self {
            user_id,
            device_id,
            device_platform,
            gateway_id,
            server_id,
            priority,
            quality_score: quality_score.clamp(0.0, 100.0),
        }
    }

    /// 计算综合评分
    ///
    /// 评分公式：优先级权重 0.6 + 质量分权重 0.4
    pub fn calculate_score(&self) -> f64 {
        let priority_score = self.priority.as_score();
        let quality_score = self.quality_score / 100.0;
        priority_score * 0.6 + quality_score * 0.4
    }

    /// 获取用户 ID
    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    /// 获取设备 ID
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// 获取设备平台
    pub fn device_platform(&self) -> &str {
        &self.device_platform
    }

    /// 获取 Gateway ID
    pub fn gateway_id(&self) -> &str {
        &self.gateway_id
    }

    /// 获取 Server ID
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// 获取优先级
    pub fn priority(&self) -> &DevicePriority {
        &self.priority
    }

    /// 获取质量分
    pub fn quality_score(&self) -> f64 {
        self.quality_score
    }

    /// 获取设备标识（user_id:device_id）
    pub fn device_identifier(&self) -> String {
        format!("{}:{}", self.user_id, self.device_id)
    }
}

/// 设备优先级枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DevicePriority {
    /// 高优先级
    High,
    /// 正常优先级
    Normal,
    /// 低优先级
    Low,
}

impl DevicePriority {
    /// 转换为评分（0.0-1.0）
    pub fn as_score(&self) -> f64 {
        match self {
            DevicePriority::High => 1.0,
            DevicePriority::Normal => 0.6,
            DevicePriority::Low => 0.3,
        }
    }
}

impl std::fmt::Display for DevicePriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DevicePriority::High => write!(f, "High"),
            DevicePriority::Normal => write!(f, "Normal"),
            DevicePriority::Low => write!(f, "Low"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_target_creation() {
        let target = RouteTarget::new(
            "user-123".to_string(),
            "device-456".to_string(),
            "ios".to_string(),
            "gateway-789".to_string(),
            "server-001".to_string(),
            DevicePriority::High,
            95.0,
        );

        assert_eq!(target.user_id(), "user-123");
        assert_eq!(target.device_id(), "device-456");
        assert_eq!(target.device_platform(), "ios");
        assert_eq!(target.gateway_id(), "gateway-789");
        assert_eq!(target.server_id(), "server-001");
        assert_eq!(target.priority(), &DevicePriority::High);
        assert_eq!(target.quality_score(), 95.0);
    }

    #[test]
    fn test_calculate_score() {
        let high_priority_high_quality = RouteTarget::new(
            "user-1".to_string(),
            "device-1".to_string(),
            "ios".to_string(),
            "gateway-1".to_string(),
            "server-1".to_string(),
            DevicePriority::High,
            100.0,
        );
        // 1.0 * 0.6 + 1.0 * 0.4 = 1.0
        assert!((high_priority_high_quality.calculate_score() - 1.0).abs() < 0.001);

        let normal_priority_medium_quality = RouteTarget::new(
            "user-2".to_string(),
            "device-2".to_string(),
            "android".to_string(),
            "gateway-2".to_string(),
            "server-2".to_string(),
            DevicePriority::Normal,
            50.0,
        );
        // 0.6 * 0.6 + 0.5 * 0.4 = 0.36 + 0.2 = 0.56
        assert!((normal_priority_medium_quality.calculate_score() - 0.56).abs() < 0.001);

        let low_priority_low_quality = RouteTarget::new(
            "user-3".to_string(),
            "device-3".to_string(),
            "web".to_string(),
            "gateway-3".to_string(),
            "server-3".to_string(),
            DevicePriority::Low,
            0.0,
        );
        // 0.3 * 0.6 + 0.0 * 0.4 = 0.18
        assert!((low_priority_low_quality.calculate_score() - 0.18).abs() < 0.001);
    }

    #[test]
    fn test_device_identifier() {
        let target = RouteTarget::new(
            "user-123".to_string(),
            "device-456".to_string(),
            "ios".to_string(),
            "gateway-789".to_string(),
            "server-001".to_string(),
            DevicePriority::High,
            95.0,
        );

        assert_eq!(target.device_identifier(), "user-123:device-456");
    }

    #[test]
    fn test_quality_score_clamp() {
        let target_over = RouteTarget::new(
            "user-1".to_string(),
            "device-1".to_string(),
            "ios".to_string(),
            "gateway-1".to_string(),
            "server-1".to_string(),
            DevicePriority::High,
            150.0, // 超过 100
        );
        assert_eq!(target_over.quality_score(), 100.0);

        let target_under = RouteTarget::new(
            "user-2".to_string(),
            "device-2".to_string(),
            "ios".to_string(),
            "gateway-2".to_string(),
            "server-2".to_string(),
            DevicePriority::High,
            -10.0, // 小于 0
        );
        assert_eq!(target_under.quality_score(), 0.0);
    }
}
