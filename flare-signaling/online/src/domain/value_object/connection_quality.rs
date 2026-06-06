//! ConnectionQuality 值对象
//!
//! 链接质量指标，用于智能路由和最优设备选择
//!
//! 设计参考：微信智能路由、Telegram多DC选择
//! - RTT（往返时延）：网络延迟
//! - 丢包率：网络稳定性
//! - 网络类型：wifi > 5g > 4g > 3g
//! - 信号强度：移动网络质量

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// 链接质量值对象
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionQuality {
    rtt_ms: i64,                    // 往返时延（毫秒）
    packet_loss_rate: f64,          // 丢包率（0.0-1.0）
    last_measure_ts: DateTime<Utc>, // 最后测量时间
    network_type: NetworkType,      // 网络类型
    signal_strength: Option<i32>,   // 信号强度（dBm，仅移动网络）
}

/// 网络类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkType {
    Ethernet,
    Wifi,
    FiveG,
    FourG,
    ThreeG,
    Unknown,
}

/// 质量等级（用于快速判断）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLevel {
    Poor = 1,      // RTT >= 200ms or Loss >= 3%
    Fair = 2,      // RTT < 200ms, Loss < 3%
    Good = 3,      // RTT < 100ms, Loss < 1%
    Excellent = 4, // RTT < 50ms, Loss < 0.1%
}

impl ConnectionQuality {
    /// 创建链接质量对象
    pub fn new(
        rtt_ms: i64,
        packet_loss_rate: f64,
        network_type: NetworkType,
        signal_strength: Option<i32>,
    ) -> Result<Self, String> {
        if rtt_ms < 0 {
            return Err("RTT cannot be negative".to_string());
        }

        if !(0.0..=1.0).contains(&packet_loss_rate) {
            return Err("Packet loss rate must be between 0.0 and 1.0".to_string());
        }

        Ok(Self {
            rtt_ms,
            packet_loss_rate,
            last_measure_ts: Utc::now(),
            network_type,
            signal_strength,
        })
    }

    /// 从 Proto 消息创建（用于适配层）
    pub fn from_proto(proto: &flare_proto::common::ConnectionQuality) -> Result<Self, String> {
        let network_type = NetworkType::from_string(&proto.network_type);
        let signal_strength = if proto.signal_strength == 0 {
            None
        } else {
            Some(proto.signal_strength)
        };

        Self::new(
            proto.rtt_ms,
            proto.packet_loss_rate,
            network_type,
            signal_strength,
        )
    }

    /// 获取 RTT
    pub fn rtt_ms(&self) -> i64 {
        self.rtt_ms
    }

    /// 获取丢包率
    pub fn packet_loss_rate(&self) -> f64 {
        self.packet_loss_rate
    }

    /// 获取网络类型
    pub fn network_type(&self) -> NetworkType {
        self.network_type
    }

    /// 获取最后测量时间
    pub fn last_measure_ts(&self) -> DateTime<Utc> {
        self.last_measure_ts
    }
    /// 获取信号强度
    pub fn signal_strength(&self) -> Option<i32> {
        self.signal_strength
    }

    ///
    /// 分级规则（参考微信、Telegram）：
    /// - Excellent: RTT < 50ms, Loss < 0.1%
    /// - Good: RTT < 100ms, Loss < 1%
    /// - Fair: RTT < 200ms, Loss < 3%
    /// - Poor: 其他
    pub fn quality_level(&self) -> QualityLevel {
        if self.rtt_ms < 50 && self.packet_loss_rate < 0.001 {
            QualityLevel::Excellent
        } else if self.rtt_ms < 100 && self.packet_loss_rate < 0.01 {
            QualityLevel::Good
        } else if self.rtt_ms < 200 && self.packet_loss_rate < 0.03 {
            QualityLevel::Fair
        } else {
            QualityLevel::Poor
        }
    }

    /// 计算质量评分（0-100）
    ///
    /// 评分算法：
    /// - RTT 占70%权重
    /// - 丢包率占30%权重
    /// - 网络类型作为基础加成
    pub fn quality_score(&self) -> f64 {
        // RTT 评分 (0-70分)
        let rtt_score = if self.rtt_ms < 50 {
            70.0 - (self.rtt_ms as f64 / 5.0) // 60-70分
        } else if self.rtt_ms < 100 {
            60.0 - ((self.rtt_ms - 50) as f64 / 2.5) // 40-60分
        } else if self.rtt_ms < 200 {
            40.0 - ((self.rtt_ms - 100) as f64 / 3.33) // 10-40分
        } else {
            10.0 - ((self.rtt_ms - 200).min(200) as f64 / 20.0) // 0-10分
        };

        // 丢包率评分 (0-30分)
        let loss_score = 30.0 - (self.packet_loss_rate * 3000.0).min(30.0);

        // 网络类型加成 (0-10分)
        let network_bonus = match self.network_type {
            NetworkType::Ethernet => 10.0,
            NetworkType::Wifi => 8.0,
            NetworkType::FiveG => 6.0,
            NetworkType::FourG => 4.0,
            NetworkType::ThreeG => 2.0,
            NetworkType::Unknown => 0.0,
        };

        (rtt_score + loss_score + network_bonus).clamp(0.0, 100.0)
    }

    /// 判断是否健康（Good 或更好）
    pub fn is_healthy(&self) -> bool {
        matches!(
            self.quality_level(),
            QualityLevel::Good | QualityLevel::Excellent
        )
    }

    /// 判断质量是否过期（超过30秒）
    pub fn is_stale(&self) -> bool {
        let now = Utc::now();
        let elapsed = now.signed_duration_since(self.last_measure_ts);
        elapsed.num_seconds() > 30
    }
}

impl NetworkType {
    /// 从字符串解析网络类型
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "ethernet" => Self::Ethernet,
            "wifi" => Self::Wifi,
            "5g" => Self::FiveG,
            "4g" => Self::FourG,
            "3g" => Self::ThreeG,
            _ => Self::Unknown,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ethernet => "ethernet",
            Self::Wifi => "wifi",
            Self::FiveG => "5g",
            Self::FourG => "4g",
            Self::ThreeG => "3g",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for QualityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Excellent => write!(f, "Excellent"),
            Self::Good => write!(f, "Good"),
            Self::Fair => write!(f, "Fair"),
            Self::Poor => write!(f, "Poor"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quality_creation() {
        let quality = ConnectionQuality::new(50, 0.01, NetworkType::Wifi, None).unwrap();

        assert_eq!(quality.rtt_ms(), 50);
        assert_eq!(quality.packet_loss_rate(), 0.01);
    }

    #[test]
    fn test_quality_level() {
        let excellent = ConnectionQuality::new(30, 0.0005, NetworkType::Wifi, None).unwrap();
        assert_eq!(excellent.quality_level(), QualityLevel::Excellent);

        let poor = ConnectionQuality::new(300, 0.05, NetworkType::ThreeG, None).unwrap();
        assert_eq!(poor.quality_level(), QualityLevel::Poor);
    }

    #[test]
    fn test_quality_score() {
        let quality = ConnectionQuality::new(50, 0.01, NetworkType::Wifi, None).unwrap();
        let score = quality.quality_score();

        assert!(score > 60.0 && score < 100.0);
    }

    #[test]
    fn test_network_type_parsing() {
        assert_eq!(NetworkType::from_string("wifi"), NetworkType::Wifi);
        assert_eq!(NetworkType::from_string("5G"), NetworkType::FiveG);
        assert_eq!(NetworkType::from_string("unknown"), NetworkType::Unknown);
    }
}
