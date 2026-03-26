//! 连接质量模型（值对象）

use std::time::Instant;

/// 连接质量指标
#[derive(Debug, Clone)]
pub struct ConnectionQualityMetrics {
    pub connection_id: String,
    pub user_id: String,
    pub device_id: String,
    pub rtt_ms: i64,
    pub rtt_avg_ms: f64,
    pub rtt_min_ms: i64,
    pub rtt_max_ms: i64,
    pub packet_loss_rate: f64,
    pub packets_sent: u64,
    pub packets_lost: u64,
    pub network_type: String,
    pub last_update: Instant,
    pub quality_level: QualityLevel,
}

/// 质量等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityLevel {
    Excellent = 4,
    Good = 3,
    Fair = 2,
    Poor = 1,
}

impl QualityLevel {
    pub fn from_metrics(rtt_ms: i64, packet_loss_rate: f64) -> Self {
        if rtt_ms < 50 && packet_loss_rate < 0.001 {
            QualityLevel::Excellent
        } else if rtt_ms < 100 && packet_loss_rate < 0.01 {
            QualityLevel::Good
        } else if rtt_ms < 200 && packet_loss_rate < 0.03 {
            QualityLevel::Fair
        } else {
            QualityLevel::Poor
        }
    }
}
