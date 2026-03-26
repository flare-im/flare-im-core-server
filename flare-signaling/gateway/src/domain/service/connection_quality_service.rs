//! 链接质量监控领域服务
//!
//! 职责：
//! - 监控连接的RTT（往返时延）
//! - 监控丢包率
//! - 评估网络质量
//! - 提供质量报告供路由决策使用

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::domain::model::{ConnectionQualityMetrics, QualityLevel};

/// 链接质量监控服务
pub struct ConnectionQualityService {
    // connection_id -> ConnectionQualityMetrics
    metrics: Arc<RwLock<HashMap<String, ConnectionQualityMetrics>>>,

    // 质量数据过期时间（默认5分钟）
    expiration: Duration,
}

impl ConnectionQualityService {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            expiration: Duration::from_secs(300),
        }
    }

    /// 记录心跳 RTT
    pub async fn record_heartbeat_rtt(
        &self,
        connection_id: &str,
        user_id: &str,
        device_id: &str,
        rtt_ms: i64,
    ) {
        let mut metrics_map = self.metrics.write().await;

        let metrics = metrics_map
            .entry(connection_id.to_string())
            .or_insert_with(|| ConnectionQualityMetrics {
                connection_id: connection_id.to_string(),
                user_id: user_id.to_string(),
                device_id: device_id.to_string(),
                rtt_ms,
                rtt_avg_ms: rtt_ms as f64,
                rtt_min_ms: rtt_ms,
                rtt_max_ms: rtt_ms,
                packet_loss_rate: 0.0,
                packets_sent: 0,
                packets_lost: 0,
                network_type: "unknown".to_string(),
                last_update: Instant::now(),
                quality_level: QualityLevel::Good,
            });

        // 更新 RTT 统计（使用滑动平均）
        metrics.rtt_ms = rtt_ms;
        metrics.rtt_avg_ms = metrics.rtt_avg_ms * 0.8 + rtt_ms as f64 * 0.2;
        metrics.rtt_min_ms = metrics.rtt_min_ms.min(rtt_ms);
        metrics.rtt_max_ms = metrics.rtt_max_ms.max(rtt_ms);
        metrics.last_update = Instant::now();

        // 重新计算质量等级
        metrics.quality_level =
            QualityLevel::from_metrics(metrics.rtt_avg_ms as i64, metrics.packet_loss_rate);

        debug!(
            connection_id = %connection_id,
            rtt_ms = rtt_ms,
            rtt_avg_ms = metrics.rtt_avg_ms,
            quality_level = ?metrics.quality_level,
            "RTT recorded"
        );
    }

    /// 记录丢包
    pub async fn record_packet_loss(
        &self,
        connection_id: &str,
        packets_sent: u64,
        packets_lost: u64,
    ) {
        let mut metrics_map = self.metrics.write().await;

        if let Some(metrics) = metrics_map.get_mut(connection_id) {
            metrics.packets_sent = packets_sent;
            metrics.packets_lost = packets_lost;

            // 计算丢包率
            if packets_sent > 0 {
                metrics.packet_loss_rate = packets_lost as f64 / packets_sent as f64;
            }

            metrics.last_update = Instant::now();

            // 重新计算质量等级
            metrics.quality_level =
                QualityLevel::from_metrics(metrics.rtt_avg_ms as i64, metrics.packet_loss_rate);

            if metrics.packet_loss_rate > 0.05 {
                warn!(
                    connection_id = %connection_id,
                    packet_loss_rate = metrics.packet_loss_rate,
                    "High packet loss detected"
                );
            }
        }
    }

    /// 更新网络类型
    pub async fn update_network_type(&self, connection_id: &str, network_type: String) {
        let mut metrics_map = self.metrics.write().await;

        if let Some(metrics) = metrics_map.get_mut(connection_id) {
            metrics.network_type = network_type;
            metrics.last_update = Instant::now();
        }
    }

    /// 获取连接质量
    pub async fn get_quality(&self, connection_id: &str) -> Option<ConnectionQualityMetrics> {
        let metrics_map = self.metrics.read().await;
        metrics_map.get(connection_id).cloned()
    }

    /// 获取用户所有设备的质量信息
    pub async fn get_user_devices_quality(&self, user_id: &str) -> Vec<ConnectionQualityMetrics> {
        let metrics_map = self.metrics.read().await;
        metrics_map
            .values()
            .filter(|m| m.user_id == user_id)
            .cloned()
            .collect()
    }

    /// 选择最优设备（基于质量等级和RTT）
    pub async fn select_best_device(&self, user_id: &str) -> Option<ConnectionQualityMetrics> {
        let mut devices = self.get_user_devices_quality(user_id).await;

        if devices.is_empty() {
            return None;
        }

        // 按质量等级降序、RTT升序排序
        devices.sort_by(|a, b| match b.quality_level.cmp(&a.quality_level) {
            std::cmp::Ordering::Equal => a.rtt_avg_ms.partial_cmp(&b.rtt_avg_ms).unwrap(),
            other => other,
        });

        devices.into_iter().next()
    }

    /// 移除连接质量记录
    pub async fn remove_connection(&self, connection_id: &str) {
        let mut metrics_map = self.metrics.write().await;
        metrics_map.remove(connection_id);
    }

    /// 清理过期数据
    pub async fn cleanup_expired(&self) {
        let mut metrics_map = self.metrics.write().await;
        let now = Instant::now();

        metrics_map.retain(|_, metrics| now.duration_since(metrics.last_update) < self.expiration);
    }
}

impl Default for ConnectionQualityService {
    fn default() -> Self {
        Self::new()
    }
}
