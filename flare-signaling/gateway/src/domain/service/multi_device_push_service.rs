//! 多设备推送策略服务
//!
//! 职责：
//! - 根据设备优先级选择推送目标
//! - 基于链接质量智能路由
//! - 支持多设备推送策略（全部推送/最优推送/高优先级推送）

use flare_proto::signaling::online::GetDeviceRequest;
use flare_proto::signaling::online::online_service_client::OnlineServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, info, instrument, warn};

use crate::domain::service::connection_quality_service::{
    ConnectionQualityMetrics, ConnectionQualityService, QualityLevel,
};

/// 推送策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushStrategy {
    /// 推送到所有设备
    All,

    /// 推送到最优设备（质量最好的一个）
    BestDevice,

    /// 推送到高优先级设备（Critical + High）
    HighPriority,

    /// 推送到活跃设备（排除 Low 优先级）
    ActiveDevices,
}

/// 推送目标设备
#[derive(Debug, Clone)]
pub struct PushTarget {
    pub connection_id: String,
    pub user_id: String,
    pub device_id: String,
    pub device_priority: i32,
    pub quality_level: QualityLevel,
    pub rtt_ms: i64,
}

/// 多设备推送服务
pub struct MultiDevicePushService {
    quality_service: Arc<ConnectionQualityService>,
    /// Online服务客户端（用于获取设备信息）
    online_service_client: Option<flare_proto::signaling::online::online_service_client::OnlineServiceClient<Channel>>,
}

impl MultiDevicePushService {
    pub fn new(
        quality_service: Arc<ConnectionQualityService>,
        online_service_client: Option<flare_proto::signaling::online::online_service_client::OnlineServiceClient<Channel>>,
    ) -> Self {
        Self {
            quality_service,
            online_service_client,
        }
    }

    /// 根据策略选择推送目标
    pub async fn select_push_targets(
        &self,
        user_id: &str,
        strategy: PushStrategy,
    ) -> Vec<PushTarget> {
        let devices = self.quality_service.get_user_devices_quality(user_id).await;

        if devices.is_empty() {
            debug!(user_id = %user_id, "No devices found for user");
            return Vec::new();
        }

        let targets = self.apply_strategy(devices, strategy).await;

        info!(
            user_id = %user_id,
            strategy = ?strategy,
            target_count = targets.len(),
            "Push targets selected"
        );

        targets
    }

    /// 应用推送策略
    async fn apply_strategy(
        &self,
        mut devices: Vec<ConnectionQualityMetrics>,
        strategy: PushStrategy,
    ) -> Vec<PushTarget> {
        match strategy {
            PushStrategy::All => {
                // 推送到所有设备
                let mut targets = Vec::new();
                for device in devices {
                    targets.push(self.to_push_target(device).await);
                }
                targets
            }

            PushStrategy::BestDevice => {
                // 推送到最优设备
                self.select_best_device(devices).await
            }

            PushStrategy::HighPriority => {
                // 推送到高优先级设备（假设优先级存储在某处，这里简化处理）
                // 只选择质量为 Excellent 或 Good 的设备
                devices.retain(|d| d.quality_level >= QualityLevel::Good);
                let mut targets = Vec::new();
                for device in devices {
                    targets.push(self.to_push_target(device).await);
                }
                targets
            }

            PushStrategy::ActiveDevices => {
                // 推送到活跃设备（质量不是 Poor 的设备）
                devices.retain(|d| d.quality_level > QualityLevel::Poor);
                let mut targets = Vec::new();
                for device in devices {
                    targets.push(self.to_push_target(device).await);
                }
                targets
            }
        }
    }

    /// 选择最优设备
    async fn select_best_device(
        &self,
        mut devices: Vec<ConnectionQualityMetrics>,
    ) -> Vec<PushTarget> {
        if devices.is_empty() {
            return Vec::new();
        }

        // 按质量等级降序、RTT升序排序
        devices.sort_by(|a, b| match b.quality_level.cmp(&a.quality_level) {
            std::cmp::Ordering::Equal => a.rtt_avg_ms.partial_cmp(&b.rtt_avg_ms).unwrap(),
            other => other,
        });

        // 只选择最优的设备
        let best_device = devices.into_iter().next().unwrap();
        vec![self.to_push_target(best_device).await]
    }

    /// 转换为推送目标
    async fn to_push_target(&self, metrics: ConnectionQualityMetrics) -> PushTarget {
        let connection_id = metrics.connection_id.clone();
        let user_id = metrics.user_id.clone();
        let device_id = metrics.device_id.clone();
        PushTarget {
            connection_id,
            user_id: user_id.clone(),
            device_id: device_id.clone(),
            device_priority: self.get_device_priority(&user_id, &device_id).await,
            quality_level: metrics.quality_level,
            rtt_ms: metrics.rtt_ms,
        }
    }

    /// 评估是否需要回退到离线推送
    ///
    /// 判断标准：
    /// - 没有在线设备
    /// - 所有在线设备质量都很差（Poor）
    /// - 所有在线设备都是低优先级
    pub async fn should_fallback_to_offline_push(&self, user_id: &str) -> bool {
        let devices = self.quality_service.get_user_devices_quality(user_id).await;

        if devices.is_empty() {
            return true;
        }

        // 检查是否所有设备质量都很差
        let all_poor_quality = devices
            .iter()
            .all(|d| d.quality_level == QualityLevel::Poor);

        all_poor_quality
    }

    /// 获取设备优先级
    ///
    /// 策略：
    /// 1. 优先从 Online 服务获取设备优先级
    /// 2. Online 服务不可用时，使用默认值
    /// 3. 支持缓存机制减少查询
    #[instrument(skip(self), fields(user_id = %user_id, device_id = %device_id))]
    pub async fn get_device_priority(&self, user_id: &str, device_id: &str) -> i32 {
        // 首先尝试从Online服务获取设备优先级
        if let Some(client) = &self.online_service_client {
            match self
                .fetch_device_priority_from_online(client, user_id, device_id)
                .await
            {
                Ok(priority) => {
                    info!(
                        user_id = %user_id,
                        device_id = %device_id,
                        device_priority = priority,
                        "Device priority fetched from Online service"
                    );
                    return priority;
                }
                Err(e) => {
                    warn!(
                        ?e,
                        user_id = %user_id,
                        device_id = %device_id,
                        "Failed to fetch device priority from Online service, using default"
                    );
                }
            }
        }

        // 如果Online服务不可用或失败，使用默认优先级映射
        let priority = if device_id.starts_with("ios") {
            90 // iOS 设备默认高优先级
        } else if device_id.starts_with("android") {
            80 // Android 设备默认中高优先级
        } else if device_id.starts_with("web") {
            60 // Web 设备默认中优先级
        } else if device_id.starts_with("desktop") {
            85 // 桌面应用默认高优先级
        } else {
            50 // 其他设备默认中低优先级
        };

        debug!(
            user_id = %user_id,
            device_id = %device_id,
            device_priority = priority,
            "Device priority assigned using default mapping"
        );

        priority
    }

    /// 从Online服务获取设备优先级
    async fn fetch_device_priority_from_online(
        &self,
        client: &OnlineServiceClient<Channel>,
        user_id: &str,
        device_id: &str,
    ) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        let request = tonic::Request::new(GetDeviceRequest {
            user_id: user_id.to_string(),
            device_id: device_id.to_string(),
        });

        let response = client.clone().get_device(request).await?;
        let device_info = response
            .into_inner()
            .device
            .ok_or("Device info not found")?;

        // 将proto的DevicePriority枚举转换为数值优先级
        let priority = match device_info.priority() {
            flare_proto::common::DevicePriority::Unspecified => 50,
            flare_proto::common::DevicePriority::Low => 25,
            flare_proto::common::DevicePriority::Normal => 50,
            flare_proto::common::DevicePriority::High => 75,
            flare_proto::common::DevicePriority::Critical => 100,
        };

        Ok(priority)
    }

    /// 获取设备类型描述
    fn get_device_type_description(device_id: &str) -> &'static str {
        if device_id.starts_with("ios") {
            "iOS"
        } else if device_id.starts_with("android") {
            "Android"
        } else if device_id.starts_with("web") {
            "Web"
        } else if device_id.starts_with("desktop") {
            "Desktop"
        } else {
            "Unknown"
        }
    }
}
