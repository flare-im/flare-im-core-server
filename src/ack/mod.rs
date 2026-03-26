//! ACK处理模块
//! 整合ACK状态管理、Redis缓存、批量处理和异步归档功能

pub mod config;
pub mod metrics;
pub mod redis_manager;
pub mod service;
pub mod traits;

use crate::ack::metrics::AckMetrics;
use crate::ack::redis_manager::RedisAckManager;
use crate::ack::service::AckService;

use std::sync::Arc;

/// ACK处理模块（精简版）
///
/// 核心功能：
/// - ACK 状态管理（内存 + Redis）
/// - 批量处理
/// - 监控指标
pub struct AckModule {
    /// ACK服务（实现 AckManager trait）
    pub service: Arc<AckService>,
    /// Redis管理器
    pub redis_manager: Arc<RedisAckManager>,
    /// 监控指标（暴露给外部使用）
    pub metrics: Arc<AckMetrics>,
}

// 重新导出类型，方便外部使用
pub use config::AckServiceConfig;
pub use redis_manager::{AckStatus, AckStatusInfo, AckType, ImportanceLevel};
pub use traits::{AckEvent, AckManager, AckTimeoutEvent};

impl AckModule {
    /// 创建新的ACK处理模块（精简版）
    ///
    /// 不需要数据库连接，只使用 Redis 进行状态管理
    pub async fn new(
        ack_config: crate::ack::config::AckServiceConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // 使用全局的 Prometheus Registry（与其他服务指标统一）
        use crate::metrics::REGISTRY;
        let metrics = Arc::new(AckMetrics::new(&REGISTRY)?);

        // 创建ACK服务
        let service = Arc::new(AckService::new(ack_config, metrics.clone()).await?);

        // 获取Redis管理器引用
        let redis_manager = service.redis_manager.clone();

        Ok(Self {
            service,
            redis_manager,
            metrics, // 暴露 metrics 供外部使用
        })
    }

    /// 记录ACK状态（简化版，不归档）
    pub async fn record_ack_status(
        &self,
        ack_info: AckStatusInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.service.record_ack_internal(ack_info).await
    }

    /// 获取ACK状态
    pub async fn get_ack_status(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<AckStatusInfo>, Box<dyn std::error::Error>> {
        self.service.get_ack_status(message_id, user_id).await
    }

    /// 检查ACK是否存在
    pub async fn exists_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.service.exists_ack(message_id, user_id).await
    }

    /// 删除ACK状态
    pub async fn delete_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.service.delete_ack(message_id, user_id).await
    }

    /// 获取模块统计信息
    pub async fn get_stats(&self) -> Result<AckModuleStats, Box<dyn std::error::Error>> {
        let service_stats = self.service.get_stats().await?;

        Ok(AckModuleStats { service_stats })
    }
}


impl AckManager for AckModule {
    async fn record_ack(&self, event: AckEvent) -> Result<(), Box<dyn std::error::Error>> {
        // 调用 AckService 的 trait 实现
        AckManager::record_ack(self.service.as_ref(), event).await
    }

    async fn get_ack_status(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<crate::ack::redis_manager::AckStatusInfo>, Box<dyn std::error::Error>> {
        self.service.get_ack_status(message_id, user_id).await
    }

    async fn batch_get_ack_status(
        &self,
        acks: Vec<(String, String)>,
    ) -> Result<Vec<crate::ack::redis_manager::AckStatusInfo>, Box<dyn std::error::Error>> {
        self.service.batch_get_ack_status(acks).await
    }

    async fn exists_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.service.exists_ack(message_id, user_id).await
    }

    async fn delete_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.service.delete_ack(message_id, user_id).await
    }
}

/// ACK模块统计信息
#[derive(Debug, Clone)]
pub struct AckModuleStats {
    /// 服务统计信息
    pub service_stats: crate::ack::service::AckServiceStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_ack_module() -> Result<(), Box<dyn std::error::Error>> {
        // 注意：这需要一个运行中的Redis实例
        let ack_config = AckServiceConfig::default();

        let module = AckModule::new(ack_config).await?;

        let ack_info = crate::ack::redis_manager::AckStatusInfo {
            message_id: "test_msg_1".to_string(),
            user_id: "user_1".to_string(),
            ack_type: Some(crate::ack::redis_manager::AckType::TransportAck),
            status: crate::ack::redis_manager::AckStatus::Received,
            timestamp: 1234567890,
            importance: crate::ack::redis_manager::ImportanceLevel::High,
        };

        // 记录ACK
        module.record_ack_status(ack_info.clone()).await?;

        // 获取ACK状态
        let retrieved = module.get_ack_status("test_msg_1", "user_1").await?;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.message_id, "test_msg_1");
        assert_eq!(retrieved.user_id, "user_1");

        // 检查ACK是否存在
        let exists = module.exists_ack("test_msg_1", "user_1").await?;
        assert!(exists);

        // 获取统计信息
        let stats = module.get_stats().await?;
        assert!(stats.service_stats.cache_size >= 1);

        Ok(())
    }
}
