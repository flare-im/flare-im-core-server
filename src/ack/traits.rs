//! ACK 管理统一接口
//! 提供统一的 ACK 能力，供各业务模块使用

use crate::ack::redis_manager::AckStatusInfo;

// 重新导出类型，方便外部使用
pub use crate::ack::redis_manager::{AckStatus, AckType, ImportanceLevel};

/// ACK 事件
#[derive(Debug, Clone)]
pub struct AckEvent {
    pub message_id: String,
    pub user_id: String,
    pub ack_type: AckType,
    pub status: AckStatus,
    pub timestamp: i64,
    pub importance: ImportanceLevel,
    pub metadata: Option<serde_json::Value>,
}

/// ACK 超时事件
#[derive(Debug, Clone)]
pub struct AckTimeoutEvent {
    pub message_id: String,
    pub user_id: String,
    pub ack_type: AckType,
    pub timeout_at: i64,
}

/// ACK 管理器 Trait
///
/// 提供统一的 ACK 状态管理能力，供各业务模块使用
pub trait AckManager: Send + Sync {
    /// 记录 ACK 事件
    async fn record_ack(&self, event: AckEvent) -> Result<(), Box<dyn std::error::Error>>;

    /// 查询 ACK 状态
    async fn get_ack_status(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<AckStatusInfo>, Box<dyn std::error::Error>>;

    /// 批量查询 ACK 状态
    async fn batch_get_ack_status(
        &self,
        acks: Vec<(String, String)>,
    ) -> Result<Vec<AckStatusInfo>, Box<dyn std::error::Error>>;

    /// 检查 ACK 是否存在
    async fn exists_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>>;

    /// 删除 ACK 状态
    async fn delete_ack(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
