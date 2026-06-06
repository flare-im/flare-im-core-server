//! # Hook仓储接口
//!
//! 定义Hook配置的仓储接口

use crate::domain::model::HookConfig;

/// Hook配置仓储接口
pub trait HookConfigRepository: Send + Sync {
    /// 加载Hook配置
    async fn load(&self) -> flare_server_core::error::Result<HookConfig>;

    /// 保存Hook配置
    async fn save(&self, config: &HookConfig) -> flare_server_core::error::Result<()>;

    /// 监听配置变更
    async fn watch<F>(&self, callback: F) -> flare_server_core::error::Result<()>
    where
        F: Fn(HookConfig) + Send + Sync + 'static;
}
