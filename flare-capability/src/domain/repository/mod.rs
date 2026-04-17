//! # Hook仓储接口
//!
//! 定义Hook配置的仓储接口

use crate::domain::model::HookConfig;

/// Hook配置仓储接口

pub trait HookConfigRepository: Send + Sync {
    /// 加载Hook配置
    async fn load(&self) -> anyhow::Result<HookConfig>;

    /// 保存Hook配置
    async fn save(&self, config: &HookConfig) -> anyhow::Result<()>;

    /// 监听配置变更
    async fn watch<F>(&self, callback: F) -> anyhow::Result<()>
    where
        F: Fn(HookConfig) + Send + Sync + 'static;
}
