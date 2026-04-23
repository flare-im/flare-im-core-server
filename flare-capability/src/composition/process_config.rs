//! 进程级 **启动配置**（Composition Root 输入 DTO，非领域模型）。
//!
//! 与 `flare_im_core::config::CapabilityServiceConfig`（全局应用配置中的片段）区分：此处仅描述 **flare-capability 二进制** 的本地覆盖项。

/// `flare-capability` 进程启动配置（Hook 配置源、执行模式等）。
#[derive(Debug, Clone)]
pub struct CapabilityServiceConfig {
    /// 配置文件路径（可选，最低优先级）
    pub config_file: Option<std::path::PathBuf>,
    /// 数据库 URL（可选，最高优先级，用于动态 API 配置）
    pub database_url: Option<String>,
    /// 配置中心端点（可选，`etcd://` / `consul://`）
    pub config_center_endpoint: Option<String>,
    /// 租户 ID（可选，用于多租户场景）
    pub tenant_id: Option<String>,
    /// 执行模式（串行 / 并发）
    pub execution_mode: crate::domain::model::ExecutionMode,
    /// 配置刷新间隔（秒）
    pub refresh_interval_secs: u64,
}

impl Default for CapabilityServiceConfig {
    fn default() -> Self {
        Self {
            config_file: Some(std::path::PathBuf::from("config/hooks.toml")),
            database_url: None,
            config_center_endpoint: None,
            tenant_id: None,
            execution_mode: crate::domain::model::ExecutionMode::Sequential,
            refresh_interval_secs: 60,
        }
    }
}
