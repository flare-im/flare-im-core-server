//! 进程级 **启动配置**（Composition Root 输入 DTO，非领域模型）。
//!
//! 与 `flare_im_service_kit::config::CapabilityServiceConfig`（全局应用配置中的片段）区分：此处仅描述 **flare-capability 二进制** 的本地覆盖项。

/// `flare-capability` 进程启动配置（Hook 配置源、执行模式等）。
#[derive(Debug, Clone)]
pub struct CapabilityServiceConfig {
    /// 配置文件路径（可选，最低优先级）
    pub config_file: Option<std::path::PathBuf>,
    /// 能力运行时配置文件（用于 capability_runtime / plugin_discovery_endpoints）
    pub runtime_config_file: Option<std::path::PathBuf>,
    /// 数据库 URL（可选，最高优先级，用于动态 API 配置）
    pub database_url: Option<String>,
    /// PostgreSQL 最大连接数
    pub postgres_max_connections: u32,
    /// PostgreSQL 最小连接数
    pub postgres_min_connections: u32,
    /// PostgreSQL 获取连接超时（秒）
    pub postgres_acquire_timeout_seconds: u64,
    /// PostgreSQL 空闲连接超时（秒）
    pub postgres_idle_timeout_seconds: u64,
    /// PostgreSQL 连接最大生命周期（秒）
    pub postgres_max_lifetime_seconds: u64,
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
            runtime_config_file: Some(std::path::PathBuf::from("config/services/capability.toml")),
            database_url: None,
            postgres_max_connections: 10,
            postgres_min_connections: 2,
            postgres_acquire_timeout_seconds: 10,
            postgres_idle_timeout_seconds: 300,
            postgres_max_lifetime_seconds: 1800,
            config_center_endpoint: None,
            tenant_id: None,
            execution_mode: crate::domain::model::ExecutionMode::Sequential,
            refresh_interval_secs: 60,
        }
    }
}
