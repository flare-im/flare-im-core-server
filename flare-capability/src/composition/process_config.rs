//! 进程级 **启动配置**（Composition Root 输入 DTO，非领域模型）。
//!
//! 与 `flare_im_core_service_kit::config::CapabilityServiceConfig`（全局应用配置中的片段）区分：此处仅描述 **flare-capability 二进制** 的本地覆盖项。

/// `flare-capability` 进程启动配置。
#[derive(Debug, Clone)]
pub struct CapabilityServiceConfig {
    /// 能力运行时配置文件（用于 capability_runtime）
    pub runtime_config_file: Option<std::path::PathBuf>,
    /// 数据库 URL（可选，Hook 治理、策略与插件登记都在这个库）
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
    /// Redis of the tenant notice channel: `FLARE_CAPABILITY_NOTICE_REDIS_URL`, else the
    /// `token_store` profile (the one the gateways subscribe to). None: no notices.
    pub notice_redis_url: Option<String>,
    /// `{token_store namespace}:notice`.
    pub notice_channel: String,
    /// Services that publish tenant config notices and their topic namespaces:
    /// `FLARE_TENANT_NOTICE_PUBLISHERS` (see [`crate::domain::model::NoticePublishers`]).
    pub notice_publishers: crate::domain::model::NoticePublishers,
}

impl Default for CapabilityServiceConfig {
    fn default() -> Self {
        Self {
            runtime_config_file: Some(std::path::PathBuf::from("config/services/capability.toml")),
            database_url: None,
            postgres_max_connections: 10,
            postgres_min_connections: 2,
            postgres_acquire_timeout_seconds: 10,
            postgres_idle_timeout_seconds: 300,
            postgres_max_lifetime_seconds: 1800,
            notice_redis_url: None,
            notice_channel: flare_im_core_service_kit::tenant_notice::notice_channel("flare"),
            notice_publishers: crate::domain::model::NoticePublishers::default(),
        }
    }
}
