//! # Hook配置管理
//!
//! 提供Hook配置的加载、合并、验证和监听能力
//!
//! 支持三种配置方式（按优先级排序）：
//! 1. **动态API配置**（最高优先级）：存储在数据库中，通过API动态管理
//! 2. **配置中心配置**：存储在etcd/Consul中，支持多租户
//! 3. **配置文件配置**（最低优先级）：存储在本地TOML文件中
//!
//! 数据库中 `tenant_id IS NULL` 的行与文件、配置中心一样是全局配置；租户登记的行按租户分桶，
//! 只作用于该租户的事件（见 [`HookConfigSnapshot`]）。多个实例之间经 PostgreSQL
//! `LISTEN/NOTIFY` 互相通知写入（见 [`HookConfigInvalidation`]）。

pub mod capability_runtime;
pub mod invalidation;
pub mod loader;
pub mod snapshot;
pub mod watcher;

pub use capability_runtime::CapabilityRuntimeConfig;
pub use invalidation::{HOOK_CONFIG_CHANNEL, HookConfigInvalidation, HookConfigListener};
pub use loader::{
    ConfigCenterLoader, ConfigLoader, ConfigLoaderItem, ConfigMerger, ConfigValidator,
    DatabaseConfigLoader, FileConfigLoader, LoadedHookConfig,
};
pub use snapshot::HookConfigSnapshot;
pub use watcher::ConfigWatcher;
