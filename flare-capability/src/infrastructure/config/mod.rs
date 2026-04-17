//! # Hook配置管理
//!
//! 提供Hook配置的加载、合并、验证和监听能力
//!
//! 支持三种配置方式（按优先级排序）：
//! 1. **动态API配置**（最高优先级）：存储在数据库中，通过API动态管理
//! 2. **配置中心配置**：存储在etcd/Consul中，支持多租户
//! 3. **配置文件配置**（最低优先级）：存储在本地TOML文件中

pub mod capability_runtime;
pub mod loader;
pub mod watcher;

pub use capability_runtime::CapabilityRuntimeConfig;
pub use loader::{
    ConfigCenterLoader, ConfigLoader, ConfigMerger, ConfigValidator, DatabaseConfigLoader,
    FileConfigLoader,
};
pub use watcher::ConfigWatcher;
