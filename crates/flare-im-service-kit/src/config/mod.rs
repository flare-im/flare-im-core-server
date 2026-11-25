//! Flare IM Core 配置模块
//!
//! 该模块提供了完整的应用程序配置管理功能，包括：
//! - 配置文件加载和解析
//! - 环境特定配置覆盖
//! - 各种服务配置定义
//! - 对象存储、数据库、消息队列等基础设施配置
//!
//! 按内聚拆分为四个子模块（原单文件 2,621 行）：
//! - [`types`]：基础设施数据类型（连接池、消息队列、对象存储、运行时）
//! - [`services`]：各服务专属配置与 `ServicesConfig` 聚合
//! - [`loader`]：`FlareAppConfig` 主聚合、全局实例与加载入口
//! - [`manager`]：配置管理器
//!
//! pub API 与拆分前完全一致（经本模块 re-export），调用方无需改动。

pub use flare_server_core::auth::{
    AuthProviderConfig, TrustedIssuerConfig as TrustedTokenIssuerConfig,
};

mod loader;
mod manager;
mod services;
mod types;

#[cfg(test)]
mod tests;

pub use loader::{FlareAppConfig, app_config, load_config, load_config_with_validation};
// tests.rs 经 use super::* 访问的 crate 内私有入口（仅测试构建需要）
#[cfg(test)]
pub(crate) use loader::{
    apply_registry_mode_override, config_candidates, default_config,
    expand_env_placeholders_in_value_with, load_config_from_source,
};
pub use manager::ConfigManager;
pub use services::*;
pub use types::*;
