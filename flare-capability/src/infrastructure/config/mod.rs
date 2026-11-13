//! 能力服务的运行时配置。Hook 配置本身在 `hook_configs`，由 Ingest 热加载，这里不读。

pub mod capability_runtime;
pub mod hook_retention;
pub mod plugin_config_key;
pub mod plugin_development;
pub mod plugin_payment_provider;
pub mod plugin_publishers;
pub mod plugin_web_bundles;

pub use capability_runtime::CapabilityRuntimeConfig;
pub use hook_retention::HookRetentionConfig;
