//! **依赖装配（Wiring）**：按 DDD 组合根惯例拆分为配置源、Hook 运行时、能力扩展栈，再由 `initialize` 串联。
//!
//! 不包含 gRPC `Server::serve` 循环（见 [`super::bootstrap::ApplicationBootstrap`]）。

mod capability_extension;
mod config_sources;
mod hook_runtime;

use std::sync::Arc;

use anyhow::Result;

use crate::infrastructure::capability::PluginRouteBook;

use crate::composition::process_config::CapabilityServiceConfig;
use crate::composition::runtime_context::ApplicationContext;

pub use capability_extension::init_capability_extension_stack;

/// 构建完整 [`ApplicationContext`]（配置 → Hook → Capability）。
pub(crate) async fn initialize(config: CapabilityServiceConfig) -> Result<ApplicationContext> {
    let sources = config_sources::prepare_config_sources(&config).await?;
    let hook = hook_runtime::build_hook_runtime(&sources);

    let plugin_routes = Arc::new(PluginRouteBook::new());

    let db_pool = sources
        .hook_config_repository
        .as_ref()
        .map(|r| r.connection_pool());
    let (capability_registry, capability_policy, capability_grpc, strom_sfu_rtc) =
        init_capability_extension_stack(db_pool, hook.hook_governance.clone(), Arc::clone(&plugin_routes))
            .await?;

    Ok(ApplicationContext {
        im_hook_plugin: hook.im_hook_plugin,
        hook_governance: hook.hook_governance,
        plugin_routes,
        strom_sfu_rtc,
        capability_registry,
        capability_policy,
        capability_grpc,
    })
}
