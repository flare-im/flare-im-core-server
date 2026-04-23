//! `HookPlugin` 与 Hook 治理依赖的读侧（配置 watcher 视图）。

mod hook_service;
mod im_hook_plugin;

pub use hook_service::HookServiceServer;
pub use im_hook_plugin::ImHookPluginServer;
