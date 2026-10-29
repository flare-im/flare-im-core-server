//! 应用层 **Handler**：用例编排（委托领域服务 / 读端口），不包含具体插件、Hook 执行细节。

mod capability_dispatch;
mod hook_command;
mod hook_query;
mod remote_dispatch;

pub use capability_dispatch::dispatch_capability_command;
pub use hook_command::HookCommandHandler;
pub use hook_query::HookQueryHandler;
pub(crate) use remote_dispatch::VerifiedDispatchOrigin;
pub(crate) use remote_dispatch::dispatch_remote;
pub mod admin_dispatch;
pub mod entity_lifecycle;
pub mod plugin_card;
pub mod plugin_state;
#[cfg(test)]
mod plugin_state_pg_tests;
#[cfg(test)]
mod plugin_test_server;
pub mod resource_bridge;
#[cfg(test)]
mod resource_bridge_realtime_tests;
#[cfg(test)]
mod resource_bridge_tests;
#[cfg(test)]
mod resource_bridge_write_tests;
