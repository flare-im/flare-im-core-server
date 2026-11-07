//! 应用层 **Command**（写路径）：跨聚合的用例入口，不含传输细节。

mod client_policy;
mod hook_materialize;
mod tenant_notice;
mod tenant_projection;

pub use client_policy::apply_client_policy;
pub use hook_materialize::materialize_hook_execution_plan;
pub use tenant_notice::publish_tenant_notice;
pub use tenant_projection::apply_tenant_projection;
