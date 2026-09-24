//! 应用层 **Query**（读路径）：无副作用的投影与目录。

mod capability_catalog;
mod hook_integration;
mod tenant_versions;

pub use capability_catalog::list_registered_capabilities;
pub use hook_integration::{HookIntegrationChannelDoc, list_hook_integration_channels};
pub use tenant_versions::tenant_projection_versions;
