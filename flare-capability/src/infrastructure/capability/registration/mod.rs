//! 能力扩展注册聚合（进程内单例容器）。

mod extension_registry;
mod grants;

pub use extension_registry::{CapabilityExtensionRegistry, RegistryInner};
pub use grants::InMemoryCapabilityGrants;
