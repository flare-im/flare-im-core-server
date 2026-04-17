//! # Flare Capability 服务层
//!
//! 应用启动与依赖注入

pub mod bootstrap;
pub mod registry;
mod wire;

pub use bootstrap::{ApplicationBootstrap, CapabilityServiceConfig};
pub use wire::{init_capability_extension_stack, ApplicationContext};
