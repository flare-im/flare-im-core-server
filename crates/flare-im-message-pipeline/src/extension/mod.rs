//! Shared extension runtime for hooks and optional capability enrichment.

mod orchestrator;
mod policy;
mod routing;

pub use orchestrator::ExtensionOrchestrator;
pub use policy::{ExtensionFailureMode, ExtensionPolicy, ExtensionRuntimePolicy};
pub use routing::ExtensionRouting;
