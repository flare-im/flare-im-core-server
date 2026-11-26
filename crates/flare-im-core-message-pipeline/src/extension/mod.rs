//! Shared extension runtime for hooks and optional capability enrichment.

mod orchestrator;
mod routing;

pub use orchestrator::ExtensionOrchestrator;
pub use routing::ExtensionRouting;
