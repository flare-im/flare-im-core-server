//! Shared hook context builders and execution service for message pipeline services.

use flare_proto::common::Message;

pub mod builder;
mod execution_service;

pub use builder::*;
pub use execution_service::{HookExecutionContext, HookExecutionService};

/// Minimal post-send view required by hook execution.
///
/// Ingest owns message preparation and WAL metadata, while orchestrator only needs a lightweight
/// submitted-message view for post-send hooks. This trait keeps hook execution shared without
/// forcing both services to share the same command model.
pub trait SubmittedMessage {
    fn message(&self) -> &Message;
    fn message_id(&self) -> &str;
}
