//! Shared hook context builders and execution service for message pipeline services.

pub mod builder;
mod execution_service;

pub use builder::*;
pub use execution_service::{HookExecutionContext, HookExecutionService};
