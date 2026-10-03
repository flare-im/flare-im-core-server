//! `CapabilityService` 与 `Administer` 子命令分发。

mod administer;
#[cfg(test)]
mod hook_secret_invalidation_pg_tests;
mod service;
#[cfg(test)]
mod tenant_hooks_pg_tests;

pub use service::{CapabilityGrpcServer, CapabilityInvocationMetrics, CapabilityMetricsSnapshot};
