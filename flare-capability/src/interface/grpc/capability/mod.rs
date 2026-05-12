//! `CapabilityService` 与 `Administer` 子命令分发。

mod administer;
mod service;

pub use service::{CapabilityGrpcServer, CapabilityInvocationMetrics, CapabilityMetricsSnapshot};
