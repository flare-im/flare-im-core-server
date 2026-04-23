//! 领域层：多限界上下文
//!
//! - **Hook**：[`model`]（配置与执行计划）、[`repository`]、[`service::HookOrchestrationService`]
//! - **HookIntegration**：[`hook_integration`]（出站 gRPC/Webhook/Local 传输分类与物化前策略）
//! - **Capability**：[`capability`]（Guard / Resolver / RTC、分发 DTO、策略端口、[`capability::execute_capability_dispatch`]）

pub mod capability;
pub mod hook_integration;
pub mod model;
pub mod repository;
pub mod service;

pub use model::*;
pub use repository::*;
pub use service::HookOrchestrationService;
