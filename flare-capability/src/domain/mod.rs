//! 领域层：双限界上下文
//!
//! - **Hook**：[`model`]（配置与执行计划）、[`repository`]、[`service::HookOrchestrationService`]
//! - **Capability**：[`capability`]（Guard / Resolver / RTC、分发命令、策略端口）

pub mod capability;
pub mod model;
pub mod repository;
pub mod service;

pub use model::*;
pub use repository::*;
pub use service::HookOrchestrationService;
