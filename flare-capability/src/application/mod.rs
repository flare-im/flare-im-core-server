//! 应用层：仅 **`commands`** / **`handler`** / **`queries`** 三包。
//!
//! - **commands**：写模型入口（如 Hook 物化）。
//! - **queries**：读模型与静态目录。
//! - **handler**：跨端口编排，**业务规则在 [`crate::domain`]**（如 [`crate::domain::service::HookOrchestrationService`]、[`crate::domain::capability::execute_capability_dispatch`]）。

pub mod commands;
pub mod handler;
pub mod queries;

pub use handler::{dispatch_capability_command, HookCommandHandler, HookQueryHandler};
