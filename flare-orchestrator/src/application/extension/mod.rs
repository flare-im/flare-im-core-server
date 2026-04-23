//! 扩展编排层：统一 Hook / Plugin 的执行入口与策略。

mod orchestrator;

pub use crate::domain::extension::{
    ExtensionFailureMode, ExtensionPolicy, ExtensionRouting, ExtensionRuntimePolicy,
};
pub use orchestrator::ExtensionOrchestrator;
