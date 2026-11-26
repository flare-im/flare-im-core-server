//! 消息 Hook 运行时
//!
//! - 一条调用链：应用 → 平台 → 部署，同来源按 `order`，每一步可放行、拒绝或改写
//! - 两个配置来源：部署文件（启动门禁）、`hook_configs`（热加载）
//! - 基于 gRPC / WebHook 的远程目标，(应用, Hook) 熔断，执行记录批量写出

pub mod adapters;
pub mod circuit;
mod config;
pub mod execution;
pub mod hook_context_data;
mod name;
mod points;
mod registry;
mod reload;
mod runtime;
mod selector;
mod stored;
mod test_call;
mod types;

pub use circuit::{
    HookCircuitAdmission, HookCircuitBreakers, HookCircuitKey, HookCircuitPolicy,
    HookCircuitResult, HookCircuitTransition,
};
pub use config::{
    DeploymentHookSummary, DeploymentSelectorSummary, HookConfig, HookConfigLoader, HookDefinition,
    HookFactory, HookSelectorConfig, HookTargetSummary, HookTransportConfig,
};
pub use name::{LoggableName, MAX_HOOK_NAME_CHARS, validate_hook_name};
pub use points::{HOOK_POINTS, HookPointInfo, PRE_SEND_HOOK_TYPE};
pub use registry::{HookChain, HookRegistry, HookScope, PreSendPlan};
pub use reload::{HOOK_CONFIG_POLL_INTERVAL, HookConfigReloader, HookConfigStore, HookScopeChange};
pub use runtime::{DEFAULT_PRE_SEND_BUDGET, HookDispatcher};
pub use selector::{HookSelector, MatchRule};
pub use stored::StoredHook;
pub use test_call::{HookTestOutcome, HookTestReport, test_call};
pub use types::{
    HookMetadata, HookSource, MessageDraft, PRE_SEND_REJECTION_PARAM, PreSendDecision, PreSendHook,
    is_pre_send_rejection, pre_send_rejection,
};
