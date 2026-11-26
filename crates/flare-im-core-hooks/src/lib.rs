//! Hook runtime for Flare IM: one call chain, its configuration sources, remote adapters, circuit
//! breakers and execution evidence.

pub mod hooks;

#[cfg(test)]
pub(crate) mod test_logs;

pub use flare_im_core_contracts::Ctx;
pub use flare_im_core_service_kit::discovery;

pub use hooks::{
    HookConfig, HookConfigLoader, HookDefinition, HookDispatcher, HookMetadata, HookRegistry,
    HookSelector, HookSelectorConfig, HookSource, HookTransportConfig, LoggableName, MatchRule,
    MessageDraft, PRE_SEND_REJECTION_PARAM, PreSendDecision, PreSendHook, PreSendPlan,
    is_pre_send_rejection, pre_send_rejection,
};
