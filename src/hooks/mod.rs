//! 消息 Hook 扩展模块
//!
//! - 提供统一的 Hook 上下文、消息草稿与执行结果定义
//! - 支持本地 Hook 注册中心与基于 gRPC/WebHook 的远程扩展
//! - 面向业务团队提供零侵入的扩展点编排能力

pub mod adapters;
mod config;
pub mod hook_context_data;
mod registry;
mod runtime;
mod selector;
mod types;

pub use config::{
    HookConfig, HookConfigLoader, HookDefinition, HookSelectorConfig, HookTransportConfig,
};
pub use registry::{GlobalHookRegistry, HookRegistry, HookRegistryBuilder, PreSendPlan};
pub use runtime::HookDispatcher;
pub use selector::{HookSelector, MatchRule};
pub use types::{
    ConversationLifecycleEvent, ConversationLifecycleEventKind, ConversationLifecycleHook,
    ConversationMemberChangeKind, ConversationMemberEvent, ConversationMemberHook, DeliveryEvent,
    DeliveryHook, GetConversationParticipantsHook, HookDecision, HookErrorPolicy, HookGroup,
    HookKind, HookMetadata, HookOutcome, MessageDraft, MessageReactionEvent, MessageReactionHook,
    MessageReadEvent, MessageReadHook, MessageRecord, PostSendHook, PreSendDecision, PreSendHook,
    RecallEvent, RecallHook,
};
