//! Hook contracts, registry, and remote adapters for Flare IM.

pub mod hooks;

pub use flare_im_contracts::Ctx;
pub use flare_im_service_kit::discovery;

pub use hooks::{
    ConversationLifecycleEvent, ConversationLifecycleEventKind, ConversationLifecycleHook,
    ConversationMemberChangeKind, ConversationMemberEvent, ConversationMemberHook, DeliveryEvent,
    DeliveryHook, GetConversationParticipantsHook, GlobalHookRegistry, HookConfig,
    HookConfigLoader, HookDecision, HookDefinition, HookDispatcher, HookErrorPolicy, HookGroup,
    HookKind, HookMetadata, HookOutcome, HookRegistry, HookRegistryBuilder, HookSelector,
    HookSelectorConfig, HookTransportConfig, MatchRule, MessageDraft, MessageReactionEvent,
    MessageReactionHook, MessageReadEvent, MessageReadHook, MessageRecord,
    PRE_SEND_REJECTION_PARAM, PostSendHook, PreSendDecision, PreSendHook, PreSendPlan, RecallEvent,
    RecallHook, is_pre_send_rejection, pre_send_rejection,
};
