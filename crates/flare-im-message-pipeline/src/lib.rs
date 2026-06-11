//! Shared message pipeline ports and MQ adapters.
//!
//! This crate owns topic fanout semantics shared by message ingest and orchestrator:
//! main queue publish, persistence-only fanout, push-only fanout, and push envelope publish.

pub mod builder;
pub mod extension;
pub mod hook;
pub mod messaging;
pub mod model;
pub mod persistence;
pub mod repository;
pub mod rpc;
pub mod service;
pub mod validation;

pub use builder::{
    PushEnvelopeBuilder, build_ack_push, build_custom_push, build_notification_push,
    build_system_push,
};
pub use extension::{
    ExtensionFailureMode, ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting,
    ExtensionRuntimePolicy,
};
pub use hook::{HookExecutionContext, HookExecutionService, SubmittedMessage};
pub use messaging::MqPushRepository;
pub use model::ConversationType;
pub use persistence::RecipientRepositoryImpl;
pub use repository::{PushRepository, RecipientRepository, needs_member_lookup};
pub use rpc::ConversationClient;
pub use service::{MessageTemporaryService, TemporaryMessageType};
pub use validation::{
    CompositeEventValidationStrategy, CompositeMessageValidationStrategy,
    EventRequiredFieldsValidationStrategy, EventTypeValidationStrategy, EventValidationStrategy,
    MessageRequiredFieldsValidationStrategy, MessageSizeValidationStrategy,
    MessageValidationStrategy, ValidationContext, ValidationResult,
};
