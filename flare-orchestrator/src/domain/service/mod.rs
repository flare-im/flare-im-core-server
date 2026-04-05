pub mod message_domain_service;
pub mod message_temporary_service;
pub mod sequence_allocator;
pub mod event_domain_service;
pub mod conversation_ensure_service;
pub mod validation_strategy;
pub mod hook_execution_service;

pub use message_domain_service::MessageDomainService;
pub use message_temporary_service::MessageTemporaryService;
pub use sequence_allocator::SequenceAllocator;
pub use event_domain_service::EventDomainService;
pub use conversation_ensure_service::{
    ConversationEnsureService, ConversationEnsureRequest, ConversationEventPublisher,
    build_conversation_ensure_request_from_message,
    build_conversation_ensure_request_from_event,
};
pub use validation_strategy::{
    // Traits
    MessageValidationStrategy, EventValidationStrategy,
    // Results
    ValidationResult, ValidationContext,
    // Message strategies
    MessageSizeValidationStrategy, MessageRequiredFieldsValidationStrategy,
    CompositeMessageValidationStrategy,
    // Event strategies
    EventTypeValidationStrategy, EventRequiredFieldsValidationStrategy,
    CompositeEventValidationStrategy,
};
pub use hook_execution_service::{HookExecutionService, HookExecutionContext};

// Re-export hook_builder from domain::builder
pub use crate::domain::builder::hook_builder::*;
