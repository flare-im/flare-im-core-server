pub mod call_signal_enrichment_service;
pub mod call_signal_notice_message_builder;
pub mod conversation_ensure_service;
pub mod event_domain_service;
pub mod hook_execution_service;
pub mod message_domain_service;
pub mod message_temporary_service;
pub mod sequence_allocator;
pub mod validation_strategy;

pub use call_signal_enrichment_service::CallSignalEnrichmentService;
pub use conversation_ensure_service::{
    ConversationEnsureRequest, ConversationEnsureService, ConversationEventPublisher,
    build_conversation_ensure_request_from_event, build_conversation_ensure_request_from_message,
};
pub use event_domain_service::EventDomainService;
pub use hook_execution_service::{HookExecutionContext, HookExecutionService};
pub use message_domain_service::MessageDomainService;
pub use message_temporary_service::MessageTemporaryService;
pub use sequence_allocator::SequenceAllocator;
pub use validation_strategy::{
    CompositeEventValidationStrategy,
    CompositeMessageValidationStrategy,
    EventRequiredFieldsValidationStrategy,
    // Event strategies
    EventTypeValidationStrategy,
    EventValidationStrategy,
    MessageRequiredFieldsValidationStrategy,
    // Message strategies
    MessageSizeValidationStrategy,
    // Traits
    MessageValidationStrategy,
    ValidationContext,
    // Results
    ValidationResult,
};

// Re-export hook_builder from domain::builder
pub use crate::domain::builder::hook_builder::*;
