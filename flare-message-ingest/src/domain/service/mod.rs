pub mod conversation_ensure_service;
pub mod message_ingest_service;

pub use conversation_ensure_service::{
    ConversationEnsureRequest, ConversationEnsureService, ConversationEventPublisher,
    build_conversation_ensure_request_from_message,
};
pub use flare_im_message_pipeline::{
    CompositeEventValidationStrategy, CompositeMessageValidationStrategy,
    EventRequiredFieldsValidationStrategy, EventTypeValidationStrategy, EventValidationStrategy,
    HookExecutionContext, HookExecutionService, MessageRequiredFieldsValidationStrategy,
    MessageSizeValidationStrategy, MessageTemporaryService, MessageValidationStrategy,
    TemporaryMessageType, ValidationContext, ValidationResult,
};
pub use message_ingest_service::MessageIngestService;

// Re-export shared hook builders.
pub use flare_im_message_pipeline::hook::builder::*;
