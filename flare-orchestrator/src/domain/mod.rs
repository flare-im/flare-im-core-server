pub mod builder;
pub mod enums;
pub mod extension;
pub mod messaging;
pub mod model;
pub mod repository;
pub mod service;

pub use enums::{MessageCategory, MessageFsmState, PersistenceMode};
pub use extension::{
    ExtensionFailureMode, ExtensionPolicy, ExtensionRouting, ExtensionRuntimePolicy,
};
pub use model::ConversationType;
pub use model::message_kind::MessageProfile;
pub use model::message_submission::{MessageDefaults, MessageSubmission};
pub use repository::{ConversationRepository, PushRepository, RecipientRepository, WalRepository};
pub use service::MessageDomainService;

// 导出事件构建器
pub use builder::{
    EventBuilder, build_custom_event, build_delete_event, build_edit_event, build_mark_event,
    build_pin_event, build_reaction_event, build_read_receipt_event, build_recall_event,
    build_unmark_event, build_unpin_event,
};
