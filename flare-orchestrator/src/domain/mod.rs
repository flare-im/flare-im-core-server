pub mod builder;
pub mod enums;
pub mod messaging;
pub mod model;
pub mod repository;
pub mod service;

pub use enums::{MessageFsmState, PersistenceMode};
pub use model::ConversationType;
pub use model::message_submission::MessageSubmission;
pub use repository::{PushRepository, RecipientRepository};
pub use service::MessageFanoutService;

// 导出事件构建器
pub use builder::{
    EventBuilder, build_custom_event, build_delete_event, build_edit_event, build_mark_event,
    build_pin_event, build_reaction_event, build_read_receipt_event, build_recall_event,
    build_unmark_event, build_unpin_event,
};
