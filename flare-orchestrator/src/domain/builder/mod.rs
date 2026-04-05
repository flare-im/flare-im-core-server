//! 领域对象构建器
//!
//! 提供统一的领域对象构建能力，包括消息、事件和推送信封的构建。
//!
//! ## 设计原则
//! - Builder 模式：流式 API，易于使用
//! - 类型安全：编译时检查必填字段
//! - 默认值：合理的默认值，减少样板代码

pub mod event_builder;
pub mod hook_builder;
pub mod push_envelope_builder;

pub use event_builder::{
    EventBuilder,
    build_recall_event,
    build_edit_event,
    build_delete_event,
    build_read_receipt_event,
    build_reaction_event,
    build_pin_event,
    build_unpin_event,
    build_mark_event,
    build_unmark_event,
    build_typing_event,
    build_custom_event,
};

pub use hook_builder::*;

pub use push_envelope_builder::{
    PushEnvelopeBuilder,
    build_ack_push,
    build_notification_push,
    build_custom_push,
    build_system_push,
};
