//! 领域对象构建器
//!
//! 提供消息摄入链路需要的领域对象构建能力。
//!
//! ## 设计原则
//! - Builder 模式：流式 API，易于使用
//! - 类型安全：编译时检查必填字段
//! - 默认值：合理的默认值，减少样板代码

pub use flare_im_message_pipeline::hook::builder::*;
pub use flare_im_message_pipeline::{
    PushEnvelopeBuilder, build_ack_push, build_custom_push, build_notification_push,
    build_system_push,
};
