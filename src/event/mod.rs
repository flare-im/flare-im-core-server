//! 事件模块
//!
//! 提供快速构建 EventEnvelope 的方法和事件类型定义
//! 具体操作类型由事件 payload 中的字段定义

pub mod builder;
pub mod types;

pub use builder::{
    from_protobuf, from_protobuf_full, from_protobuf_with_current_timestamp,
    from_protobuf_with_source, from_protobuf_with_timestamp,
    EventEnvelopeBuilder,
};
pub use types::{
    is_ack_event, is_custom_event, is_event, is_message_event,
    is_notification_event, is_system_event,
};

// 重新导出 EventEnvelope 以便使用（与 `pub mod types` 并存；常量见 `types::types`）
pub use flare_server_core::event_bus::EventEnvelope;
