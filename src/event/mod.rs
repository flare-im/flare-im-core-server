//! 事件模块
//!
//! 提供快速构建 EventEnvelope 的方法和事件类型定义
//! 具体操作类型由事件 payload 中的字段定义

pub mod main_queue_payload;
pub mod topic_envelope;
pub mod types;

pub use main_queue_payload::{
    MqEnvelopeDecodeError, decode_mq_envelope, mq_envelope_for_main_queue_event,
    mq_envelope_for_main_queue_message,
};
pub use topic_envelope::{
    CONVERSATION_UPDATE_TYPE_REMOVE, CONVERSATION_UPDATE_TYPE_SUMMARY,
    CONVERSATION_UPDATE_TYPE_UNREAD, EVENT_TYPE_CONVERSATION_ENSURE, EVENT_TYPE_MESSAGE_CREATED,
    EVENT_TYPE_OPERATION_CONVERSATION_ENSURE, EVENT_TYPE_OPERATION_DELETED,
    EVENT_TYPE_OPERATION_EDITED, EVENT_TYPE_OPERATION_MARK, EVENT_TYPE_OPERATION_PIN,
    EVENT_TYPE_OPERATION_REACTION, EVENT_TYPE_OPERATION_READ_RECEIPT,
    EVENT_TYPE_OPERATION_RECALLED, EVENT_TYPE_OPERATION_UNMARK, EVENT_TYPE_OPERATION_UNPIN,
    EventBusPublishError, ImTopicEventPublisher, conversation_update_envelope,
    encode_topic_event_envelope, event_type_str_from_proto_event, message_envelope_from_message,
    message_to_topic_event_envelope, publish_proto_as_server_event_envelope, to_event_envelope,
    topic_event_envelope_from_event,
};
pub use types::{
    is_ack_event, is_custom_event, is_event, is_message_event, is_notification_event,
    is_system_event,
};

// 重新导出 EventEnvelope 以便使用（与 `pub mod types` 并存；常量见 `types::types`）
pub use crate::EventEnvelope;
