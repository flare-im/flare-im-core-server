//! 接口层：消息与事件消费者
//!
//! ## 消费者列表
//! - `MessageCreatedHandler`: 处理 TOPIC_MESSAGE_CREATED（消息创建）
//! - `MessageEventsHandler`: 处理 TOPIC_MESSAGE_EVENTS（操作事件）

mod message_consumer;
mod operation_consumer;

pub use message_consumer::{MessageCreatedConsumerFactory, MessageCreatedHandler};
pub use operation_consumer::{MessageEventsConsumerFactory, MessageEventsHandler};
