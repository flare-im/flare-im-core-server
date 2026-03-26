//! 接口层：消息与事件消费者

mod message_consumer;
mod operation_consumer;

pub use message_consumer::{MessageEventConsumerFactory, MessageEventHandler};
pub use operation_consumer::{OperationEventConsumerFactory, OperationEventHandler};
