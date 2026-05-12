//! 接口层：推送消费者
//!
//! ## 消费者列表
//! - `PushEventHandler`: 处理 TOPIC_PUSH_EVENTS（事件推送）
//! - `PushMessageHandler`: 处理 TOPIC_PUSH_MESSAGES（消息推送）
//! - `PushMainHandler`: 处理 TOPIC_MESSAGE_MAIN（主队列推送）
//! - `PushHandler`: 处理 TOPIC_PUSH_ENVELOPE（统一推送信封：ACK、通知、CustomData、系统消息）

pub mod event_consumer;
pub mod main_consumer;
pub mod message_consumer;
pub mod push_consumer;

// 导出 Handler
pub use event_consumer::PushEventHandler;
pub use main_consumer::PushMainHandler;
pub use message_consumer::PushMessageHandler;
pub use push_consumer::PushHandler;
