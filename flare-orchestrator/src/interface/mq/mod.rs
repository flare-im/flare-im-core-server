//! MQ 消费者模块
//!
//! 处理来自 MQ 的消息

pub mod storage_consumer;

pub use storage_consumer::StorageConsumerHandler;
