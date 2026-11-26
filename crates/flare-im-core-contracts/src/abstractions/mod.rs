//! IM 侧端口与抽象（存储载荷、发布端口、`event_type` 与信封转换等）。
//! JetStream Topic 名与消费者组见 [crate::constants]。
//!
//! 通用 MQ 与 JSON Topic 总线请用 [flare_server_core::mq]、[flare_server_core::TopicEventBus]。

pub mod decorator;
pub mod state;
pub mod storage_payload;
