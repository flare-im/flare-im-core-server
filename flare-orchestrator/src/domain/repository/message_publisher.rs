//! 消息事件发布器端口（委托 flare-im-core 统一抽象）
//!
//! 便于后续接入 NATS / Pulsar 等后端而不改动业务代码。

pub use flare_im_core::abstractions::messaging::MessageEventPublisher;
