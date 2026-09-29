//! 接口层：推送消费者
//!
//! ## 消费者列表
//! - `PushEventHandler`: 处理 TOPIC_PUSH_EVENTS（事件推送）
//! - `PushMessageHandler`: 处理 TOPIC_PUSH_MESSAGES（消息推送）
//! - `PushHandler`: 处理 TOPIC_PUSH_ENVELOPE（统一推送信封：ACK、通知、CustomData、系统消息）

pub mod event_consumer;
pub mod message_consumer;
pub mod push_consumer;

use flare_server_core::mq::consumer::{ConsumerError, MessageResult};

/// A source message may be acknowledged only after its dead-letter copy is
/// durably accepted. Propagate publish failure to the consumer runtime, which
/// retains/nacks the source (or applies its own configured failure policy).
fn acknowledge_after_dlq<E: std::fmt::Display>(
    publish: Result<(), E>,
) -> Result<MessageResult, ConsumerError> {
    publish
        .map(|()| MessageResult::Ack)
        .map_err(|error| ConsumerError::DeadLetter(error.to_string()))
}

// 导出 Handler
pub use event_consumer::PushEventHandler;
pub use message_consumer::PushMessageHandler;
pub use push_consumer::PushHandler;

#[cfg(test)]
mod dlq_tests {
    use super::*;

    #[test]
    fn source_is_acknowledged_only_after_dlq_publish_succeeds() {
        assert!(matches!(
            acknowledge_after_dlq::<&str>(Ok(())),
            Ok(MessageResult::Ack)
        ));
        assert!(matches!(
            acknowledge_after_dlq(Err("publisher unavailable")),
            Err(ConsumerError::DeadLetter(_))
        ));
    }
}
