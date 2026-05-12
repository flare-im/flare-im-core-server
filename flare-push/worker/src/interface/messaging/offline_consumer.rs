//! 离线推送消费者 - 处理 TOPIC_PUSH_OFFLINE 中的 PushTaskEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_OFFLINE 中的 PushTaskEnvelope 消息
//! 2. 处理离线推送逻辑（当前为占位实现）
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息

use std::sync::Arc;

use flare_proto::common::PushTaskEnvelope;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use prost::Message as _;
use tracing::instrument;

use crate::infrastructure::mq::dlq_publisher::DlqPublisher;

/// 离线推送消费者处理器
pub struct OfflinePushHandler {
    #[allow(dead_code)]
    dlq: Arc<DlqPublisher>,
}

impl OfflinePushHandler {
    pub fn new(dlq: Arc<DlqPublisher>) -> Self {
        Self { dlq }
    }

    fn decode_task_envelope(message: &Message) -> Result<PushTaskEnvelope, ConsumerError> {
        PushTaskEnvelope::decode(message.payload.as_slice())
            .map_err(|e| ConsumerError::Deserialization(format!("PushTaskEnvelope: {}", e)))
    }
}

#[async_trait::async_trait]
impl MessageHandler for OfflinePushHandler {
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        let envelope = Self::decode_task_envelope(&message)?;

        let ctx = &message.context.ctx;

        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_id = %envelope.user_id,
            tenant_id = %envelope.tenant_id,
            message_id = %envelope.message_id,
            conversation_id = %envelope.conversation_id,
            "[离线推送占位实现] offline task received"
        );

        // TODO: 实现离线推送逻辑
        Ok(MessageResult::Ack)
    }

    fn name(&self) -> &str {
        "push-offline-handler"
    }
}

/// 离线推送消费者工厂
pub struct OfflinePushConsumerFactory;

impl OfflinePushConsumerFactory {
    pub fn create_handler(dlq: Arc<DlqPublisher>) -> Arc<dyn MessageHandler> {
        Arc::new(OfflinePushHandler::new(dlq))
    }

    pub fn topic() -> &'static str {
        flare_im_core::constants::topics::TOPIC_PUSH_OFFLINE
    }

    pub fn consumer_group() -> &'static str {
        flare_im_core::constants::groups::PUSH_WORKER_GROUP_DEFAULT
    }
}
