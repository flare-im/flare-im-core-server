//! 存储消费者处理器 - 处理 TOPIC_MESSAGE_MAIN 中的消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_MAIN 中的 MqEnvelope 消息
//! 2. 消费者组由 [crate::config::MessageOrchestratorConfig] 的 `KafkaConsumerConfig::consumer_group`（默认 `ORCHESTRATOR_MAIN_GROUP_DEFAULT`）与 [ConsumerConfig::kafka_consumer_group_override] 决定
//! 3. 反序列化 MqEnvelope 并调用 StorageHandler 处理
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 StorageHandler 处理业务

use std::sync::Arc;

use flare_proto::common::MqEnvelope;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::handlers::StorageHandler;

/// 存储消费者处理器
///
/// 处理 `TOPIC_MESSAGE_MAIN` 中的消息；Kafka `group.id` 见编排器 `KafkaConsumerConfig` 与 `wire::initialize` 中的 `ConsumerConfig`。
pub struct StorageConsumerHandler {
    /// 存储处理器（编排层）
    storage_handler: Arc<StorageHandler>,
}

impl StorageConsumerHandler {
    /// 创建新的存储消费者处理器
    pub fn new(storage_handler: Arc<StorageHandler>) -> Self {
        Self { storage_handler }
    }
}

#[async_trait::async_trait]
impl MessageHandler for StorageConsumerHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 从 headers 中重建上下文
    /// 3. 调用 StorageHandler.handle_envelope()
    /// 4. 返回处理结果
    ///
    /// # 参数
    /// - `message`: MQ 消息
    ///
    /// # 返回
    /// - `Ok(MessageResult::Ack)`: 处理成功
    /// - `Err(ConsumerError)`: 处理失败
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        // 1. 反序列化 MqEnvelope（直接解码，不需要 content-type 校验）
        let envelope = message.decode_protobuf::<MqEnvelope>().map_err(|e| {
            tracing::error!(
                error = %e,
                topic = %message.context.topic,
                "Failed to deserialize MqEnvelope"
            );
            ConsumerError::Deserialization(format!("Failed to deserialize MqEnvelope: {}", e))
        })?;

        tracing::debug!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            "Processing MqEnvelope from TOPIC_MESSAGE_MAIN"
        );

        // 2. 从 headers 中重建上下文
        let ctx = &message.context.ctx;

        // 3. 调用 StorageHandler 处理
        match self.storage_handler.handle_envelope(ctx, envelope).await {
            Ok(()) => {
                tracing::debug!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Successfully processed MqEnvelope"
                );
                Ok(MessageResult::Ack)
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    "Failed to process MqEnvelope"
                );
                // 根据错误类型决定是否重试
                Err(ConsumerError::Handler(format!(
                    "StorageHandler error: {}",
                    e
                )))
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-consumer-handler"
    }
}
