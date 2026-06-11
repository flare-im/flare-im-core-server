//! 操作事件消费者 - 处理 TOPIC_MESSAGE_EVENTS 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_EVENTS 中的 MqEnvelope 消息
//! 2. 反序列化 MqEnvelope 并验证 payload_kind 为 EVENT
//! 3. 调用 MessageOperationCommandHandler 处理操作事件
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 MessageOperationCommandHandler 处理业务

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_contracts::context_from_mq_metadata;
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::commands::ProcessEventCommand;
use crate::application::handlers::MessageOperationCommandHandler;
use crate::convert::{enrich_operation_event_from_ctx, event_from_proto};

/// 操作事件消费者处理器
///
/// 处理 `TOPIC_MESSAGE_EVENTS` 中的 MqEnvelope 消息，负责操作事件持久化
pub struct MessageEventsHandler {
    /// 操作事件处理器（应用层）
    operation_handler: Arc<MessageOperationCommandHandler>,
}

impl MessageEventsHandler {
    /// 创建新的操作事件消费者处理器
    ///
    /// # 参数
    /// - `operation_handler`: 消息操作处理器
    ///
    /// # 返回
    /// - `Self`: 操作事件消费者处理器实例
    pub fn new(operation_handler: Arc<MessageOperationCommandHandler>) -> Self {
        Self { operation_handler }
    }
}

#[async_trait]
impl MessageHandler for MessageEventsHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 验证 payload_kind 为 EVENT
    /// 3. 从 payload 提取 Event
    /// 4. 从 headers 中重建上下文
    /// 5. 调用 MessageOperationCommandHandler.handle()
    /// 6. 返回处理结果
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
        // 1. 反序列化 MqEnvelope
        let envelope = message.decode_protobuf::<MqEnvelope>().map_err(|e| {
            tracing::error!(
                error = %e,
                topic = %message.context.topic,
                "Failed to deserialize MqEnvelope"
            );
            ConsumerError::Deserialization(format!("Failed to deserialize MqEnvelope: {}", e))
        })?;

        tracing::trace!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            "Processing MqEnvelope from TOPIC_MESSAGE_EVENTS"
        );

        // 2. 验证 payload_kind
        if envelope.payload_kind != MqPayloadKind::Event as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected EVENT, skipping"
            );
            return Ok(MessageResult::Ack);
        }

        // 3. 从 payload oneof 提取 Event
        let proto_event = match &envelope.payload {
            Some(mq_envelope::Payload::Event(e)) => e,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Event payload is missing or not Event variant"
                );
                return Err(ConsumerError::Deserialization(
                    "Event payload is missing or not Event variant".to_string(),
                ));
            }
        };

        // 4. 从外层 MQ headers + 内层 MqEnvelope headers 中重建上下文。
        let mut merged_headers = message.context.headers.clone();
        for (key, value) in &envelope.headers {
            merged_headers.insert(key.clone(), value.clone());
        }
        let ctx = context_from_mq_metadata(&merged_headers);

        // 5. 转换为领域事件并补全上下文
        let mut event = event_from_proto(proto_event);
        enrich_operation_event_from_ctx(&mut event, &ctx);

        // 6. 调用 Application 层
        match self
            .operation_handler
            .handle(&ctx, ProcessEventCommand { event })
            .await
        {
            Ok(result) => {
                tracing::trace!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    message_id = %result.message_id,
                    conversation_id = %result.conversation_id,
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
                Err(ConsumerError::Handler(format!(
                    "MessageOperationCommandHandler error: {}",
                    e
                )))
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-message-events-handler"
    }
}

/// 操作事件消费者工厂
///
/// 提供创建操作事件处理器的便捷方法
pub struct MessageEventsConsumerFactory;

impl MessageEventsConsumerFactory {
    /// 创建操作事件处理器
    ///
    /// # 参数
    /// - `operation_handler`: 消息操作处理器
    ///
    /// # 返回
    /// - `Arc<dyn MessageHandler>`: 操作事件处理器实例
    pub fn create_handler(
        operation_handler: Arc<MessageOperationCommandHandler>,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(MessageEventsHandler::new(operation_handler))
    }

    /// 获取订阅的主题
    ///
    /// # 返回
    /// - `&'static str`: 主题名称
    pub fn topic() -> &'static str {
        flare_im_contracts::constants::topics::TOPIC_MESSAGE_EVENTS
    }

    /// 获取消费者组名称
    ///
    /// # 返回
    /// - `&'static str`: 消费者组名称
    pub fn consumer_group() -> &'static str {
        flare_im_contracts::constants::groups::STORAGE_GROUP_DEFAULT
    }
}
