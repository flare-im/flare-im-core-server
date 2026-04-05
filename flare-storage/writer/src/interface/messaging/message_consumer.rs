//! 消息创建消费者 - 处理 TOPIC_MESSAGE_CREATED 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_CREATED 中的 MqEnvelope 消息
//! 2. 反序列化 MqEnvelope 并验证 payload_kind 为 MESSAGE
//! 3. 调用 MessagePersistenceCommandHandler 处理消息持久化
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 MessagePersistenceCommandHandler 处理业务

use std::sync::Arc;

use async_trait::async_trait;
use flare_proto::common::{mq_envelope, MqEnvelope, MqPayloadKind};
use flare_server_core::mq::consumer::{
    MessageHandler, Message, MessageResult, ConsumerError,
};
use tracing::instrument;

use crate::application::handlers::MessagePersistenceCommandHandler;

// 类型别名，简化泛型参数
type IdempotencyRepo =
    crate::infrastructure::persistence::repository::redis_idempotency::RedisIdempotencyRepository;
type HotCacheRepo =
    crate::infrastructure::persistence::repository::redis_cache::RedisHotCacheRepository;
type ArchiveRepo =
    crate::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
type EventStreamRepo =
    crate::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
type WalCleanupRepo =
    crate::infrastructure::persistence::repository::redis_wal_cleanup::RedisWalCleanupRepository;
type AckPub = crate::infrastructure::messaging::ack_publisher::MqAckPublisher;

type MessagePersistenceHandler = MessagePersistenceCommandHandler<
    IdempotencyRepo,
    HotCacheRepo,
    ArchiveRepo,
    EventStreamRepo,
    WalCleanupRepo,
    AckPub,
>;

/// 消息创建消费者处理器
///
/// 处理 `TOPIC_MESSAGE_CREATED` 中的 MqEnvelope 消息，负责消息持久化
pub struct MessageCreatedHandler {
    /// 消息持久化处理器（应用层）
    persistence_handler: Arc<MessagePersistenceHandler>,
}

impl MessageCreatedHandler {
    /// 创建新的消息创建消费者处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Self`: 消息创建消费者处理器实例
    pub fn new(persistence_handler: Arc<MessagePersistenceHandler>) -> Self {
        Self { persistence_handler }
    }
}

#[async_trait]
impl MessageHandler for MessageCreatedHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 验证 payload_kind 为 MESSAGE
    /// 3. 从 payload 提取 Message
    /// 4. 从 headers 中重建上下文
    /// 5. 调用 MessagePersistenceCommandHandler.handle()
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
        let envelope = message.decode_protobuf::<MqEnvelope>()
            .map_err(|e| {
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
            "Processing MqEnvelope from TOPIC_MESSAGE_CREATED"
        );

        // 2. 验证 payload_kind
        if envelope.payload_kind != MqPayloadKind::Message as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected MESSAGE, skipping"
            );
            return Ok(MessageResult::Ack);
        }

        // 3. 从 payload oneof 提取 Message
        let proto_message = match &envelope.payload {
            Some(mq_envelope::Payload::Message(m)) => m,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Message payload is missing or not Message variant"
                );
                return Err(ConsumerError::Deserialization(
                    "Message payload is missing or not Message variant".to_string(),
                ));
            }
        };

        // 4. 从 headers 中重建上下文
        let ctx = &message.context.ctx;

        // 5. 转换为命令
        let cmd = crate::convert::message_command_from_proto(proto_message.clone());

        // 6. 调用 Application 层
        match self.persistence_handler.handle(ctx, cmd).await {
            Ok(Some(result)) => {
                tracing::debug!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    message_id = %result.message_id,
                    conversation_id = %result.conversation_id,
                    deduplicated = result.deduplicated,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Successfully processed MqEnvelope"
                );
                Ok(MessageResult::Ack)
            }
            Ok(None) => {
                tracing::debug!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Message processed but no result returned (operation message)"
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
                Err(ConsumerError::Handler(format!("MessagePersistenceCommandHandler error: {}", e)))
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-message-created-handler"
    }
}

/// 消息创建消费者工厂
///
/// 提供创建消息创建处理器的便捷方法
pub struct MessageCreatedConsumerFactory;

impl MessageCreatedConsumerFactory {
    /// 创建消息创建处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Arc<dyn MessageHandler>`: 消息创建处理器实例
    pub fn create_handler(
        persistence_handler: Arc<MessagePersistenceHandler>,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(MessageCreatedHandler::new(persistence_handler))
    }

    /// 获取订阅的主题
    ///
    /// # 返回
    /// - `&'static str`: 主题名称
    pub fn topic() -> &'static str {
        flare_im_core::constants::topics::TOPIC_MESSAGE_CREATED
    }

    /// 获取消费者组名称
    ///
    /// # 返回
    /// - `&'static str`: 消费者组名称
    pub fn consumer_group() -> &'static str {
        flare_im_core::constants::groups::STORAGE_GROUP_DEFAULT
    }
}
