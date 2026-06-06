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

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::{Ctx, context_from_mq_metadata};
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::commands::ProcessStoreMessageCommand;
use crate::application::handlers::MessagePersistenceCommandHandler;
use crate::domain::model::TenantContext;

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

struct DecodedStoreMessage {
    ctx: Ctx,
    command: ProcessStoreMessageCommand,
    envelope_id: String,
    conversation_id: String,
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
        Self {
            persistence_handler,
        }
    }

    fn decode_store_message(
        message: &Message,
    ) -> Result<Option<DecodedStoreMessage>, ConsumerError> {
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
            "Processing MqEnvelope from TOPIC_MESSAGE_CREATED"
        );

        if envelope.payload_kind != MqPayloadKind::Message as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected MESSAGE, skipping"
            );
            return Ok(None);
        }

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

        let mut merged_headers = message.context.headers.clone();
        for (key, value) in &envelope.headers {
            merged_headers.insert(key.clone(), value.clone());
        }
        let ctx = context_from_mq_metadata(&merged_headers);

        let mut command = crate::convert::message_command_from_proto(proto_message.clone());
        for (key, value) in merged_headers {
            command.metadata.insert(key, value);
        }
        if command.tenant.is_none()
            && let Some(tenant_id) = ctx.tenant_id().filter(|tenant_id| !tenant_id.is_empty())
        {
            command.tenant = Some(TenantContext {
                tenant_id: flare_im_core::utils::normalize_tenant_id(tenant_id),
                user_id: ctx.user_id().map(ToString::to_string),
            });
        }

        Ok(Some(DecodedStoreMessage {
            ctx,
            command,
            envelope_id: envelope.envelope_id,
            conversation_id: envelope.conversation_id,
        }))
    }

    fn batch_group_key(decoded: &DecodedStoreMessage) -> String {
        decoded
            .command
            .tenant
            .as_ref()
            .map(|tenant| tenant.tenant_id.clone())
            .or_else(|| decoded.ctx.tenant_id().map(ToString::to_string))
            .unwrap_or_else(|| "0".to_string())
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
    #[instrument(skip(self, message), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        let Some(decoded) = Self::decode_store_message(&message)? else {
            return Ok(MessageResult::Ack);
        };

        // 6. 调用 Application 层
        match self
            .persistence_handler
            .handle(&decoded.ctx, decoded.command)
            .await
        {
            Ok(Some(result)) => {
                tracing::trace!(
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
                tracing::trace!(
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
                Err(ConsumerError::Handler(format!(
                    "MessagePersistenceCommandHandler error: {}",
                    e
                )))
            }
        }
    }

    #[instrument(skip(self, messages), fields(batch_size = messages.len()))]
    async fn handle_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<MessageResult>, ConsumerError> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![MessageResult::Ack; messages.len()];
        let mut groups: BTreeMap<String, (Ctx, Vec<ProcessStoreMessageCommand>)> = BTreeMap::new();
        let mut decoded_count = 0usize;

        for (index, message) in messages.iter().enumerate() {
            let Some(decoded) = Self::decode_store_message(message)? else {
                results[index] = MessageResult::Ack;
                continue;
            };

            tracing::trace!(
                envelope_id = %decoded.envelope_id,
                conversation_id = %decoded.conversation_id,
                "Decoded message for batch persistence"
            );

            decoded_count += 1;
            let key = Self::batch_group_key(&decoded);
            let entry = groups
                .entry(key)
                .or_insert_with(|| (decoded.ctx.clone(), Vec::new()));
            entry.1.push(decoded.command);
        }

        for (tenant_id, (ctx, commands)) in groups {
            let batch_size = commands.len();
            self.persistence_handler
                .handle_batch(&ctx, commands)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        tenant_id = %tenant_id,
                        batch_size,
                        "Failed to process message persistence batch"
                    );
                    ConsumerError::Handler(format!(
                        "MessagePersistenceCommandHandler batch error: {}",
                        e
                    ))
                })?;
        }

        tracing::trace!(
            decoded_count,
            result_count = results.len(),
            "Successfully processed message persistence batch"
        );
        Ok(results)
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-message-created-handler"
    }

    fn supports_batch(&self) -> bool {
        true
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
