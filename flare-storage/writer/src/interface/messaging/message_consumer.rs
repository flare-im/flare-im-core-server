//! 消息事件消费者：订阅消息 topic，负责消息持久化命令下发
//! 使用 flare-im-core 的 event 模块和 flare-server-core 的 EventEnvelope

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::constants::groups::STORAGE_GROUP_DEFAULT;
use flare_im_core::constants::topics::TOPIC_MESSAGE_CREATED as TOPIC_MESSAGE_STORAGE;
use flare_im_core::event::types::types as im_event_types;
use flare_proto::common::Message;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::warn;

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

/// 消息事件处理器：专门处理消息创建事件
pub struct MessageEventHandler {
    persistence_handler: Arc<MessagePersistenceHandler>,
}

impl MessageEventHandler {
    /// 创建新的消息事件处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Self`: 消息事件处理器实例
    pub fn new(persistence_handler: Arc<MessagePersistenceHandler>) -> Self {
        Self {
            persistence_handler,
        }
    }
}

#[async_trait]
impl EventHandler for MessageEventHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        // 验证事件类型
        if envelope.event_type != im_event_types::MESSAGE {
            warn!(
                event_type = %envelope.event_type,
                partition_key = %envelope.partition_key,
                "unexpected event_type in message topic, skip"
            );
            return Ok(());
        }

        // 从 payload 解析 Message
        let message = Message::decode(&*envelope.payload).map_err(|e| {
            FlareError::deserialization_error(format!("Failed to decode Message: {}", e))
        })?;

        // 转换为命令
        let cmd = crate::convert::message_command_from_proto(message);

        // 处理消息持久化
        self.persistence_handler
            .handle(ctx, cmd)
            .await
            .map_err(|e| FlareError::general_error(e.to_string()))?;

        Ok(())
    }

    fn name(&self) -> &str {
        "message-event-handler"
    }
}

/// 消息事件消费者工厂
///
/// 提供创建消息事件处理器的便捷方法
pub struct MessageEventConsumerFactory;

impl MessageEventConsumerFactory {
    /// 创建消息事件处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Arc<dyn EventHandler>`: 消息事件处理器实例
    pub fn create_handler(
        persistence_handler: Arc<MessagePersistenceHandler>,
    ) -> Arc<dyn EventHandler> {
        Arc::new(MessageEventHandler::new(persistence_handler))
    }

    /// 获取订阅的主题
    ///
    /// # 返回
    /// - `&'static str`: 主题名称
    pub fn topic() -> &'static str {
        TOPIC_MESSAGE_STORAGE
    }

    /// 获取消费者组名称
    ///
    /// # 返回
    /// - `&'static str`: 消费者组名称
    pub fn consumer_group() -> &'static str {
        STORAGE_GROUP_DEFAULT
    }
}
