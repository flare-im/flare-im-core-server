use std::sync::Arc;

use anyhow::Result;
use flare_server_core::context::Ctx;
use redis::{AsyncCommands, aio::ConnectionManager};
use std::convert::TryInto;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::repository::MessageIdempotencyRepository;

pub struct RedisIdempotencyRepository {
    client: Arc<redis::Client>,
    ttl_seconds: u64,
}

impl RedisIdempotencyRepository {
    pub fn new(client: Arc<redis::Client>, config: &StorageWriterConfig) -> Self {
        Self {
            client,
            ttl_seconds: config.redis_idempotency_ttl_seconds,
        }
    }
}

impl MessageIdempotencyRepository for RedisIdempotencyRepository {
    #[instrument(skip(self), fields(message_id))]
    async fn is_new(&self, ctx: &Ctx, message_id: &str) -> Result<bool> {
        let _ = ctx; // 上下文用于日志追踪
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;

        let key = format!("storage:idempotency:{}", message_id);
        let is_new: bool = conn.set_nx(&key, 1).await?;
        if is_new && self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&key, ttl).await?;
        }

        Ok(is_new)
    }

    #[instrument(skip(self), fields(client_msg_id, sender_id))]
    async fn is_new_by_client_msg_id(&self, ctx: &Ctx, client_msg_id: &str, sender_id: Option<&str>) -> Result<bool> {
        let _ = ctx; // 上下文用于日志追踪
        if client_msg_id.is_empty() {
            return Ok(true);
        }

        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;

        // 使用 sender_id + client_msg_id 作为key，提高去重精度
        // 这样可以避免不同用户使用相同client_msg_id时的冲突
        let key = if let Some(sender) = sender_id {
            format!("storage:idempotency:client:{}:{}", sender, client_msg_id)
        } else {
            format!("storage:idempotency:client:{}", client_msg_id)
        };

        let is_new: bool = conn.set_nx(&key, 1).await?;
        if is_new && self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&key, ttl).await?;
        }

        Ok(is_new)
    }
}
