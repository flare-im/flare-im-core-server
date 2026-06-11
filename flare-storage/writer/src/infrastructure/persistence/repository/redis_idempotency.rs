use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_server_core::error::Result;
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

    fn message_key(message_id: &str) -> String {
        format!("storage:idempotency:{}", message_id)
    }

    fn client_message_key(client_msg_id: &str, sender_id: Option<&str>) -> String {
        if let Some(sender) = sender_id {
            format!("storage:idempotency:client:{}:{}", sender, client_msg_id)
        } else {
            format!("storage:idempotency:client:{}", client_msg_id)
        }
    }
}

impl MessageIdempotencyRepository for RedisIdempotencyRepository {
    #[instrument(skip(self), fields(message_id))]
    async fn is_new(&self, ctx: &Ctx, message_id: &str) -> Result<bool> {
        let _ = ctx; // 上下文用于日志追踪
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;

        let key = Self::message_key(message_id);
        let is_new: bool = conn.set_nx(&key, 1).await?;
        if is_new && self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&key, ttl).await?;
        }

        Ok(is_new)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn release(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        if message_id.is_empty() {
            return Ok(());
        }
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;
        let key = Self::message_key(message_id);
        let _: () = conn.del(key).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(client_msg_id, sender_id))]
    async fn is_new_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        sender_id: Option<&str>,
    ) -> Result<bool> {
        let _ = ctx; // 上下文用于日志追踪
        if client_msg_id.is_empty() {
            return Ok(true);
        }

        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;

        // 使用 sender_id + client_msg_id 作为key，提高去重精度
        // 这样可以避免不同用户使用相同client_msg_id时的冲突
        let key = Self::client_message_key(client_msg_id, sender_id);

        let is_new: bool = conn.set_nx(&key, 1).await?;
        if is_new && self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&key, ttl).await?;
        }

        Ok(is_new)
    }

    #[instrument(skip(self), fields(client_msg_id, sender_id))]
    async fn release_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        sender_id: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        if client_msg_id.is_empty() {
            return Ok(());
        }

        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;
        let key = Self::client_message_key(client_msg_id, sender_id);
        let _: () = conn.del(key).await?;
        Ok(())
    }
}
