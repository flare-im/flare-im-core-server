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

    // is_new_by_client_msg_id / release_by_client_msg_id 走 trait 默认实现：
    // 唯一作用域规则在 domain::scoped_client_idempotency_key，最终 key 与旧覆盖逐字一致
    // （"storage:idempotency:" 前缀由 message_key 统一追加）。此处不再 fork 键空间。
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repository::scoped_client_idempotency_key;

    fn ctx_with_tenant(tenant_id: &str) -> Ctx {
        Ctx::default().with_tenant_id(tenant_id).into()
    }

    #[test]
    fn client_message_key_is_scoped_by_tenant_sender_and_conversation() {
        // 共享作用域规则 + message_key 前缀 = 与历史 Redis key 逐字一致（键空间不迁移）。
        let scoped = scoped_client_idempotency_key(
            &ctx_with_tenant("tenant-a"),
            "client-1",
            Some("sender-1"),
            Some("conv-a"),
        );
        let key = RedisIdempotencyRepository::message_key(&scoped);

        assert_eq!(
            key,
            "storage:idempotency:client:tenant-a:sender-1:conv-a:client-1"
        );
        assert_ne!(
            scoped,
            scoped_client_idempotency_key(
                &ctx_with_tenant("tenant-a"),
                "client-1",
                Some("sender-1"),
                Some("conv-b"),
            )
        );
        assert_ne!(
            scoped,
            scoped_client_idempotency_key(
                &ctx_with_tenant("tenant-b"),
                "client-1",
                Some("sender-1"),
                Some("conv-a"),
            )
        );
    }
}
