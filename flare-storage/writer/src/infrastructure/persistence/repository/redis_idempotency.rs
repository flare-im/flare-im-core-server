use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
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

    fn message_key(ctx: &Ctx, message_id: &str) -> Result<String> {
        let tenant_id = ctx.tenant_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system("tenant required for idempotency key")
        })?;
        if !is_valid_tenant_id(tenant_id) || normalize_tenant_id(tenant_id) != tenant_id {
            return Err(flare_server_core::error::FlareError::system(
                "canonical tenant required for idempotency key",
            ));
        }
        Ok(format!("storage:idempotency:{tenant_id}:{message_id}"))
    }
}

impl MessageIdempotencyRepository for RedisIdempotencyRepository {
    #[instrument(skip(self), fields(message_id))]
    async fn is_new(&self, ctx: &Ctx, message_id: &str) -> Result<bool> {
        let key = Self::message_key(ctx, message_id)?;
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;
        let is_new: bool = conn.set_nx(&key, 1).await?;
        if is_new && self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&key, ttl).await?;
        }

        Ok(is_new)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn release(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        if message_id.is_empty() {
            return Ok(());
        }
        let key = Self::message_key(ctx, message_id)?;
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;
        let _: () = conn.del(key).await?;
        Ok(())
    }

    // is_new_by_client_msg_id / release_by_client_msg_id 走 trait 默认实现：
    // 客户端 ID 仍由 domain::scoped_client_idempotency_key 生成，Redis 前缀再加
    // 可信 tenant 一层；旧键不双读，避免旧的裸 server-id 键跨租户碰撞。
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
        // 同一 client ID 在两个租户的 Redis key 必须不同；旧键不双读。
        let scoped = scoped_client_idempotency_key(
            &ctx_with_tenant("tenant-a"),
            "client-1",
            Some("sender-1"),
            Some("conv-a"),
        )
        .unwrap();
        let key =
            RedisIdempotencyRepository::message_key(&ctx_with_tenant("tenant-a"), &scoped).unwrap();

        assert_eq!(
            key,
            "storage:idempotency:tenant-a:client:tenant-a:sender-1:conv-a:client-1"
        );
        assert_ne!(
            RedisIdempotencyRepository::message_key(&ctx_with_tenant("tenant-a"), "same").unwrap(),
            RedisIdempotencyRepository::message_key(&ctx_with_tenant("tenant-b"), "same").unwrap(),
        );
        assert_ne!(
            scoped,
            scoped_client_idempotency_key(
                &ctx_with_tenant("tenant-a"),
                "client-1",
                Some("sender-1"),
                Some("conv-b"),
            )
            .unwrap()
        );
        assert_ne!(
            key,
            RedisIdempotencyRepository::message_key(&ctx_with_tenant("tenant-b"), &scoped).unwrap()
        );
        assert!(RedisIdempotencyRepository::message_key(&Ctx::default(), "same").is_err());
        assert_ne!(
            scoped,
            scoped_client_idempotency_key(
                &ctx_with_tenant("tenant-b"),
                "client-1",
                Some("sender-1"),
                Some("conv-a"),
            )
            .unwrap()
        );
    }

    /// 缺租户的上下文不再落到租户 "0" 的去重键空间（W11-T）；显式租户 "0" 照常。
    #[test]
    fn client_message_key_requires_a_tenant() {
        for missing in [Ctx::default(), ctx_with_tenant(""), ctx_with_tenant("  ")] {
            assert!(
                scoped_client_idempotency_key(&missing, "client-1", Some("sender-1"), None)
                    .is_err()
            );
        }
        assert_eq!(
            scoped_client_idempotency_key(&ctx_with_tenant("0"), "client-1", None, None).unwrap(),
            "client:0:0:0:client-1"
        );
    }
}
