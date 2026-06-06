use std::sync::Arc;

use flare_im_core::Ctx;
use flare_im_core::wal_pending_index_key;
use flare_server_core::error::Result;
use redis::aio::ConnectionManager;
use tracing::instrument;

use crate::domain::repository::WalCleanupRepository;

pub struct RedisWalCleanupRepository {
    client: Arc<redis::Client>,
    wal_key: String,
}

impl RedisWalCleanupRepository {
    pub fn new(client: Arc<redis::Client>, wal_key: String) -> Self {
        Self { client, wal_key }
    }
}

impl WalCleanupRepository for RedisWalCleanupRepository {
    #[instrument(skip(self), fields(message_id))]
    async fn remove(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let mut conn = ConnectionManager::new(self.client.as_ref().clone()).await?;
        let pending_key = wal_pending_index_key(&self.wal_key);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HDEL")
            .arg(&self.wal_key)
            .arg(message_id)
            .cmd("ZREM")
            .arg(&pending_key)
            .arg(message_id);
        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;
        Ok(())
    }
}
