use std::sync::Arc;

use anyhow::Result;
use flare_server_core::context::Ctx;
use redis::{AsyncCommands, aio::ConnectionManager};
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
        let _: () = conn.hdel(&self.wal_key, message_id).await?;
        Ok(())
    }
}
