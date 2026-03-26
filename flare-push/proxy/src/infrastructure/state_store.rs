use std::sync::Arc;

use anyhow::Result;
use redis::AsyncCommands;

use crate::config::PushProxyConfig;

/// 最小状态：仅任务粗粒度状态（供 QueryPushStatus）。
pub struct RedisStateStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisStateStore {
    pub fn new(config: Arc<PushProxyConfig>) -> Result<Self> {
        let client = redis::Client::open(config.redis_url.as_str())?;
        Ok(Self {
            client,
            key_prefix: config.redis_key_prefix.clone(),
        })
    }

    fn key_task_status(&self, task_id: &str) -> String {
        format!("{}:task:{}", self.key_prefix, task_id)
    }

    async fn conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    pub async fn save_task_status(&self, task_id: &str, status: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let key = self.key_task_status(task_id);
        let _: () = conn.set(key, status).await?;
        Ok(())
    }

    pub async fn get_task_status(&self, task_id: &str) -> Result<Option<String>> {
        let mut conn = self.conn().await?;
        let key = self.key_task_status(task_id);
        let status: Option<String> = conn.get(key).await?;
        Ok(status)
    }
}
