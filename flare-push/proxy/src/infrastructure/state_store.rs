use std::sync::Arc;

use flare_server_core::error::Result;
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

    fn key_task_status(&self, tenant_id: &str, task_id: &str) -> String {
        task_status_key(&self.key_prefix, tenant_id, task_id)
    }

    async fn conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    pub async fn save_task_status(
        &self,
        tenant_id: &str,
        task_id: &str,
        status: &str,
    ) -> Result<()> {
        let mut conn = self.conn().await?;
        let key = self.key_task_status(tenant_id, task_id);
        let _: () = conn.set(key, status).await?;
        Ok(())
    }

    pub async fn get_task_status(&self, tenant_id: &str, task_id: &str) -> Result<Option<String>> {
        let mut conn = self.conn().await?;
        let key = self.key_task_status(tenant_id, task_id);
        let status: Option<String> = conn.get(key).await?;
        Ok(status)
    }
}

fn task_status_key(prefix: &str, tenant_id: &str, task_id: &str) -> String {
    format!("{prefix}:task:v2:{tenant_id}:{task_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_task_ids_in_two_tenants_have_distinct_status_keys() {
        assert_ne!(
            task_status_key("push", "t1", "same-task"),
            task_status_key("push", "t2", "same-task")
        );
        assert_eq!(
            task_status_key("push", "t1", "same-task"),
            "push:task:v2:t1:same-task"
        );
    }
}
