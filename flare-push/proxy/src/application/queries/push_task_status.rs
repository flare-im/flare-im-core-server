//! 查询侧：Redis 粗粒度任务状态（供 QueryPushStatus）。

use std::sync::Arc;

use anyhow::Result;
use flare_server_core::context::Ctx;

use crate::infrastructure::RedisStateStore;

#[derive(Clone)]
pub struct PushTaskStatusQuery {
    store: Arc<RedisStateStore>,
}

impl PushTaskStatusQuery {
    pub fn new(store: Arc<RedisStateStore>) -> Self {
        Self { store }
    }

    pub async fn get_task_status(&self, _ctx: &Ctx, task_id: &str) -> Result<Option<String>> {
        self.store.get_task_status(task_id).await
    }
}
