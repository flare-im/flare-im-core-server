//! 查询侧：Redis 粗粒度任务状态（供 QueryPushStatus）。

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_server_core::error::Result;

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
