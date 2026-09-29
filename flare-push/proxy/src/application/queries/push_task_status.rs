//! 查询侧：Redis 粗粒度任务状态（供 QueryPushStatus）。

use std::sync::Arc;

use flare_im_contracts::Ctx;
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

    pub async fn get_task_status(&self, ctx: &Ctx, task_id: &str) -> Result<Option<String>> {
        let tenant_id = ctx.tenant_id().ok_or_else(|| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::PermissionDenied,
                "verified tenant required for push task status",
            )
            .build_error()
        })?;
        self.store.get_task_status(tenant_id, task_id).await
    }
}
