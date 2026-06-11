//! 同步领域服务（DATA `DataPacket` / `sync_request`）
//!
//! 网关 **零解析**：补全 `device_id` 后透传 [`ISyncPort::forward_sync`]；语义校验与 `SyncRes` 组装全部由 sync-orchestrator 负责。

use std::sync::Arc;

use flare_core::common::error::{FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::{Sync, SyncRes};

use crate::domain::ports::ISyncPort;

pub struct SyncService {
    port: Arc<dyn ISyncPort>,
}

impl SyncService {
    pub fn new(port: Arc<dyn ISyncPort>) -> Self {
        Self { port }
    }

    /// 处理同步：仅连接态补全 → 下游返回 `SyncRes`；传输/RPC 失败返回领域错误。
    pub async fn execute(&self, tx: &Ctx, _connection_id: &str, mut sync: Sync) -> Result<SyncRes> {
        if sync.device_id.is_empty() {
            sync.device_id = tx.device_id().map(str::to_string).unwrap_or_default();
        }

        self.port
            .forward_sync(tx, sync)
            .await
            .map_err(|e| FlareError::system(format!("sync forward: {e}")))
    }
}
