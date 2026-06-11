//! 同步出站端口：网关 **不解析** `Sync.kind` / `payload`，仅透传至下游（如 sync-orchestrator `ExecuteSync`）。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_proto::common::{Sync as ClientSync, SyncRes};

#[async_trait]
pub trait ISyncPort: Send + std::marker::Sync {
    /// 原样转发 `Sync`，返回下游 `SyncRes`（业务成败见 `SyncRes.status`，与 DATA 回包一致）。
    async fn forward_sync(
        &self,
        tx: &Ctx,
        sync: ClientSync,
    ) -> flare_server_core::error::Result<SyncRes>;
}
