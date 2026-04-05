//! [`ISyncPort`] → sync-orchestrator `ExecuteSync`，**不读** `Sync` 内容。

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_im_core::utils::require_user_id_from_context;
use flare_proto::common::{Sync, SyncRes};
use flare_server_core::client::request_with_context;
use tonic::Status;

use crate::domain::ports::ISyncPort;

use super::storage_sync_grpc_pool::StorageSyncGrpcPool;

pub struct StorageSyncPort {
    pool: Arc<StorageSyncGrpcPool>,
}

impl StorageSyncPort {
    pub fn new(pool: Arc<StorageSyncGrpcPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ISyncPort for StorageSyncPort {
    async fn forward_sync(&self, tx: &Ctx, sync: Sync) -> anyhow::Result<SyncRes> {
        let _ = require_user_id_from_context(tx).map_err(|s| anyhow::anyhow!("{}", s))?;
        let mut client = self
            .pool
            .ensure_sync_client()
            .await
            .map_err(|e| anyhow::anyhow!("sync client: {e}"))?;
        let res = client
            .execute_sync(request_with_context(sync, tx))
            .await
            .map_err(|s: Status| anyhow::anyhow!("ExecuteSync RPC: {}", s))?
            .into_inner();
        Ok(res)
    }
}
