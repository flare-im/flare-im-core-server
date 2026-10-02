//! [`ISyncPort`] → sync-orchestrator `ExecuteSync`，**不读** `Sync` 内容。

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::require_user_id_from_context;
use flare_proto::common::{Sync, SyncRes};
use flare_server_core::SignedContextSigner;
use tonic::Status;

use crate::domain::ports::ISyncPort;

use super::storage_sync_grpc_pool::StorageSyncGrpcPool;

pub struct StorageSyncPort {
    pool: Arc<StorageSyncGrpcPool>,
    /// 代已认证连接签同步请求的 `x-flare-ctx`；sync-orchestrator 验签后把它原样转发给会话与
    /// Storage Reader 的读接口。
    signer: Option<SignedContextSigner>,
}

impl StorageSyncPort {
    pub fn new(pool: Arc<StorageSyncGrpcPool>, signer: Option<SignedContextSigner>) -> Self {
        Self { pool, signer }
    }
}

#[async_trait]
impl ISyncPort for StorageSyncPort {
    async fn forward_sync(
        &self,
        tx: &Ctx,
        sync: Sync,
    ) -> flare_server_core::error::Result<SyncRes> {
        let _ = require_user_id_from_context(tx)
            .map_err(|s| flare_server_core::error::FlareError::system(s.to_string()))?;
        let mut client = self.pool.ensure_sync_client().await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("sync client: {e}"))
        })?;
        let request = crate::infrastructure::connection_context::signed_connection_read(
            sync,
            tx,
            self.signer.as_ref(),
        )?;
        let res = client
            .execute_sync(request)
            .await
            .map_err(|s: Status| {
                flare_server_core::error::FlareError::system(format!("ExecuteSync RPC: {}", s))
            })?
            .into_inner();
        Ok(res)
    }
}
