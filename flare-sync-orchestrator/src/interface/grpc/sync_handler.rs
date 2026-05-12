//! gRPC：`ExecuteSync` 与 DATA 信道共用 `flare.common.v1.Sync` / `SyncRes`。

use std::sync::Arc;

use flare_grpc_proto::sync::sync_service_server::SyncService;
use flare_im_core::utils::require_user_id_from_context;
use flare_proto::common::{Sync, SyncRes};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::extract_ctx_from_request_opt;
use tonic::{Request, Response, Status};

use crate::application::handlers::SyncOrchestrationHandler;
use crate::application::ports::MemorySyncCursorCache;
use crate::infrastructure::rpc::GrpcSyncAdapters;

/// gRPC 入口：`user_id` 仅从认证上下文读取。
pub struct SyncOrchestratorGrpcHandler {
    inner: Arc<SyncOrchestrationHandler<GrpcSyncAdapters>>,
}

impl SyncOrchestratorGrpcHandler {
    pub fn new() -> Self {
        let cache = Arc::new(MemorySyncCursorCache::new());
        let infra = Arc::new(GrpcSyncAdapters);
        let inner = Arc::new(SyncOrchestrationHandler::new(infra, cache));
        Self { inner }
    }
}

#[tonic::async_trait]
impl SyncService for SyncOrchestratorGrpcHandler {
    async fn execute_sync(&self, request: Request<Sync>) -> Result<Response<SyncRes>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let sync = request.into_inner();
        let user_id = require_user_id_from_context(&ctx).map_err(|e| Status::unauthenticated(e))?;
        let res = self
            .inner
            .execute_sync(&ctx, &user_id, sync)
            .await
            .into_grpc()?;
        Ok(Response::new(res))
    }
}
