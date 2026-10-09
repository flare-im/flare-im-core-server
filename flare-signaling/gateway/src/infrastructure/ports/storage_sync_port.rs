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
            .map_err(sync_rpc_error)?
            .into_inner();
        Ok(res)
    }
}

/// Preserve downstream classifications, not their display strings. Structured internal
/// ErrorDetail is authoritative; legacy peers without details retain the gRPC code.
fn sync_rpc_error(status: Status) -> flare_server_core::error::FlareError {
    use flare_server_core::error::{ErrorCode, FlareError};
    if !status.details().is_empty()
        && let Some(detail) = flare_server_core::error::grpc::decode_error_detail(&status)
        && ErrorCode::from_u32(detail.code.max(0) as u32).is_some()
    {
        return flare_server_core::error::from_error_detail(&detail);
    }
    let code = match status.code() {
        tonic::Code::PermissionDenied => ErrorCode::PermissionDenied,
        tonic::Code::Unauthenticated => ErrorCode::AuthenticationRequired,
        tonic::Code::NotFound => ErrorCode::MessageNotFound,
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => ErrorCode::InvalidParameter,
        tonic::Code::Unavailable => ErrorCode::ServiceUnavailable,
        tonic::Code::DeadlineExceeded => ErrorCode::OperationTimeout,
        tonic::Code::ResourceExhausted => ErrorCode::ResourceExhausted,
        tonic::Code::Unimplemented => ErrorCode::OperationNotSupported,
        tonic::Code::Cancelled
        | tonic::Code::Aborted
        | tonic::Code::AlreadyExists
        | tonic::Code::FailedPrecondition => ErrorCode::OperationFailed,
        _ => ErrorCode::InternalError,
    };
    FlareError::localized(code, status.message())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::error::{ErrorCode, FlareError};

    #[test]
    fn sync_rpc_error_preserves_permission_denied_with_and_without_details() {
        for status in [
            Status::permission_denied("conversation access denied"),
            Status::from(FlareError::localized(
                ErrorCode::PermissionDenied,
                "conversation access denied",
            )),
        ] {
            let error = sync_rpc_error(status);
            assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
            assert_eq!(error.reason(), "conversation access denied");
        }
    }

    #[test]
    fn sync_rpc_error_does_not_turn_dependency_failures_into_denials() {
        for (status, expected) in [
            (
                Status::unavailable("dependency unavailable"),
                ErrorCode::ServiceUnavailable,
            ),
            (
                Status::deadline_exceeded("dependency timeout"),
                ErrorCode::OperationTimeout,
            ),
            (
                Status::invalid_argument("invalid target"),
                ErrorCode::InvalidParameter,
            ),
        ] {
            let error = sync_rpc_error(status);
            assert_eq!(error.code(), Some(expected));
        }
    }

    #[test]
    fn sync_rpc_error_unknown_detail_falls_back_to_grpc_not_display_text() {
        let detail = flare_proto::common::ErrorDetail {
            code: 123_456,
            reason: "PermissionDenied".into(),
            message: "PermissionDenied".into(),
            ..Default::default()
        };
        let status = flare_server_core::error::grpc::build_status(tonic::Code::Unavailable, detail);
        assert_eq!(
            sync_rpc_error(status).code(),
            Some(ErrorCode::ServiceUnavailable)
        );
        assert_eq!(
            sync_rpc_error(Status::internal("PermissionDenied")).code(),
            Some(ErrorCode::InternalError),
        );
    }
}
