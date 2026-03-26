//! 同步编排错误：统一为 `FlareError`，经 `flare-server-core::error::proto::to_rpc_status` / `grpc::Status` 暴露给客户端。

use crate::domain::SyncDomainError;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tonic::Status;

/// 将下游 `tonic::Status` 分类为 `FlareError`，便于客户端按 `ErrorCode::is_retryable()` 做退避重试（飞书式体验）。
pub fn flare_from_tonic_status(status: &Status) -> FlareError {
    use tonic::Code;
    let msg = status.message().to_string();
    match status.code() {
        Code::Ok => FlareError::localized(ErrorCode::GeneralError, "unexpected OK in sync error mapper"),
        Code::Cancelled => ErrorBuilder::new(ErrorCode::OperationFailed, msg)
            .param("grpc_code", "CANCELLED")
            .build_error(),
        Code::Unknown => ErrorBuilder::new(ErrorCode::InternalError, msg)
            .param("grpc_code", "UNKNOWN")
            .build_error(),
        Code::InvalidArgument => ErrorBuilder::new(ErrorCode::InvalidParameter, msg)
            .param("grpc_code", "INVALID_ARGUMENT")
            .build_error(),
        Code::DeadlineExceeded => ErrorBuilder::new(ErrorCode::OperationTimeout, msg)
            .param("grpc_code", "DEADLINE_EXCEEDED")
            .build_error(),
        Code::NotFound => ErrorBuilder::new(ErrorCode::MessageNotFound, msg)
            .param("grpc_code", "NOT_FOUND")
            .build_error(),
        Code::AlreadyExists => ErrorBuilder::new(ErrorCode::OperationFailed, msg)
            .param("grpc_code", "ALREADY_EXISTS")
            .build_error(),
        Code::PermissionDenied => ErrorBuilder::new(ErrorCode::PermissionDenied, msg)
            .param("grpc_code", "PERMISSION_DENIED")
            .build_error(),
        Code::ResourceExhausted => ErrorBuilder::new(ErrorCode::ResourceExhausted, msg)
            .param("grpc_code", "RESOURCE_EXHAUSTED")
            .build_error(),
        Code::FailedPrecondition => ErrorBuilder::new(ErrorCode::OperationFailed, msg)
            .param("grpc_code", "FAILED_PRECONDITION")
            .build_error(),
        Code::Aborted => ErrorBuilder::new(ErrorCode::OperationFailed, msg)
            .param("grpc_code", "ABORTED")
            .build_error(),
        Code::OutOfRange => ErrorBuilder::new(ErrorCode::InvalidParameter, msg)
            .param("grpc_code", "OUT_OF_RANGE")
            .build_error(),
        Code::Unimplemented => ErrorBuilder::new(ErrorCode::OperationNotSupported, msg)
            .param("grpc_code", "UNIMPLEMENTED")
            .build_error(),
        Code::Internal => ErrorBuilder::new(ErrorCode::InternalError, msg)
            .param("grpc_code", "INTERNAL")
            .build_error(),
        Code::Unavailable => ErrorBuilder::new(ErrorCode::ServiceUnavailable, msg)
            .param("grpc_code", "UNAVAILABLE")
            .build_error(),
        Code::DataLoss => ErrorBuilder::new(ErrorCode::InternalError, msg)
            .param("grpc_code", "DATA_LOSS")
            .build_error(),
        Code::Unauthenticated => ErrorBuilder::new(ErrorCode::AuthenticationFailed, msg)
            .param("grpc_code", "UNAUTHENTICATED")
            .build_error(),
    }
}

pub fn discovery_unavailable(service: &str, cause: impl std::fmt::Display) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::ServiceUnavailable,
        format!("同步依赖服务 `{service}` 不可用，请稍后重试"),
    )
    .param("service", service.to_string())
    .details(cause.to_string())
    .build_error()
}

/// 同步 RPC 公共参数校验（尽早失败，减少下游无效负载）。
pub fn require_nonempty_conversation_id(conversation_id: &str) -> Result<(), FlareError> {
    if conversation_id.trim().is_empty() {
        return Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "conversation_id 不能为空")
                .param("field", "conversation_id")
                .build_error(),
        );
    }
    Ok(())
}

pub fn require_same_user(authenticated_user_id: &str, claimed_user_id: &str) -> Result<(), FlareError> {
    if authenticated_user_id != claimed_user_id {
        return Err(
            ErrorBuilder::new(ErrorCode::PermissionDenied, "禁止访问其他用户的同步游标")
                .param("authenticated_user_id", authenticated_user_id.to_string())
                .param("claimed_user_id", claimed_user_id.to_string())
                .build_error(),
        );
    }
    Ok(())
}

impl From<SyncDomainError> for FlareError {
    fn from(e: SyncDomainError) -> Self {
        match e {
            SyncDomainError::CursorRegression { previous, attempted } => ErrorBuilder::new(
                ErrorCode::SyncCursorRegression,
                "同步游标不可回退，请重新拉取快照后再上报",
            )
            .param("previous_seq", previous.to_string())
            .param("attempted_seq", attempted.to_string())
            .details("若多端并发，请以较大 last_seq 为准或触发全量同步".to_string())
            .build_error(),
        }
    }
}
