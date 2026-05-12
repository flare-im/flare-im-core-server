//! gRPC 错误转换：将 tonic::Status 转换为 FlareError
//!
//! 本模块属于 infrastructure 层，处理框架相关的错误转换。
//! 使用 flare-server-core 提供的统一错误处理机制。

use flare_im_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tonic::Status;

/// 将下游 `tonic::Status` 分类为 `FlareError`，便于客户端按 `ErrorCode::is_retryable()` 做退避重试。
///
/// ## 设计说明
/// 使用 flare-server-core 的错误转换机制，保持错误处理的一致性。
/// 当 Status 包含结构化 ErrorDetail 时，优先解析；否则根据 gRPC Code 映射到 ErrorCode。
pub fn flare_from_tonic_status(status: &Status) -> FlareError {
    use tonic::Code;

    // 尝试从 Status details 解析结构化错误（如果启用 proto feature）
    #[cfg(feature = "proto")]
    if let Some(detail) = flare_server_core::error::grpc::decode_error_detail(status) {
        // 从结构化错误构建 FlareError
        return FlareError::localized(ErrorCode::from(detail.code), detail.reason)
            .with_details(detail.message);
    }

    // 降级：根据 gRPC Code 映射到 ErrorCode
    let msg = status.message().to_string();
    match status.code() {
        Code::Ok => FlareError::localized(
            ErrorCode::GeneralError,
            "unexpected OK in sync error mapper",
        ),
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
