//! 同步编排错误：统一为 `FlareError`，gRPC 层经 `IntoGrpc` / `tonic::Status` 暴露给客户端。

use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};

// 重导出 infrastructure 层的错误转换函数
pub use crate::infrastructure::grpc_error::flare_from_tonic_status;

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

pub fn require_same_user(
    authenticated_user_id: &str,
    claimed_user_id: &str,
) -> Result<(), FlareError> {
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
