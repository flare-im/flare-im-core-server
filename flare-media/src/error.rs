//! 本 crate 统一错误：复用 `flare_server_core::error`。
//! gRPC 接口层将 `FlareError` 转为 `tonic::Status`（details 含 `flare_proto::common::ErrorDetail`），见 `IntoGrpc`。

use flare_server_core::context::ContextError;

pub use flare_server_core::error::{
    ErrorCode, FlareError, InfraResultExt, Result, map_infra_error,
};
pub use flare_server_core::error::grpc::IntoGrpc;

/// 上下文取消 / deadline 与领域 `FlareError` 的桥接。
#[inline]
pub fn map_context_error(e: ContextError) -> FlareError {
    flare_server_core::flare_err_details!(
        ErrorCode::GeneralError,
        "context check failed",
        e.to_string()
    )
}
