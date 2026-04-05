//! 本 crate 统一错误：复用 `flare_server_core::error`。
//! gRPC 接口层将 `FlareError` 转为 `tonic::Status`（details 含 `flare_proto::common::ErrorDetail`），见 `IntoGrpc`。

pub use flare_server_core::error::{
    ErrorBuilder, ErrorCode, FlareError, Result, map_infra_error, InfraResultExt,
};
pub use flare_server_core::error::grpc::IntoGrpc;

use flare_server_core::context::Context;

/// 要求已认证用户 ID（Command / Query / 仓储侧）。
pub fn require_user_id(ctx: &Context) -> Result<String> {
    ctx.user_id()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::AuthenticationRequired, "user_id is required")
                .build_error()
        })
}
