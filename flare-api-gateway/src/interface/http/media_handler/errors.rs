//! Media RPC 失败到对外 HTTP 错误的映射。
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_server_core::http::HttpApiError as GatewayError;

/// 越权与不存在按 403/404 返回固定 reason，不带 Media 的原始文案；其余失败保持原样。
///
/// Media 的 NotFound 文案就是调用方传入的 file_id（跨租户查不到时也是它），越权文案描述的是
/// 内部授权规则，两者都不该出现在响应里。此前这两类都被当成未知错误映射为 500。
pub(crate) fn media_rpc_error(err: FlareError) -> GatewayError {
    let (code, reason) = match err.code() {
        Some(ErrorCode::PermissionDenied) => (ErrorCode::HttpForbidden, "FORBIDDEN"),
        Some(ErrorCode::UserNotFound | ErrorCode::MessageNotFound) => {
            (ErrorCode::HttpNotFound, "NOT_FOUND")
        }
        _ => return GatewayError(err),
    };
    tracing::debug!(error = %err, "media rpc denied the request");
    GatewayError(ErrorBuilder::new(code, reason).build_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    async fn body_of(err: GatewayError) -> (u16, serde_json::Value) {
        let response = err.into_response();
        let status = response.status().as_u16();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    /// 经 gRPC 往返后的真实形状：Media 的领域错误编码成 Status，网关客户端再还原成 FlareError。
    fn through_grpc(err: FlareError) -> FlareError {
        FlareError::from(tonic::Status::from(err))
    }

    #[tokio::test]
    async fn not_found_and_denied_map_to_typed_statuses_without_echoing_media_text() {
        let not_found = through_grpc(flare_server_core::flare_err_details!(
            ErrorCode::MessageNotFound,
            "metadata not found",
            "secret-file-id"
        ));
        let (status, body) = body_of(media_rpc_error(not_found)).await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], 404);
        assert_eq!(body["reason"], "NOT_FOUND");
        assert!(!body.to_string().contains("secret-file-id"), "{body}");

        let denied = through_grpc(flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "media access is not authorized"
        ));
        let (status, body) = body_of(media_rpc_error(denied)).await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], 403);
        assert_eq!(body["reason"], "FORBIDDEN");
        assert!(!body.to_string().contains("authorized"), "{body}");
    }

    /// 其余失败（Media 不可用、签名上下文被拒等）仍是服务端错误，不能伪装成 403/404。
    #[tokio::test]
    async fn other_failures_stay_server_errors() {
        for status in [
            tonic::Status::unavailable("media down"),
            tonic::Status::unauthenticated("signed context required"),
            tonic::Status::internal("boom"),
        ] {
            let (code, _) = body_of(media_rpc_error(FlareError::from(status))).await;
            assert_eq!(code, 500);
        }
    }
}
