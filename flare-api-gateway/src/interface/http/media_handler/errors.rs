//! Media RPC 失败到对外 HTTP 错误的映射。所有 media 路由（文件、引用、上传、分片、直传、
//! 处理）都经这一个函数出错，保证拒绝码与响应体形状一致。
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_server_core::http::HttpApiError as GatewayError;

/// Media 的失败按类别回固定 reason，响应体里不带 Media 的原始文案。
///
/// - 越权 → 403 `FORBIDDEN`；不存在（含他人或别的租户的上传会话）→ 404 `NOT_FOUND`；
///   参数不合法 → 400 `BAD_REQUEST`。
/// - 其余（Media 不可用、内部故障、签名上下文被拒等）仍是 500，reason 固定为
///   `MEDIA_REQUEST_FAILED`。
///
/// Media 的文案会带内部标识：NotFound 的是调用方传入的 file_id（跨租户查不到时也是它），
/// 上传会话错误带 upload_id，存储错误带对象 key，越权文案描述内部授权规则。这些只进日志。
pub(crate) fn media_rpc_error(err: FlareError) -> GatewayError {
    let (code, reason) = match err.code() {
        Some(ErrorCode::PermissionDenied) => (ErrorCode::HttpForbidden, "FORBIDDEN"),
        Some(ErrorCode::UserNotFound | ErrorCode::MessageNotFound) => {
            (ErrorCode::HttpNotFound, "NOT_FOUND")
        }
        Some(
            ErrorCode::InvalidParameter
            | ErrorCode::BadRequest
            | ErrorCode::MessageFormatError
            | ErrorCode::InvalidCommand,
        ) => (ErrorCode::HttpBadRequest, "BAD_REQUEST"),
        _ => {
            tracing::warn!(error = %err, "media rpc failed");
            return GatewayError(
                ErrorBuilder::new(ErrorCode::HttpInternalServerError, "MEDIA_REQUEST_FAILED")
                    .build_error(),
            );
        }
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

    /// 上传类路由的参数错误（分片越界、会话状态不对、调用方点名的 key 等）是调用方错误。
    #[tokio::test]
    async fn invalid_arguments_map_to_bad_request_without_echoing_media_text() {
        for invalid in [
            flare_server_core::flare_err_details!(
                ErrorCode::InvalidParameter,
                "part number out of range",
                "part_number=9 total_parts=2 upload_id=secret-upload-id"
            ),
            flare_server_core::flare_err_details!(
                ErrorCode::InvalidParameter,
                "chunk size exceeds limit",
                "chunk_size=99 max=8 upload_id=secret-upload-id"
            ),
            flare_server_core::flare_err!(
                ErrorCode::MessageFormatError,
                "secret-upload-id is malformed"
            ),
        ] {
            let (status, body) = body_of(media_rpc_error(through_grpc(invalid))).await;
            assert_eq!(status, 400, "{body}");
            assert_eq!(body["code"], 400);
            assert_eq!(body["reason"], "BAD_REQUEST");
            assert!(!body.to_string().contains("secret-upload-id"), "{body}");
        }
    }

    /// 其余失败（Media 不可用、签名上下文被拒、存储故障等）仍是服务端错误，不能伪装成
    /// 4xx；响应体同样不带 Media 文案（存储错误里有对象 key）。
    #[tokio::test]
    async fn other_failures_stay_server_errors_without_echoing_media_text() {
        for status in [
            tonic::Status::unavailable("media down secret-object-key"),
            tonic::Status::unauthenticated("signed context required secret-object-key"),
            tonic::Status::internal("boom secret-object-key"),
            tonic::Status::from(flare_server_core::flare_err_details!(
                ErrorCode::NetworkError,
                "failed to presign s3 put url",
                "key=secret-object-key"
            )),
        ] {
            let (code, body) = body_of(media_rpc_error(FlareError::from(status))).await;
            assert_eq!(code, 500);
            assert_eq!(body["reason"], "MEDIA_REQUEST_FAILED");
            assert!(!body.to_string().contains("secret-object-key"), "{body}");
        }
    }
}
