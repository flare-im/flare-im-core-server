//! 下游 gRPC 失败到 HTTP 错误的通用映射（会话、在线、消息事件与撤回）。
//!
//! HTTP 发送与 Media 各有自己的映射（`message_handler::send_failure`、
//! `media_handler::errors::media_rpc_error`），暂时性故障的口径与这里相同：下游不可用或超时
//! 回可重试的 503/504，不回显下游文本。
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_server_core::http::HttpApiError as GatewayError;

/// 下游不可用（gRPC `UNAVAILABLE`，含连不上下游）或超过请求超时（`DEADLINE_EXCEEDED`，含网关
/// 按 `GRPC_REQUEST_TIMEOUT_SECS` 放弃的调用）是暂时性故障：回 503 `SERVICE_UNAVAILABLE`，与
/// HTTP 发送相同（W12-T），调用方退避重试；响应体不带下游文本，原文只进本网关日志。
///
/// 其余失败保持各接口原来的 500 与 `reason`。
pub(crate) fn downstream_failure(reason: &'static str, err: FlareError) -> GatewayError {
    if matches!(
        err.code(),
        Some(ErrorCode::ServiceUnavailable | ErrorCode::OperationTimeout)
    ) {
        tracing::warn!(error = %err, reason, "downstream unavailable or timed out");
        return GatewayError(
            ErrorBuilder::new(ErrorCode::HttpServiceUnavailable, "SERVICE_UNAVAILABLE")
                .build_error(),
        );
    }
    GatewayError::internal(reason, err.to_string())
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

    /// 经 gRPC 往返后的形状：下游回的 `Status` 由客户端转成 `FlareError`。
    #[tokio::test]
    async fn unavailable_and_deadline_are_retryable_503_without_downstream_text() {
        for status in [
            tonic::Status::unavailable("online down at 10.0.0.7"),
            tonic::Status::deadline_exceeded("online slow at 10.0.0.7"),
        ] {
            let (code, body) = body_of(downstream_failure(
                "GET_DEVICE_FAILED",
                FlareError::from(status),
            ))
            .await;
            assert_eq!(code, 503, "{body}");
            assert_eq!(body["reason"], "SERVICE_UNAVAILABLE", "{body}");
            assert!(!body.to_string().contains("10.0.0.7"), "{body}");
        }
    }

    /// 其余失败不变：仍是 500 与接口自己的原因。
    #[tokio::test]
    async fn other_failures_keep_the_endpoint_reason() {
        let (code, body) = body_of(downstream_failure(
            "GET_DEVICE_FAILED",
            FlareError::from(tonic::Status::internal("boom")),
        ))
        .await;
        assert_eq!(code, 500, "{body}");
        assert_eq!(body["reason"], "GET_DEVICE_FAILED", "{body}");
    }
}
