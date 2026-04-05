use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

/// 网关错误类型
#[derive(Debug, Error)]
pub enum GatewayError {
    /// 配置错误
    #[error("Configuration error: {0}")]
    Config(String),

    /// 参数验证错误
    #[error("Validation error: {0}")]
    Validation(String),

    /// gRPC 调用错误
    #[error("gRPC error: {0}")]
    Grpc(String),

    /// 序列化错误
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// 未找到资源
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// 未授权
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// 限流错误
    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    /// 超时错误
    #[error("Request timeout")]
    Timeout,

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, GatewayError>;

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, error_code, message) = match self {
            GatewayError::Config(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "CONFIG_ERROR", msg)
            }
            GatewayError::Validation(msg) => {
                (StatusCode::BAD_REQUEST, "VALIDATION_ERROR", msg)
            }
            GatewayError::Grpc(msg) => {
                (StatusCode::BAD_GATEWAY, "GRPC_ERROR", msg)
            }
            GatewayError::Serialization(msg) => {
                (StatusCode::BAD_REQUEST, "SERIALIZATION_ERROR", msg)
            }
            GatewayError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, "NOT_FOUND", msg)
            }
            GatewayError::Unauthorized(msg) => {
                (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", msg)
            }
            GatewayError::RateLimitExceeded => {
                (StatusCode::TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED", "Too many requests".to_string())
            }
            GatewayError::Timeout => {
                (StatusCode::REQUEST_TIMEOUT, "TIMEOUT", "Request timeout".to_string())
            }
            GatewayError::Internal(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg)
            }
        };

        let body = Json(json!({
            "success": false,
            "error": {
                "code": error_code,
                "message": message,
            }
        }));

        (status, body).into_response()
    }
}

// 实现从其他错误类型的转换
impl From<serde_json::Error> for GatewayError {
    fn from(err: serde_json::Error) -> Self {
        GatewayError::Serialization(err.to_string())
    }
}

impl From<tonic::Status> for GatewayError {
    fn from(status: tonic::Status) -> Self {
        GatewayError::Grpc(status.message().to_string())
    }
}

impl From<anyhow::Error> for GatewayError {
    fn from(err: anyhow::Error) -> Self {
        GatewayError::Internal(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GatewayError::Validation("invalid parameter".to_string());
        assert_eq!(err.to_string(), "Validation error: invalid parameter");
    }

    #[test]
    fn test_error_from_tonic() {
        let status = tonic::Status::invalid_argument("test error");
        let err: GatewayError = status.into();
        assert!(matches!(err, GatewayError::Grpc(_)));
    }
}
