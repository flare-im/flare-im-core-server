use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use flare_core_base::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_server_core::http::ApiResponse;
use thiserror::Error;

/// 网关错误类型（统一承载 flare-core-base 的 FlareError）
#[derive(Debug, Error, Clone)]
#[error("{0}")]
pub struct GatewayError(pub FlareError);

/// 结果类型别名
pub type Result<T> = std::result::Result<T, GatewayError>;

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = status_from_flare_error(&self.0);
        let body: ApiResponse<()> = ApiResponse::from(self.0);
        let body = Json(body);
        (status, body).into_response()
    }
}

impl GatewayError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self(
            ErrorBuilder::new(ErrorCode::HttpNotFound, "NOT_FOUND")
                .details(message)
                .build_error(),
        )
    }

    pub fn bad_request(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self(
            ErrorBuilder::new(ErrorCode::HttpBadRequest, reason)
                .details(message)
                .build_error(),
        )
    }

    pub fn internal(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self(
            ErrorBuilder::new(ErrorCode::HttpInternalServerError, reason)
                .details(message)
                .build_error(),
        )
    }
}

fn status_from_flare_error(error: &FlareError) -> StatusCode {
    match error.code() {
        Some(ErrorCode::HttpBadRequest) => StatusCode::BAD_REQUEST,
        Some(ErrorCode::HttpUnauthorized) => StatusCode::UNAUTHORIZED,
        Some(ErrorCode::HttpForbidden) => StatusCode::FORBIDDEN,
        Some(ErrorCode::HttpNotFound) => StatusCode::NOT_FOUND,
        Some(ErrorCode::HttpMethodNotAllowed) => StatusCode::METHOD_NOT_ALLOWED,
        Some(ErrorCode::HttpRequestTimeout) => StatusCode::REQUEST_TIMEOUT,
        Some(ErrorCode::HttpConflict) => StatusCode::CONFLICT,
        Some(ErrorCode::HttpUnprocessableEntity) => StatusCode::UNPROCESSABLE_ENTITY,
        Some(ErrorCode::HttpTooManyRequests) => StatusCode::TOO_MANY_REQUESTS,
        Some(ErrorCode::HttpBadGateway) => StatusCode::BAD_GATEWAY,
        Some(ErrorCode::HttpServiceUnavailable) => StatusCode::SERVICE_UNAVAILABLE,
        Some(ErrorCode::HttpGatewayTimeout) => StatusCode::GATEWAY_TIMEOUT,
        Some(ErrorCode::HttpInternalServerError) => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// 实现从其他错误类型的转换
impl From<serde_json::Error> for GatewayError {
    fn from(err: serde_json::Error) -> Self {
        GatewayError::bad_request("SERIALIZATION_ERROR", err.to_string())
    }
}

impl From<tonic::Status> for GatewayError {
    fn from(status: tonic::Status) -> Self {
        GatewayError(
            ErrorBuilder::new(ErrorCode::HttpBadGateway, "GRPC_ERROR")
                .details(status.message())
                .build_error(),
        )
    }
}

impl From<anyhow::Error> for GatewayError {
    fn from(err: anyhow::Error) -> Self {
        GatewayError::internal("INTERNAL_ERROR", err.to_string())
    }
}

impl From<FlareError> for GatewayError {
    fn from(err: FlareError) -> Self {
        GatewayError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GatewayError::bad_request("VALIDATION_ERROR", "invalid parameter");
        assert!(err.to_string().contains("错误 [BAD_REQUEST]"));
    }

    #[test]
    fn test_error_from_tonic() {
        let status = tonic::Status::invalid_argument("test error");
        let err: GatewayError = status.into();
        let localized = err.0.as_localized().expect("must be localized");
        assert_eq!(localized.code, ErrorCode::HttpBadGateway);
    }
}
