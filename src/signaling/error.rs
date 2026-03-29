use thiserror::Error;

/// 信令服务错误类型
#[derive(Debug, Error)]
pub enum SignalingError {
    /// 用户未找到
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// 连接未找到
    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    /// 网关未找到
    #[error("Gateway not found: {0}")]
    GatewayNotFound(String),

    /// 服务未找到
    #[error("Service not found: {0}")]
    ServiceNotFound(String),

    /// 无效的参数
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),

    /// 其他错误
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// 信令服务结果类型
pub type SignalingResult<T> = Result<T, SignalingError>;

impl From<SignalingError> for tonic::Status {
    fn from(err: SignalingError) -> Self {
        match err {
            SignalingError::UserNotFound(_) => tonic::Status::not_found(err.to_string()),
            SignalingError::ConnectionNotFound(_) => tonic::Status::not_found(err.to_string()),
            SignalingError::GatewayNotFound(_) => tonic::Status::not_found(err.to_string()),
            SignalingError::ServiceNotFound(_) => tonic::Status::not_found(err.to_string()),
            SignalingError::InvalidParameter(_) => tonic::Status::invalid_argument(err.to_string()),
            SignalingError::Internal(_) => tonic::Status::internal(err.to_string()),
            SignalingError::Other(e) => tonic::Status::internal(format!("Internal error: {}", e)),
        }
    }
}
