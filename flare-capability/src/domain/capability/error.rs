//! 能力扩展领域错误、`GuardRejection` / `GuardDecision`、统一 `Result`。
//!
//! 与 [`super::ports`] 中的 Guard trait 同用本模块类型，避免循环依赖。

use flare_core_base::error::{ErrorCode, FlareError};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GuardRejection {
    pub guard_id: String,
    pub code: String,
    pub message: String,
    pub tenant_id: Option<String>,
    pub ext: Value,
}

impl GuardRejection {
    pub fn new(
        guard_id: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            guard_id: guard_id.into(),
            code: code.into(),
            message: message.into(),
            tenant_id: None,
            ext: Value::Null,
        }
    }
}

/// PreSendGuard 的判定结果。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GuardDecision {
    Allow,
    Reject(GuardRejection),
}

#[derive(Debug, thiserror::Error)]
pub enum CapabilityError {
    #[error("capability not registered: {0}")]
    NotRegistered(String),

    #[error("capability timeout: {0}")]
    Timeout(String),

    #[error("guard rejected: {0:?}")]
    Rejected(GuardRejection),

    #[error("operation not supported: {0}")]
    NotSupported(String),

    #[error("policy denied: {0}")]
    PolicyDenied(String),

    #[error("system: {0}")]
    System(String),

    #[error(transparent)]
    Flare(#[from] FlareError),
}

pub type Result<T> = std::result::Result<T, CapabilityError>;

impl CapabilityError {
    pub fn into_flare(self) -> FlareError {
        match self {
            CapabilityError::NotRegistered(msg) => {
                FlareError::localized(ErrorCode::HttpNotFound, msg)
            }
            CapabilityError::Timeout(msg) => FlareError::timeout(msg),
            CapabilityError::Rejected(r) => {
                FlareError::localized(ErrorCode::PermissionDenied, r.message.clone())
            }
            CapabilityError::NotSupported(msg) => {
                FlareError::localized(ErrorCode::OperationNotSupported, msg)
            }
            CapabilityError::PolicyDenied(msg) => {
                FlareError::localized(ErrorCode::PermissionDenied, msg)
            }
            CapabilityError::System(msg) => FlareError::system(msg),
            CapabilityError::Flare(e) => e,
        }
    }
}
