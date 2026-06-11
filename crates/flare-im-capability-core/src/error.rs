//! Capability contract errors and guard decisions.

use flare_core_base::error::{ErrorBuilder, ErrorCode, FlareError};
use serde_json::Value;
use std::fmt;

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GuardDecision {
    Allow,
    Reject(GuardRejection),
}

#[derive(Debug)]
pub enum CapabilityError {
    NotRegistered(String),
    Timeout(String),
    Rejected(Box<GuardRejection>),
    NotSupported(String),
    PolicyDenied(String),
    System(String),
    Flare(FlareError),
}

pub type Result<T> = std::result::Result<T, CapabilityError>;

impl CapabilityError {
    pub fn into_flare(self) -> FlareError {
        match self {
            CapabilityError::NotRegistered(msg) => {
                FlareError::localized(ErrorCode::HttpNotFound, msg)
            }
            CapabilityError::Timeout(msg) => FlareError::timeout(msg),
            CapabilityError::Rejected(rejection) => {
                let rejection = *rejection;
                ErrorBuilder::new(ErrorCode::PermissionDenied, rejection.code)
                    .details(rejection.message)
                    .build_error()
            }
            CapabilityError::NotSupported(msg) => {
                FlareError::localized(ErrorCode::OperationNotSupported, msg)
            }
            CapabilityError::PolicyDenied(msg) => {
                FlareError::localized(ErrorCode::PermissionDenied, msg)
            }
            CapabilityError::System(msg) => FlareError::system(msg),
            CapabilityError::Flare(err) => err,
        }
    }
}

impl From<FlareError> for CapabilityError {
    fn from(value: FlareError) -> Self {
        Self::Flare(value)
    }
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapabilityError::NotRegistered(msg) => write!(f, "capability not registered: {msg}"),
            CapabilityError::Timeout(msg) => write!(f, "capability timeout: {msg}"),
            CapabilityError::Rejected(rejection) => write!(f, "guard rejected: {rejection:?}"),
            CapabilityError::NotSupported(msg) => write!(f, "operation not supported: {msg}"),
            CapabilityError::PolicyDenied(msg) => write!(f, "policy denied: {msg}"),
            CapabilityError::System(msg) => write!(f, "system: {msg}"),
            CapabilityError::Flare(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for CapabilityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CapabilityError::Flare(err) => Some(err),
            _ => None,
        }
    }
}
