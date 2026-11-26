//! Capability contract errors.

use flare_core_base::error::{ErrorBuilder, ErrorCode, FlareError};
use std::fmt;

#[derive(Debug)]
pub enum CapabilityError {
    NotRegistered(String),
    Timeout(String),
    NotSupported(String),
    PolicyDenied(String),
    /// A plugin platform refusal (plugin platform v2 §4): `code` is a platform code such as
    /// `PLUGIN_DISABLED` or `PLUGIN_CARD_UNBOUND` (`[A-Z0-9_]{1,64}`), carried as the error
    /// reason so the API gateway answers with it; `message` says why.
    Plugin {
        code: String,
        message: String,
    },
    System(String),
    Flare(FlareError),
}

pub type Result<T> = std::result::Result<T, CapabilityError>;

impl CapabilityError {
    pub fn plugin(code: &str, message: impl Into<String>) -> Self {
        Self::Plugin {
            code: code.to_owned(),
            message: message.into(),
        }
    }

    /// The platform code of a [`Self::Plugin`] refusal.
    pub fn plugin_code(&self) -> Option<&str> {
        match self {
            Self::Plugin { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn into_flare(self) -> FlareError {
        match self {
            CapabilityError::NotRegistered(msg) => {
                FlareError::localized(ErrorCode::HttpNotFound, msg)
            }
            CapabilityError::Timeout(msg) => FlareError::timeout(msg),
            CapabilityError::NotSupported(msg) => {
                FlareError::localized(ErrorCode::OperationNotSupported, msg)
            }
            CapabilityError::PolicyDenied(msg) => {
                FlareError::localized(ErrorCode::PermissionDenied, msg)
            }
            CapabilityError::Plugin { code, message } => {
                ErrorBuilder::new(ErrorCode::PermissionDenied, code)
                    .details(message)
                    .build_error()
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
            CapabilityError::NotSupported(msg) => write!(f, "operation not supported: {msg}"),
            CapabilityError::PolicyDenied(msg) => write!(f, "policy denied: {msg}"),
            CapabilityError::Plugin { code, message } => write!(f, "{code}: {message}"),
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
