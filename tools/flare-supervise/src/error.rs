//! Supervisor error helpers on top of `flare_server_core::error`.
//!
//! The supervisor only reports operator-facing failures, so every error is a
//! `FlareError::system` carrying the failed step and its cause.

pub use flare_server_core::error::{FlareError, Result};

/// Attach the failed step to an error or a missing value.
pub trait Context<T> {
    fn context(self, message: &str) -> Result<T>;
    fn with_context(self, message: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E: std::fmt::Display> Context<T> for std::result::Result<T, E> {
    fn context(self, message: &str) -> Result<T> {
        self.map_err(|err| FlareError::system(format!("{message}: {err}")))
    }

    fn with_context(self, message: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|err| FlareError::system(format!("{}: {err}", message())))
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, message: &str) -> Result<T> {
        self.ok_or_else(|| FlareError::system(message))
    }

    fn with_context(self, message: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| FlareError::system(message()))
    }
}

/// Return early with a system error built from a format string.
macro_rules! fail {
    ($($arg:tt)*) => {
        return Err($crate::error::FlareError::system(format!($($arg)*)))
    };
}

/// Return early with a system error when the condition does not hold.
macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            $crate::error::fail!($($arg)*);
        }
    };
}

pub(crate) use {ensure, fail};
