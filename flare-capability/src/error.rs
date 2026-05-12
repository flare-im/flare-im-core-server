//! 本 crate 统一错误：复用 `flare_server_core::error`。

pub use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result, map_infra_error};
