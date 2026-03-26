//! 错误转换工具模块
//!
//! 提供统一的错误转换函数，将各种错误类型转换为 `flare_core::common::error::FlareError`

use flare_core::common::error::FlareError;
use flare_server_core::error::FlareError as ServerFlareError;

/// 将 `flare_server_core::error::FlareError` 转换为 `flare_core::common::error::FlareError`
pub fn server_error_to_core(error: ServerFlareError) -> FlareError {
    match error {
        ServerFlareError::Localized {
            code,
            reason,
            details,
            params,
            timestamp,
        } => {
            // 将 server error code 映射到 core error code
            let core_code = map_server_code_to_core(code);
            FlareError::Localized {
                code: core_code,
                reason,
                details,
                params,
                timestamp,
            }
        }
        ServerFlareError::System(message) => FlareError::System(message),
        ServerFlareError::Io(message) => FlareError::Io(message),
    }
}

/// 将 `anyhow::Error` 转换为 `flare_core::common::error::FlareError`
pub fn anyhow_error_to_core(error: anyhow::Error) -> FlareError {
    // 尝试从 anyhow::Error 中提取底层错误
    if let Some(flare_err) = error.downcast_ref::<FlareError>() {
        return flare_err.clone();
    }

    // 如果无法提取，则作为系统错误
    FlareError::system(error.to_string())
}

/// 将 `String` 错误转换为 `flare_core::common::error::FlareError`
pub fn string_error_to_core(message: String) -> FlareError {
    FlareError::system(message)
}

/// 将 `Box<dyn std::error::Error>` 转换为 `flare_core::common::error::FlareError`
pub fn boxed_error_to_core(error: Box<dyn std::error::Error>) -> FlareError {
    FlareError::system(error.to_string())
}

/// 将 server error code 映射到 core error code。
/// SAFETY: 仅当两处 ErrorCode 枚举定义（变体及 repr）完全一致时安全；若任一方修改需同步审查。
fn map_server_code_to_core(
    server_code: flare_server_core::error::ErrorCode,
) -> flare_core::common::error::code::ErrorCode {
    unsafe { std::mem::transmute(server_code) }
}

/// 便捷宏：将错误转换为 CoreResult
#[macro_export]
macro_rules! to_core_result {
    ($expr:expr) => {
        $expr.map_err(|e| $crate::infrastructure::error::anyhow_error_to_core(e))
    };
}

/// 便捷宏：将 server error 转换为 CoreResult
#[macro_export]
macro_rules! server_to_core_result {
    ($expr:expr) => {
        $expr.map_err(|e| $crate::infrastructure::error::server_error_to_core(e))
    };
}
