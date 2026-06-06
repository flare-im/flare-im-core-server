//! Server-core 到 flare-core 的错误边界转换。

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

/// 将 server error code 映射到 core error code。
/// SAFETY: 仅当两处 ErrorCode 枚举定义（变体及 repr）完全一致时安全；若任一方修改需同步审查。
fn map_server_code_to_core(
    server_code: flare_server_core::error::ErrorCode,
) -> flare_core::common::error::code::ErrorCode {
    unsafe { std::mem::transmute(server_code) }
}
