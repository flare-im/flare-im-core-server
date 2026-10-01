//! Server-core 到 flare-core 的错误边界转换。

use flare_core::common::error::FlareError;
use flare_core::common::error::code::ErrorCode as CoreErrorCode;
use flare_server_core::error::ErrorCode as ServerErrorCode;
use flare_server_core::error::FlareError as ServerFlareError;
use tracing::warn;

/// flare-server-core 的错误码在 flare-core 里没有对应码时用的兜底码。
///
/// 一般性的操作失败：不可重试，也不属于认证类，客户端不会因此去换令牌或终止会话。
/// 原始的原因文本保留在错误的 `reason` 里。
pub const FALLBACK_CORE_ERROR_CODE: CoreErrorCode = CoreErrorCode::OperationFailed;

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
///
/// 两边的枚举不是同一个：flare-server-core 另有 `BadRequest`、`SyncCursorRegression` 与 HTTP
/// 状态码一族，flare-core 没有。以前按内存表示直接转换，这些码会得到 flare-core 枚举里不存在的
/// 判别值，属于未定义行为。现在逐个变体对照；flare-core 表达不了的用
/// [`FALLBACK_CORE_ERROR_CODE`] 并记一条 warn。
fn map_server_code_to_core(server_code: ServerErrorCode) -> CoreErrorCode {
    core_code_for(server_code).unwrap_or_else(|| {
        warn!(
            server_code = ?server_code,
            fallback = FALLBACK_CORE_ERROR_CODE.as_str(),
            "server error code has no flare-core equivalent, using the fallback code"
        );
        FALLBACK_CORE_ERROR_CODE
    })
}

/// flare-core 中与 `server_code` 对应的错误码；没有对应码时为 `None`。
///
/// 不写通配分支：flare-server-core 新增变体时这里编译失败，必须在这里决定它的映射。
fn core_code_for(server_code: ServerErrorCode) -> Option<CoreErrorCode> {
    use CoreErrorCode as C;
    use ServerErrorCode as S;

    let code = match server_code {
        // 连接
        S::ConnectionFailed => C::ConnectionFailed,
        S::ConnectionTimeout => C::ConnectionTimeout,
        S::ConnectionClosed => C::ConnectionClosed,
        S::ConnectionRefused => C::ConnectionRefused,
        S::ConnectionLimitExceeded => C::ConnectionLimitExceeded,
        S::NotConnected => C::NotConnected,
        S::ConnectionReconnecting => C::ConnectionReconnecting,
        // 认证
        S::AuthenticationFailed => C::AuthenticationFailed,
        S::AuthenticationExpired => C::AuthenticationExpired,
        S::AuthenticationInvalid => C::AuthenticationInvalid,
        S::AuthenticationRequired => C::AuthenticationRequired,
        S::PermissionDenied => C::PermissionDenied,
        S::TokenInvalid => C::TokenInvalid,
        S::TokenExpired => C::TokenExpired,
        // 协议
        S::ProtocolError => C::ProtocolError,
        S::ProtocolVersionMismatch => C::ProtocolVersionMismatch,
        S::ProtocolNotSupported => C::ProtocolNotSupported,
        S::MessageFormatError => C::MessageFormatError,
        S::MessageTooLarge => C::MessageTooLarge,
        S::InvalidCommand => C::InvalidCommand,
        // 消息
        S::MessageSendFailed => C::MessageSendFailed,
        S::MessageDeliveryFailed => C::MessageDeliveryFailed,
        S::MessageNotFound => C::MessageNotFound,
        S::MessageExpired => C::MessageExpired,
        S::MessageRateLimitExceeded => C::MessageRateLimitExceeded,
        S::MessageDecodeFailed => C::MessageDecodeFailed,
        // 用户
        S::UserNotFound => C::UserNotFound,
        S::UserOffline => C::UserOffline,
        S::UserBlocked => C::UserBlocked,
        S::UserQuotaExceeded => C::UserQuotaExceeded,
        S::UserSessionLimitExceeded => C::UserSessionLimitExceeded,
        // 系统
        S::InternalError => C::InternalError,
        S::ServiceUnavailable => C::ServiceUnavailable,
        S::ResourceExhausted => C::ResourceExhausted,
        S::ConfigurationError => C::ConfigurationError,
        S::DatabaseError => C::DatabaseError,
        // 网络
        S::NetworkError => C::NetworkError,
        S::NetworkTimeout => C::NetworkTimeout,
        S::NetworkUnreachable => C::NetworkUnreachable,
        S::NetworkConnectionLost => C::NetworkConnectionLost,
        // 序列化
        S::SerializationError => C::SerializationError,
        S::DeserializationError => C::DeserializationError,
        S::EncodingError => C::EncodingError,
        // 通用
        S::GeneralError => C::GeneralError,
        S::InvalidParameter => C::InvalidParameter,
        S::OperationNotSupported => C::OperationNotSupported,
        S::OperationFailed => C::OperationFailed,
        S::OperationTimeout => C::OperationTimeout,
        S::UnknownError => C::UnknownError,

        // 以下只有 flare-server-core 有，按语义归到 flare-core 的码
        S::BadRequest | S::HttpBadRequest | S::HttpUnprocessableEntity => C::InvalidParameter,
        S::HttpUnauthorized => C::AuthenticationFailed,
        S::HttpForbidden => C::PermissionDenied,
        S::HttpMethodNotAllowed => C::OperationNotSupported,
        S::HttpRequestTimeout | S::HttpGatewayTimeout => C::OperationTimeout,
        S::HttpTooManyRequests => C::ResourceExhausted,
        S::HttpInternalServerError => C::InternalError,
        S::HttpBadGateway | S::HttpServiceUnavailable => C::ServiceUnavailable,

        // flare-core 没有能表达的码：同步游标回退（应重拉快照）、资源不存在、冲突
        S::SyncCursorRegression | S::HttpNotFound | S::HttpConflict => return None,
    };
    Some(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// flare-server-core 的全部错误码。枚举没有迭代器，按 `from_u32` 扫一遍数字空间
    /// （HTTP 一族用 400–504，其余在 1000–9999）。
    fn all_server_codes() -> Vec<ServerErrorCode> {
        (0..=10_000).filter_map(ServerErrorCode::from_u32).collect()
    }

    /// 只有 flare-server-core 才有的码，以及它们在 flare-core 里的归宿（`None` = 兜底）。
    fn server_only_codes() -> Vec<(ServerErrorCode, Option<CoreErrorCode>)> {
        use CoreErrorCode as C;
        use ServerErrorCode as S;
        vec![
            (S::BadRequest, Some(C::InvalidParameter)),
            (S::SyncCursorRegression, None),
            (S::HttpBadRequest, Some(C::InvalidParameter)),
            (S::HttpUnauthorized, Some(C::AuthenticationFailed)),
            (S::HttpForbidden, Some(C::PermissionDenied)),
            (S::HttpNotFound, None),
            (S::HttpMethodNotAllowed, Some(C::OperationNotSupported)),
            (S::HttpRequestTimeout, Some(C::OperationTimeout)),
            (S::HttpConflict, None),
            (S::HttpUnprocessableEntity, Some(C::InvalidParameter)),
            (S::HttpTooManyRequests, Some(C::ResourceExhausted)),
            (S::HttpInternalServerError, Some(C::InternalError)),
            (S::HttpBadGateway, Some(C::ServiceUnavailable)),
            (S::HttpServiceUnavailable, Some(C::ServiceUnavailable)),
            (S::HttpGatewayTimeout, Some(C::OperationTimeout)),
        ]
    }

    /// 扫描确实覆盖了全部变体：共有的 49 个加只有 server-core 才有的 15 个。
    #[test]
    fn the_scan_sees_every_server_code() {
        let all = all_server_codes();
        assert_eq!(all.len(), 49 + server_only_codes().len());
        for (code, _) in server_only_codes() {
            assert!(all.contains(&code), "{code:?}");
        }
    }

    /// 每个 server-core 码都映射成 flare-core 枚举里真实存在的变体。按内存表示转换时，
    /// `BadRequest`（9005）等码得到的判别值在 flare-core 里不存在，这里会失败。
    #[test]
    fn every_server_code_maps_to_a_real_core_variant() {
        for server_code in all_server_codes() {
            // 只读数值、不格式化映射结果：判别值无效时格式化它本身就是未定义行为
            let raw = map_server_code_to_core(server_code).as_u32();
            assert!(
                CoreErrorCode::from_u32(raw).is_some(),
                "{server_code:?} became the invalid flare-core discriminant {raw}"
            );
        }
    }

    /// 两边都有的码保持数值与名字不变。
    #[test]
    fn shared_codes_map_to_the_same_code() {
        let mut shared = 0;
        for server_code in all_server_codes() {
            let Some(expected) = CoreErrorCode::from_u32(server_code.as_u32()) else {
                continue;
            };
            shared += 1;
            let core_code = map_server_code_to_core(server_code);
            assert_eq!(core_code, expected, "{server_code:?}");
            assert_eq!(core_code.as_str(), server_code.as_str(), "{server_code:?}");
        }
        assert_eq!(shared, 49);
    }

    /// 只有 server-core 才有的码按语义归位，没有对应码的落到兜底码。
    #[test]
    fn server_only_codes_map_by_meaning_or_fall_back() {
        for (server_code, expected) in server_only_codes() {
            assert_eq!(
                CoreErrorCode::from_u32(server_code.as_u32()),
                None,
                "{server_code:?} is not server-only"
            );
            assert_eq!(core_code_for(server_code), expected, "{server_code:?}");
            assert_eq!(
                map_server_code_to_core(server_code),
                expected.unwrap_or(FALLBACK_CORE_ERROR_CODE),
                "{server_code:?}"
            );
        }
    }

    /// 兜底码不可重试、不属于认证类：客户端既不会盲目重试，也不会因此去换令牌。
    #[test]
    fn the_fallback_is_neither_retryable_nor_an_auth_failure() {
        assert!(!FALLBACK_CORE_ERROR_CODE.is_retryable());
        assert_ne!(
            FALLBACK_CORE_ERROR_CODE.category(),
            flare_core::common::error::ErrorCategory::Authentication
        );
    }

    /// 转换整条错误时只换错误码，原因、详情与参数原样保留。
    #[test]
    fn localized_errors_keep_their_reason_across_the_boundary() {
        let error = ServerFlareError::localized(
            ServerErrorCode::HttpServiceUnavailable,
            "upstream unavailable",
        );
        let core = server_error_to_core(error);
        assert_eq!(core.code(), Some(CoreErrorCode::ServiceUnavailable));
        assert_eq!(core.reason(), "upstream unavailable");
        assert!(CoreErrorCode::ServiceUnavailable.is_retryable());

        let error = ServerFlareError::localized(ServerErrorCode::SyncCursorRegression, "behind");
        let core = server_error_to_core(error);
        assert_eq!(core.code(), Some(FALLBACK_CORE_ERROR_CODE));
        assert_eq!(core.reason(), "behind");
    }
}
