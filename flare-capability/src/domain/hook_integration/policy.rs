//! Hook 写入时的校验：控制面把关，存进去却加载不了的行不让写。错误只带 Hook 名与头名，不带任何取值。

use crate::domain::model::{HookConfigItem, HookTransportConfig, LoggableName};
use flare_im_core_hooks::hooks::adapters::is_reserved_webhook_header;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result as FlareResult};

fn rejected(
    code: ErrorCode,
    reason: impl Into<String>,
    hook: &HookConfigItem,
) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(code, reason.into())
        .details(format!("hook={}", LoggableName(&hook.name)))
        .build_error()
}

/// 校验单条 Hook 配置：名称、`order`、超时、目标与 Webhook 的签名密钥和自定义头。
///
/// - Webhook 写了空的 `secret`：签名函数对空密钥报错。不签名就不写。
/// - Webhook 自定义头用了保留头名（`Content-Type`、`Flare-Signature`、`x-flare-signature`、
///   `X-Hook-Signature`，不区分大小写）：签名函数拒绝。
pub fn validate_hook_item(hook: &HookConfigItem) -> FlareResult<()> {
    if hook.name.is_empty() {
        return Err(rejected(
            ErrorCode::ConfigurationError,
            "Hook name cannot be empty",
            hook,
        ));
    }
    if hook.order < 0 || hook.order > 1000 {
        return Err(rejected(
            ErrorCode::ConfigurationError,
            "Hook order must be between 0 and 1000",
            hook,
        ));
    }
    if hook.timeout_ms == 0 || hook.timeout_ms > 30000 {
        return Err(rejected(
            ErrorCode::ConfigurationError,
            "Hook timeout must be between 1ms and 30000ms",
            hook,
        ));
    }
    match &hook.transport {
        HookTransportConfig::Grpc {
            endpoint,
            service_name,
            ..
        } => {
            if endpoint.as_ref().is_none_or(|s| s.is_empty())
                && service_name.as_ref().is_none_or(|s| s.is_empty())
            {
                return Err(rejected(
                    ErrorCode::InvalidParameter,
                    "gRPC hook transport requires non-empty `endpoint` or `service_name` (with registry)",
                    hook,
                ));
            }
        }
        HookTransportConfig::Webhook {
            endpoint,
            secret,
            headers,
        } => {
            if endpoint.is_empty() {
                return Err(rejected(
                    ErrorCode::InvalidParameter,
                    "webhook hook transport requires non-empty `endpoint` URL",
                    hook,
                ));
            }
            if secret.as_ref().is_some_and(|secret| secret.is_empty()) {
                return Err(rejected(
                    ErrorCode::ConfigurationError,
                    "Webhook signing secret is empty: omit `secret` to send unsigned, or set a non-empty value",
                    hook,
                ));
            }
            if let Some(header) = headers
                .keys()
                .find(|header| is_reserved_webhook_header(header))
            {
                return Err(rejected(
                    ErrorCode::ConfigurationError,
                    format!(
                        "Webhook header {header} is reserved: custom headers cannot set Content-Type, Flare-Signature, x-flare-signature or X-Hook-Signature"
                    ),
                    hook,
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::HookSelectorConfig;
    use std::collections::HashMap;

    fn sample_item(transport: HookTransportConfig) -> HookConfigItem {
        HookConfigItem {
            name: "test-hook".into(),
            description: None,
            enabled: true,
            order: 100,
            timeout_ms: 1000,
            require_success: true,
            selector: HookSelectorConfig::default(),
            transport,
            metadata: HashMap::new(),
        }
    }

    fn grpc(endpoint: Option<&str>) -> HookTransportConfig {
        HookTransportConfig::Grpc {
            endpoint: endpoint.map(str::to_string),
            service_name: None,
            registry_type: None,
            namespace: None,
            load_balance: None,
            metadata: HashMap::new(),
        }
    }

    fn webhook(endpoint: &str, secret: Option<&str>, header: Option<&str>) -> HookTransportConfig {
        HookTransportConfig::Webhook {
            endpoint: endpoint.into(),
            secret: secret.map(str::to_string),
            headers: header
                .map(|name| HashMap::from([(name.to_string(), "header-value".to_string())]))
                .unwrap_or_default(),
        }
    }

    #[test]
    fn grpc_requires_endpoint_or_service_name() {
        let err = validate_hook_item(&sample_item(grpc(None))).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
        assert!(validate_hook_item(&sample_item(grpc(Some("http://127.0.0.1:50051")))).is_ok());
    }

    #[test]
    fn webhook_requires_non_empty_url() {
        let err = validate_hook_item(&sample_item(webhook("", None, None))).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    /// 空密钥和保留头在写入时就拒绝；错误里只有头名，没有取值。
    #[test]
    fn empty_secrets_and_reserved_headers_are_refused_without_their_values() {
        assert!(validate_hook_item(&sample_item(webhook("https://e", Some(""), None))).is_err());
        for header in ["Content-Type", "FLARE-SIGNATURE", "x-hook-signature"] {
            let error = validate_hook_item(&sample_item(webhook("https://e", None, Some(header))))
                .unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(printed.contains(header), "{printed}");
            assert!(!printed.contains("header-value"), "{printed}");
        }
        assert!(
            validate_hook_item(&sample_item(webhook("https://e", None, Some("X-Service")))).is_ok()
        );
    }

    #[test]
    fn order_and_timeout_are_bounded() {
        let mut item = sample_item(grpc(Some("http://127.0.0.1:50051")));
        item.order = 1001;
        assert!(validate_hook_item(&item).is_err());
        item.order = 1000;
        assert!(validate_hook_item(&item).is_ok());
        item.timeout_ms = 0;
        assert!(validate_hook_item(&item).is_err());
        item.timeout_ms = 30_001;
        assert!(validate_hook_item(&item).is_err());
    }
}
