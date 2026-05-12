//! Hook 集成 **不变式**：在物化 [`crate::domain::model::HookExecutionPlan`] 之前校验配置，避免无效适配器创建。

use crate::domain::model::{HookConfigItem, HookTransportConfig};
use crate::error::{ErrorBuilder, ErrorCode, Result as FlareResult};

/// 校验单条 Hook 配置是否可安全物化为执行计划（与传输类型相关的不变式）。
pub fn validate_hook_item_for_materialization(item: &HookConfigItem) -> FlareResult<()> {
    if !item.enabled {
        return Ok(());
    }
    match &item.transport {
        HookTransportConfig::Grpc {
            endpoint,
            service_name,
            ..
        } => {
            if endpoint.as_ref().map_or(true, |s| s.is_empty())
                && service_name.as_ref().map_or(true, |s| s.is_empty())
            {
                return Err(
                    ErrorBuilder::new(
                        ErrorCode::InvalidParameter,
                        "gRPC hook transport requires non-empty `endpoint` or `service_name` (with registry)",
                    )
                    .build_error(),
                );
            }
            Ok(())
        }
        HookTransportConfig::Webhook { endpoint, .. } => {
            if endpoint.is_empty() {
                return Err(ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "webhook hook transport requires non-empty `endpoint` URL",
                )
                .build_error());
            }
            Ok(())
        }
        HookTransportConfig::Local { target } => {
            if target.trim().is_empty() {
                return Err(ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "local plugin hook transport requires non-empty `target`",
                )
                .build_error());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::HookSelectorConfig;
    use std::collections::HashMap;

    fn sample_item(enabled: bool, transport: HookTransportConfig) -> HookConfigItem {
        HookConfigItem {
            name: "test-hook".into(),
            version: None,
            description: None,
            enabled,
            priority: 100,
            group: None,
            timeout_ms: 1000,
            max_retries: 0,
            error_policy: "fail_fast".into(),
            require_success: true,
            selector: HookSelectorConfig::default(),
            transport,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn disabled_item_skips_validation() {
        let item = sample_item(
            false,
            HookTransportConfig::Grpc {
                endpoint: None,
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
        );
        assert!(validate_hook_item_for_materialization(&item).is_ok());
    }

    #[test]
    fn grpc_requires_endpoint_or_service_name() {
        let item = sample_item(
            true,
            HookTransportConfig::Grpc {
                endpoint: None,
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
        );
        let err = validate_hook_item_for_materialization(&item).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn grpc_ok_with_endpoint() {
        let item = sample_item(
            true,
            HookTransportConfig::Grpc {
                endpoint: Some("http://127.0.0.1:50051".into()),
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
        );
        assert!(validate_hook_item_for_materialization(&item).is_ok());
    }

    #[test]
    fn webhook_requires_non_empty_url() {
        let item = sample_item(
            true,
            HookTransportConfig::Webhook {
                endpoint: String::new(),
                secret: None,
                headers: HashMap::new(),
            },
        );
        let err = validate_hook_item_for_materialization(&item).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn local_requires_non_empty_target() {
        let item = sample_item(
            true,
            HookTransportConfig::Local {
                target: "   ".into(),
            },
        );
        let err = validate_hook_item_for_materialization(&item).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }
}
