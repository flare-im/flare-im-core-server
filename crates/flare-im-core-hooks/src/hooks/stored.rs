//! `hook_configs` 的一行：控制面写的 JSON 在这里解成运行时的定义。
//!
//! 行由 Capability 校验后写入。这里仍当外部输入对待：错误只带行号，不带任何配置取值。

use std::collections::HashMap;

use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use serde::Deserialize;
use serde_json::Value;

use super::config::{HookDefinition, HookSelectorConfig, HookTransportConfig};
use super::registry::HookScope;
use super::types::HookSource;

/// 库里的一条 Hook（`hook_type = 'pre_send'` 且启用）。
#[derive(Debug, Clone)]
pub struct StoredHook {
    pub id: i64,
    pub tenant_id: Option<String>,
    pub name: String,
    pub description: Option<String>,
    pub order: i32,
    pub timeout_ms: i64,
    pub require_success: bool,
    pub selector: Value,
    pub transport: Value,
    pub metadata: Option<Value>,
}

/// `transport_config` 列。`service_name` 经注册中心发现；`registry_type`、`namespace`、`load_balance`
/// 运行时不用。
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StoredTransport {
    Grpc {
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        service_name: Option<String>,
        #[serde(default)]
        metadata: HashMap<String, String>,
    },
    Webhook {
        endpoint: String,
        #[serde(default)]
        secret: Option<String>,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

fn row_error(id: i64, reason: &str) -> FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, reason)
        .details(format!("hook_configs.id={id}"))
        .build_error()
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

impl StoredHook {
    /// 这一行属于哪个范围。
    pub fn scope(&self) -> HookScope {
        match non_empty(self.tenant_id.clone()) {
            Some(app) => HookScope::Application(app),
            None => HookScope::Platform,
        }
    }

    pub fn source(&self) -> HookSource {
        match self.scope() {
            HookScope::Application(_) => HookSource::Application,
            HookScope::Platform => HookSource::Platform,
        }
    }

    /// 解成运行时的定义，并做部署文件同样的加载校验。
    pub fn definition(&self) -> Result<HookDefinition> {
        let selector: HookSelectorConfig = serde_json::from_value(self.selector.clone())
            .map_err(|_| row_error(self.id, "invalid selector_config"))?;
        let transport: StoredTransport = serde_json::from_value(self.transport.clone())
            .map_err(|_| row_error(self.id, "invalid transport_config"))?;
        let metadata = match self.metadata.clone() {
            Some(value) if !value.is_null() => {
                serde_json::from_value(value).map_err(|_| row_error(self.id, "invalid metadata"))?
            }
            _ => HashMap::new(),
        };
        let transport = match transport {
            StoredTransport::Grpc {
                endpoint,
                service_name,
                metadata,
            } => {
                let endpoint = match (non_empty(service_name), non_empty(endpoint)) {
                    (Some(service), _) => format!("discovery://{service}"),
                    (None, Some(endpoint)) => endpoint,
                    (None, None) => {
                        return Err(row_error(self.id, "gRPC hook has no target"));
                    }
                };
                HookTransportConfig::Grpc { endpoint, metadata }
            }
            StoredTransport::Webhook {
                endpoint,
                secret,
                headers,
            } => HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            },
        };
        let timeout_ms = u64::try_from(self.timeout_ms)
            .ok()
            .filter(|timeout| *timeout > 0)
            .ok_or_else(|| row_error(self.id, "invalid timeout_ms"))?;
        let definition = HookDefinition {
            name: self.name.clone(),
            description: self.description.clone(),
            enabled: true,
            order: self.order,
            timeout_ms,
            require_success: self.require_success,
            selector,
            transport,
            metadata,
        };
        definition.validate()?;
        Ok(definition)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn row(transport: Value) -> StoredHook {
        StoredHook {
            id: 7,
            tenant_id: Some("acme".into()),
            name: "audit".into(),
            description: None,
            order: 3,
            timeout_ms: 500,
            require_success: false,
            selector: json!({
                "tenants": [],
                "conversation_types": ["single"],
                "message_types": []
            }),
            transport,
            metadata: Some(json!({"policy": "audit"})),
        }
    }

    /// 控制面写的 JSON（含运行时不用的键）解得开；服务名走注册中心。
    #[test]
    fn rows_written_by_the_control_plane_load() {
        let definition = row(json!({
            "type": "grpc",
            "endpoint": null,
            "service_name": "risk-hook",
            "registry_type": null,
            "namespace": null,
            "load_balance": "round_robin",
            "metadata": {"x-api-key": "k"}
        }))
        .definition()
        .unwrap();
        assert!(matches!(
            definition.transport,
            HookTransportConfig::Grpc { ref endpoint, .. } if endpoint == "discovery://risk-hook"
        ));
        assert_eq!(definition.order, 3);
        assert!(!definition.require_success);
        assert_eq!(definition.selector.conversation_types, vec!["single"]);
        assert_eq!(
            definition.metadata.get("policy").map(String::as_str),
            Some("audit")
        );

        let webhook = row(json!({
            "type": "webhook",
            "endpoint": "https://audit.example.com/hook",
            "secret": "s",
            "headers": {}
        }))
        .definition()
        .unwrap();
        assert!(matches!(
            webhook.transport,
            HookTransportConfig::Webhook {
                secret: Some(_),
                ..
            }
        ));
    }

    /// 坏行报错只带行号，不带取值。`local` 不是库里的目标。
    #[test]
    fn invalid_rows_are_reported_without_their_values() {
        for transport in [
            json!({"type": "grpc", "metadata": {"x-api-key": "row-secret"}}),
            json!({"type": "local", "target": "row-secret"}),
            json!({"type": "webhook"}),
            json!({"type": "webhook", "endpoint": "https://e", "headers": {"Content-Type": "row-secret"}}),
        ] {
            let error = row(transport).definition().unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(!printed.contains("row-secret"), "{printed}");
        }
    }

    #[test]
    fn a_row_without_a_tenant_is_a_platform_hook() {
        let mut hook = row(json!({"type": "webhook", "endpoint": "https://e"}));
        assert_eq!(hook.scope(), HookScope::Application("acme".into()));
        assert_eq!(hook.source(), HookSource::Application);
        hook.tenant_id = None;
        assert_eq!(hook.scope(), HookScope::Platform);
        assert_eq!(hook.source(), HookSource::Platform);
    }
}
