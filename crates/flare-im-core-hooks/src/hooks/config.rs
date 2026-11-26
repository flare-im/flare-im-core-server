use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

use super::adapters::is_reserved_webhook_header;
use super::points::PRE_SEND_HOOK_TYPE;
use super::registry::{HookRegistry, PreSendPlan};
use super::selector::{HookSelector, MatchRule};
use super::types::{HookMetadata, HookSource, PreSendHook};

/// 部署文件：`[[pre_send]]` 是唯一的表，其他表是配置错误。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HookConfig {
    pub pre_send: Vec<HookDefinition>,
}

/// 选择器的唯一定义：部署文件、`hook_configs.selector_config` 和管理面都用它，字段都是运行时实际判断的
/// （见 [`HookDefinition::selector`]），空表示全部。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HookSelectorConfig {
    pub tenants: Vec<String>,
    pub conversation_types: Vec<String>,
    pub message_types: Vec<String>,
}

/// `Debug` 手写：Webhook 的签名密钥与头的值、gRPC metadata 的值不进日志。
#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookTransportConfig {
    Grpc {
        endpoint: String,
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
    Local {
        target: String,
    },
}

impl std::fmt::Debug for HookTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Grpc { endpoint, metadata } => f
                .debug_struct("Grpc")
                .field("endpoint", endpoint)
                .field("metadata_keys", &metadata.keys().collect::<Vec<_>>())
                .finish(),
            Self::Webhook {
                endpoint,
                secret,
                headers,
            } => f
                .debug_struct("Webhook")
                .field("endpoint", endpoint)
                .field(
                    "secret",
                    &secret.as_ref().map(|_| "<redacted>").unwrap_or("<none>"),
                )
                .field("header_names", &headers.keys().collect::<Vec<_>>())
                .finish(),
            Self::Local { target } => f.debug_struct("Local").field("target", target).finish(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HookDefinition {
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    /// 同一来源内的先后，小的先执行。
    pub order: i32,
    pub timeout_ms: u64,
    /// 必需：目标不可用时回可重试的 503；否则跳过。
    pub require_success: bool,
    pub selector: HookSelectorConfig,
    pub transport: HookTransportConfig,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Default for HookDefinition {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: None,
            enabled: true,
            order: 0,
            timeout_ms: 3_000,
            require_success: true,
            selector: HookSelectorConfig::default(),
            transport: HookTransportConfig::Local {
                target: String::new(),
            },
            metadata: HashMap::new(),
        }
    }
}

impl HookDefinition {
    pub fn selector(&self) -> HookSelector {
        HookSelector {
            tenants: if self.selector.tenants.is_empty() {
                MatchRule::Any
            } else {
                MatchRule::of(self.selector.tenants.clone())
            },
            conversation_types: if self.selector.conversation_types.is_empty() {
                MatchRule::Any
            } else {
                MatchRule::of(self.selector.conversation_types.clone())
            },
            message_types: if self.selector.message_types.is_empty() {
                MatchRule::Any
            } else {
                MatchRule::of(self.selector.message_types.clone())
            },
        }
    }

    pub fn metadata(&self, source: HookSource) -> HookMetadata {
        HookMetadata::default()
            .with_source(source)
            .with_name(self.name.clone())
            .with_description(self.description.clone())
            .with_order(self.order)
            .with_timeout(Duration::from_millis(self.timeout_ms))
            .with_require_success(self.require_success)
    }

    /// 配置加载时就该拒绝的：Webhook 写了空密钥，或自定义头占用保留名（签名头或 Content-Type，不区分
    /// 大小写）。错误里有 Hook 名与头名，没有任何取值。
    pub fn validate(&self) -> Result<()> {
        self.validate_from(None)
    }

    fn validate_from(&self, path: Option<&Path>) -> Result<()> {
        if let HookTransportConfig::Webhook {
            secret: Some(secret),
            ..
        } = &self.transport
            && secret.is_empty()
        {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "webhook signing secret is empty: omit `secret` to send unsigned, or set a non-empty value",
            )
            .details(format!("hook={}", self.name))
            .build_error());
        }
        if let HookTransportConfig::Webhook { headers, .. } = &self.transport
            && let Some(header) = headers
                .keys()
                .find(|header| is_reserved_webhook_header(header))
        {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "webhook header name is reserved: custom headers cannot set a signature header or Content-Type",
            )
            .details(match path {
                Some(path) => format!(
                    "path={}, hook={}, header={header}",
                    path.display(),
                    self.name
                ),
                None => format!("hook={}, header={header}", self.name),
            })
            .build_error());
        }
        Ok(())
    }

    /// `FLARE_REGISTRY_MODE=static` 没有注册中心：gRPC 目标必须是明确的 http(s) 地址。
    pub fn validate_static_route(&self) -> Result<()> {
        if let HookTransportConfig::Grpc { endpoint, .. } = &self.transport {
            let explicit_http = endpoint.starts_with("http://") || endpoint.starts_with("https://");
            if !explicit_http || tonic::transport::Endpoint::from_shared(endpoint.clone()).is_err()
            {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    format!(
                        "pre_send hook {} requires an explicit private http(s) gRPC URI in FLARE_REGISTRY_MODE=static; discovery:// routes cannot resolve",
                        self.name
                    ),
                )
                .build_error());
            }
        }
        Ok(())
    }

    /// 物化成调用链上的一条。
    pub fn plan(&self, source: HookSource, factory: &dyn HookFactory) -> Result<PreSendPlan> {
        let selector = self.selector();
        let handler = factory.build_pre_send(self, &selector)?;
        Ok(PreSendPlan::new(self.metadata(source), selector, handler))
    }

    /// 部署摘要里的一条：不带密钥、头和 metadata 的取值。
    fn summary(&self) -> DeploymentHookSummary {
        let target = match &self.transport {
            HookTransportConfig::Grpc { endpoint, .. } => HookTargetSummary {
                kind: "grpc",
                endpoint: without_query(endpoint),
            },
            HookTransportConfig::Webhook { endpoint, .. } => HookTargetSummary {
                kind: "webhook",
                endpoint: without_query(endpoint),
            },
            HookTransportConfig::Local { target } => HookTargetSummary {
                kind: "local",
                endpoint: target.clone(),
            },
        };
        DeploymentHookSummary {
            name: self.name.clone(),
            hook_type: PRE_SEND_HOOK_TYPE,
            description: self.description.clone(),
            enabled: self.enabled,
            order: self.order,
            required: self.require_success,
            timeout_ms: self.timeout_ms,
            target,
            selector: DeploymentSelectorSummary {
                tenants: self.selector.tenants.clone(),
                conversation_types: self.selector.conversation_types.clone(),
                message_types: self.selector.message_types.clone(),
            },
        }
    }
}

/// URL 的查询串可能带凭据，摘要里去掉。
fn without_query(endpoint: &str) -> String {
    endpoint
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .to_string()
}

/// `hook_deployment_summary.hooks` 里的一条（控制面只读展示）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeploymentHookSummary {
    pub name: String,
    pub hook_type: &'static str,
    pub description: Option<String>,
    pub enabled: bool,
    pub order: i32,
    pub required: bool,
    pub timeout_ms: u64,
    pub target: HookTargetSummary,
    pub selector: DeploymentSelectorSummary,
}

/// 目标：类型与地址。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HookTargetSummary {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub endpoint: String,
}

/// 选择器（空表示全部）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeploymentSelectorSummary {
    pub tenants: Vec<String>,
    pub conversation_types: Vec<String>,
    pub message_types: Vec<String>,
}

pub trait HookFactory: Send + Sync {
    fn build_pre_send(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Arc<dyn PreSendHook>>;
}

pub struct HookConfigLoader {
    candidate_paths: Vec<PathBuf>,
}

impl Default for HookConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl HookConfigLoader {
    pub fn new() -> Self {
        Self {
            candidate_paths: Vec::new(),
        }
    }

    pub fn add_candidate<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.candidate_paths.push(path.into());
        self
    }

    pub fn load(&self) -> Result<HookConfig> {
        if self.candidate_paths.is_empty() {
            let config: HookConfig = toml::from_str(include_str!(
                "../../../../config/hooks.core.toml"
            ))
            .map_err(|_| {
                ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "invalid embedded core hook profile",
                )
                .build_error()
            })?;
            config.validate()?;
            Self::log_loaded(&config, "embedded:hooks.core.toml", "core");
            return Ok(config);
        }
        let mut merged = HookConfig::default();
        for path in &self.candidate_paths {
            let config = if path.is_dir() {
                self.load_from_directory(path)?
            } else if path.is_file() {
                self.load_from_file(path)?
            } else {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "explicit hook config path does not exist or is not readable",
                )
                .details(format!("path={}", path.display()))
                .build_error());
            };
            Self::log_loaded(&config, &path.display().to_string(), "configured");
            merged.merge(config);
        }
        Ok(merged)
    }

    fn log_loaded(config: &HookConfig, source: &str, profile: &str) {
        tracing::info!(
            source,
            profile,
            pre_send = config.pre_send.len(),
            "hooks loaded: {} (profile={})",
            config.pre_send.len(),
            profile
        );
    }

    fn load_from_file(&self, path: &Path) -> Result<HookConfig> {
        let content = fs::read_to_string(path).map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "failed to read hook config")
                .details(format!("path={}, err={err}", path.display()))
                .build_error()
        })?;
        let config: HookConfig = toml::from_str(&content).map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "invalid hook config format")
                // TOML errors include source lines, potentially containing secrets.
                .details(format!(
                    "path={}, byte_range={:?}",
                    path.display(),
                    err.span()
                ))
                .build_error()
        })?;
        config.validate_from(Some(path))?;
        Ok(config)
    }

    fn load_from_directory(&self, dir: &Path) -> Result<HookConfig> {
        let mut merged = HookConfig::default();
        let mut entries = fs::read_dir(dir)
            .map_err(|err| {
                ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "failed to read hook config dir",
                )
                .details(format!("path={}, err={err}", dir.display()))
                .build_error()
            })?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|err| {
                ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "failed to enumerate hook config dir",
                )
                .details(format!("path={}, err={err}", dir.display()))
                .build_error()
            })?;
        entries.sort_by_key(|entry| entry.path());

        let mut profile_count = 0;
        for entry in entries {
            if entry
                .path()
                .extension()
                .map(|ext| ext == "toml")
                .unwrap_or(false)
            {
                let cfg = self.load_from_file(&entry.path())?;
                merged.merge(cfg);
                profile_count += 1;
            }
        }

        if profile_count == 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "explicit hook config directory contains no TOML profiles",
            )
            .details(format!("path={}", dir.display()))
            .build_error());
        }

        Ok(merged)
    }
}

impl HookConfig {
    /// Rejects, when the configuration is loaded, what would otherwise only fail on every
    /// delivery (see [`HookDefinition::validate`]).
    pub fn validate(&self) -> Result<()> {
        self.validate_from(None)
    }

    /// [`Self::validate`]; `path` (the profile the configuration came from) goes into the error.
    fn validate_from(&self, path: Option<&Path>) -> Result<()> {
        self.pre_send
            .iter()
            .try_for_each(|def| def.validate_from(path))
    }

    fn merge(&mut self, other: HookConfig) {
        self.pre_send.extend(other.pre_send);
    }

    /// 启用的条数（停用的定义不算门禁，不能出现在安装日志里）。
    pub fn enabled_count(&self) -> usize {
        self.pre_send.iter().filter(|def| def.enabled).count()
    }

    /// 部署摘要。
    pub fn summary(&self) -> Vec<DeploymentHookSummary> {
        self.pre_send.iter().map(HookDefinition::summary).collect()
    }

    /// 把启用的定义装进注册表的部署段。
    pub fn install(&self, registry: &HookRegistry, factory: &dyn HookFactory) -> Result<()> {
        for def in &self.pre_send {
            if !def.enabled {
                tracing::trace!(hook = %def.name, "pre-send hook disabled, skip");
                continue;
            }
            let selector = def.selector();
            let handler = factory.build_pre_send(def, &selector)?;
            registry.register_pre_send(def.metadata(HookSource::Deployment), selector, handler);
        }
        Ok(())
    }
}

#[cfg(test)]
mod selector_config_tests {
    use super::*;

    fn parse(toml_src: &str) -> HookConfig {
        toml::from_str(toml_src).expect("hook config toml")
    }

    #[test]
    fn omitted_or_empty_tenants_selects_every_tenant() {
        let cfg = parse(
            r#"
[[pre_send]]
name = "a"
[pre_send.selector]
conversation_types = ["single"]
[pre_send.transport]
type = "local"
target = "noop"

[[pre_send]]
name = "b"
[pre_send.selector]
tenants = []
[pre_send.transport]
type = "local"
target = "noop"
"#,
        );
        for def in &cfg.pre_send {
            assert!(
                matches!(def.selector().tenants, MatchRule::Any),
                "{}",
                def.name
            );
            assert!(def.selector().tenants.matches(Some("0")));
            assert!(def.selector().tenants.matches(Some("acme")));
        }
    }

    #[test]
    fn listed_tenants_become_exact_match() {
        let cfg = parse(
            r#"
[[pre_send]]
name = "a"
[pre_send.selector]
tenants = ["0"]
[pre_send.transport]
type = "local"
target = "noop"
"#,
        );
        let tenants = cfg.pre_send[0].selector().tenants;
        assert!(tenants.matches(Some("0")));
        assert!(!tenants.matches(Some("acme")));
    }

    #[test]
    fn a_mandatory_hook_config_without_tenants_matches_every_tenant() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mandatory_pre_send.toml");
        let content = std::fs::read_to_string(&root).expect("mandatory_pre_send.toml fixture");
        let cfg = parse(&content);
        assert!(!cfg.pre_send.is_empty());
        for def in &cfg.pre_send {
            assert!(
                def.selector().tenants.matches(Some("acme")),
                "{} must not be pinned to tenant \"0\"",
                def.name
            );
        }
    }
}

#[cfg(test)]
mod load_time_validation_tests {
    use super::*;

    fn definition(transport: &str) -> String {
        format!("[[pre_send]]\nname = \"h\"\n[pre_send.transport]\n{transport}\n")
    }

    /// 删掉的字段不再被接受：`priority` 换成 `order`，`error_policy`、`max_retries`、`version` 没有了。
    #[test]
    fn removed_fields_are_configuration_errors() {
        for field in [
            "priority = 100",
            "error_policy = \"FailFast\"",
            "max_retries = 1",
            "version = \"1\"",
        ] {
            assert!(
                toml::from_str::<HookConfig>(&format!("[[pre_send]]\nname = \"h\"\n{field}\n"))
                    .is_err(),
                "{field}"
            );
        }
        for table in ["post_send", "delivery", "recall"] {
            assert!(
                toml::from_str::<HookConfig>(&format!("[[{table}]]\nname = \"h\"\n")).is_err(),
                "{table}"
            );
        }
    }

    /// 摘要不带密钥、头的取值和 URL 查询串。
    #[test]
    fn the_deployment_summary_carries_no_credentials() {
        let config: HookConfig = toml::from_str(
            r#"
[[pre_send]]
name = "audit"
order = 7
[pre_send.transport]
type = "webhook"
endpoint = "https://audit.example.com/hook?token=query-value"
secret = "signing-value"
[pre_send.transport.headers]
Authorization = "Bearer header-value"
"#,
        )
        .unwrap();
        let summary = config.summary();
        assert_eq!(summary[0].order, 7);
        assert_eq!(summary[0].target.kind, "webhook");
        assert_eq!(summary[0].target.endpoint, "https://audit.example.com/hook");
        let printed = serde_json::to_string(&summary).unwrap();
        for value in ["query-value", "signing-value", "header-value"] {
            assert!(!printed.contains(value), "{printed}");
        }
    }

    #[test]
    fn transport_debug_never_prints_secrets_or_values() {
        let webhook = HookTransportConfig::Webhook {
            endpoint: "http://127.0.0.1:9/hook".into(),
            secret: Some("debug-secret".into()),
            headers: HashMap::from([("Authorization".into(), "Bearer debug-header".into())]),
        };
        let grpc = HookTransportConfig::Grpc {
            endpoint: "http://127.0.0.1:9".into(),
            metadata: HashMap::from([("x-api-key".into(), "debug-metadata".into())]),
        };
        let printed = format!("{webhook:?} {grpc:?}");
        for secret in ["debug-secret", "debug-header", "debug-metadata"] {
            assert!(!printed.contains(secret), "{printed}");
        }
        assert!(printed.contains("Authorization") && printed.contains("x-api-key"));
    }

    /// A reserved webhook header fails when the profile is loaded, case-insensitively, instead
    /// of on every delivery; the error names the hook and the header but not its value.
    #[test]
    fn reserved_webhook_headers_are_rejected_when_the_profile_is_loaded() {
        let dir = std::env::temp_dir().join(format!("flare-hooks-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("hooks.toml");
        let value = "w14h-header-value-not-for-errors";
        for header in [
            "Content-Type",
            "content-type",
            "Flare-Signature",
            "FLARE-SIGNATURE",
            "x-flare-signature",
            "X-Hook-Signature",
            "x-hook-signature",
        ] {
            let profile = definition(&format!(
                "type = \"webhook\"\nendpoint = \"http://127.0.0.1:9/hook\"\n[pre_send.transport.headers]\n\"{header}\" = \"{value}\""
            ));
            let config: HookConfig = toml::from_str(&profile).unwrap();
            let error = config.validate().unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(printed.contains(header), "{printed}");
            assert!(!printed.contains(value), "{printed}");

            std::fs::write(&path, &profile).unwrap();
            let error = HookConfigLoader::new()
                .add_candidate(&path)
                .load()
                .unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(printed.contains("reserved"), "{printed}");
            assert!(printed.contains(header), "{printed}");
            assert!(!printed.contains(value), "{printed}");
        }

        // Ordinary custom headers still load.
        let profile = definition(
            "type = \"webhook\"\nendpoint = \"http://127.0.0.1:9/hook\"\n[pre_send.transport.headers]\nAuthorization = \"Bearer t\"\nX-Signature-Version = \"2\"",
        );
        std::fs::write(&path, &profile).unwrap();
        assert!(HookConfigLoader::new().add_candidate(&path).load().is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
