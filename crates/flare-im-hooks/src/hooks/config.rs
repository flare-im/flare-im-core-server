use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

use super::adapters::is_reserved_webhook_header;
use super::registry::HookRegistry;
use super::selector::{HookSelector, MatchRule};
use super::types::{
    DeliveryHook, HookErrorPolicy, HookKind, HookMetadata, PostSendHook, PreSendHook, RecallHook,
};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HookConfig {
    pub pre_send: Vec<HookDefinition>,
    pub post_send: Vec<HookDefinition>,
    pub delivery: Vec<HookDefinition>,
    pub recall: Vec<HookDefinition>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HookSelectorConfig {
    pub tenants: Vec<String>,
    pub conversation_types: Vec<String>,
    pub message_types: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HookDefinition {
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub priority: i32,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub error_policy: HookErrorPolicy,
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
            version: None,
            description: None,
            enabled: true,
            priority: 0,
            timeout_ms: 3_000,
            max_retries: 0,
            error_policy: HookErrorPolicy::FailFast,
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

    pub fn metadata(&self, kind: HookKind) -> HookMetadata {
        HookMetadata::default()
            .with_kind(kind)
            .with_name(self.name.clone())
            .with_version(self.version.clone())
            .with_description(self.description.clone())
            .with_priority(self.priority)
            .with_timeout(Duration::from_millis(self.timeout_ms))
            .with_error_policy(self.error_policy)
            .with_require_success(self.require_success)
    }
}

pub trait HookFactory: Send + Sync {
    fn build_pre_send(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Option<Arc<dyn PreSendHook>>>;

    fn build_post_send(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Option<Arc<dyn PostSendHook>>>;

    fn build_delivery(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Option<Arc<dyn DeliveryHook>>>;

    fn build_recall(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Option<Arc<dyn RecallHook>>>;
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
            post_send = config.post_send.len(),
            delivery = config.delivery.len(),
            recall = config.recall.len(),
            "hooks loaded: {} (profile={})",
            config.pre_send.len()
                + config.post_send.len()
                + config.delivery.len()
                + config.recall.len(),
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
    /// delivery: a webhook custom header named like a reserved header (the signature headers
    /// or Content-Type, case-insensitively). The error names the hook and the header, never a
    /// header value.
    pub fn validate(&self) -> Result<()> {
        self.validate_from(None)
    }

    /// [`Self::validate`]; `path` (the profile the configuration came from) goes into the error.
    fn validate_from(&self, path: Option<&Path>) -> Result<()> {
        for def in self
            .pre_send
            .iter()
            .chain(&self.post_send)
            .chain(&self.delivery)
            .chain(&self.recall)
        {
            if let HookTransportConfig::Webhook { headers, .. } = &def.transport
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
                        def.name
                    ),
                    None => format!("hook={}, header={header}", def.name),
                })
                .build_error());
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: HookConfig) {
        self.pre_send.extend(other.pre_send);
        self.post_send.extend(other.post_send);
        self.delivery.extend(other.delivery);
        self.recall.extend(other.recall);
    }

    pub async fn install(
        &self,
        registry: Arc<HookRegistry>,
        factory: &dyn HookFactory,
    ) -> Result<()> {
        for def in &self.pre_send {
            if !def.enabled {
                tracing::trace!(hook = %def.name, "pre-send hook disabled, skip");
                continue;
            }
            let selector = def.selector();
            if let Some(handler) = factory.build_pre_send(def, &selector)? {
                registry
                    .register_pre_send(def.metadata(HookKind::PreSend), selector, handler)
                    .await;
            }
        }

        for def in &self.post_send {
            if !def.enabled {
                tracing::trace!(hook = %def.name, "post-send hook disabled, skip");
                continue;
            }
            let selector = def.selector();
            if let Some(handler) = factory.build_post_send(def, &selector)? {
                registry
                    .register_post_send(def.metadata(HookKind::PostSend), selector, handler)
                    .await;
            }
        }

        for def in &self.delivery {
            if !def.enabled {
                tracing::trace!(hook = %def.name, "delivery hook disabled, skip");
                continue;
            }
            let selector = def.selector();
            if let Some(handler) = factory.build_delivery(def, &selector)? {
                registry
                    .register_delivery(def.metadata(HookKind::Delivery), selector, handler)
                    .await;
            }
        }

        for def in &self.recall {
            if !def.enabled {
                tracing::trace!(hook = %def.name, "recall hook disabled, skip");
                continue;
            }
            let selector = def.selector();
            if let Some(handler) = factory.build_recall(def, &selector)? {
                registry
                    .register_recall(def.metadata(HookKind::Recall), selector, handler)
                    .await;
            }
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
    fn shipped_social_hook_config_matches_every_tenant() {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/hooks.social.toml");
        let content = std::fs::read_to_string(&root).expect("config/hooks.social.toml");
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

    /// `error_policy` ignores case and underscores, so profiles written for either engine load
    /// in Message Ingest (before, only `FailFast`, `Retry` and `Ignore` did); an unknown value
    /// is still a load error.
    #[test]
    fn error_policy_spellings_are_equivalent_and_unknown_values_are_rejected() {
        let cases = [
            ("FailFast", HookErrorPolicy::FailFast),
            ("fail_fast", HookErrorPolicy::FailFast),
            ("failfast", HookErrorPolicy::FailFast),
            ("FAIL_FAST", HookErrorPolicy::FailFast),
            ("Retry", HookErrorPolicy::Retry),
            ("retry", HookErrorPolicy::Retry),
            ("RETRY", HookErrorPolicy::Retry),
            ("Ignore", HookErrorPolicy::Ignore),
            ("ignore", HookErrorPolicy::Ignore),
            ("_IGNORE_", HookErrorPolicy::Ignore),
        ];
        for (value, expected) in cases {
            assert_eq!(HookErrorPolicy::parse(value), Some(expected), "{value}");
            let config: HookConfig = toml::from_str(&format!(
                "{}error_policy = \"{value}\"\n",
                "[[pre_send]]\nname = \"h\"\n"
            ))
            .unwrap_or_else(|error| panic!("{value}: {error}"));
            assert_eq!(config.pre_send[0].error_policy, expected, "{value}");
        }
        for value in ["", "abort", "fail-fast", "fail fast", "ignored", "retries"] {
            assert_eq!(HookErrorPolicy::parse(value), None, "{value:?}");
            assert!(
                toml::from_str::<HookConfig>(&format!(
                    "[[pre_send]]\nname = \"h\"\nerror_policy = \"{value}\"\n"
                ))
                .is_err(),
                "{value:?}"
            );
        }
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
