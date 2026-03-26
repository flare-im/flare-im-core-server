use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{ErrorBuilder, ErrorCode, Result};

use super::registry::HookRegistry;
use super::selector::{HookSelector, MatchRule};
use super::types::{
    DeliveryHook, HookErrorPolicy, HookKind, HookMetadata, PostSendHook, PreSendHook, RecallHook,
};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct HookConfig {
    pub pre_send: Vec<HookDefinition>,
    pub post_send: Vec<HookDefinition>,
    pub delivery: Vec<HookDefinition>,
    pub recall: Vec<HookDefinition>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct HookSelectorConfig {
    pub tenants: Vec<String>,
    pub conversation_types: Vec<String>,
    pub message_types: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
#[serde(default)]
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
            candidate_paths: vec![
                PathBuf::from("config/hooks.toml"),
                PathBuf::from("config/hooks.d"),
            ],
        }
    }

    pub fn add_candidate<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.candidate_paths.push(path.into());
        self
    }

    pub fn load(&self) -> Result<HookConfig> {
        for path in &self.candidate_paths {
            if path.is_dir() {
                if let Ok(cfg) = self.load_from_directory(path) {
                    return Ok(cfg);
                }
            } else if path.is_file() && let Ok(cfg) = self.load_from_file(path) {
                return Ok(cfg);
            }
        }
        Ok(HookConfig::default())
    }

    fn load_from_file(&self, path: &Path) -> Result<HookConfig> {
        let content = fs::read_to_string(path).map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "failed to read hook config")
                .details(format!("path={}, err={err}", path.display()))
                .build_error()
        })?;
        toml::from_str(&content).map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "invalid hook config format")
                .details(format!("path={}, err={err}", path.display()))
                .build_error()
        })
    }

    fn load_from_directory(&self, dir: &Path) -> Result<HookConfig> {
        let mut merged = HookConfig::default();
        if !dir.exists() {
            return Ok(merged);
        }

        let mut entries = fs::read_dir(dir)
            .map_err(|err| {
                ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "failed to read hook config dir",
                )
                .details(format!("path={}, err={err}", dir.display()))
                .build_error()
            })?
            .filter_map(|entry| entry.ok())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            if entry
                .path()
                .extension()
                .map(|ext| ext == "toml")
                .unwrap_or(false)
            {
                let cfg = self.load_from_file(&entry.path())?;
                merged.merge(cfg);
            }
        }

        Ok(merged)
    }
}

impl HookConfig {
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
                tracing::info!(hook = %def.name, "pre-send hook disabled, skip");
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
                tracing::info!(hook = %def.name, "post-send hook disabled, skip");
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
                tracing::info!(hook = %def.name, "delivery hook disabled, skip");
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
                tracing::info!(hook = %def.name, "recall hook disabled, skip");
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
