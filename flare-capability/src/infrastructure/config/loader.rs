//! # Hook配置加载器
//!
//! 提供从不同源加载Hook配置的能力

use std::path::PathBuf;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use flare_server_core::error::{AnyhowContext, Result};
use tracing::{debug, error, info, warn};

use crate::domain::model::HookConfig;
use crate::infrastructure::persistence::postgres_config::PostgresHookConfigRepository;
use flare_server_core::KvStore;

/// Hook配置加载器接口
pub trait ConfigLoader: Send + Sync {
    /// 加载Hook配置
    async fn load(&self) -> Result<HookConfig>;
}

/// ConfigLoader 的枚举封装，用于在 Rust 2024 下避免 `dyn` + async trait 带来的
/// `E0038: trait is not dyn compatible` 问题。
///
/// 业务侧统一通过该枚举来做多态分发，而不是使用 `Arc<dyn ConfigLoader>`.
#[derive(Debug)]
pub enum ConfigLoaderItem {
    File(FileConfigLoader),
    Database(DatabaseConfigLoader),
    ConfigCenter(ConfigCenterLoader),
}

impl ConfigLoaderItem {
    pub async fn load(&self) -> Result<HookConfig> {
        match self {
            ConfigLoaderItem::File(loader) => loader.load().await,
            ConfigLoaderItem::Database(loader) => loader.load().await,
            ConfigLoaderItem::ConfigCenter(loader) => loader.load().await,
        }
    }
}

/// Hook配置合并器
///
/// 按优先级合并多个配置源的Hook配置
pub struct ConfigMerger;

impl ConfigMerger {
    /// 合并多个配置
    ///
    /// 优先级：动态API（数据库） > 配置中心 > 配置文件
    /// 同名Hook高优先级覆盖低优先级
    pub fn merge(configs: Vec<HookConfig>) -> HookConfig {
        let mut merged = HookConfig::default();

        // 按顺序合并，后面的配置会覆盖前面的同名配置
        for config in configs {
            Self::merge_hook_list(&mut merged.pre_send, config.pre_send);
            Self::merge_hook_list(&mut merged.post_send, config.post_send);
            Self::merge_hook_list(&mut merged.delivery, config.delivery);
            Self::merge_hook_list(&mut merged.recall, config.recall);
            Self::merge_hook_list(&mut merged.session_create, config.session_create);
            Self::merge_hook_list(&mut merged.session_update, config.session_update);
            Self::merge_hook_list(&mut merged.session_delete, config.session_delete);
            Self::merge_hook_list(&mut merged.user_login, config.user_login);
            Self::merge_hook_list(&mut merged.user_logout, config.user_logout);
            Self::merge_hook_list(&mut merged.user_online, config.user_online);
            Self::merge_hook_list(&mut merged.user_offline, config.user_offline);
            Self::merge_hook_list(&mut merged.push_pre_send, config.push_pre_send);
            Self::merge_hook_list(&mut merged.push_post_send, config.push_post_send);
            Self::merge_hook_list(&mut merged.push_delivery, config.push_delivery);
        }

        merged
    }

    fn merge_hook_list(
        target: &mut Vec<crate::domain::model::HookConfigItem>,
        source: Vec<crate::domain::model::HookConfigItem>,
    ) {
        for hook in source {
            if let Some(existing) = target.iter_mut().find(|h| h.name == hook.name) {
                // 高优先级覆盖
                *existing = hook;
            } else {
                target.push(hook);
            }
        }
    }
}

/// Hook配置验证器
pub struct ConfigValidator;

impl ConfigValidator {
    /// 验证Hook配置
    pub fn validate(config: &HookConfig) -> Result<()> {
        // 验证所有Hook类型
        let all_hooks = config
            .pre_send
            .iter()
            .chain(config.post_send.iter())
            .chain(config.delivery.iter())
            .chain(config.recall.iter())
            .chain(config.session_create.iter())
            .chain(config.session_update.iter())
            .chain(config.session_delete.iter())
            .chain(config.user_login.iter())
            .chain(config.user_logout.iter())
            .chain(config.user_online.iter())
            .chain(config.user_offline.iter())
            .chain(config.push_pre_send.iter())
            .chain(config.push_post_send.iter())
            .chain(config.push_delivery.iter());

        for hook in all_hooks {
            Self::validate_hook(hook)?;
        }

        Ok(())
    }

    fn validate_hook(hook: &crate::domain::model::HookConfigItem) -> Result<()> {
        if hook.name.is_empty() {
            return Err(config_error("Hook name cannot be empty"));
        }

        if hook.priority < 0 || hook.priority > 1000 {
            return Err(config_error("Hook priority must be between 0 and 1000"));
        }

        if hook.timeout_ms == 0 || hook.timeout_ms > 30000 {
            return Err(config_error("Hook timeout must be between 1ms and 30000ms"));
        }

        Ok(())
    }
}

/// 配置文件加载器
#[derive(Debug)]
pub struct FileConfigLoader {
    path: PathBuf,
}

impl FileConfigLoader {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }
}

impl ConfigLoader for FileConfigLoader {
    async fn load(&self) -> Result<HookConfig> {
        if !self.path.exists() {
            debug!(path = %self.path.display(), "Hook config file not found, using default config");
            return Ok(HookConfig::default());
        }

        let content = tokio::fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("Failed to read hook config file: {}", self.path.display()))?;

        let config: HookConfig = toml::from_str(&content).with_context(|| {
            format!("Failed to parse hook config file: {}", self.path.display())
        })?;

        debug!(
            path = %self.path.display(),
            hooks_count = config.pre_send.len() + config.post_send.len() + config.delivery.len() + config.recall.len(),
            "Loaded hook config from file"
        );

        Ok(config)
    }
}

/// 数据库配置加载器（动态API配置）
#[derive(Debug)]
pub struct DatabaseConfigLoader {
    repository: Arc<PostgresHookConfigRepository>,
    tenant_id: Option<String>,
}

impl DatabaseConfigLoader {
    pub fn new(repository: Arc<PostgresHookConfigRepository>, tenant_id: Option<String>) -> Self {
        Self {
            repository,
            tenant_id,
        }
    }
}

impl ConfigLoader for DatabaseConfigLoader {
    async fn load(&self) -> Result<HookConfig> {
        let config = self
            .repository
            .load_all(self.tenant_id.as_deref())
            .await
            .context("Failed to load hook config from database")?;

        info!(
            tenant_id = ?self.tenant_id,
            hooks_count = config.pre_send.len() + config.post_send.len() + config.delivery.len() + config.recall.len(),
            "Loaded hook config from database"
        );

        Ok(config)
    }
}

/// 配置中心加载器（etcd/Consul）
pub struct ConfigCenterLoader {
    endpoint: String,
    tenant_id: Option<String>,
    config_key: String,
    // 添加服务发现客户端
    discovery_client: Option<Arc<flare_server_core::ServiceDiscover>>,
    // 添加工厂用于创建后端
    factory: Option<flare_server_core::DiscoveryFactory>,
    // 添加KV存储
    kv_store: Option<Arc<KvStore>>,
}

impl ConfigCenterLoader {
    pub fn new(endpoint: String, tenant_id: Option<String>) -> Self {
        // 构建配置键：/flare/hooks/{tenant_id}/config 或 /flare/hooks/config
        let config_key = if let Some(ref tenant) = tenant_id {
            format!("/flare/hooks/{}/config", tenant)
        } else {
            "/flare/hooks/config".to_string()
        };

        Self {
            endpoint,
            tenant_id,
            config_key,
            discovery_client: None,
            factory: None,
            kv_store: None,
        }
    }

    /// 设置服务发现客户端
    pub fn with_discovery_client(
        mut self,
        client: Arc<flare_server_core::ServiceDiscover>,
    ) -> Self {
        self.discovery_client = Some(client);
        self
    }

    /// 设置发现工厂
    pub fn with_factory(mut self, factory: flare_server_core::DiscoveryFactory) -> Self {
        self.factory = Some(factory);
        self
    }

    /// 设置KV存储
    pub fn with_kv_store(mut self, kv_store: Arc<KvStore>) -> Self {
        self.kv_store = Some(kv_store);
        self
    }

    /// 解析endpoint，支持etcd://host:port和consul://host:port格式
    fn parse_endpoint(&self) -> Result<(String, u16)> {
        if let Some(addr) = self.endpoint.strip_prefix("etcd://") {
            let parts: Vec<&str> = addr.split(':').collect();
            if parts.len() != 2 {
                return Err(config_error(format!(
                    "Invalid etcd endpoint format: {}",
                    self.endpoint
                )));
            }
            let host = parts[0].to_string();
            let port = parts[1].parse::<u16>().context("Invalid etcd port")?;
            Ok((host, port))
        } else if let Some(addr) = self.endpoint.strip_prefix("consul://") {
            let parts: Vec<&str> = addr.split(':').collect();
            if parts.len() != 2 {
                return Err(config_error(format!(
                    "Invalid consul endpoint format: {}",
                    self.endpoint
                )));
            }
            let host = parts[0].to_string();
            let port = parts[1].parse::<u16>().context("Invalid consul port")?;
            Ok((host, port))
        } else {
            Err(config_error(format!(
                "Unsupported config center endpoint format: {}",
                self.endpoint
            )))
        }
    }
}

impl std::fmt::Debug for ConfigCenterLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigCenterLoader")
            .field("endpoint", &self.endpoint)
            .field("tenant_id", &self.tenant_id)
            .field("config_key", &self.config_key)
            .field(
                "discovery_client",
                &self.discovery_client.as_ref().map(|_| "ServiceDiscover"),
            )
            .finish()
    }
}

impl ConfigLoader for ConfigCenterLoader {
    async fn load(&self) -> Result<HookConfig> {
        let (host, port) = self.parse_endpoint()?;

        // 根据endpoint类型选择不同的配置中心客户端
        if self.endpoint.starts_with("etcd://") {
            // 使用etcd-client连接etcd并读取配置
            use etcd_client::Client;

            let endpoints = vec![format!("http://{}:{}", host, port)];
            let mut client = Client::connect(endpoints, None)
                .await
                .context("Failed to connect to etcd")?;

            let resp = client
                .get(self.config_key.clone(), None)
                .await
                .context("Failed to get config from etcd")?;

            if let Some(kv) = resp.kvs().first() {
                // etcd-client返回的是bytes，需要转换为字符串
                let value = String::from_utf8(kv.value().to_vec())
                    .context("Failed to parse etcd value as UTF-8 string")?;

                // 解析配置（支持TOML或JSON格式）
                let config: HookConfig = if value.trim_start().starts_with('{') {
                    // JSON格式
                    serde_json::from_str(&value).context("Failed to parse etcd config as JSON")?
                } else {
                    // TOML格式
                    toml::from_str(&value).context("Failed to parse etcd config as TOML")?
                };

                info!(
                    endpoint = %self.endpoint,
                    config_key = %self.config_key,
                    hooks_count = config.pre_send.len() + config.post_send.len() + config.delivery.len() + config.recall.len(),
                    "Loaded hook config from etcd"
                );

                Ok(config)
            } else {
                warn!(
                    endpoint = %self.endpoint,
                    config_key = %self.config_key,
                    "Config not found in etcd, using default config"
                );
                Ok(HookConfig::default())
            }
        } else if self.endpoint.starts_with("consul://") {
            // 使用flare-server-core的KV存储模块连接Consul并读取配置

            // 如果有KV存储，使用KV存储读取配置；否则使用HTTP客户端
            if let Some(kv_store) = &self.kv_store {
                // 使用KV存储从Consul KV存储中读取配置
                match kv_store.get_string(&self.config_key).await {
                    Ok(Some(value)) => {
                        // 解析配置（支持TOML或JSON格式）
                        let config: HookConfig = if value.trim_start().starts_with('{') {
                            // JSON格式
                            serde_json::from_str(&value)
                                .context("Failed to parse consul config as JSON")?
                        } else {
                            // TOML格式
                            toml::from_str(&value)
                                .context("Failed to parse consul config as TOML")?
                        };

                        info!(
                            endpoint = %self.endpoint,
                            config_key = %self.config_key,
                            hooks_count = config.pre_send.len() + config.post_send.len() + config.delivery.len() + config.recall.len(),
                            "Loaded hook config from consul via KV store"
                        );

                        Ok(config)
                    }
                    Ok(None) => {
                        warn!(
                            endpoint = %self.endpoint,
                            config_key = %self.config_key,
                            "Config not found in consul"
                        );
                        Ok(HookConfig::default())
                    }
                    Err(e) => {
                        error!(
                            endpoint = %self.endpoint,
                            config_key = %self.config_key,
                            error = %e,
                            "Failed to get config from consul via KV store"
                        );
                        Err(flare_server_core::error::FlareError::system(format!(
                            "Failed to get config from consul via KV store: {}",
                            e
                        )))
                    }
                }
            } else {
                // 直接使用HTTP客户端从Consul KV存储中读取配置
                // 构造Consul KV URL
                let key = self.config_key.trim_start_matches('/');
                let url = format!("http://{}:{}/v1/kv/{}", host, port, key);

                // 使用HTTP客户端从Consul KV存储中读取配置
                let http_client = reqwest::Client::new();
                match http_client.get(&url).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            let body: Vec<serde_json::Value> = response
                                .json()
                                .await
                                .context("Failed to parse consul response")?;

                            if let Some(first) = body.first() {
                                // 从响应中提取Value字段并进行base64解码
                                if let Some(value_base64) =
                                    first.get("Value").and_then(|v| v.as_str())
                                {
                                    // base64解码
                                    let value_bytes = general_purpose::STANDARD
                                        .decode(value_base64)
                                        .context("Failed to decode base64 value from consul")?;
                                    let value = String::from_utf8(value_bytes)
                                        .context("Failed to parse consul value as UTF-8 string")?;

                                    // 解析配置（支持TOML或JSON格式）
                                    let config: HookConfig = if value.trim_start().starts_with('{')
                                    {
                                        // JSON格式
                                        serde_json::from_str(&value)
                                            .context("Failed to parse consul config as JSON")?
                                    } else {
                                        // TOML格式
                                        toml::from_str(&value)
                                            .context("Failed to parse consul config as TOML")?
                                    };

                                    info!(
                                        endpoint = %self.endpoint,
                                        config_key = %self.config_key,
                                        hooks_count = config.pre_send.len() + config.post_send.len() + config.delivery.len() + config.recall.len(),
                                        "Loaded hook config from consul"
                                    );

                                    Ok(config)
                                } else {
                                    warn!(
                                        endpoint = %self.endpoint,
                                        config_key = %self.config_key,
                                        "Config value not found in consul response"
                                    );
                                    Ok(HookConfig::default())
                                }
                            } else {
                                warn!(
                                    endpoint = %self.endpoint,
                                    config_key = %self.config_key,
                                    "Empty response from consul"
                                );
                                Ok(HookConfig::default())
                            }
                        } else {
                            warn!(
                                endpoint = %self.endpoint,
                                config_key = %self.config_key,
                                status = %response.status(),
                                "Failed to get config from consul"
                            );
                            Ok(HookConfig::default())
                        }
                    }
                    Err(e) => {
                        error!(
                            endpoint = %self.endpoint,
                            config_key = %self.config_key,
                            error = %e,
                            "Failed to get config from consul"
                        );
                        Err(flare_server_core::error::FlareError::system(format!(
                            "Failed to get config from consul: {}",
                            e
                        )))
                    }
                }
            }
        } else {
            Err(config_error(format!(
                "Unsupported config center endpoint format: {}",
                self.endpoint
            )))
        }
    }
}

fn config_error(reason: impl Into<String>) -> flare_server_core::error::FlareError {
    flare_server_core::error::ErrorBuilder::new(
        flare_server_core::error::ErrorCode::ConfigurationError,
        reason,
    )
    .build_error()
}
