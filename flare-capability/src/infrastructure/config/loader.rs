//! # Hook配置加载器
//!
//! 提供从不同源加载Hook配置的能力

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use flare_im_hooks::HookErrorPolicy;
use flare_im_hooks::hooks::adapters::is_reserved_webhook_header;
use flare_server_core::error::{AnyhowContext, Result};
use tracing::{debug, error, info, warn};

use crate::domain::model::{HookConfig, HookTransportConfig, LoggableName, validate_hook_name};
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
    /// 加载本源的配置。文件与配置中心只有全局配置；数据库在多租户进程中另按租户分桶。
    pub async fn load(&self) -> Result<LoadedHookConfig> {
        match self {
            ConfigLoaderItem::File(loader) => loader.load().await.map(LoadedHookConfig::global),
            ConfigLoaderItem::Database(loader) => loader.load_buckets().await,
            ConfigLoaderItem::ConfigCenter(loader) => {
                loader.load().await.map(LoadedHookConfig::global)
            }
        }
    }

    /// 只重新加载一个租户的配置：只有按租户分桶的数据库源支持，其余返回 `None`
    /// （调用方改为全量重载）。
    pub async fn load_tenant(&self, tenant_id: &str) -> Option<Result<HookConfig>> {
        match self {
            ConfigLoaderItem::Database(loader) => loader.load_tenant(tenant_id).await,
            ConfigLoaderItem::File(_) | ConfigLoaderItem::ConfigCenter(_) => None,
        }
    }

    /// 本源把租户的行与全局行分开加载（未设 `TENANT_ID` 的数据库源）。
    pub fn buckets_by_tenant(&self) -> bool {
        match self {
            ConfigLoaderItem::Database(loader) => loader.buckets_by_tenant(),
            ConfigLoaderItem::File(_) | ConfigLoaderItem::ConfigCenter(_) => false,
        }
    }
}

/// 一个配置源一次加载的结果。
#[derive(Debug, Clone, Default)]
pub struct LoadedHookConfig {
    /// 对所有租户生效的配置。
    pub global: HookConfig,
    /// 各租户自己登记的配置（只含该租户的 Hook），键为租户 ID。
    pub tenants: HashMap<String, HookConfig>,
    /// 本次加载中有行解析失败的租户：沿用该源上一次成功加载的该租户配置。
    pub failed_tenants: HashSet<String>,
}

impl LoadedHookConfig {
    /// 只有全局配置的加载结果（文件、配置中心、固定租户的数据库）。
    pub fn global(config: HookConfig) -> Self {
        Self {
            global: config,
            ..Self::default()
        }
    }

    /// 本次解析失败的租户沿用 `previous`（同一配置源上一次的结果）中该租户的配置。
    pub fn inherit_failed_tenants(&mut self, previous: Option<&LoadedHookConfig>) {
        for tenant in std::mem::take(&mut self.failed_tenants) {
            match previous.and_then(|previous| previous.tenants.get(&tenant)) {
                Some(config) => {
                    self.tenants.insert(tenant, config.clone());
                }
                None => {
                    self.tenants.remove(&tenant);
                }
            }
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
    ///
    /// 逐类型合并全部 15 类 Hook（[`HookConfig::merge_overriding`]）。以前这里逐个列字段、漏了
    /// `get_conversation_participants`，该类 Hook 合并后永远为空。
    pub fn merge(configs: Vec<HookConfig>) -> HookConfig {
        let mut merged = HookConfig::default();

        // 按顺序合并，后面的配置会覆盖前面的同名配置
        for config in configs {
            merged.merge_overriding(config);
        }

        merged
    }
}

/// Hook配置验证器
pub struct ConfigValidator;

impl ConfigValidator {
    /// 验证Hook配置（全部 15 类 Hook，见 [`HookConfig::all_hooks`]）
    pub fn validate(config: &HookConfig) -> Result<()> {
        for hook in config.all_hooks() {
            Self::validate_hook(hook)?;
        }

        Ok(())
    }

    /// 校验单条 Hook 配置（名称、优先级、超时、分组、错误策略与 Webhook 的签名密钥和自定义头），
    /// 规则同 [`Self::validate`]。
    ///
    /// 以下几项以前要到每次投递才失败（或被静默改写），现在加载（文件、配置中心、库里的行）与
    /// Administer 写入时就拒绝，错误只带 Hook 名与头名、不带任何取值：
    /// - Webhook 写了 `secret = ""`：签名函数对空密钥报 `ConfigurationError`。不签名就不写 `secret`。
    /// - Webhook 自定义头用了保留头名（`Content-Type`、`Flare-Signature`、`x-flare-signature`、
    ///   `X-Hook-Signature`，不区分大小写）：签名函数拒绝。
    /// - `error_policy` 不认识（解析不区分大小写、忽略下划线，见 `HookErrorPolicy::parse`）：
    ///   以前按 fail_fast 处理。
    pub fn validate_hook(hook: &crate::domain::model::HookConfigItem) -> Result<()> {
        if let HookTransportConfig::Webhook {
            secret: Some(secret),
            ..
        } = &hook.transport
            && secret.is_empty()
        {
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::ConfigurationError,
                "Webhook signing secret is empty: omit `secret` to send unsigned, or set a non-empty value",
            )
            .details(format!("hook={}", LoggableName(&hook.name)))
            .build_error());
        }

        if let HookTransportConfig::Webhook { headers, .. } = &hook.transport
            && let Some(header) = headers
                .keys()
                .find(|header| is_reserved_webhook_header(header))
        {
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::ConfigurationError,
                format!(
                    "Webhook header {header} is reserved: custom headers cannot set Content-Type, Flare-Signature, x-flare-signature or X-Hook-Signature"
                ),
            )
            .details(format!("hook={}", LoggableName(&hook.name)))
            .build_error());
        }

        if HookErrorPolicy::parse(&hook.error_policy).is_none() {
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::ConfigurationError,
                "Hook error_policy must be fail_fast, retry or ignore (case and underscores do not matter)",
            )
            .details(format!("hook={}", LoggableName(&hook.name)))
            .build_error());
        }

        if hook.name.is_empty() {
            return Err(config_error("Hook name cannot be empty"));
        }

        // 创建与改名时才强制的名称规则（`validate_hook_name`：不超过 128 个字符、不含控制字符）。
        // 规则之前已经存下或写在配置文件里的名称照常加载，已有部署不受影响；这里只告警，名称在
        // 日志与错误信息里一律经 `LoggableName` 转义。
        if let Err(rule) = validate_hook_name(&hook.name) {
            warn!(
                hook = %LoggableName(&hook.name),
                rule,
                "hook name breaks the naming rule for new hooks; it is still loaded, rename it"
            );
        }

        if hook.priority < 0 || hook.priority > 1000 {
            return Err(config_error("Hook priority must be between 0 and 1000"));
        }

        if hook.timeout_ms == 0 || hook.timeout_ms > 30000 {
            return Err(config_error("Hook timeout must be between 1ms and 30000ms"));
        }

        if hook.resolved_group().is_err() {
            return Err(config_error(
                "Hook group must be validation, critical or business",
            ));
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
///
/// - 进程设了 `TENANT_ID`（单租户 / 私有化部署）：加载全局行与该租户的行，都作为全局配置，
///   行为同以前。
/// - 未设（多租户进程）：全局行作为全局配置，各租户经 `CapabilityService/Administer` 登记的行
///   按租户分桶；执行时只取消息所属租户的那一桶（见 [`super::HookConfigSnapshot`]）。
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

    /// 按进程的租户模式加载（见类型说明）。
    pub async fn load_buckets(&self) -> Result<LoadedHookConfig> {
        let loaded = match self.tenant_id.as_deref() {
            Some(tenant) => LoadedHookConfig::global(
                self.repository
                    .load_all(Some(tenant))
                    .await
                    .context("Failed to load hook config from database")?,
            ),
            None => {
                let rows = self
                    .repository
                    .load_by_tenant()
                    .await
                    .context("Failed to load hook config from database")?;
                LoadedHookConfig {
                    global: rows.global,
                    tenants: rows.tenants,
                    failed_tenants: rows.failed_tenants,
                }
            }
        };

        info!(
            tenant_id = ?self.tenant_id,
            hooks_count = loaded.global.hook_count(),
            tenants = loaded.tenants.len(),
            tenant_hooks_count = loaded.tenants.values().map(HookConfig::hook_count).sum::<usize>(),
            failed_tenants = loaded.failed_tenants.len(),
            "Loaded hook config from database"
        );

        Ok(loaded)
    }

    /// 未固定租户：全局行与各租户的行分桶加载。
    pub fn buckets_by_tenant(&self) -> bool {
        self.tenant_id.is_none()
    }

    /// 只加载一个租户自己登记的行；固定了租户的进程不分桶，返回 `None`。
    pub async fn load_tenant(&self, tenant_id: &str) -> Option<Result<HookConfig>> {
        if !self.buckets_by_tenant() {
            return None;
        }
        Some(
            self.repository
                .load_tenant(tenant_id)
                .await
                .context("Failed to load tenant hook config from database"),
        )
    }
}

impl ConfigLoader for DatabaseConfigLoader {
    /// 全局配置（多租户进程不含各租户的行，租户配置见 [`DatabaseConfigLoader::load_buckets`]）。
    async fn load(&self) -> Result<HookConfig> {
        self.load_buckets().await.map(|loaded| loaded.global)
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
                    // consul 对「键不存在」返回 404，而 KV 客户端把它包成 Err 而不是
                    // Ok(None)——于是「钩子未配置」这个完全正常的状态每 30 秒打一条
                    // ERROR。运维看到 ERROR 会当成服务异常排查，而它其实无害
                    // （下面的 watcher 收到 Err 只是 warn 并跳过，配置照常从数据库加载）。
                    // 在调用侧把 404 归位到已有的「未配置」语义上。
                    Err(e) if e.to_string().contains("404") => {
                        debug!(
                            endpoint = %self.endpoint,
                            config_key = %self.config_key,
                            "consul 中没有钩子配置（键不存在），使用默认值"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::HookConfigItem;
    use crate::infrastructure::config::HookConfigSnapshot;
    use flare_im_hooks::HookGroup;

    /// 与 `hook_configs.hook_type`、[`HookConfig`] 字段名相同的 15 类 Hook。
    const ALL_TYPES: [&str; 15] = [
        "pre_send",
        "post_send",
        "delivery",
        "recall",
        "session_create",
        "session_update",
        "session_delete",
        "user_login",
        "user_logout",
        "user_online",
        "user_offline",
        "push_pre_send",
        "push_post_send",
        "push_delivery",
        "get_conversation_participants",
    ];

    fn parse(toml_src: &str) -> HookConfig {
        toml::from_str(toml_src).expect("hook config toml")
    }

    /// 每类一条名为 `{type}-{tag}`、超时为 `timeout_ms` 的 Hook。
    fn one_of_each(tag: &str, timeout_ms: u64) -> HookConfig {
        let mut toml_src = String::new();
        for ty in ALL_TYPES {
            toml_src.push_str(&format!(
                "[[{ty}]]\nname = \"{ty}-{tag}\"\nenabled = true\npriority = 100\ntimeout_ms = {timeout_ms}\n[{ty}.selector]\n[{ty}.transport]\ntype = \"local\"\ntarget = \"noop\"\n\n"
            ));
        }
        parse(&toml_src)
    }

    fn participants(config: &HookConfig) -> Vec<(String, u64)> {
        config
            .get_conversation_participants
            .iter()
            .map(|hook| (hook.name.clone(), hook.timeout_ms))
            .collect()
    }

    /// 修复前 `merge` 逐个列字段、漏了 `get_conversation_participants`：合并后该类为空，
    /// 经快照查询（`CoreHookRegistry::get_conversation_participants_hooks` 的来源）同样为空。
    #[test]
    fn merge_keeps_every_hook_type_including_conversation_participants() {
        let file = one_of_each("file", 100);
        let mut database = one_of_each("file", 200);
        database
            .get_conversation_participants
            .extend(one_of_each("db", 300).get_conversation_participants);

        let merged = ConfigMerger::merge(vec![file.clone(), database.clone()]);
        assert_eq!(merged.hook_count(), ALL_TYPES.len() + 1);
        // 同名由高优先级（后面的）覆盖，新名字追加。
        assert_eq!(
            participants(&merged),
            [
                ("get_conversation_participants-file".to_string(), 200),
                ("get_conversation_participants-db".to_string(), 300),
            ]
        );
        assert!(merged.all_hooks().all(|hook| hook.timeout_ms != 100));

        let snapshot = HookConfigSnapshot::build(
            &[
                Some(LoadedHookConfig::global(file)),
                Some(LoadedHookConfig::global(database)),
            ],
            &HookConfigSnapshot::default(),
        )
        .unwrap();
        assert_eq!(
            participants(&snapshot.for_tenant("tenant-a")),
            participants(&merged)
        );
    }

    /// 校验同样覆盖 `get_conversation_participants`（修复前跳过这一类）。
    #[test]
    fn validation_covers_conversation_participants_hooks() {
        let mut config = one_of_each("v", 100);
        assert!(ConfigValidator::validate(&config).is_ok());
        config.get_conversation_participants[0].priority = 2000;
        assert!(ConfigValidator::validate(&config).is_err());
    }

    /// 规则之前写进文件或库里的名称（带换行、超过 128 个字符）照常加载，快照里仍有这两条；
    /// 只各告警一次，告警里的名称已转义，伪造不了日志行。
    #[test]
    fn names_breaking_the_new_rule_still_load_and_are_reported_escaped() {
        let long = "n".repeat(129);
        let config = parse(&format!(
            "[[pre_send]]\nname = \"legacy\\nWARN forged line\"\nenabled = true\npriority = 100\ntimeout_ms = 500\n[pre_send.selector]\n[pre_send.transport]\ntype = \"local\"\ntarget = \"noop\"\n\n\
             [[pre_send]]\nname = \"{long}\"\nenabled = true\npriority = 100\ntimeout_ms = 500\n[pre_send.selector]\n[pre_send.transport]\ntype = \"local\"\ntarget = \"noop\"\n"
        ));
        assert_eq!(config.pre_send[0].name, "legacy\nWARN forged line");

        let capture = crate::test_logs::LogCapture::start(tracing::Level::WARN);
        assert!(ConfigValidator::validate(&config).is_ok());
        let output = capture.finish();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "{output}");
        assert!(
            lines[0].contains("hook=legacy\\nWARN forged line")
                && lines[0].contains("control characters"),
            "{output}"
        );
        assert!(
            lines[1].contains(&long) && lines[1].contains("128 characters"),
            "{output}"
        );

        let snapshot = HookConfigSnapshot::build(
            &[Some(LoadedHookConfig::global(config))],
            &HookConfigSnapshot::default(),
        )
        .unwrap();
        assert_eq!(snapshot.for_tenant("tenant-a").pre_send.len(), 2);
    }

    const EMPTY_SECRET_PROFILE: &str = r#"
[[post_send]]
name = "audit-empty-secret"
enabled = true
priority = 0
timeout_ms = 500
[post_send.selector]
[post_send.transport]
type = "webhook"
endpoint = "https://audit.example.com/hook"
secret = ""
[post_send.transport.headers]
Authorization = "Bearer w13h-header-value-not-for-errors"
"#;

    /// 文件或配置中心里 `secret = ""` 的 Webhook 在加载（合成快照）时就被拒绝，错误说明原因与
    /// Hook 名、不带任何取值；修复前加载通过，之后每次投递都报 `ConfigurationError`。
    #[test]
    fn an_empty_webhook_secret_is_rejected_when_the_config_is_loaded() {
        let config = parse(EMPTY_SECRET_PROFILE);
        let error = ConfigValidator::validate(&config).unwrap_err();
        let printed = format!("{error} {error:?}");
        assert!(printed.contains("signing secret is empty"), "{printed}");
        assert!(printed.contains("audit-empty-secret"), "{printed}");
        assert!(!printed.contains("w13h-header-value"), "{printed}");

        // 加载路径（启动与定时刷新都经 `HookConfigSnapshot::build`）拒绝整份全局配置。
        assert!(
            HookConfigSnapshot::build(
                &[Some(LoadedHookConfig::global(config))],
                &HookConfigSnapshot::default(),
            )
            .is_err()
        );

        // 不写 `secret`（不签名）与非空密钥照常通过。
        for replacement in ["", "secret = \"w13h-non-empty\""] {
            let config = parse(&EMPTY_SECRET_PROFILE.replace("secret = \"\"", replacement));
            assert!(
                ConfigValidator::validate(&config).is_ok(),
                "{replacement:?}"
            );
        }
    }

    /// 保留的 Webhook 头（不区分大小写）在加载时就拒绝（修复前加载通过、每次投递报
    /// `ConfigurationError`）；错误带头名与 Hook 名、不带取值；普通自定义头照常通过。
    #[test]
    fn reserved_webhook_headers_are_rejected_when_the_config_is_loaded() {
        let value = "w14h-header-value-not-for-errors";
        for header in [
            "Content-Type",
            "content-type",
            "Flare-Signature",
            "flare-signature",
            "x-flare-signature",
            "X-Flare-Signature",
            "X-Hook-Signature",
            "x-hook-signature",
        ] {
            let profile = EMPTY_SECRET_PROFILE
                .replace("secret = \"\"", "secret = \"w14h-non-empty\"")
                .replace(
                    "Authorization = \"Bearer w13h-header-value-not-for-errors\"",
                    &format!("\"{header}\" = \"{value}\""),
                );
            let config = parse(&profile);
            let error = ConfigValidator::validate(&config).unwrap_err();
            let printed = format!("{error} {error:?}");
            assert!(printed.contains("reserved"), "{printed}");
            assert!(printed.contains(header), "{printed}");
            assert!(printed.contains("audit-empty-secret"), "{printed}");
            assert!(!printed.contains(value), "{printed}");
            assert!(
                HookConfigSnapshot::build(
                    &[Some(LoadedHookConfig::global(config))],
                    &HookConfigSnapshot::default(),
                )
                .is_err(),
                "{header}"
            );
        }
        let profile = EMPTY_SECRET_PROFILE.replace("secret = \"\"", "secret = \"w14h-non-empty\"");
        assert!(ConfigValidator::validate(&parse(&profile)).is_ok());
    }

    /// `error_policy` 不区分大小写、忽略下划线（与 Message Ingest 同一套解析）；不认识的取值在
    /// 加载时拒绝（修复前静默按 fail_fast）。
    #[test]
    fn unknown_error_policies_are_rejected_when_the_config_is_loaded() {
        let profile = |policy: &str| {
            EMPTY_SECRET_PROFILE
                .replace("secret = \"\"", "secret = \"w14h-non-empty\"")
                .replace(
                    "timeout_ms = 500",
                    &format!("timeout_ms = 500\nerror_policy = \"{policy}\""),
                )
        };
        for policy in [
            "fail_fast",
            "FailFast",
            "failfast",
            "retry",
            "Retry",
            "ignore",
            "Ignore",
            "IGNORE",
        ] {
            assert!(
                ConfigValidator::validate(&parse(&profile(policy))).is_ok(),
                "{policy}"
            );
        }
        for policy in ["abort", "fail-fast", "", "ignored"] {
            let config = parse(&profile(policy));
            let error = ConfigValidator::validate(&config).unwrap_err();
            assert!(
                format!("{error} {error:?}").contains("error_policy"),
                "{policy:?}: {error:?}"
            );
            assert!(
                HookConfigSnapshot::build(
                    &[Some(LoadedHookConfig::global(config))],
                    &HookConfigSnapshot::default(),
                )
                .is_err(),
                "{policy:?}"
            );
        }
    }

    fn shipped_profile(name: &str) -> HookConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../config")
            .join(name);
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        toml::from_str(&content).unwrap_or_else(|error| {
            panic!("{name} does not parse as a Capability hook config: {error}")
        })
    }

    /// 仓库里的 Hook 配置样例也能被 Capability 的加载器解析与校验（Ingest 的加载器另有
    /// `flare-im-hooks` 的 `config_fail_closed` 用例）。
    #[test]
    fn shipped_hook_profiles_load_in_the_capability_engine() {
        for name in [
            "hooks.core.toml",
            "hooks.social.toml",
            "hooks.toml",
            "hooks.example.toml",
            "hooks.social.example.toml",
            "hooks.business.example.toml",
        ] {
            let config = shipped_profile(name);
            ConfigValidator::validate(&config)
                .unwrap_or_else(|error| panic!("{name} fails validation: {error}"));
        }
    }

    /// 共用样例里 Message Ingest 写法的 `Ignore` 在 Capability 中也是 ignore（修复前成了 fail_fast）。
    #[test]
    fn shipped_error_policies_mean_the_same_in_both_engines() {
        use crate::domain::model::HookExecutionPlan;
        use flare_im_hooks::HookErrorPolicy;
        let config = shipped_profile("hooks.business.example.toml");
        let policies: Vec<HookErrorPolicy> = config
            .pre_send
            .iter()
            .chain(&config.post_send)
            .chain(&config.delivery)
            .chain(&config.recall)
            .map(|hook| {
                HookExecutionPlan::from_hook_config(hook.clone(), "pre_send")
                    .metadata()
                    .error_policy
            })
            .collect();
        assert!(policies.contains(&HookErrorPolicy::Ignore), "{policies:?}");
        assert!(
            policies.contains(&HookErrorPolicy::FailFast),
            "{policies:?}"
        );
    }

    /// 样例里意在影响主链决策的 Hook（要求成功的 pre_send 与 recall）不能落进 business 组：
    /// Capability 引擎对 business 组的拒绝只记日志、主链照常继续。
    #[test]
    fn example_decision_hooks_are_not_in_the_business_group() {
        for name in [
            "hooks.social.toml",
            "hooks.example.toml",
            "hooks.social.example.toml",
            "hooks.business.example.toml",
        ] {
            let config = shipped_profile(name);
            let decisions: Vec<&HookConfigItem> = config
                .pre_send
                .iter()
                .chain(config.recall.iter())
                .filter(|hook| hook.require_success)
                .collect();
            for hook in decisions {
                assert_ne!(
                    hook.resolved_group().unwrap(),
                    HookGroup::Business,
                    "{name}: {} would only be logged when it rejects",
                    hook.name
                );
            }
        }
    }
}
