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

use crate::domain::model::{
    HOOK_TYPES, HookConfig, HookTransportConfig, LoggableName, validate_hook_name,
};
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

    /// 启动时本源加载失败，Capability 就不启动：只有本进程自己的 Hook 文件（见
    /// [`FileConfigLoader`]）。数据库与配置中心启动时加载失败照旧只记告警、等下一次刷新。
    pub fn required_at_startup(&self) -> bool {
        matches!(self, ConfigLoaderItem::File(_))
    }

    /// 日志里的配置源类别。
    pub fn source_name(&self) -> &'static str {
        match self {
            ConfigLoaderItem::File(_) => "file",
            ConfigLoaderItem::Database(_) => "database",
            ConfigLoaderItem::ConfigCenter(_) => "config_center",
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

    /// 同 [`Self::validate`]，只是不对命名规则告警：配置文件加载器用它先查一遍，文件的 Hook
    /// 随后合成快照时还要经 [`Self::validate`]，告警每次加载只打一次。
    fn check(config: &HookConfig) -> Result<()> {
        config.all_hooks().try_for_each(Self::check_hook)
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
        Self::check_hook(hook)?;

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

        Ok(())
    }

    /// [`Self::validate_hook`] 里报错的那些检查（不含命名规则告警）。
    fn check_hook(hook: &crate::domain::model::HookConfigItem) -> Result<()> {
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

/// 配置文件加载器：本进程自己的 Hook 文件（`CONFIG_FILE`，默认 `config/hooks.toml`，最低优先级）。
///
/// - 文件不存在：该文件可选，没有文件 Hook（行为同以前）。
/// - 文件存在却读不了（无权限、是目录、不是 UTF-8、符号链接指向不存在的文件）、不是合法的 Hook
///   配置（TOML 语法或字段类型错误、顶层写了不认识的 Hook 类型）或校验不过（见
///   [`ConfigValidator::validate_hook`]）：报配置错误。启动时 Capability 因此拒绝启动；运行中的
///   重载沿用该文件上一次加载成功的配置并记错误日志（见 [`super::ConfigWatcher`]）。以前读不了、
///   解析不了时只记一条告警，按没有文件 Hook 启动，平台写在文件里的审核、合规 Hook 静默失效。
///
/// 错误只带文件路径、出错的行列、Hook 名与头名，不带文件内容：TOML 解析器的报错会原样引用出错的
/// 那一行，字段类型错误还会带上取值，可能正是 Webhook 密钥或鉴权头。
#[derive(Debug)]
pub struct FileConfigLoader {
    path: PathBuf,
}

impl FileConfigLoader {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    /// 读取并校验文件；文件不存在时返回 `Ok(None)`（规则见类型说明）。
    pub async fn read(&self) -> Result<Option<HookConfig>> {
        let content = match tokio::fs::read_to_string(&self.path).await {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // 链接本身在、指向的文件不在：多半是配置档没装好（启动脚本用符号链接选配置档），
                // 不当成「没有文件」。
                if tokio::fs::symlink_metadata(&self.path).await.is_ok() {
                    return Err(self.error("is a symbolic link to a missing file"));
                }
                return Ok(None);
            }
            Err(error) => return Err(self.error(format!("cannot be read: {error}"))),
        };
        let config = parse_hook_file(&content).map_err(|reason| self.error(reason))?;
        ConfigValidator::check(&config)
            .map_err(|error| self.error(format!("is invalid: {}", reason_with_details(&error))))?;
        Ok(Some(config))
    }

    /// 启动时先于配置中心与数据库读一次（见 `prepare_config_sources`），文件有问题时返回错误、
    /// Capability 拒绝启动，报错不被随后的数据库不可达之类的错误盖住；读到的 Hook 条数（或文件
    /// 不存在）记一条 INFO。
    pub async fn load_at_startup(&self) -> Result<()> {
        match self.read().await? {
            Some(config) => info!(
                path = %self.path.display(),
                hooks_count = config.hook_count(),
                "Loaded hook config file"
            ),
            None => info!(
                path = %self.path.display(),
                "Hook config file not found; no hooks from file"
            ),
        }
        Ok(())
    }

    fn error(&self, reason: impl std::fmt::Display) -> flare_server_core::error::FlareError {
        config_error(format!("Hook config file {} {reason}", self.path.display()))
    }
}

impl ConfigLoader for FileConfigLoader {
    async fn load(&self) -> Result<HookConfig> {
        let Some(config) = self.read().await? else {
            debug!(path = %self.path.display(), "Hook config file not found, using default config");
            return Ok(HookConfig::default());
        };

        debug!(
            path = %self.path.display(),
            hooks_count = config.hook_count(),
            "Loaded hook config from file"
        );

        Ok(config)
    }
}

/// 把文件内容解析成 Hook 配置；失败时返回不带文件内容的原因（见 [`FileConfigLoader`]）。
fn parse_hook_file(content: &str) -> std::result::Result<HookConfig, String> {
    let invalid = |error: toml::de::Error| match error.span() {
        Some(span) => {
            let (line, column) = line_and_column(content, span.start);
            format!("is not a valid hook configuration (line {line}, column {column})")
        }
        None => "is not a valid hook configuration".to_string(),
    };
    // 先按表解析拿到顶层键：顶层只能是 15 类 Hook。写错的类型名（`[[pre_sned]]`）以前被静默
    // 忽略，那一节的 Hook 就不加载。
    let table: toml::Table = toml::from_str(content).map_err(invalid)?;
    if let Some(unknown) = table.keys().find(|key| !HOOK_TYPES.contains(&key.as_str())) {
        return Err(format!(
            "has an unknown hook type `{}` (hook types: {})",
            LoggableName(unknown),
            HOOK_TYPES.join(", ")
        ));
    }
    toml::from_str(content).map_err(invalid)
}

/// `offset`（字节）所在的行与列，都从 1 起算；列按字符计。
fn line_and_column(content: &str, offset: usize) -> (usize, usize) {
    let before = content.get(..offset).unwrap_or(content);
    let line_start = before.rfind('\n').map_or(0, |index| index + 1);
    (
        before.matches('\n').count() + 1,
        before[line_start..].chars().count() + 1,
    )
}

/// 配置错误的原因加细节（Hook 名、头名，不带取值）：`FlareError` 的 Display 不含细节。
fn reason_with_details(error: &flare_server_core::error::FlareError) -> String {
    match error {
        flare_server_core::error::FlareError::Localized {
            reason,
            details: Some(details),
            ..
        } => format!("{reason} ({details})"),
        other => other.reason().to_string(),
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

/// 测试用的 Hook 文件：临时目录与样例内容（`watcher`、`config_sources` 的用例也用）。
#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;

    /// 样例里的密钥与鉴权头取值：任何报错与日志里都不能出现。
    pub(crate) const SECRET: &str = "r75b-secret-not-for-errors";

    /// 一条 post_send Webhook Hook，第 10 行是 `secret`、第 12 行是鉴权头。
    pub(crate) fn webhook_profile(name: &str) -> String {
        format!(
            "[[post_send]]\nname = \"{name}\"\nenabled = true\npriority = 0\ntimeout_ms = 500\n\
             [post_send.selector]\n[post_send.transport]\ntype = \"webhook\"\n\
             endpoint = \"https://audit.example.com/hook\"\nsecret = \"{SECRET}\"\n\
             [post_send.transport.headers]\nAuthorization = \"Bearer {SECRET}\"\n"
        )
    }

    /// 坏掉的文件：(内容, 报错里应有的说明)。前两份 TOML 解析器自己的报错会带出密钥。
    pub(crate) fn broken_profiles() -> Vec<(String, &'static str)> {
        vec![
            // 第 10 行的字符串没有收尾。
            (
                webhook_profile("audit").replace(
                    &format!("secret = \"{SECRET}\""),
                    &format!("secret = \"{SECRET}"),
                ),
                "not a valid hook configuration (line 10,",
            ),
            // 第 3 行字段类型不对。
            (
                webhook_profile("audit")
                    .replace("enabled = true", &format!("enabled = \"{SECRET}\"")),
                "not a valid hook configuration (line 3,",
            ),
            // 顶层 Hook 类型写错。
            (
                webhook_profile("audit").replace("post_send", "post_sned"),
                "unknown hook type `post_sned`",
            ),
            // 能解析，校验不过。
            (
                webhook_profile("audit").replace("priority = 0", "priority = 5000"),
                "is invalid: Hook priority must be between 0 and 1000",
            ),
        ]
    }

    /// 临时目录，用完删除。
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new() -> Self {
            let path = std::env::temp_dir().join(format!("flare-r75b-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        /// 目录下名为 `name` 的路径（不创建）。
        pub(crate) fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        pub(crate) fn write(&self, name: &str, content: impl AsRef<[u8]>) -> PathBuf {
            let path = self.path(name);
            std::fs::write(&path, content).unwrap();
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{SECRET, TempDir, broken_profiles, webhook_profile};
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

    fn shipped_profile_path(name: &str) -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../config")
            .join(name)
    }

    fn shipped_profile(name: &str) -> HookConfig {
        let path = shipped_profile_path(name);
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        toml::from_str(&content).unwrap_or_else(|error| {
            panic!("{name} does not parse as a Capability hook config: {error}")
        })
    }

    /// 仓库里的 Hook 配置样例也能被 Capability 的加载器解析与校验（Ingest 的加载器另有
    /// `flare-im-hooks` 的 `config_fail_closed` 用例），作为 Capability 自己的 Hook 文件
    /// （`CONFIG_FILE`）也能加载：顶层没有不认识的 Hook 类型。
    #[tokio::test]
    async fn shipped_hook_profiles_load_in_the_capability_engine() {
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
            let from_file = FileConfigLoader::new(shipped_profile_path(name))
                .read()
                .await
                .unwrap_or_else(|error| panic!("{name} fails as a Capability hook file: {error}"))
                .unwrap_or_else(|| panic!("{name} is missing"));
            assert_eq!(from_file.hook_count(), config.hook_count(), "{name}");
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

    fn printed(error: &flare_server_core::error::FlareError) -> String {
        format!("{error} {error:?}")
    }

    /// 文件不存在（文件可选）：没有文件 Hook，行为同以前。
    #[tokio::test]
    async fn a_missing_hook_file_means_no_file_hooks() {
        let dir = TempDir::new();
        let loader = FileConfigLoader::new(dir.path("hooks.toml"));
        assert!(loader.read().await.unwrap().is_none());
        loader.load_at_startup().await.unwrap();
        let loaded = ConfigLoaderItem::File(loader).load().await.unwrap();
        assert!(loaded.global.is_empty());
        assert!(loaded.tenants.is_empty());
    }

    /// 解析不了（语法、字段类型、顶层 Hook 类型写错）或校验不过的文件报配置错误，说明带文件路径与
    /// 出错的行列；不带文件内容（TOML 解析器自己的报错会带出那一行的密钥）。修复前加载器只记告警，
    /// 按没有文件 Hook 启动。
    #[tokio::test]
    async fn a_broken_hook_file_is_an_error_naming_the_file_but_not_its_content() {
        let dir = TempDir::new();
        let valid = dir.write("hooks.toml", webhook_profile("audit"));
        let config = FileConfigLoader::new(&valid).read().await.unwrap().unwrap();
        assert_eq!(config.post_send.len(), 1);

        for (index, (content, expected)) in broken_profiles().into_iter().enumerate() {
            if index < 2 {
                let raw = toml::from_str::<HookConfig>(&content)
                    .unwrap_err()
                    .to_string();
                assert!(
                    raw.contains(SECRET),
                    "parser error is expected to echo the source: {raw}"
                );
            }
            let path = dir.write("hooks.toml", &content);
            for error in [
                FileConfigLoader::new(&path).read().await.unwrap_err(),
                FileConfigLoader::new(&path)
                    .load_at_startup()
                    .await
                    .unwrap_err(),
                ConfigLoaderItem::File(FileConfigLoader::new(&path))
                    .load()
                    .await
                    .unwrap_err(),
            ] {
                let printed = printed(&error);
                assert_eq!(
                    error.code(),
                    Some(flare_server_core::error::ErrorCode::ConfigurationError)
                );
                // 路径与原因在 Display 里（启动失败与日志打印的都是它）。
                let shown = error.to_string();
                assert!(shown.contains(&path.display().to_string()), "{shown}");
                assert!(shown.contains(expected), "{shown}");
                assert!(!printed.contains(SECRET), "{printed}");
            }
        }
    }

    /// 校验错误带 Hook 名（细节以前只在 Debug 里，启动失败的报错看不到）。
    #[tokio::test]
    async fn an_invalid_hook_in_the_file_is_named_in_the_error() {
        let dir = TempDir::new();
        let path = dir.write(
            "hooks.toml",
            webhook_profile("audit-empty-secret")
                .replace(&format!("secret = \"{SECRET}\""), "secret = \"\""),
        );
        let error = FileConfigLoader::new(&path).read().await.unwrap_err();
        let shown = error.to_string();
        assert!(shown.contains("signing secret is empty"), "{shown}");
        assert!(shown.contains("hook=audit-empty-secret"), "{shown}");
        assert!(!printed(&error).contains(SECRET), "{error:?}");
    }

    /// 文件在却读不了：是目录、不是 UTF-8、符号链接指向不存在的文件、没有读权限。
    #[tokio::test]
    async fn an_unreadable_hook_file_is_an_error() {
        let dir = TempDir::new();
        let directory = dir.path("hooks.d");
        std::fs::create_dir(&directory).unwrap();
        let not_utf8 = dir.write("latin1.toml", [b'#', b' ', 0xff, 0xfe, b'\n']);
        let mut cases = vec![(directory, "cannot be read"), (not_utf8, "cannot be read")];

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dangling = dir.path("hooks.toml");
            std::os::unix::fs::symlink("hooks.social.toml", &dangling).unwrap();
            cases.push((dangling, "symbolic link to a missing file"));

            let locked = dir.write("locked.toml", webhook_profile("audit"));
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
            // 以 root 跑时权限位挡不住读，这一项不适用。
            if std::fs::read(&locked).is_err() {
                cases.push((locked, "cannot be read"));
            }
        }

        for (path, expected) in cases {
            let error = FileConfigLoader::new(&path).read().await.unwrap_err();
            let shown = error.to_string();
            assert!(shown.contains(&path.display().to_string()), "{shown}");
            assert!(shown.contains(expected), "{shown}");
            assert!(!printed(&error).contains(SECRET), "{error:?}");
        }
    }

    #[test]
    fn error_positions_are_one_based_lines_and_character_columns() {
        let content = "a = 1\nname = \"审核\"x\n";
        assert_eq!(line_and_column(content, 0), (1, 1));
        assert_eq!(line_and_column(content, 6), (2, 1));
        let offset = content.find('x').unwrap();
        assert_eq!(line_and_column(content, offset), (2, 12));
        assert_eq!(line_and_column(content, content.len() + 5), (3, 1));
    }
}
