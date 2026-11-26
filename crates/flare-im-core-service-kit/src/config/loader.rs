//! `FlareAppConfig` 主聚合、全局实例与配置加载入口。
//!
//! 从 mod.rs 拆出（原单文件 2,621 行）；pub API 经 mod.rs re-export。

use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use crate::{Config, RegistryConfig, ServerConfig, ServiceConfig};
use flare_server_core::error::{AnyhowContext, Result};
use serde::Deserialize;
use toml::Value;
use tracing::warn;

use super::manager;
use super::services::*;
use super::types::*;
use super::types::{
    DEFAULT_STREAM_DUPLICATE_WINDOW_SECONDS, DEFAULT_STREAM_MAX_AGE_SECONDS,
    DEFAULT_STREAM_MAX_BYTES, DEFAULT_STREAM_NUM_REPLICAS, default_mq_backend,
    positive_stream_option,
};

/// 全局应用配置实例，使用 OnceLock 确保只初始化一次
static APP_CONFIG: OnceLock<FlareAppConfig> = OnceLock::new();

struct LoadedConfig {
    config: FlareAppConfig,
    source_path: Option<PathBuf>,
}

/// Flare 应用配置主结构体
#[derive(Debug, Clone, Deserialize)]
pub struct FlareAppConfig {
    /// 核心配置
    #[serde(flatten)]
    pub core: Config,
    /// Process-local deployment override; never inferred from DNS or TOML.
    #[serde(skip)]
    static_registry_mode: bool,
    /// 日志配置
    #[serde(default)]
    pub logging: LoggingConfig,
    /// 进程级指标策略
    #[serde(default)]
    pub metrics: MetricsPolicyConfig,
    /// Redis 配置映射
    #[serde(default)]
    pub redis: HashMap<String, RedisPoolConfig>,
    /// JetStream 配置映射
    #[serde(default)]
    pub jetstream: HashMap<String, JetStreamClusterConfig>,
    /// MQ 后端选择，默认 NATS。
    #[serde(default)]
    pub mq: MqBackendConfig,
    /// Kafka 配置映射。默认不启用；选择 Kafka 后作为主 MQ 后端配置。
    #[serde(default)]
    pub kafka: HashMap<String, KafkaClusterConfig>,
    /// PostgreSQL 配置映射
    #[serde(default)]
    pub postgres: HashMap<String, PostgresInstanceConfig>,
    /// 对象存储配置映射
    #[serde(default)]
    pub object_storage: HashMap<String, ObjectStoreConfig>,
    /// 服务配置
    #[serde(default)]
    pub services: ServicesConfig,
}

impl FlareAppConfig {
    /// True only when the process explicitly selected `FLARE_REGISTRY_MODE=static`.
    /// An absent registry in ordinary configuration is not an opt-in to static routing.
    pub fn uses_static_registry(&self) -> bool {
        self.static_registry_mode
    }

    /// 返回当前 MQ 后端标识（`nats` \| `kafka`）。
    pub fn mq_default_backend(&self) -> &str {
        self.mq.default_backend.as_str()
    }

    /// 是否配置为使用 Kafka 作为默认 MQ 后端。
    pub fn mq_uses_kafka(&self) -> bool {
        self.mq.default_backend == "kafka"
    }

    /// 是否配置为使用 NATS JetStream。
    pub fn mq_uses_nats(&self) -> bool {
        self.mq.default_backend == "nats"
    }

    /// 获取核心配置
    pub fn base(&self) -> &Config {
        &self.core
    }

    /// 获取日志配置
    pub fn logging(&self) -> &LoggingConfig {
        &self.logging
    }

    /// 获取 Redis 配置
    pub fn redis_profile(&self, name: &str) -> Option<&RedisPoolConfig> {
        self.redis.get(name)
    }

    /// 获取 JetStream 配置
    pub fn jetstream_profile(&self, name: &str) -> Option<&JetStreamClusterConfig> {
        self.jetstream.get(name)
    }

    /// 获取 NATS JetStream 拓扑配置。错误的 stream 选项不能静默退回默认保留期。
    pub fn jetstream_topology(&self) -> Vec<JetStreamTopologySpec> {
        self.try_jetstream_topology()
            .expect("invalid JetStream topology configuration")
    }

    fn try_jetstream_topology(&self) -> Result<Vec<JetStreamTopologySpec>> {
        let mut specs = Vec::new();
        for (profile_name, profile) in &self.jetstream {
            for key in profile.options.keys() {
                if !matches!(
                    key.as_str(),
                    "max_age_seconds" | "duplicate_window_seconds" | "max_bytes" | "num_replicas"
                ) {
                    return Err(flare_server_core::error::FlareError::system(format!(
                        "jetstream.{profile_name}.options.{key} is unsupported; stream retention/storage are fixed to limits/file, and consumer ack settings belong to service consumer configuration"
                    )));
                }
            }
            let stream_name = profile
                .stream_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty());
            if stream_name.is_none() && profile.subjects.is_empty() && profile.options.is_empty() {
                // A connection-only profile does not own a stream.
                continue;
            }
            let stream_name = stream_name.ok_or_else(|| {
                flare_server_core::error::FlareError::system(format!(
                    "jetstream.{profile_name}.stream_name is required for stream topology"
                ))
            })?;
            if profile.subjects.is_empty()
                || profile
                    .subjects
                    .iter()
                    .any(|subject| subject.trim().is_empty())
            {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "jetstream.{profile_name}.subjects must contain non-empty subjects"
                )));
            }

            let max_age_seconds = positive_stream_option(
                profile_name,
                &profile.options,
                "max_age_seconds",
                DEFAULT_STREAM_MAX_AGE_SECONDS,
            )?;
            let duplicate_window_seconds = positive_stream_option(
                profile_name,
                &profile.options,
                "duplicate_window_seconds",
                DEFAULT_STREAM_DUPLICATE_WINDOW_SECONDS,
            )?;
            let max_bytes = positive_stream_option(
                profile_name,
                &profile.options,
                "max_bytes",
                DEFAULT_STREAM_MAX_BYTES as u64,
            )?;
            let max_bytes = i64::try_from(max_bytes).map_err(|_| {
                flare_server_core::error::FlareError::system(format!(
                    "jetstream.{profile_name}.options.max_bytes exceeds i64::MAX"
                ))
            })?;
            let num_replicas = positive_stream_option(
                profile_name,
                &profile.options,
                "num_replicas",
                DEFAULT_STREAM_NUM_REPLICAS as u64,
            )?;
            if !(1..=5).contains(&num_replicas) {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "jetstream.{profile_name}.options.num_replicas must be between 1 and 5"
                )));
            }
            specs.push(JetStreamTopologySpec {
                stream_name: stream_name.to_string(),
                subjects: profile.subjects.clone(),
                max_age: Duration::from_secs(max_age_seconds),
                duplicate_window: Duration::from_secs(duplicate_window_seconds),
                num_replicas: num_replicas as usize,
                max_bytes,
            });
        }
        specs.sort_by(|a, b| a.stream_name.cmp(&b.stream_name));
        for duplicate in specs.windows(2) {
            if duplicate[0].stream_name == duplicate[1].stream_name {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "JetStream stream {} is declared by multiple profiles",
                    duplicate[0].stream_name
                )));
            }
        }
        Ok(specs)
    }

    /// 获取 Kafka 配置
    pub fn kafka_profile(&self, name: &str) -> Option<&KafkaClusterConfig> {
        self.kafka.get(name)
    }

    /// 获取 PostgreSQL 配置
    pub fn postgres_profile(&self, name: &str) -> Option<&PostgresInstanceConfig> {
        self.postgres.get(name)
    }

    /// 获取对象存储配置
    pub fn object_store_profile(&self, name: &str) -> Option<&ObjectStoreConfig> {
        self.object_storage.get(name)
    }

    /// 获取接入网关服务配置
    pub fn access_gateway_service(&self) -> AccessGatewayServiceConfig {
        self.services.access_gateway.clone().unwrap_or_default()
    }

    /// 获取 API Gateway 服务配置
    pub fn api_gateway_service(&self) -> ApiGatewayServiceConfig {
        self.services.api_gateway.clone().unwrap_or_default()
    }

    /// 获取管理网关服务配置
    pub fn admin_gateway_service(&self) -> AdminGatewayServiceConfig {
        self.services.admin_gateway.clone().unwrap_or_default()
    }

    /// 获取媒体服务配置
    pub fn media_service(&self) -> MediaServiceConfig {
        self.services.media.clone().unwrap_or_default()
    }

    /// 获取推送服务器服务配置
    pub fn push_server_service(&self) -> PushServerServiceConfig {
        self.services.push_server.clone().unwrap_or_default()
    }

    /// 获取推送代理服务配置
    pub fn push_proxy_service(&self) -> PushProxyServiceConfig {
        self.services.push_proxy.clone().unwrap_or_default()
    }

    /// 获取推送工作服务配置
    pub fn push_worker_service(&self) -> PushWorkerServiceConfig {
        self.services.push_worker.clone().unwrap_or_default()
    }

    /// 获取消息编排服务配置（主消息流 fanout、消息操作事件）
    pub fn orchestrator_service(&self) -> MessageOrchestratorServiceConfig {
        self.services
            .message_orchestrator
            .clone()
            .unwrap_or_default()
    }

    /// 获取消息摄入服务配置（上行发送、WAL、Pre/PostSend Hook）
    pub fn message_ingest_service(&self) -> MessageIngestServiceConfig {
        self.services.message_ingest.clone().unwrap_or_default()
    }

    /// 获取信令在线服务配置
    pub fn signaling_online_service(&self) -> SignalingOnlineServiceConfig {
        self.services.signaling_online.clone().unwrap_or_default()
    }

    /// 获取信令路由服务配置
    pub fn signaling_route_service(&self) -> SignalingRouteServiceConfig {
        self.services.signaling_route.clone().unwrap_or_default()
    }

    /// 获取能力服务配置
    pub fn capability_service(&self) -> CapabilityServiceConfig {
        self.services.capability.clone().unwrap_or_default()
    }

    /// 获取存储读取服务配置
    pub fn storage_reader_service(&self) -> StorageReaderServiceConfig {
        self.services.storage_reader.clone().unwrap_or_default()
    }

    /// 获取存储写入服务配置
    pub fn storage_writer_service(&self) -> StorageWriterServiceConfig {
        self.services.storage_writer.clone().unwrap_or_default()
    }

    /// 获取会话服务配置
    pub fn conversation_service(&self) -> ConversationServiceConfig {
        self.services.conversation.clone().unwrap_or_default()
    }

    /// 获取同步编排服务配置
    pub fn sync_orchestrator_service(&self) -> SyncOrchestratorServiceConfig {
        self.services.sync_orchestrator.clone().unwrap_or_default()
    }

    /// 组合服务配置
    pub fn compose_service_config(
        &self,
        runtime: &ServiceRuntimeConfig,
        fallback_name: &str,
    ) -> Config {
        let mut cfg = self.core.clone();
        cfg.service.name = runtime
            .service_name
            .as_ref()
            .cloned()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| fallback_name.to_string());

        if let Some(server) = runtime.server.as_ref() {
            if let Some(address) = server.address.as_ref() {
                cfg.server.address = address.clone();
            }
            if let Some(port) = server.port {
                cfg.server.port = port;
            }
        }

        if self.static_registry_mode {
            // A service-level registry section must not silently re-enable
            // registration in a single-Cell static deployment.
            cfg.registry = None;
        } else if let Some(registry) = runtime.registry.as_ref() {
            cfg.registry = Some(registry.clone());
        }

        cfg
    }

    /// 确保配置有默认值
    fn ensure_defaults(&mut self) {
        if self.core.server.address.is_empty() {
            self.core.server.address = "0.0.0.0".to_string();
        }
        if self.core.server.port == 0 {
            self.core.server.port = 50051;
        }
        if self.mq.default_backend.trim().is_empty() {
            self.mq.default_backend = default_mq_backend();
        }
        self.mq.default_backend = self.mq.default_backend.to_ascii_lowercase();
    }

    /// 验证配置引用
    ///
    /// 检查服务配置中引用的基础设施配置是否存在
    ///
    /// # 返回
    /// 如果所有引用都有效，返回 Ok(())，否则返回错误信息
    pub fn validate_references(&self) -> Result<()> {
        self.try_jetstream_topology()?;
        match self.mq.default_backend.as_str() {
            "nats" => {}
            "kafka" => {
                if self.kafka.is_empty() {
                    return Err(flare_server_core::error::FlareError::system(
                        "mq.default_backend is kafka but no [kafka.*] profile is configured"
                            .to_string(),
                    ));
                }
            }
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq.default_backend '{}'; expected 'nats' or 'kafka'",
                    other
                )));
            }
        }

        // 验证接入网关配置
        if let Some(cfg) = &self.services.access_gateway {
            if let Some(token_store) = &cfg.token_store {
                self.redis_profile(token_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (token_store)",
                        token_store
                    ))
                })?;
            }
            if let Some(session_store) = &cfg.session_store {
                self.redis_profile(session_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (session_store)",
                        session_store
                    ))
                })?;
            }
        }

        // 验证媒体服务配置
        if let Some(cfg) = &self.services.media {
            if let Some(metadata_store) = &cfg.metadata_store {
                self.postgres_profile(metadata_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "PostgreSQL config '{}' not found (metadata_store)",
                        metadata_store
                    ))
                })?;
            }
            if let Some(metadata_cache) = &cfg.metadata_cache {
                self.redis_profile(metadata_cache).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (metadata_cache)",
                        metadata_cache
                    ))
                })?;
            }
            if let Some(object_store) = &cfg.object_store {
                self.object_store_profile(object_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Object storage config '{}' not found (object_store)",
                        object_store
                    ))
                })?;
            }
            if let Some(upload_session_store) = &cfg.upload_session_store {
                self.redis_profile(upload_session_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (upload_session_store)",
                        upload_session_store
                    ))
                })?;
            }
        }

        if let Some(cfg) = &self.services.push_server {
            if let Some(jetstream) = &cfg.jetstream {
                self.jetstream_profile(jetstream).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "JetStream config '{}' not found (push_server)",
                        jetstream
                    ))
                })?;
            }
            if let Some(redis) = &cfg.redis {
                self.redis_profile(redis).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (push_server)",
                        redis
                    ))
                })?;
            }
        }

        if let Some(cfg) = &self.services.push_worker
            && let Some(jetstream) = &cfg.jetstream
        {
            self.jetstream_profile(jetstream).ok_or_else(|| {
                flare_server_core::error::FlareError::system(format!(
                    "JetStream config '{}' not found (push_worker)",
                    jetstream
                ))
            })?;
        }

        // 验证消息编排服务配置
        if let Some(cfg) = &self.services.message_orchestrator {
            if let Some(jetstream) = &cfg.jetstream {
                self.jetstream_profile(jetstream).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "JetStream config '{}' not found (message_orchestrator)",
                        jetstream
                    ))
                })?;
            }
            if let Some(wal_store) = &cfg.wal_store {
                self.redis_profile(wal_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (wal_store)",
                        wal_store
                    ))
                })?;
            }
            if let Some(redis_store) = &cfg.redis_store {
                self.redis_profile(redis_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (message_orchestrator redis_store)",
                        redis_store
                    ))
                })?;
            }
        }

        // 验证消息摄入服务配置
        if let Some(cfg) = &self.services.message_ingest {
            if let Some(jetstream) = &cfg.jetstream {
                self.jetstream_profile(jetstream).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "JetStream config '{}' not found (message_ingest)",
                        jetstream
                    ))
                })?;
            }
            if let Some(wal_store) = &cfg.wal_store {
                self.redis_profile(wal_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (message_ingest wal_store)",
                        wal_store
                    ))
                })?;
            }
        }

        if let Some(cfg) = &self.services.signaling_online
            && let Some(redis) = &cfg.redis
        {
            self.redis_profile(redis).ok_or_else(|| {
                flare_server_core::error::FlareError::system(format!(
                    "Redis config '{}' not found (signaling_online)",
                    redis
                ))
            })?;
        }

        // 验证存储读取服务配置
        if let Some(cfg) = &self.services.storage_reader {
            if let Some(redis) = &cfg.redis {
                self.redis_profile(redis).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (storage_reader)",
                        redis
                    ))
                })?;
            }
            if let Some(postgres) = &cfg.postgres {
                self.postgres_profile(postgres).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "PostgreSQL config '{}' not found (storage_reader)",
                        postgres
                    ))
                })?;
            }
        }

        if let Some(cfg) = &self.services.capability
            && let Some(postgres) = &cfg.postgres
        {
            self.postgres_profile(postgres).ok_or_else(|| {
                flare_server_core::error::FlareError::system(format!(
                    "PostgreSQL config '{}' not found (capability)",
                    postgres
                ))
            })?;
        }

        // 验证存储写入服务配置
        if let Some(cfg) = &self.services.storage_writer {
            if let Some(jetstream) = &cfg.jetstream {
                self.jetstream_profile(jetstream).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "JetStream config '{}' not found (storage_writer)",
                        jetstream
                    ))
                })?;
            }
            if let Some(postgres) = &cfg.postgres {
                self.postgres_profile(postgres).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "PostgreSQL config '{}' not found (storage_writer)",
                        postgres
                    ))
                })?;
            }
            if let Some(wal_store) = &cfg.wal_store {
                self.redis_profile(wal_store).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (storage_writer.wal_store)",
                        wal_store
                    ))
                })?;
            }
        }

        if let Some(cfg) = &self.services.conversation {
            if let Some(redis) = &cfg.redis {
                self.redis_profile(redis).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis config '{}' not found (conversation)",
                        redis
                    ))
                })?;
            }
            if let Some(jetstream) = &cfg.jetstream {
                self.jetstream_profile(jetstream).ok_or_else(|| {
                    flare_server_core::error::FlareError::system(format!(
                        "JetStream config '{}' not found (conversation)",
                        jetstream
                    ))
                })?;
            }
        }

        Ok(())
    }
}

/// 环境变量覆盖 `[mq]`（优先级高于 `config/environments/{FLARE_ENV}.toml` 合并结果）。
///
/// - `FLARE_MQ_DEFAULT_BACKEND`：`nats` \| `kafka`
fn apply_mq_env_overrides(cfg: &mut FlareAppConfig) {
    if let Ok(v) = env::var("FLARE_MQ_DEFAULT_BACKEND") {
        let t = v.trim().to_ascii_lowercase();
        if !t.is_empty() {
            cfg.mq.default_backend = t;
        }
    }
}

/// Select the existing configured registry or the explicit single-Cell static
/// mode. Static mode disables registration/discovery; it does not implement
/// DNS/SRV or synthesize service records.
pub(crate) fn apply_registry_mode_override(
    cfg: &mut FlareAppConfig,
    mode: Option<&str>,
) -> std::result::Result<(), &'static str> {
    match mode {
        None | Some("configured") => {
            cfg.static_registry_mode = false;
            Ok(())
        }
        Some("static") => {
            cfg.static_registry_mode = true;
            cfg.core.registry = None;
            Ok(())
        }
        Some(_) => Err("FLARE_REGISTRY_MODE must be 'configured' or 'static'"),
    }
}

/// 加载配置
///
/// # 参数
/// * `path` - 配置路径，可以是目录或文件。如果为 None，尝试加载 "config" 目录或 "config.toml" 文件
///
/// # 返回
/// 返回全局配置实例（使用 OnceLock 确保只初始化一次）
///
/// # 示例
/// ```ignore
/// // 从默认路径加载配置
/// let config = load_config(None);
///
/// // 从指定路径加载配置
/// let config = load_config(Some("config"));
/// ```
/// 计算配置候选路径。
///
/// 单独抽出来是为了能测：这里的取舍曾经导致过一次很难查的线上式故障。
pub(crate) fn config_candidates(path: Option<&str>, env_config_path: Option<&str>) -> Vec<PathBuf> {
    match path {
        None | Some("config") => env_config_path
            .into_iter()
            .map(PathBuf::from)
            .chain([
                PathBuf::from("config"),
                PathBuf::from("config.toml"),
                PathBuf::from("flare-im-core-server/config"),
            ])
            .collect(),
        Some("./config") => vec![
            PathBuf::from("./config"),
            PathBuf::from("config"),
            PathBuf::from("flare-im-core-server/config"),
        ],
        Some(p) => vec![PathBuf::from(p)],
    }
}

pub fn load_config(path: Option<&str>) -> &'static FlareAppConfig {
    // 确定配置文件候选路径：优先使用传入路径，再尝试 config / flare-im-core-server/config（便于从 workspace 根目录运行）
    //
    // 没传路径时**也要认 FLARE_CONFIG_PATH**。这里曾经只看相对 CWD 的那几个候选，
    // 于是配置有了两个入口、两种行为：
    //
    //   load_app_config_from_env()  → 读 FLARE_CONFIG_PATH（多数服务走这条）
    //   load_config(None)           → 只找 CWD 下的 config/（注册路径走这条）
    //
    // 谁先跑谁定下 APP_CONFIG（OnceLock）。容器里 CWD 是 /home/flare、没有 config/，
    // 于是先跑注册的服务全部候选落空 → load_with_fallback **静默回退到默认配置**
    // （只有一条 warn），注册中心地址变成默认的 localhost:28500。
    // 表现是 api-gateway / admin-gateway 注册失败反复重启，而同一份 base.toml 下
    // 其余 13 个服务一切正常——排查时极易误判成配置文件或网络问题。
    let env_config_path = env::var("FLARE_CONFIG_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let candidates = config_candidates(path, env_config_path.as_deref());
    // 使用 OnceLock 确保配置只初始化一次
    APP_CONFIG.get_or_init(|| {
        // 使用备选方案加载配置
        let loaded = load_with_fallback(&candidates);
        let mut cfg = loaded.config;
        // 加载环境特定配置
        if let Err(err) = manager::ConfigManager::load_environment_config_from_root(
            &mut cfg,
            loaded.source_path.as_deref(),
        ) {
            panic!("failed to load active environment config: {err}");
        }
        apply_mq_env_overrides(&mut cfg);
        cfg.ensure_defaults();
        crate::metrics::set_tenant_labels(cfg.metrics.tenant_labels.unwrap_or(true));
        match env::var("FLARE_REGISTRY_MODE") {
            Ok(mode) => apply_registry_mode_override(&mut cfg, Some(&mode))
                .expect("invalid FLARE_REGISTRY_MODE"),
            Err(env::VarError::NotPresent) => apply_registry_mode_override(&mut cfg, None)
                .expect("default registry mode must be valid"),
            Err(env::VarError::NotUnicode(_)) => {
                panic!("FLARE_REGISTRY_MODE must be valid UTF-8")
            }
        }
        if cfg.static_registry_mode {
            tracing::info!(
                "Core service registration and discovery disabled in explicit static mode"
            );
        }
        // Stream limits affect message retention and cannot use the optional
        // reference-validation fallback below.
        cfg.try_jetstream_topology()
            .expect("invalid JetStream topology configuration");
        // 验证配置引用（可选，生产环境建议启用）
        if let Err(e) = cfg.validate_references() {
            warn!("configuration reference validation failed: {}", e);
            // 注意：这里只警告，不失败，允许配置在开发环境中不完整
            // 生产环境应该确保所有引用都有效
        }
        cfg
    })
}

/// 加载并验证配置
///
/// 与 `load_config` 相同，但会严格验证配置引用，如果验证失败会返回错误
///
/// # 参数
/// * `path` - 配置路径
/// * `strict` - 是否严格验证（如果为 true，验证失败会返回错误）
///
/// # 返回
/// 成功返回配置实例，失败返回错误
pub fn load_config_with_validation(
    path: Option<&str>,
    strict: bool,
) -> Result<&'static FlareAppConfig> {
    // 加载配置
    let config = load_config(path);

    // 根据 strict 参数决定是否严格验证配置引用
    if strict {
        config
            .validate_references()
            .with_context(|| "configuration validation failed")?;
    } else if let Err(e) = config.validate_references() {
        warn!("configuration reference validation failed: {}", e);
    }

    Ok(config)
}

/// 获取应用配置
pub fn app_config() -> &'static FlareAppConfig {
    APP_CONFIG.get().expect("configuration not initialised")
}

/// 使用备选方案加载配置
///
/// 按照候选路径列表依次尝试加载配置，如果都失败则使用默认配置
fn load_with_fallback(candidates: &[PathBuf]) -> LoadedConfig {
    for path in candidates {
        match load_config_from_source(path) {
            Ok(mut cfg) => {
                cfg.ensure_defaults();
                tracing::info!(config_path = %path.display(), "loaded config from path");
                return LoadedConfig {
                    config: cfg,
                    source_path: Some(path.clone()),
                };
            }
            Err(err) => {
                warn!("failed to load config from {}: {err}", path.display());
            }
        }
    }

    warn!("no configuration source succeeded, falling back to defaults");
    LoadedConfig {
        config: default_config(),
        source_path: None,
    }
}

/// 从源加载配置
///
/// 根据路径类型（文件或目录）加载配置
pub(crate) fn load_config_from_source(path: &Path) -> Result<FlareAppConfig> {
    // 检查配置路径是否存在
    if !path.exists() {
        return Err(flare_server_core::error::FlareError::system(format!(
            "configuration path {} does not exist",
            path.display()
        )));
    }

    // 获取路径元数据
    let metadata = path
        .metadata()
        .context(format!("unable to read metadata for {}", path.display()))?;

    // 根据路径类型加载配置
    if metadata.is_dir() {
        load_config_from_directory(path)
    } else {
        load_config_from_file(path)
    }
}

/// 从文件加载配置
///
/// 读取并解析 TOML 配置文件
fn load_config_from_file(path: &Path) -> Result<FlareAppConfig> {
    let value = load_toml_value(path)?;
    let mut cfg: FlareAppConfig = value.try_into().context(format!(
        "invalid config format: {}",
        Path::new(path).display()
    ))?;
    // 确保配置有默认值
    cfg.ensure_defaults();
    Ok(cfg)
}

/// 从目录加载配置
fn load_config_from_directory(path: &Path) -> Result<FlareAppConfig> {
    let base_file = path.join("base.toml");
    if !base_file.exists() {
        return Err(flare_server_core::error::FlareError::system(format!(
            "missing base configuration: {}",
            base_file.display()
        )));
    }

    let mut merged = load_toml_value(&base_file)?;

    if !merged.is_table() {
        return Err(flare_server_core::error::FlareError::system(format!(
            "base configuration must be a table: {}",
            base_file.display()
        )));
    }

    merge_directory(&mut merged, &path.join("shared"))?;
    merge_directory(&mut merged, &path.join("services"))?;
    merge_directory(&mut merged, &path.join("overrides"))?;

    let cfg: FlareAppConfig = merged.try_into().context(format!(
        "invalid configuration after merging {}",
        path.display()
    ))?;

    Ok(cfg)
}

/// 合并目录中的配置
fn merge_directory(root: &mut Value, dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let mut entries = fs::read_dir(dir)
        .context(format!("unable to read config directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(OsStr::to_str)
                .map(|ext| ext.eq_ignore_ascii_case("toml"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let value = load_toml_value(&entry.path())?;
        merge_value(root, value);
    }

    Ok(())
}

/// 加载 TOML 值
pub(crate) fn load_toml_value(path: &Path) -> Result<Value> {
    let content = fs::read_to_string(path)
        .context(format!("unable to read config fragment {}", path.display()))?;
    let mut value: Value = toml::from_str(&content).context(format!(
        "invalid TOML content in fragment {}",
        path.display()
    ))?;
    expand_env_placeholders_in_value(&mut value, path)?;
    Ok(value)
}

pub(crate) fn expand_env_placeholders_in_value(value: &mut Value, path: &Path) -> Result<()> {
    expand_env_placeholders_in_value_with(value, path, |name| env::var(name).ok())
}

pub(crate) fn expand_env_placeholders_in_value_with<F>(
    value: &mut Value,
    path: &Path,
    mut lookup: F,
) -> Result<()>
where
    F: FnMut(&str) -> Option<String>,
{
    fn visit<F>(value: &mut Value, path: &Path, lookup: &mut F) -> Result<()>
    where
        F: FnMut(&str) -> Option<String>,
    {
        match value {
            Value::String(raw) => {
                *raw = expand_env_placeholders(raw, path, lookup)?;
            }
            Value::Array(items) => {
                for item in items {
                    visit(item, path, lookup)?;
                }
            }
            Value::Table(table) => {
                for (_, item) in table.iter_mut() {
                    visit(item, path, lookup)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    visit(value, path, &mut lookup)
}

pub(crate) fn expand_env_placeholders<F>(input: &str, path: &Path, lookup: &mut F) -> Result<String>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;

    while let Some(start_rel) = input[cursor..].find("${") {
        let start = cursor + start_rel;
        output.push_str(&input[cursor..start]);
        let name_start = start + 2;
        let Some(end_rel) = input[name_start..].find('}') else {
            return Err(flare_server_core::error::FlareError::system(format!(
                "unclosed environment placeholder in {}",
                path.display()
            )));
        };
        let end = name_start + end_rel;
        let body = &input[name_start..end];

        // 支持 `${NAME}` 与 `${NAME:-默认值}` 两种写法，语义与 shell 一致：
        // 前者「必须设置，否则报错」，后者「未设置或为空时取默认值」。
        //
        // 加默认值这一档是为了让同一份 base.toml 同时服务两种跑法：本机开发直接用
        // 默认值（localhost:25432 那套，行为与此前完全一致），容器里由 compose 注入
        // 环境变量覆盖成服务名。此前没有默认值语法，于是配置文件里根本不敢写占位符
        // ——写了本机就起不来——中间件地址只能硬编码，容器化部署也就无从谈起。
        //
        // 默认值里不能出现 `}`：占位符以第一个 `}` 结束。真需要的话在环境变量里给。
        let (name, default) = match body.find(":-") {
            Some(idx) => (&body[..idx], Some(&body[idx + 2..])),
            None => (body, None),
        };

        if !is_valid_env_placeholder_name(name) {
            return Err(flare_server_core::error::FlareError::system(format!(
                "invalid environment placeholder '${{{}}}' in {}",
                body,
                path.display()
            )));
        }

        // 取值为空串时也走默认值：compose 里未赋值的变量会展开成空串而不是「未设置」，
        // 按「未设置」处理才符合直觉，否则会得到 `postgres://:@host` 这种连不上的串。
        let resolved = lookup(name).filter(|value| !value.is_empty());
        let value = match (resolved, default) {
            (Some(value), _) => value,
            (None, Some(default)) => default.to_string(),
            (None, None) => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "environment variable {} required by {} is not set",
                    name,
                    path.display()
                )));
            }
        };
        output.push_str(&value);
        cursor = end + 1;
    }

    output.push_str(&input[cursor..]);
    Ok(output)
}

fn is_valid_env_placeholder_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// 合并值
///
/// 将覆盖值合并到基础值中
fn merge_value(base: &mut Value, overlay: Value) {
    match overlay {
        Value::Table(overlay_table) => {
            // 如果基础值也是表，则递归合并
            if let Value::Table(base_table) = base {
                for (key, overlay_value) in overlay_table.into_iter() {
                    match base_table.get_mut(&key) {
                        Some(base_value) => merge_value(base_value, overlay_value),
                        None => {
                            base_table.insert(key, overlay_value);
                        }
                    }
                }
            } else {
                // 如果基础值不是表，则直接替换
                *base = Value::Table(overlay_table);
            }
        }
        other => {
            // 对于非表值，直接替换
            *base = other;
        }
    }
}

/// 默认配置
pub(crate) fn default_config() -> FlareAppConfig {
    FlareAppConfig {
        core: Config {
            service: ServiceConfig {
                name: "flare-im-core".to_string(),
                version: "0.1.0".to_string(),
            },
            server: ServerConfig {
                address: "0.0.0.0".to_string(),
                port: 50051,
            },
            registry: Some(RegistryConfig {
                registry_type: "consul".to_string(),
                endpoints: vec!["http://localhost:28500".to_string()],
                namespace: "flare".to_string(),
                ttl: 30,
                load_balance_strategy: "consistent_hash".to_string(),
            }),
            mesh: None,
            storage: None,
        },
        static_registry_mode: false,
        logging: LoggingConfig::default(),
        metrics: MetricsPolicyConfig::default(),
        redis: HashMap::new(),
        jetstream: HashMap::new(),
        mq: MqBackendConfig::default(),
        kafka: HashMap::new(),
        postgres: HashMap::new(),
        object_storage: HashMap::new(),
        services: ServicesConfig::default(),
    }
}
