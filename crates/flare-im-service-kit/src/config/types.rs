//! 基础设施配置数据类型：连接池、消息队列、对象存储、运行时与指标策略。
//!
//! 从 mod.rs 拆出（原单文件 2,621 行）；类型定义保持原样，pub API 经 mod.rs re-export。

use std::collections::HashMap;
use std::time::Duration;

use crate::RegistryConfig;
use flare_server_core::error::Result;
use serde::Deserialize;

/// Redis 连接池配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RedisPoolConfig {
    /// Redis 服务器地址
    pub url: String,
    /// 命名空间前缀
    #[serde(default)]
    pub namespace: Option<String>,
    /// 数据库编号
    #[serde(default)]
    pub database: Option<u32>,
    /// 过期时间（秒）
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// NATS JetStream 集群配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct JetStreamClusterConfig {
    /// NATS server URL
    pub url: String,
    /// 客户端标识
    #[serde(default)]
    pub client_id: Option<String>,
    /// 超时时间（毫秒）
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// 重试次数
    #[serde(default)]
    pub retries: Option<u32>,
    /// 重试退避（毫秒）
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    /// JetStream stream 名称
    #[serde(default)]
    pub stream_name: Option<String>,
    /// Stream 绑定的 subjects
    #[serde(default)]
    pub subjects: Vec<String>,
    /// 其他选项
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JetStreamTopologySpec {
    pub stream_name: String,
    pub subjects: Vec<String>,
    pub max_age: Duration,
    pub duplicate_window: Duration,
    pub num_replicas: usize,
    pub max_bytes: i64,
}

// Keep these fallback values in sync with flare-server-core's NatsStreamSpec::new.
// The checked-in base.toml states all four limits explicitly, so production does not
// depend on these compatibility defaults.
pub(crate) const DEFAULT_STREAM_MAX_AGE_SECONDS: u64 = 7 * 24 * 3600;
pub(crate) const DEFAULT_STREAM_DUPLICATE_WINDOW_SECONDS: u64 = 10 * 60;
pub(crate) const DEFAULT_STREAM_MAX_BYTES: i64 = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_STREAM_NUM_REPLICAS: usize = 1;

pub(crate) fn positive_stream_option(
    profile_name: &str,
    options: &HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<u64> {
    let Some(raw) = options.get(key) else {
        return Ok(default);
    };
    let value = raw.parse::<u64>().map_err(|_| {
        flare_server_core::error::FlareError::system(format!(
            "jetstream.{profile_name}.options.{key} must be a positive integer"
        ))
    })?;
    if value == 0 {
        return Err(flare_server_core::error::FlareError::system(format!(
            "jetstream.{profile_name}.options.{key} must be a positive integer"
        )));
    }
    Ok(value)
}

/// MQ 后端选择（`[mq]`）。
///
/// 手动指定方式（优先级从高到低）：
/// 1. 环境变量 `FLARE_MQ_DEFAULT_BACKEND`（`nats` 或 `kafka`）
/// 2. `config/environments/{FLARE_ENV}.toml` 根级 `[mq]`（由 [`ConfigManager::load_environment_config`] 合并）
/// 3. `config/base.toml` 中的 `[mq]`
///
/// `nats` 表示 NATS JetStream。生产运行时 Kafka 与 JetStream 二选一。
#[derive(Debug, Clone, Deserialize)]
pub struct MqBackendConfig {
    /// 当前选择的 MQ 后端（已小写、去首尾空格，见 [`Self::ensure_defaults`]）。
    #[serde(default = "default_mq_backend")]
    pub default_backend: String,
}

impl Default for MqBackendConfig {
    fn default() -> Self {
        Self {
            default_backend: default_mq_backend(),
        }
    }
}

pub(crate) fn default_mq_backend() -> String {
    "nats".to_string()
}

/// Kafka 集群配置。仅在 `[mq].default_backend = "kafka"` 时作为主 MQ 后端使用。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct KafkaClusterConfig {
    pub brokers: Vec<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub acks: Option<String>,
    #[serde(default)]
    pub compression: Option<String>,
    #[serde(default)]
    pub linger_ms: Option<u64>,
    #[serde(default)]
    pub batch_size_bytes: Option<usize>,
    #[serde(default)]
    pub message_timeout_ms: Option<u64>,
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    #[serde(default)]
    pub enable_idempotence: Option<bool>,
    #[serde(default)]
    pub max_in_flight_requests_per_connection: Option<u32>,
    #[serde(default)]
    pub replication_factor: Option<i16>,
    #[serde(default)]
    pub min_insync_replicas: Option<i16>,
    #[serde(default)]
    pub partitions: Option<i32>,
    #[serde(default)]
    pub retention_ms: Option<i64>,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// PostgreSQL 数据库实例配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PostgresInstanceConfig {
    /// 数据库连接 URL
    pub url: String,
    /// 最大连接数
    #[serde(default)]
    pub max_connections: Option<u32>,
    /// 最小连接数
    #[serde(default)]
    pub min_connections: Option<u32>,
    /// 获取连接超时（秒）
    #[serde(default)]
    pub acquire_timeout_seconds: Option<u64>,
    /// 空闲连接超时（秒）
    #[serde(default)]
    pub idle_timeout_seconds: Option<u64>,
    /// 连接最大生命周期（秒）
    #[serde(default)]
    pub max_lifetime_seconds: Option<u64>,
}

impl PostgresInstanceConfig {
    pub fn max_connections_or(&self, default: u32) -> u32 {
        self.max_connections.unwrap_or(default).max(1)
    }

    pub fn min_connections_or(&self, default: u32) -> u32 {
        self.min_connections
            .unwrap_or(default)
            .min(self.max_connections_or(default.max(1)))
    }

    pub fn acquire_timeout_or(&self, default_seconds: u64) -> Duration {
        Duration::from_secs(
            self.acquire_timeout_seconds
                .unwrap_or(default_seconds)
                .max(1),
        )
    }

    pub fn idle_timeout_or(&self, default_seconds: u64) -> Duration {
        Duration::from_secs(self.idle_timeout_seconds.unwrap_or(default_seconds).max(1))
    }

    pub fn max_lifetime_or(&self, default_seconds: u64) -> Duration {
        Duration::from_secs(self.max_lifetime_seconds.unwrap_or(default_seconds).max(1))
    }
}

/// 对象存储配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ObjectStoreConfig {
    /// 存储类型（如 minio, s3, oss 等）
    pub profile_type: String,
    /// 存储服务端点（服务自身访问对象存储用，通常是内网地址）
    #[serde(default)]
    pub endpoint: Option<String>,
    /// 对外端点：只用于生成预签名 URL，服务自身从不连接它。
    ///
    /// 反代部署下这两个地址必然不同：浏览器要拿到公网可达的 URL，而服务自己
    /// 走内网更短也更可靠。此前两者共用 `endpoint`，于是只能二选一——
    /// 填内网地址浏览器直传失败（127.0.0.1 连不上），填公网地址则服务自身
    /// 启动时的桶检查要走公网 TLS，自签证书下直接起不来
    /// （aws-sdk-s3 的 default-https-client 用编译进二进制的 webpki 根证书，
    /// 挂 CA、设 SSL_CERT_FILE/AWS_CA_BUNDLE 都没用）。
    ///
    /// 分开是安全的：SigV4 签的是 method、路径、query 和 host 等头部，
    /// **不含 scheme**，而预签名是纯离线计算，不需要连通性。
    #[serde(default)]
    pub public_endpoint: Option<String>,
    /// 访问密钥
    #[serde(default)]
    pub access_key: Option<String>,
    /// 秘密密钥
    #[serde(default)]
    pub secret_key: Option<String>,
    /// 存储桶名称
    #[serde(default)]
    pub bucket: Option<String>,
    /// 区域
    #[serde(default)]
    pub region: Option<String>,
    /// 是否使用 SSL
    #[serde(default)]
    pub use_ssl: Option<bool>,
    /// CDN 基础 URL
    #[serde(default)]
    pub cdn_base_url: Option<String>,
    /// 上传路径前缀
    #[serde(default)]
    pub upload_prefix: Option<String>,
    /// 预签名URL过期时间（秒）
    #[serde(default)]
    pub presign_url_ttl_seconds: Option<u64>,
    /// 是否优先使用预签名 URL
    #[serde(default)]
    pub use_presign: Option<bool>,
    /// 桶内统一的根路径前缀（支持多租户或业务隔离）
    #[serde(default)]
    pub bucket_root_prefix: Option<String>,
    /// 是否强制使用 path-style 访问（默认非 AWS 端点自动启用）
    #[serde(default)]
    pub force_path_style: Option<bool>,
}

/// 服务端点配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServiceEndpointConfig {
    /// 服务地址
    pub address: Option<String>,
    /// 服务端口
    pub port: Option<u16>,
}

/// 服务运行时配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServiceRuntimeConfig {
    /// 服务名称
    #[serde(default)]
    pub service_name: Option<String>,
    /// 服务器配置
    #[serde(default)]
    pub server: Option<ServiceEndpointConfig>,
    /// 注册中心配置
    #[serde(default)]
    pub registry: Option<RegistryConfig>,
    /// Prometheus `/metrics` 出口：`[services.<name>.metrics]`。
    ///
    /// 由共享运行时统一挂载；已有专属 `metrics_*` 字段的服务（ingest、orchestrator、
    /// storage-writer、push-worker、signaling-gateway）不使用此段，避免同一进程重复监听。
    #[serde(default)]
    pub metrics: Option<ServiceMetricsSection>,
}

/// 服务级 Prometheus 出口配置段。
///
/// 段存在即默认启用；启用时必须给出端口，不能靠默认值与其他服务撞端口。
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct ServiceMetricsSection {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub path: Option<String>,
}

/// 进程级指标策略：`[metrics]`。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsPolicyConfig {
    /// 是否把真实租户写进 `tenant_id` 标签（设计 §13.2）：私有化默认开，云部署关闭。
    #[serde(default)]
    pub tenant_labels: Option<bool>,
}
