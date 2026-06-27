//! Flare IM Core 配置模块
//!
//! 该模块提供了完整的应用程序配置管理功能，包括：
//! - 配置文件加载和解析
//! - 环境特定配置覆盖
//! - 各种服务配置定义
//! - 对象存储、数据库、消息队列等基础设施配置

// 首先导入需要的模块和类型
use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{Config, RegistryConfig, ServerConfig, ServiceConfig};
use flare_server_core::error::{AnyhowContext, Result};
use serde::Deserialize;
use std::sync::OnceLock;
use toml::Value;
use tracing::warn;

pub use flare_server_core::auth::{
    AuthProviderConfig, TrustedIssuerConfig as TrustedTokenIssuerConfig,
};

// 导入配置管理器模块
mod manager;
pub use manager::ConfigManager;

/// 全局应用配置实例，使用 OnceLock 确保只初始化一次
static APP_CONFIG: OnceLock<FlareAppConfig> = OnceLock::new();

struct LoadedConfig {
    config: FlareAppConfig,
    source_path: Option<PathBuf>,
}

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

fn default_mq_backend() -> String {
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
    /// 存储服务端点
    #[serde(default)]
    pub endpoint: Option<String>,
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
}

/// 接入网关服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AccessGatewayServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// 信令服务名（通过服务发现获取地址）
    #[serde(default)]
    pub signaling_service: Option<String>,
    /// 消息编排服务名（通过服务发现获取地址）
    #[serde(default)]
    pub message_service: Option<String>,
    /// 推送服务名（通过服务发现获取地址）
    #[serde(default)]
    pub push_service: Option<String>,
    /// Route 服务名（通过服务发现获取地址，用于 SVID 路由）
    #[serde(default)]
    pub route_service: Option<String>,
    /// 默认 SVID（Service ID），默认 "svid.im"
    #[serde(default)]
    pub default_svid: Option<String>,
    /// 是否使用 Route 服务进行消息路由（默认 true）
    #[serde(default = "default_true")]
    pub use_route_service: bool,
    /// 长连接鉴权 provider 配置
    #[serde(default)]
    pub auth: AuthProviderConfig,
    /// 令牌密钥
    #[serde(default)]
    pub token_secret: Option<String>,
    /// 令牌发行方
    #[serde(default)]
    pub token_issuer: Option<String>,
    /// 令牌过期时间（秒）
    #[serde(default)]
    pub token_ttl_seconds: Option<u64>,
    /// 额外信任的 JWT 发行方（如 Social 登录 token，用于长连接鉴权）
    #[serde(default)]
    pub trusted_token_issuers: Vec<TrustedTokenIssuerConfig>,
    /// 令牌存储
    #[serde(default)]
    pub token_store: Option<String>,
    /// 会话存储
    #[serde(default)]
    pub session_store: Option<String>,
    /// 会话存储过期时间（秒）
    #[serde(default)]
    pub session_store_ttl_seconds: Option<u64>,
    /// 网关ID（用于跨地区路由，如果不设置则自动生成）
    #[serde(default)]
    pub gateway_id: Option<String>,
    /// 网关所在地区（用于跨地区路由）
    #[serde(default)]
    pub region: Option<String>,
    /// 压缩算法（none/gzip/zstd，默认 none）
    #[serde(default)]
    pub compression_algorithm: Option<String>,
    /// 是否启用加密（默认 false）
    #[serde(default)]
    pub enable_encryption: Option<bool>,
    /// 加密密钥（32字节，hex编码或直接字符串，如果启用加密但未设置则使用默认密钥）
    #[serde(default)]
    pub encryption_key: Option<String>,
    /// 同步拉取限流开关（tenant + user 双令牌桶）
    #[serde(default)]
    pub sync_pull_rate_limit_enabled: Option<bool>,
    /// 单用户同步拉取令牌补充速率（requests/second）
    #[serde(default)]
    pub sync_pull_user_requests_per_second: Option<u32>,
    /// 单用户同步拉取突发容量
    #[serde(default)]
    pub sync_pull_user_burst: Option<u32>,
    /// 单租户同步拉取令牌补充速率（requests/second）
    #[serde(default)]
    pub sync_pull_tenant_requests_per_second: Option<u32>,
    /// 单租户同步拉取突发容量
    #[serde(default)]
    pub sync_pull_tenant_burst: Option<u32>,
}

/// API Gateway 服务配置（业务系统和三方 HTTP facade）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ApiGatewayServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// 信令服务名（通过服务发现获取地址）
    #[serde(default)]
    pub signaling_service: Option<String>,
    /// 消息编排服务名（通过服务发现获取地址）
    #[serde(default)]
    pub message_service: Option<String>,
    /// 推送服务名（通过服务发现获取地址）
    #[serde(default)]
    pub push_service: Option<String>,
    /// 存储读取服务名（通过服务发现获取地址）
    #[serde(default)]
    pub storage_service: Option<String>,
    /// 媒体服务名（通过服务发现获取地址）
    #[serde(default)]
    pub media_service: Option<String>,
    /// Capability 服务名（Hook 扩展 + 插件，通过服务发现获取地址）
    #[serde(default)]
    pub capability_service: Option<String>,
    /// 信令路由服务名（通过服务发现获取地址）
    #[serde(default)]
    pub route_service: Option<String>,
    /// 是否使用信令路由服务（默认不使用，向后兼容）
    #[serde(default)]
    pub use_route_service: Option<bool>,
    /// 默认 SVID (Service ID)，用于通过 Route 服务转发消息
    #[serde(default)]
    pub default_svid: Option<String>,
    /// JWT Token 密钥
    #[serde(default)]
    pub token_secret: Option<String>,
    /// JWT Token 发行方
    #[serde(default)]
    pub token_issuer: Option<String>,
    /// JWT Token 过期时间（秒）
    #[serde(default)]
    pub token_ttl_seconds: Option<u64>,
    /// 额外信任的 JWT 发行方（如业务系统登录 token，用于 API Gateway 鉴权）
    #[serde(default)]
    pub trusted_token_issuers: Vec<TrustedTokenIssuerConfig>,
}

/// 管理网关服务配置（内网管理 API 入口）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AdminGatewayServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JWT Token 密钥。未配置时可由启动入口回退到 api_gateway 配置。
    #[serde(default)]
    pub token_secret: Option<String>,
    /// JWT Token 发行方。未配置时可由启动入口回退到 api_gateway 配置。
    #[serde(default)]
    pub token_issuer: Option<String>,
    /// JWT Token 过期时间（秒）
    #[serde(default)]
    pub token_ttl_seconds: Option<u64>,
    /// 额外信任的 JWT 发行方（如业务管理后台 token）
    #[serde(default)]
    pub trusted_token_issuers: Vec<TrustedTokenIssuerConfig>,
}

/// 媒体服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MediaServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// 元数据存储
    #[serde(default)]
    pub metadata_store: Option<String>,
    /// 元数据缓存
    #[serde(default)]
    pub metadata_cache: Option<String>,
    /// 对象存储配置
    #[serde(default)]
    pub object_store: Option<String>,
    /// Redis 过期时间（秒）
    #[serde(default)]
    pub redis_ttl_seconds: Option<i64>,
    /// 本地存储目录
    #[serde(default)]
    pub local_storage_dir: Option<String>,
    /// 本地基础 URL
    #[serde(default)]
    pub local_base_url: Option<String>,
    /// CDN 基础 URL
    #[serde(default)]
    pub cdn_base_url: Option<String>,
    /// 孤立文件宽限时间（秒）
    #[serde(default)]
    pub orphan_grace_seconds: Option<i64>,
    /// 上传会话存储
    #[serde(default)]
    pub upload_session_store: Option<String>,
    /// 分块上传目录
    #[serde(default)]
    pub chunk_upload_dir: Option<String>,
    /// 分块过期时间（秒）
    #[serde(default)]
    pub chunk_ttl_seconds: Option<i64>,
    /// 最大分块大小（字节）
    #[serde(default)]
    pub max_chunk_size_bytes: Option<i64>,
}

/// 推送服务器服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PushServerServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JetStream 配置
    #[serde(default)]
    pub jetstream: Option<String>,
    /// 消费者组
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// 临时消息推送入口主题
    #[serde(default)]
    pub push_message_topic: Option<String>,
    /// 临时事件推送入口主题
    #[serde(default)]
    pub push_event_topic: Option<String>,
    /// ACK/通知/自定义统一推送入口主题
    #[serde(default)]
    pub push_envelope_topic: Option<String>,
    /// 在线推送任务主题
    #[serde(default)]
    pub push_online_topic: Option<String>,
    /// 离线推送任务主题
    #[serde(default)]
    pub push_offline_topic: Option<String>,
    /// 推送死信主题
    #[serde(default)]
    pub push_dlq_topic: Option<String>,
    /// ConversationReadService gRPC endpoint，用于大群 pure ping 按页解析成员。
    #[serde(default)]
    pub conversation_read_endpoint: Option<String>,
    /// 大群 pure ping 解析成员的分页大小。
    #[serde(default)]
    pub event_ping_participant_page_size: Option<i32>,
    /// 大群 pure ping 在 Push Server 解析成员前按会话合并的窗口（毫秒），0 表示关闭。
    #[serde(default)]
    pub event_ping_coalesce_window_ms: Option<u64>,
    /// Redis 配置
    #[serde(default)]
    pub redis: Option<String>,
    /// 在线状态查询后端：redis 或 grpc。
    #[serde(default)]
    pub online_status_backend: Option<String>,
    /// 在线状态过期时间（秒）
    #[serde(default)]
    pub online_ttl_seconds: Option<u64>,
    /// 默认租户 ID
    #[serde(default)]
    pub default_tenant_id: Option<String>,
    /// Hook 配置
    #[serde(default)]
    pub hook_config: Option<String>,
    /// Hook 配置目录
    #[serde(default)]
    pub hook_config_dir: Option<String>,
    /// ACK 服务配置（从业务模块配置中读取，不再使用独立的 ack.yaml）
    #[serde(default)]
    pub ack: Option<AckServiceConfigSection>,
}

/// 推送代理服务配置（PushService gRPC 入队边界）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PushProxyServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
}

/// ACK 服务配置段（集成到业务模块配置中）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AckServiceConfigSection {
    /// Redis 默认过期时间（秒）
    #[serde(default = "default_ack_redis_ttl")]
    pub redis_ttl: u64,
    /// 内存缓存容量
    #[serde(default = "default_ack_cache_capacity")]
    pub cache_capacity: usize,
    /// 批量处理间隔（毫秒）
    #[serde(default = "default_ack_batch_interval_ms")]
    pub batch_interval_ms: u64,
    /// 批量处理大小
    #[serde(default = "default_ack_batch_size")]
    pub batch_size: usize,
}

fn default_ack_redis_ttl() -> u64 {
    3600
}
fn default_ack_cache_capacity() -> usize {
    10000
}
fn default_ack_batch_interval_ms() -> u64 {
    100
}
fn default_ack_batch_size() -> usize {
    100
}

/// 推送工作服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PushWorkerServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JetStream 配置
    #[serde(default)]
    pub jetstream: Option<String>,
    /// 消费者组
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// 在线推送任务主题
    #[serde(default)]
    pub push_online_topic: Option<String>,
    /// 离线推送任务主题
    #[serde(default)]
    pub push_offline_topic: Option<String>,
    /// 推送死信主题
    #[serde(default)]
    pub push_dlq_topic: Option<String>,
    /// 信令端点
    #[serde(default)]
    pub signaling_endpoint: Option<String>,
    /// 离线提供者
    #[serde(default)]
    pub offline_provider: Option<String>,
    /// 未配置离线推送提供者时的有界本地 parking 容量
    #[serde(default)]
    pub offline_parking_capacity: Option<usize>,
    /// 事件 ping 防抖窗口（毫秒），0 表示关闭
    #[serde(default)]
    pub event_ping_debounce_window_ms: Option<u64>,
    /// Prometheus 指标出口开关
    #[serde(default)]
    pub metrics_enabled: Option<bool>,
    /// Prometheus 指标监听地址
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Prometheus 指标监听端口
    #[serde(default)]
    pub metrics_port: Option<u16>,
    /// Prometheus 指标路径
    #[serde(default)]
    pub metrics_path: Option<String>,
    /// Hook 配置
    #[serde(default)]
    pub hook_config: Option<String>,
    /// Hook 配置目录
    #[serde(default)]
    pub hook_config_dir: Option<String>,
}

/// 消息编排服务配置（主消息流 fanout、消息操作事件）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MessageOrchestratorServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JetStream 配置
    #[serde(default)]
    pub jetstream: Option<String>,
    /// 消息流 subject（新消息落库，与 constants::topics::TOPIC_MESSAGE_STORAGE 对齐）
    #[serde(default)]
    pub storage_subject: Option<String>,
    /// 操作事件流 subject（与 constants::topics::TOPIC_MESSAGE_EVENTS 对齐）
    #[serde(default)]
    pub operation_subject: Option<String>,
    /// 推送流 subject（与 constants::topics::TOPIC_PUSH_MESSAGES 对齐）
    #[serde(default)]
    pub push_subject: Option<String>,
    /// 消息主链路 DLQ topic / subject
    #[serde(default)]
    pub message_dlq_topic: Option<String>,
    /// 消息主链路 retry topic / subject
    #[serde(default)]
    pub message_retry_topic: Option<String>,
    /// retry topic 转发回原 topic 前的延迟毫秒
    #[serde(default)]
    pub message_retry_delay_ms: Option<u64>,
    /// Redis 配置 profile（用于 seq 分配等非 WAL 运行时状态）
    #[serde(default)]
    pub redis_store: Option<String>,
    /// WAL 存储
    #[serde(default)]
    pub wal_store: Option<String>,
    /// WAL 哈希键
    #[serde(default)]
    pub wal_hash_key: Option<String>,
    /// WAL 过期时间（秒）
    #[serde(default)]
    pub wal_ttl_seconds: Option<u64>,
    /// 是否启用 WAL 后台回放
    #[serde(default)]
    pub wal_replay_enabled: Option<bool>,
    /// WAL 后台回放巡检间隔（毫秒）
    #[serde(default)]
    pub wal_replay_interval_ms: Option<u64>,
    /// WAL 后台回放错误退避（毫秒）
    #[serde(default)]
    pub wal_replay_error_backoff_ms: Option<u64>,
    /// WAL 后台回放单批上限
    #[serde(default)]
    pub wal_replay_batch_limit: Option<usize>,
    /// WAL 后台回放 claim 租约时间（毫秒）
    #[serde(default)]
    pub wal_replay_claim_lease_ms: Option<u64>,
    /// Hook 配置
    #[serde(default)]
    pub hook_config: Option<String>,
    /// Hook 配置目录
    #[serde(default)]
    pub hook_config_dir: Option<String>,
    /// Conversation 服务类型（用于自动创建 conversation，如果配置了 registry，会自动发现）
    #[serde(default)]
    pub conversation_service_type: Option<String>,
    /// 每个用户 sync 变更索引保留的最大版本条数。
    #[serde(default)]
    pub user_sync_index_max_changes_per_user: Option<usize>,
    /// 用户 sync 变更索引与会话状态缓存 TTL（秒）；user_version 本身不设置 TTL。
    #[serde(default)]
    pub user_sync_index_ttl_seconds: Option<u64>,
    /// 超过该收件人数的持久消息推送切换为 notify+pull ping；0 表示关闭。
    #[serde(default)]
    pub large_conversation_push_threshold: Option<usize>,
    /// 持久消息是否启用 inline event 推送；关闭后全量退化为 notify+pull ping。
    #[serde(default)]
    pub inline_message_push_enabled: Option<bool>,
    /// 高频单聊 conversation ensure 缓存容量
    #[serde(default)]
    pub conversation_ensure_cache_capacity: Option<u64>,
    /// 高频单聊 conversation ensure 缓存 TTL（秒）
    #[serde(default)]
    pub conversation_ensure_cache_ttl_seconds: Option<u64>,
    /// Prometheus 指标出口开关
    #[serde(default)]
    pub metrics_enabled: Option<bool>,
    /// Prometheus 指标监听地址
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Prometheus 指标监听端口
    #[serde(default)]
    pub metrics_port: Option<u16>,
    /// Prometheus 指标路径
    #[serde(default)]
    pub metrics_path: Option<String>,
}

/// 消息摄入服务配置。
///
/// 当前复用消息编排配置结构中的运行时/MQ/WAL/Hook 字段，但配置键独立为
/// `[services.message_ingest]`，避免摄入服务与 fanout 编排服务共享运行时参数。
pub type MessageIngestServiceConfig = MessageOrchestratorServiceConfig;

/// 信令在线服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SignalingOnlineServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// Redis 配置
    #[serde(default)]
    pub redis: Option<String>,
    /// 在线状态过期时间（秒）
    #[serde(default)]
    pub online_ttl_seconds: Option<u64>,
    /// 在线状态前缀
    #[serde(default)]
    pub presence_prefix: Option<String>,
}

/// 信令路由服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SignalingRouteServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// 默认业务服务端点（可选，通过环境变量配置）
    #[serde(default)]
    pub default_services: Option<Vec<(String, String)>>,
}

/// 能力服务（Hook + Capability gRPC）配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct CapabilityServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// PostgreSQL profile（`base.toml` 中 `[postgres.*]`），Hook 配置与能力策略库；可被 `DATABASE_URL` 覆盖
    #[serde(default)]
    pub postgres: Option<String>,
}

/// 存储读取服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageReaderServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// Redis 配置（可选，用于缓存）
    #[serde(default)]
    pub redis: Option<String>,
    /// PostgreSQL profile（`base.toml` 中 `[postgres.*]`），与 `services/storage-reader.toml` 中 `postgres = "media"` 等对齐；可被 `STORAGE_POSTGRES_URL` / `POSTGRES_URL` 覆盖
    #[serde(default)]
    pub postgres: Option<String>,
    /// 默认分页大小
    #[serde(default)]
    pub default_page_size: Option<u32>,
    /// 最大分页大小
    #[serde(default)]
    pub max_page_size: Option<u32>,
}

/// 存储写入服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageWriterServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JetStream 配置
    #[serde(default)]
    pub jetstream: Option<String>,
    /// 消费者组
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// 消息流 subject（新消息落库，与 constants::topics::TOPIC_MESSAGE_STORAGE 对齐）
    #[serde(default)]
    pub message_subject: Option<String>,
    /// 操作事件流 subject（Recall/Edit/Read 等，与 constants::topics::TOPIC_MESSAGE_EVENTS 对齐）
    #[serde(default)]
    pub operation_subject: Option<String>,
    /// 消息写入链路 DLQ topic / subject
    #[serde(default)]
    pub message_dlq_topic: Option<String>,
    /// 消息写入链路 retry topic / subject
    #[serde(default)]
    pub message_retry_topic: Option<String>,
    /// retry topic 转发回原 topic 前的延迟毫秒
    #[serde(default)]
    pub message_retry_delay_ms: Option<u64>,
    /// PostgreSQL 配置（可选，用于归档）
    #[serde(default)]
    pub postgres: Option<String>,
    /// WAL 存储
    #[serde(default)]
    pub wal_store: Option<String>,
    /// WAL 哈希键
    #[serde(default)]
    pub wal_hash_key: Option<String>,
    /// WAL 过期时间（秒）
    #[serde(default)]
    pub wal_ttl_seconds: Option<u64>,
    /// Redis 会话尾部热缓存保留消息条数。
    #[serde(default)]
    pub redis_hot_tail_limit: Option<usize>,
    /// 批量大小
    #[serde(default)]
    pub batch_size: Option<u32>,
    /// 批量间隔（毫秒）
    #[serde(default)]
    pub batch_interval_ms: Option<u64>,
    /// Prometheus 指标出口开关
    #[serde(default)]
    pub metrics_enabled: Option<bool>,
    /// Prometheus 指标监听地址
    #[serde(default)]
    pub metrics_address: Option<String>,
    /// Prometheus 指标监听端口
    #[serde(default)]
    pub metrics_port: Option<u16>,
    /// Prometheus 指标路径
    #[serde(default)]
    pub metrics_path: Option<String>,
}

/// 会话策略配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SessionPolicyConfig {
    /// 冲突解决策略
    #[serde(default)]
    pub conflict_resolution: Option<String>,
    /// 最大设备数
    #[serde(default)]
    pub max_devices: Option<i32>,
    /// 是否允许匿名用户
    #[serde(default)]
    pub allow_anonymous: Option<bool>,
    /// 是否允许历史同步
    #[serde(default)]
    pub allow_history_sync: Option<bool>,
}

/// 会话服务配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConversationServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// JetStream 配置（配置则启用 ReadReceipt 消费者，未读数零成本）
    #[serde(default)]
    pub jetstream: Option<String>,
    /// 操作事件流 subject（与 constants::topics::TOPIC_MESSAGE_EVENTS 对齐，仅消费 ReadReceipt）
    #[serde(default)]
    pub operation_subject: Option<String>,
    /// ReadReceipt 消费者 group（与 constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT 对齐）
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// Redis 配置
    #[serde(default)]
    pub redis: Option<String>,
    /// PostgreSQL 配置（可选，用于会话元数据存储）
    #[serde(default)]
    pub postgres: Option<String>,
    /// 会话状态前缀
    #[serde(default)]
    pub conversation_state_prefix: Option<String>,
    /// 会话未读前缀
    #[serde(default)]
    pub conversation_unread_prefix: Option<String>,
    /// 用户游标前缀
    #[serde(default)]
    pub user_cursor_prefix: Option<String>,
    /// 在线状态前缀
    #[serde(default)]
    pub presence_prefix: Option<String>,
    /// 大群精确未读写扩散阈值；成员数超过阈值时未读计数近似化，0 表示始终精确。
    #[serde(default)]
    pub large_conversation_precise_unread_threshold: Option<i32>,
    /// 存储读取服务名（通过服务发现获取地址，可选）
    #[serde(default)]
    pub storage_reader_service: Option<String>,
    /// 最近消息限制
    #[serde(default)]
    pub recent_message_limit: Option<i32>,
    /// 默认策略配置
    #[serde(default)]
    pub default_policy: Option<SessionPolicyConfig>,
}

/// 同步编排服务配置（统一 SyncService 入口）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SyncOrchestratorServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
}

/// 日志配置
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LoggingConfig {
    /// 日志级别：trace, debug, info, warn, error
    /// 默认：debug（开发环境推荐，生产环境建议使用 info）
    /// 可以通过环境变量 RUST_LOG 覆盖，例如：RUST_LOG=info cargo run
    #[serde(default = "default_log_level")]
    pub level: String,
    /// 是否显示目标模块名称
    #[serde(default = "default_false")]
    pub with_target: bool,
    /// 是否显示线程ID
    #[serde(default = "default_true")]
    pub with_thread_ids: bool,
    /// 是否显示文件名
    #[serde(default = "default_true")]
    pub with_file: bool,
    /// 是否显示行号
    #[serde(default = "default_true")]
    pub with_line_number: bool,
    /// 是否使用 ANSI 颜色。默认 None 表示自动检测（输出到 TTY 时开启，重定向到文件时关闭，便于阅读日志文件）
    #[serde(default)]
    pub with_ansi: Option<bool>,
}

fn default_log_level() -> String {
    "debug".to_string()
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

/// Flare 应用配置主结构体
#[derive(Debug, Clone, Deserialize)]
pub struct FlareAppConfig {
    /// 核心配置
    #[serde(flatten)]
    pub core: Config,
    /// 日志配置
    #[serde(default)]
    pub logging: LoggingConfig,
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

    /// 获取 NATS JetStream 拓扑配置。stream/subject 由用户在 `[jetstream.*]` 中指定。
    pub fn jetstream_topology(&self) -> Vec<JetStreamTopologySpec> {
        let mut specs = self
            .jetstream
            .values()
            .filter_map(|profile| {
                let stream_name = profile
                    .stream_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())?;
                if profile.subjects.is_empty() {
                    return None;
                }
                Some(JetStreamTopologySpec {
                    stream_name: stream_name.to_string(),
                    subjects: profile.subjects.clone(),
                })
            })
            .collect::<Vec<_>>();
        specs.sort_by(|a, b| a.stream_name.cmp(&b.stream_name));
        specs.dedup_by(|a, b| a.stream_name == b.stream_name);
        specs
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

        if let Some(registry) = runtime.registry.as_ref() {
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
pub fn load_config(path: Option<&str>) -> &'static FlareAppConfig {
    // 确定配置文件候选路径：优先使用传入路径，再尝试 config / flare-im-core/config（便于从 workspace 根目录运行）
    let candidates: Vec<PathBuf> = match path {
        None | Some("config") => vec![
            PathBuf::from("config"),
            PathBuf::from("config.toml"),
            PathBuf::from("flare-im-core/config"),
        ],
        Some("./config") => vec![
            PathBuf::from("./config"),
            PathBuf::from("config"),
            PathBuf::from("flare-im-core/config"),
        ],
        Some(p) => vec![PathBuf::from(p)],
    };

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
fn load_config_from_source(path: &Path) -> Result<FlareAppConfig> {
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
fn load_toml_value(path: &Path) -> Result<Value> {
    let content = fs::read_to_string(path)
        .context(format!("unable to read config fragment {}", path.display()))?;
    let mut value: Value = toml::from_str(&content).context(format!(
        "invalid TOML content in fragment {}",
        path.display()
    ))?;
    expand_env_placeholders_in_value(&mut value, path)?;
    Ok(value)
}

fn expand_env_placeholders_in_value(value: &mut Value, path: &Path) -> Result<()> {
    expand_env_placeholders_in_value_with(value, path, |name| env::var(name).ok())
}

fn expand_env_placeholders_in_value_with<F>(
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

fn expand_env_placeholders<F>(input: &str, path: &Path, lookup: &mut F) -> Result<String>
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
        let name = &input[name_start..end];
        if !is_valid_env_placeholder_name(name) {
            return Err(flare_server_core::error::FlareError::system(format!(
                "invalid environment placeholder '${{{}}}' in {}",
                name,
                path.display()
            )));
        }
        let value = lookup(name).ok_or_else(|| {
            flare_server_core::error::FlareError::system(format!(
                "environment variable {} required by {} is not set",
                name,
                path.display()
            ))
        })?;
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
fn default_config() -> FlareAppConfig {
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
        logging: LoggingConfig::default(),
        redis: HashMap::new(),
        jetstream: HashMap::new(),
        mq: MqBackendConfig::default(),
        kafka: HashMap::new(),
        postgres: HashMap::new(),
        object_storage: HashMap::new(),
        services: ServicesConfig::default(),
    }
}

/// 服务配置集合
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServicesConfig {
    /// 接入网关服务配置
    #[serde(default, rename = "access_gateway")]
    pub access_gateway: Option<AccessGatewayServiceConfig>,
    /// API Gateway 服务配置（业务系统和三方 HTTP facade）
    #[serde(default, rename = "api_gateway")]
    pub api_gateway: Option<ApiGatewayServiceConfig>,
    /// 管理网关服务配置（内网管理 API 入口）
    #[serde(default, rename = "admin_gateway")]
    pub admin_gateway: Option<AdminGatewayServiceConfig>,
    /// 媒体服务配置
    #[serde(default, rename = "media")]
    pub media: Option<MediaServiceConfig>,
    /// 推送服务器服务配置
    #[serde(default, rename = "push_server")]
    pub push_server: Option<PushServerServiceConfig>,
    /// 推送代理服务配置
    #[serde(default, rename = "push_proxy")]
    pub push_proxy: Option<PushProxyServiceConfig>,
    /// 推送工作服务配置
    #[serde(default, rename = "push_worker")]
    pub push_worker: Option<PushWorkerServiceConfig>,
    /// 消息编排服务配置（主消息流 fanout、消息操作事件）
    #[serde(default, rename = "message_orchestrator")]
    pub message_orchestrator: Option<MessageOrchestratorServiceConfig>,
    /// 消息摄入服务配置
    #[serde(default, rename = "message_ingest")]
    pub message_ingest: Option<MessageIngestServiceConfig>,
    /// 信令在线服务配置
    #[serde(default, rename = "signaling_online")]
    pub signaling_online: Option<SignalingOnlineServiceConfig>,
    /// 信令路由服务配置
    #[serde(default, rename = "signaling_route")]
    pub signaling_route: Option<SignalingRouteServiceConfig>,
    /// 存储读取服务配置
    #[serde(default, rename = "storage_reader")]
    pub storage_reader: Option<StorageReaderServiceConfig>,
    /// 存储写入服务配置
    #[serde(default, rename = "storage_writer")]
    pub storage_writer: Option<StorageWriterServiceConfig>,
    /// 会话服务配置
    #[serde(default, rename = "conversation")]
    pub conversation: Option<ConversationServiceConfig>,
    /// 同步编排服务配置
    #[serde(default, rename = "sync_orchestrator")]
    pub sync_orchestrator: Option<SyncOrchestratorServiceConfig>,
    /// 能力服务配置（Hook + Capability）
    #[serde(default, rename = "capability")]
    pub capability: Option<CapabilityServiceConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_placeholders_expand_in_nested_toml_values() {
        let mut value: Value = toml::from_str(
            r#"
            [object_storage.default]
            access_key = "${FLARE_TEST_ACCESS_KEY}"
            endpoint = "https://${FLARE_TEST_BUCKET}.example.com"
            subjects = ["flare.${FLARE_TEST_BUCKET}.*"]
            "#,
        )
        .expect("valid toml");

        expand_env_placeholders_in_value_with(&mut value, Path::new("config/test.toml"), |name| {
            match name {
                "FLARE_TEST_ACCESS_KEY" => Some("ak-test".to_string()),
                "FLARE_TEST_BUCKET" => Some("media".to_string()),
                _ => None,
            }
        })
        .expect("placeholders expand");

        let storage = value
            .get("object_storage")
            .and_then(|v| v.get("default"))
            .expect("object storage profile");
        assert_eq!(
            storage.get("access_key").and_then(Value::as_str),
            Some("ak-test")
        );
        assert_eq!(
            storage.get("endpoint").and_then(Value::as_str),
            Some("https://media.example.com")
        );
        assert_eq!(
            storage
                .get("subjects")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_str),
            Some("flare.media.*")
        );
    }

    #[test]
    fn env_placeholders_fail_when_variable_is_missing() {
        let mut value = Value::String("${FLARE_TEST_MISSING_SECRET}".to_string());
        let err = expand_env_placeholders_in_value_with(
            &mut value,
            Path::new("config/test.toml"),
            |_| None,
        )
        .expect_err("missing env var must fail");

        assert!(
            err.to_string().contains("FLARE_TEST_MISSING_SECRET"),
            "error should name the missing variable: {err}"
        );
    }

    #[test]
    fn mq_default_backend_accepts_only_canonical_values() {
        let mut config = default_config();
        config.mq.default_backend = "nats".to_string();
        config
            .validate_references()
            .expect("nats is the canonical JetStream backend selector");

        config.mq.default_backend = "jetstream".to_string();
        let err = config
            .validate_references()
            .expect_err("jetstream is a profile family, not a backend selector");
        assert!(
            err.to_string().contains("expected 'nats' or 'kafka'"),
            "error should describe canonical MQ backend values: {err}"
        );
    }
}
