//! 各服务的专属配置结构体与 `ServicesConfig` 聚合。
//!
//! 从 mod.rs 拆出（原单文件 2,621 行）；类型定义保持原样，pub API 经 mod.rs re-export。

use serde::Deserialize;

use super::types::*;
use flare_server_core::auth::{
    AuthProviderConfig, TrustedIssuerConfig as TrustedTokenIssuerConfig,
};

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
    /// 额外信任的 JWT 发行方（如业务系统的登录 token，用于长连接鉴权）
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
    /// 网关ID（用于跨地区路由、服务发现实例 ID 与网关租约）。`FLARE_ENV=production` 时必填；
    /// 其余环境不设置则取「主机名 + WS 端口」，重启不变
    #[serde(default)]
    pub gateway_id: Option<String>,
    /// 网关所在地区（用于跨地区路由）
    #[serde(default)]
    pub region: Option<String>,
    /// 网关所在可用区：注册进服务发现的实例元数据，供同可用区优先与故障定位使用
    #[serde(default)]
    pub zone: Option<String>,
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
    /// 租户运行时投影所在的 PostgreSQL profile（`base.toml` 中 `[postgres.*]`，读 `tenants` 表）；
    /// 可被 `ACCESS_GATEWAY_TENANT_RUNTIME_POSTGRES_URL` 覆盖。不配置则不校验租户状态、配额回落全局。
    #[serde(default)]
    pub postgres: Option<String>,
    /// 未知租户接入策略：`lenient`（默认，放行）/ `strict`（拒绝）；可被 `ACCESS_GATEWAY_TENANT_POLICY` 覆盖。
    #[serde(default)]
    pub tenant_policy: Option<String>,
}

/// API Gateway 服务配置（业务系统和三方 HTTP facade）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ApiGatewayServiceConfig {
    /// 运行时配置
    #[serde(flatten)]
    pub runtime: ServiceRuntimeConfig,
    /// 租户运行时投影库 profile（`tenants` 表）。
    #[serde(default)]
    pub postgres: Option<String>,
    /// 未知/不可用投影的接入策略：lenient 或 strict。
    #[serde(default)]
    pub tenant_policy: Option<String>,
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
    /// 刷新令牌有效期（秒，长效）。缺省 30 天。接入令牌过期后凭刷新令牌换新，支撑 7x24 免重登。
    #[serde(default)]
    pub refresh_token_ttl_seconds: Option<u64>,
    /// 令牌撤销/轮换存储的 redis profile 名（对应 `[redis.<name>]`）。缺省用约定名 `token_store`。
    /// 接上后：刷新令牌轮换会作废旧的、校验会查撤销位；不配且无 `token_store` profile 则为无状态（撤销为空操作）。
    #[serde(default)]
    pub token_store: Option<String>,
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
    #[serde(default)]
    pub admin_image_upload_max_bytes: Option<u64>,
    #[serde(default)]
    pub admin_image_upload_max_pixels: Option<u64>,
    #[serde(default)]
    pub admin_image_upload_max_dimension: Option<u32>,
    #[serde(default)]
    pub admin_image_upload_allowed_mime_types: Option<Vec<String>>,
    #[serde(default)]
    pub application_media_validators: Vec<String>,
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
    /// 大群 pure ping 在 Push Server 解析成员前按会话合并的窗口（毫秒），0 表示关闭。
    #[serde(default)]
    pub event_ping_coalesce_window_ms: Option<u64>,
    /// Redis 配置
    #[serde(default)]
    pub redis: Option<String>,
    /// 在线状态查询后端：redis 或 grpc。
    #[serde(default)]
    pub online_status_backend: Option<String>,
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
    /// 设备有多条推送绑定时依次尝试的厂商（`apns`、`fcm`、`getui`、`jpush`），缺省
    /// `["apns", "getui", "fcm", "jpush"]`；没列出的厂商按缺省顺序排在后面。
    #[serde(default)]
    pub binding_order: Option<Vec<String>>,
    /// 按厂商的并发、速率与熔断：`[services.push_worker.vendors.<apns|fcm|getui|jpush>]`。
    #[serde(default)]
    pub vendors: Option<PushWorkerVendorsConfig>,
}

/// Push Worker 各厂商的治理参数；没写的厂商、没写的字段用缺省值。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PushWorkerVendorsConfig {
    #[serde(default)]
    pub apns: Option<PushVendorGovernanceConfig>,
    #[serde(default)]
    pub fcm: Option<PushVendorGovernanceConfig>,
    #[serde(default)]
    pub getui: Option<PushVendorGovernanceConfig>,
    #[serde(default)]
    pub jpush: Option<PushVendorGovernanceConfig>,
}

/// 一个厂商的并发、速率与熔断参数。缺省值见 Push Worker（APNs 256/2000、FCM 256/1000、
/// 个推 64/300；熔断 0.5、20、10、30000、3）。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PushVendorGovernanceConfig {
    /// 本 worker 同时向这个厂商发出的请求上限。
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    /// 每个应用凭据每秒至多发多少条。
    #[serde(default)]
    pub rate_per_sec: Option<u32>,
    /// 令牌桶容量，缺省等于 `rate_per_sec`。
    #[serde(default)]
    pub burst: Option<u32>,
    /// 滚动窗口里失败占比达到它就熔断。
    #[serde(default)]
    pub failure_ratio: Option<f64>,
    /// 滚动窗口的调用数。
    #[serde(default)]
    pub window: Option<usize>,
    /// 窗口里至少这么多次调用才下结论。
    #[serde(default)]
    pub min_calls: Option<usize>,
    /// 熔断后多久放探测请求（毫秒）。
    #[serde(default)]
    pub open_ms: Option<u64>,
    /// 半开时同时放行的探测数；这么多次成功就恢复。
    #[serde(default)]
    pub half_open_probes: Option<u32>,
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
    /// 租户运行时投影所在的 PostgreSQL profile（读 `tenants` 表的 quota.core）；
    /// 可被 `MESSAGE_INGEST_TENANT_RUNTIME_POSTGRES_URL` 覆盖。不配置则配额回落全局限流配置。
    #[serde(default)]
    pub postgres: Option<String>,
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
    /// 发送入口限流开关
    #[serde(default)]
    pub send_rate_limit_enabled: Option<bool>,
    /// tenant 维度每秒发送上限，0 表示不限制
    #[serde(default)]
    pub send_rate_limit_tenant_per_second: Option<u32>,
    /// tenant+sender 维度每秒发送上限，0 表示不限制
    #[serde(default)]
    pub send_rate_limit_tenant_sender_per_second: Option<u32>,
    /// tenant+conversation 维度每秒发送上限，0 表示不限制
    #[serde(default)]
    pub send_rate_limit_tenant_conversation_per_second: Option<u32>,
    /// 发送限流 fixed-window 大小（毫秒）
    #[serde(default)]
    pub send_rate_limit_window_ms: Option<u64>,
    /// 发送限流本地保留 key 上限
    #[serde(default)]
    pub send_rate_limit_max_tracked_keys: Option<usize>,
    /// WAL 后 MQ publish 阶段超时（毫秒），0 表示不启用额外阶段超时
    #[serde(default)]
    pub send_publish_timeout_ms: Option<u64>,
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
    /// stdout 日志格式：`text`（默认）或 `json`（一行一个事件，供采集器解析）。
    /// 环境变量 `FLARE_LOG_FORMAT` 优先；写错名称时拒绝启动。
    #[serde(default)]
    pub format: Option<String>,
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
