//! Access Gateway 配置
//!
//! 连接与稳定性相关项（max_connections、heartbeat、auth_timeout、send_timeout）对接 flare-core
//! ServerConfig，便于扩容连接数与调优消息收发稳定性；可通过环境变量覆盖。

use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::config::{
    AccessGatewayServiceConfig, AuthProviderConfig, FlareAppConfig, RedisPoolConfig,
    TrustedTokenIssuerConfig,
};
use flare_im_service_kit::gateway::require_secure_token_secret;
use flare_server_core::auth::AuthProviderMode;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};

use crate::domain::service::SyncPullRateLimitConfig;

#[derive(Debug, Clone)]
pub struct AccessGatewayConfig {
    pub signaling_service: String,
    pub route_service: Option<String>, // Route 服务（新增）
    pub message_service: String,
    pub push_service: String,
    pub default_svid: String,      // 默认 SVID（新增，默认 "svid.im"）
    pub use_route_service: bool,   // 是否使用 Route 服务（新增，默认 true）
    pub default_tenant_id: String, // 默认租户ID（新增，默认 "0"）
    /// Prometheus 指标端点。网关此前**注册了指标却从不暴露**：
    /// wire.rs 里只打了一句 "Prometheus metrics initialized" 日志，
    /// 没有像 ingest/orchestrator/storage-writer 那样起 serve_prometheus_metrics，
    /// 于是整个网关侧观测面（连接数、推送成功/失败/耗时）无处可读。
    pub metrics: flare_im_service_kit::metrics::MetricsEndpointConfig,
    pub auth_provider: AuthProviderConfig,
    pub token_secret: Option<String>,
    pub token_issuer: String,
    pub token_ttl_seconds: u64,
    pub trusted_token_issuers: Vec<TrustedTokenIssuerConfig>,
    pub token_store_redis_url: Option<String>,
    /// 撤销键空间的 namespace：**必须与 api-gateway 的 token_store profile 一致**，
    /// 否则两边读写不同 key，建连时查不到 api-gateway 写的撤销位。
    pub token_store_namespace: Option<String>,
    // ACK上报配置（使用 gRPC，无需 JetStream）
    pub use_ack_report: bool,
    // 跨地区网关路由配置
    pub gateway_id: Option<String>,
    pub region: Option<String>,
    // 压缩和加密配置
    pub compression_algorithm: Option<String>,
    pub enable_encryption: bool,
    pub encryption_key: Option<String>,

    // 连接与稳定性（对接 flare-core ServerConfig，便于扩容与调优）
    /// 最大连接数，默认 50_000，可通过 ACCESS_GATEWAY_MAX_CONNECTIONS 覆盖
    pub max_connections: usize,
    /// 连接空闲超时（秒），默认 300，flare-core 用于清理长时间无活动的连接
    pub connection_timeout_secs: u64,
    /// 心跳间隔（秒），默认 30
    pub heartbeat_interval_secs: u64,
    /// 心跳超时（秒），默认 90，超过未收到 PING/PONG 或业务消息则断开
    pub heartbeat_timeout_secs: u64,
    /// 认证超时（秒），默认 30，连接建立后须在此时间内完成认证
    pub auth_timeout_secs: u64,
    /// 并发握手上限。flare-core 默认 1024——它是**握手闸门**而不是连接总数：
    /// 同一时刻最多 N 个连接在做握手，握完即释放名额。网关此前从不设置它，
    /// 于是无论 fd 和 max_connections 调到多大，接入速率都被 1024 卡住
    /// （压测实测：fd 给到 30 万、max_connections 给到 25 万，网关持有的
    /// 连接数仍恒定在 1038 = 1024 + 基线）。
    pub max_handshake_concurrency: usize,
    /// 下行发送超时（秒），默认 10，单帧发送超过此时长则放弃并记录
    pub send_timeout_secs: u64,
    /// 同步拉取限流开关，默认开启
    pub sync_pull_rate_limit_enabled: bool,
    /// 单用户同步拉取令牌补充速率（requests/second）
    pub sync_pull_user_requests_per_second: u32,
    /// 单用户同步拉取突发容量
    pub sync_pull_user_burst: u32,
    /// 单租户同步拉取令牌补充速率（requests/second）
    pub sync_pull_tenant_requests_per_second: u32,
    /// 单租户同步拉取突发容量
    pub sync_pull_tenant_burst: u32,
}

impl AccessGatewayConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service = app.access_gateway_service();

        let token_profile: Option<RedisPoolConfig> = service
            .token_store
            .as_deref()
            .and_then(|name| app.redis_profile(name))
            .cloned();

        // 使用服务注册发现获取服务名（必须配置）
        // 注意：服务名必须与注册中心中的服务类型一致（不带 flare- 前缀）
        let signaling_service = service
            .signaling_service
            .clone()
            .unwrap_or_else(|| "signaling-online".to_string());

        let message_service = service
            .message_service
            .clone()
            .unwrap_or_else(|| "message-orchestrator".to_string());

        let push_service = service
            .push_service
            .clone()
            .unwrap_or_else(|| "push-server".to_string());

        // Route 服务配置
        let route_service = service.route_service.clone();

        // 默认 SVID（新增）
        let default_svid = service
            .default_svid
            .clone()
            .or_else(|| std::env::var("ACCESS_GATEWAY_DEFAULT_SVID").ok())
            .unwrap_or_else(|| "svid.im".to_string());

        // 默认租户ID（新增，默认 "0"）
        let default_tenant_id = std::env::var("ACCESS_GATEWAY_DEFAULT_TENANT_ID")
            .ok()
            .map(normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string());

        // 是否使用 Route 服务（新增，默认 true）
        let use_route_service = if let Some(use_route) =
            std::env::var("ACCESS_GATEWAY_USE_ROUTE_SERVICE")
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
        {
            use_route
        } else {
            service.use_route_service
        };

        let auth_provider = resolve_auth_provider(&service)?;
        let token_secret = match auth_provider.mode {
            AuthProviderMode::CoreJwt => Some(require_secure_token_secret(
                "ACCESS_GATEWAY_TOKEN_SECRET",
                service.token_secret.as_deref(),
                "services.access_gateway.token_secret",
            )?),
            AuthProviderMode::HttpHook => None,
        };

        let token_issuer = service
            .token_issuer
            .unwrap_or_else(|| "flare-im-core".to_string());

        let token_ttl_seconds = service.token_ttl_seconds.unwrap_or(3600);

        let trusted_token_issuers = service.trusted_token_issuers.clone();

        // ACK上报配置（使用 gRPC，默认开启）
        let use_ack_report = std::env::var("ACCESS_GATEWAY_USE_ACK_REPORT")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true); // 默认开启

        // 跨地区网关路由配置
        let gateway_id = std::env::var("GATEWAY_ID")
            .ok()
            .or_else(|| service.gateway_id.clone());

        let region = std::env::var("GATEWAY_REGION")
            .ok()
            .or_else(|| service.region.clone());

        // 压缩算法配置（支持环境变量覆盖）
        let compression_algorithm = std::env::var("GATEWAY_COMPRESSION_ALGORITHM")
            .ok()
            .or_else(|| service.compression_algorithm.clone());

        // 加密配置（支持环境变量覆盖）
        let enable_encryption = std::env::var("GATEWAY_ENABLE_ENCRYPTION")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .or(service.enable_encryption)
            .unwrap_or(false);

        let encryption_key = std::env::var("GATEWAY_ENCRYPTION_KEY")
            .ok()
            .or_else(|| service.encryption_key.clone());

        let max_connections = std::env::var("ACCESS_GATEWAY_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50_000);
        let connection_timeout_secs = std::env::var("ACCESS_GATEWAY_CONNECTION_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300);
        let heartbeat_interval_secs = std::env::var("ACCESS_GATEWAY_HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let heartbeat_timeout_secs = std::env::var("ACCESS_GATEWAY_HEARTBEAT_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(90);
        let auth_timeout_secs = std::env::var("ACCESS_GATEWAY_AUTH_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        // 默认从 flare-core 的 1024 提到 16384：握手是短暂动作，闸门开大不会
        // 常驻占用资源，但闸门太小会直接封死接入速率上限。
        let max_handshake_concurrency = std::env::var("ACCESS_GATEWAY_MAX_HANDSHAKE_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16384);
        let send_timeout_secs = std::env::var("ACCESS_GATEWAY_SEND_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);
        let sync_pull_defaults = SyncPullRateLimitConfig::default();
        let sync_pull_rate_limit_enabled = env_bool("ACCESS_GATEWAY_SYNC_PULL_RATE_LIMIT_ENABLED")
            .or(service.sync_pull_rate_limit_enabled)
            .unwrap_or(sync_pull_defaults.enabled);
        let sync_pull_user_requests_per_second =
            env_u32("ACCESS_GATEWAY_SYNC_PULL_USER_REQUESTS_PER_SECOND")
                .or(service.sync_pull_user_requests_per_second)
                .unwrap_or(sync_pull_defaults.user_requests_per_second);
        let sync_pull_user_burst = env_u32("ACCESS_GATEWAY_SYNC_PULL_USER_BURST")
            .or(service.sync_pull_user_burst)
            .unwrap_or(sync_pull_defaults.user_burst);
        let sync_pull_tenant_requests_per_second =
            env_u32("ACCESS_GATEWAY_SYNC_PULL_TENANT_REQUESTS_PER_SECOND")
                .or(service.sync_pull_tenant_requests_per_second)
                .unwrap_or(sync_pull_defaults.tenant_requests_per_second);
        let sync_pull_tenant_burst = env_u32("ACCESS_GATEWAY_SYNC_PULL_TENANT_BURST")
            .or(service.sync_pull_tenant_burst)
            .unwrap_or(sync_pull_defaults.tenant_burst);

        // 指标端点：与 ingest/orchestrator/storage-writer 同一套约定
        // （*_METRICS_ENABLED / _ADDRESS / _PORT / _PATH），默认开启。
        let metrics = {
            let enabled = env_bool("ACCESS_GATEWAY_METRICS_ENABLED").unwrap_or(true);
            let address = std::env::var("ACCESS_GATEWAY_METRICS_ADDRESS")
                .unwrap_or_else(|_| "0.0.0.0".to_string());
            let port = std::env::var("ACCESS_GATEWAY_METRICS_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(19183);
            let path = std::env::var("ACCESS_GATEWAY_METRICS_PATH")
                .unwrap_or_else(|_| "/metrics".to_string());
            let mut cfg = flare_im_service_kit::metrics::MetricsEndpointConfig::new(address, port)
                .with_path(path);
            cfg.enabled = enabled;
            cfg
        };

        Ok(Self {
            metrics,
            signaling_service,
            route_service,
            message_service,
            push_service,
            default_svid,
            use_route_service,
            default_tenant_id,
            auth_provider,
            token_secret,
            token_issuer,
            token_ttl_seconds,
            trusted_token_issuers,
            token_store_redis_url: token_profile.as_ref().map(|p| p.url.clone()),
            token_store_namespace: token_profile.as_ref().and_then(|p| p.namespace.clone()),
            use_ack_report,
            gateway_id,
            region,
            compression_algorithm,
            enable_encryption,
            encryption_key,
            max_connections,
            connection_timeout_secs,
            heartbeat_interval_secs,
            heartbeat_timeout_secs,
            auth_timeout_secs,
            max_handshake_concurrency,
            send_timeout_secs,
            sync_pull_rate_limit_enabled,
            sync_pull_user_requests_per_second,
            sync_pull_user_burst,
            sync_pull_tenant_requests_per_second,
            sync_pull_tenant_burst,
        })
    }
}

impl AccessGatewayConfig {
    pub fn sync_pull_rate_limit_config(&self) -> SyncPullRateLimitConfig {
        SyncPullRateLimitConfig {
            enabled: self.sync_pull_rate_limit_enabled,
            user_requests_per_second: self.sync_pull_user_requests_per_second,
            user_burst: self.sync_pull_user_burst,
            tenant_requests_per_second: self.sync_pull_tenant_requests_per_second,
            tenant_burst: self.sync_pull_tenant_burst,
        }
    }
}

fn resolve_auth_provider(service: &AccessGatewayServiceConfig) -> Result<AuthProviderConfig> {
    let mut auth = service.auth.clone();

    if let Ok(mode) = std::env::var("ACCESS_GATEWAY_AUTH_MODE") {
        auth.mode = mode
            .parse::<AuthProviderMode>()
            .map_err(|err| config_error(format!("invalid ACCESS_GATEWAY_AUTH_MODE: {err}")))?;
    }
    if let Ok(hook_url) = std::env::var("ACCESS_GATEWAY_AUTH_HOOK_URL") {
        auth.hook_url = non_empty(hook_url);
    }
    if let Ok(timeout_ms) = std::env::var("ACCESS_GATEWAY_AUTH_HOOK_TIMEOUT_MS") {
        auth.hook_timeout_ms = timeout_ms.parse::<u64>().map_err(|err| {
            config_error(format!(
                "invalid ACCESS_GATEWAY_AUTH_HOOK_TIMEOUT_MS: {err}"
            ))
        })?;
    }
    if let Ok(secret_header) = std::env::var("ACCESS_GATEWAY_AUTH_HOOK_SECRET_HEADER") {
        auth.hook_secret_header = secret_header.trim().to_string();
    }
    if let Ok(secret) = std::env::var("ACCESS_GATEWAY_AUTH_HOOK_SECRET") {
        auth.hook_secret = non_empty(secret);
    }

    if auth.mode == AuthProviderMode::HttpHook {
        let hook_url = auth.hook_url.as_deref().ok_or_else(|| {
            config_error(
                "services.access_gateway.auth.hook_url is required when auth.mode=http_hook",
            )
        })?;
        if !(hook_url.starts_with("http://") || hook_url.starts_with("https://")) {
            return Err(config_error(
                "services.access_gateway.auth.hook_url must be http:// or https://",
            ));
        }
        if auth.hook_timeout_ms == 0 {
            return Err(config_error(
                "services.access_gateway.auth.hook_timeout_ms cannot be 0",
            ));
        }
        if auth.hook_secret_header.trim().is_empty() {
            return Err(config_error(
                "services.access_gateway.auth.hook_secret_header cannot be empty",
            ));
        }
    }

    Ok(auth)
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn env_bool(key: &str) -> Option<bool> {
    std::env::var(key).ok().and_then(|v| v.parse::<bool>().ok())
}

fn env_u32(key: &str) -> Option<u32> {
    std::env::var(key).ok().and_then(|v| v.parse::<u32>().ok())
}

fn config_error(reason: impl Into<String>) -> FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, reason).build_error()
}
