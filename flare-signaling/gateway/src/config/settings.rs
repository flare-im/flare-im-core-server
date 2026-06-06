//! Access Gateway 配置
//!
//! 连接与稳定性相关项（max_connections、heartbeat、auth_timeout、send_timeout）对接 flare-core
//! ServerConfig，便于扩容连接数与调优消息收发稳定性；可通过环境变量覆盖。

use flare_im_core::config::{FlareAppConfig, RedisPoolConfig, TrustedTokenIssuerConfig};
use flare_im_core::gateway::require_secure_token_secret;
use flare_im_core::utils::normalize_tenant_id;
use flare_server_core::error::Result;

#[derive(Debug, Clone)]
pub struct AccessGatewayConfig {
    pub signaling_service: String,
    pub route_service: Option<String>, // Route 服务（新增）
    pub message_service: String,
    pub push_service: String,
    pub default_svid: String,      // 默认 SVID（新增，默认 "svid.im"）
    pub use_route_service: bool,   // 是否使用 Route 服务（新增，默认 true）
    pub default_tenant_id: String, // 默认租户ID（新增，默认 "0"）
    pub token_secret: String,
    pub token_issuer: String,
    pub token_ttl_seconds: u64,
    pub trusted_token_issuers: Vec<TrustedTokenIssuerConfig>,
    pub token_store_redis_url: Option<String>,
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
    /// 下行发送超时（秒），默认 10，单帧发送超过此时长则放弃并记录
    pub send_timeout_secs: u64,
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

        // Route 服务配置（新增）
        // Route 服务配置（新增）
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

        let token_secret = require_secure_token_secret(
            "ACCESS_GATEWAY_TOKEN_SECRET",
            service.token_secret.as_deref(),
            "services.access_gateway.token_secret",
        )?;

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
        let send_timeout_secs = std::env::var("ACCESS_GATEWAY_SEND_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);

        Ok(Self {
            signaling_service,
            route_service,
            message_service,
            push_service,
            default_svid,
            use_route_service,
            default_tenant_id,
            token_secret,
            token_issuer,
            token_ttl_seconds,
            trusted_token_issuers,
            token_store_redis_url: token_profile.as_ref().map(|p| p.url.clone()),
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
            send_timeout_secs,
        })
    }
}
