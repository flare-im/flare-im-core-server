use flare_server_core::auth::{AuthProviderConfig, AuthProviderMode};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use serde::Deserialize;
use std::str::FromStr;

pub use crate::clients::DownstreamGrpcConfig as GatewayGrpcConfig;
use crate::discovery::{discovery_route_authority, is_discovery_route_authority};
use crate::service_names::{
    CONVERSATION, MEDIA, MESSAGE_INGEST, ORCHESTRATOR, SIGNALING_ONLINE, STORAGE_READER,
};

/// 网关环境配置作用域。
///
/// 每个运行进程只读取自己作用域下的变量，避免 public gateway 和 admin gateway
/// 在生产部署中误共享认证、端口或下游路由配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayEnvScope {
    Api,
    Admin,
}

impl GatewayEnvScope {
    pub fn env_prefix(self) -> &'static str {
        match self {
            Self::Api => "FLARE_API_GATEWAY",
            Self::Admin => "FLARE_ADMIN_GATEWAY",
        }
    }

    fn default_port(self) -> &'static str {
        match self {
            Self::Api => "50050",
            Self::Admin => "50051",
        }
    }

    fn default_tracing_service_name(self) -> &'static str {
        match self {
            Self::Api => "flare-api-gateway",
            Self::Admin => "flare-admin-gateway",
        }
    }
}

/// IM Gateway 运行配置。
#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySettings {
    /// HTTP 服务器配置
    pub server: ServerConfig,
    /// gRPC 客户端配置
    pub grpc: GatewayGrpcConfig,
    /// 认证配置
    pub auth: AuthProviderConfig,
    /// 限流配置
    pub rate_limit: RateLimitConfig,
    /// 追踪配置
    pub tracing: TracingConfig,
}

/// HTTP 服务器配置
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    pub bind: String,
    /// HTTP 端口
    pub port: u16,
    /// 请求超时(秒)
    pub timeout_secs: u64,
    /// 最大请求体大小(字节)
    pub max_body_size: usize,
}

/// 限流配置
#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    /// 是否启用
    pub enabled: bool,
    /// 每秒请求数
    pub requests_per_second: u32,
    /// 突发容量
    pub burst_capacity: u32,
}

/// 追踪配置
#[derive(Debug, Clone, Deserialize)]
pub struct TracingConfig {
    /// 是否启用
    pub enabled: bool,
    /// 服务名称
    pub service_name: String,
    /// 采样率 (0.0 - 1.0)
    pub sample_rate: f64,
}

impl GatewaySettings {
    /// 从 API Gateway 环境变量加载配置。
    pub fn from_env() -> Result<Self> {
        Self::from_env_for(GatewayEnvScope::Api)
    }

    /// 从指定 Gateway 作用域的环境变量加载配置。
    pub fn from_env_for(scope: GatewayEnvScope) -> Result<Self> {
        dotenvy::dotenv().ok();
        Self::from_env_source(scope, |key| std::env::var(key).ok())
    }

    /// 从可注入的环境变量源加载配置，便于测试配置边界。
    pub fn from_env_source<F>(scope: GatewayEnvScope, mut source: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let server = ServerConfig {
            bind: env_or(&mut source, scope, "SERVER_BIND", "0.0.0.0"),
            port: parse_env(&mut source, scope, "SERVER_PORT", scope.default_port())?,
            timeout_secs: parse_env(&mut source, scope, "SERVER_TIMEOUT_SECS", "30")?,
            max_body_size: parse_env(&mut source, scope, "SERVER_MAX_BODY_SIZE", "10485760")?,
        };

        let grpc = GatewayGrpcConfig {
            media_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MEDIA_SERVICE_URL",
                &discovery_route_authority(MEDIA),
            ),
            message_ingest_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_INGEST_SERVICE_URL",
                &discovery_route_authority(MESSAGE_INGEST),
            ),
            message_orchestrator_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_ORCHESTRATOR_SERVICE_URL",
                &discovery_route_authority(ORCHESTRATOR),
            ),
            conversation_service_url: env_or(
                &mut source,
                scope,
                "GRPC_CONVERSATION_SERVICE_URL",
                &discovery_route_authority(CONVERSATION),
            ),
            online_service_url: env_or(
                &mut source,
                scope,
                "GRPC_ONLINE_SERVICE_URL",
                &discovery_route_authority(SIGNALING_ONLINE),
            ),
            storage_reader_service_url: env_or(
                &mut source,
                scope,
                "GRPC_STORAGE_READER_SERVICE_URL",
                &discovery_route_authority(STORAGE_READER),
            ),
            media_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MEDIA_STATIC_FALLBACK",
                "http://127.0.0.1:60081",
            ),
            message_ingest_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_INGEST_STATIC_FALLBACK",
                "http://127.0.0.1:50182",
            ),
            message_orchestrator_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK",
                "http://127.0.0.1:50181",
            ),
            conversation_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_CONVERSATION_STATIC_FALLBACK",
                "http://127.0.0.1:50090",
            ),
            online_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_ONLINE_STATIC_FALLBACK",
                "http://127.0.0.1:50061",
            ),
            storage_reader_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_STORAGE_READER_STATIC_FALLBACK",
                "http://127.0.0.1:60083",
            ),
            connect_timeout_secs: parse_env(&mut source, scope, "GRPC_CONNECT_TIMEOUT_SECS", "5")?,
            request_timeout_secs: parse_env(&mut source, scope, "GRPC_REQUEST_TIMEOUT_SECS", "10")?,
        };

        let auth = AuthProviderConfig {
            mode: parse_env(&mut source, scope, "AUTH_MODE", "core_jwt")?,
            hook_url: env_value(&mut source, scope, "AUTH_HOOK_URL")
                .filter(|value| !value.trim().is_empty()),
            hook_timeout_ms: parse_env(&mut source, scope, "AUTH_HOOK_TIMEOUT_MS", "800")?,
            hook_secret_header: env_or(
                &mut source,
                scope,
                "AUTH_HOOK_SECRET_HEADER",
                "x-flare-auth-hook-secret",
            ),
            hook_secret: env_value(&mut source, scope, "AUTH_HOOK_SECRET")
                .filter(|value| !value.trim().is_empty()),
        };

        let rate_limit = RateLimitConfig {
            enabled: parse_env(&mut source, scope, "RATE_LIMIT_ENABLED", "true")?,
            requests_per_second: parse_env(&mut source, scope, "RATE_LIMIT_RPS", "1000")?,
            burst_capacity: parse_env(&mut source, scope, "RATE_LIMIT_BURST", "2000")?,
        };

        let tracing = TracingConfig {
            enabled: parse_env(&mut source, scope, "TRACING_ENABLED", "true")?,
            service_name: env_or(
                &mut source,
                scope,
                "TRACING_SERVICE_NAME",
                scope.default_tracing_service_name(),
            ),
            sample_rate: parse_env(&mut source, scope, "TRACING_SAMPLE_RATE", "1.0")?,
        };

        let settings = GatewaySettings {
            server,
            grpc,
            auth,
            rate_limit,
            tracing,
        };

        // 验证配置
        settings.validate()?;

        Ok(settings)
    }

    /// 验证配置
    fn validate(&self) -> Result<()> {
        // 验证端口范围
        if self.server.port == 0 {
            return Err(config_error("Server port cannot be 0"));
        }

        // 验证超时
        if self.server.timeout_secs == 0 {
            return Err(config_error("Server timeout cannot be 0"));
        }

        for (label, route) in [
            ("media", self.grpc.media_service_url.as_str()),
            (
                "message-ingest",
                self.grpc.message_ingest_service_url.as_str(),
            ),
            (
                "message-orchestrator",
                self.grpc.message_orchestrator_service_url.as_str(),
            ),
            ("conversation", self.grpc.conversation_service_url.as_str()),
            ("online", self.grpc.online_service_url.as_str()),
            (
                "storage-reader",
                self.grpc.storage_reader_service_url.as_str(),
            ),
        ] {
            if !is_valid_grpc_route(route) {
                return Err(config_error(format!(
                    "gRPC route for {label} must be discovery://, http://, or https://"
                )));
            }
        }

        if self.auth.mode == AuthProviderMode::HttpHook {
            let Some(hook_url) = self.auth.hook_url.as_deref() else {
                return Err(config_error(
                    "AUTH_HOOK_URL is required in the gateway env scope when AUTH_MODE=http_hook",
                ));
            };
            if !(hook_url.starts_with("http://") || hook_url.starts_with("https://")) {
                return Err(config_error("AUTH_HOOK_URL must be http:// or https://"));
            }
            if self.auth.hook_timeout_ms == 0 {
                return Err(config_error("AUTH_HOOK_TIMEOUT_MS cannot be 0"));
            }
            if self.auth.hook_secret_header.trim().is_empty() {
                return Err(config_error("AUTH_HOOK_SECRET_HEADER cannot be empty"));
            }
        }

        // 验证限流配置
        if self.rate_limit.enabled && self.rate_limit.requests_per_second == 0 {
            return Err(config_error("Rate limit RPS cannot be 0 when enabled"));
        }

        // 验证追踪采样率
        if self.tracing.sample_rate < 0.0 || self.tracing.sample_rate > 1.0 {
            return Err(config_error(
                "Tracing sample rate must be between 0.0 and 1.0",
            ));
        }

        Ok(())
    }
}

fn config_error(reason: impl Into<String>) -> FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, reason).build_error()
}

pub fn require_secure_token_secret(
    env_key: &str,
    configured: Option<&str>,
    config_path: &str,
) -> Result<String> {
    let secret = std::env::var(env_key)
        .ok()
        .or_else(|| configured.map(ToOwned::to_owned))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            config_error(format!(
                "{config_path} is required; set {env_key} or configure a deployment secret"
            ))
        })?;

    if is_weak_token_secret(&secret) {
        return Err(config_error(format!(
            "{config_path} is weak or uses a known development placeholder; set {env_key} to a random secret with at least 32 bytes"
        )));
    }

    Ok(secret)
}

fn is_weak_token_secret(secret: &str) -> bool {
    let normalized = secret.trim().to_ascii_lowercase();
    secret.len() < 32
        || matches!(
            normalized.as_str(),
            "insecure-secret" | "change-me" | "change-me-in-production" | "secret" | "password"
        )
        || normalized.contains("change-me")
        || normalized.starts_with("insecure")
}

fn is_valid_grpc_route(route: &str) -> bool {
    route.starts_with("http://")
        || route.starts_with("https://")
        || is_discovery_route_authority(route)
}

fn env_key(scope: GatewayEnvScope, name: &str) -> String {
    format!("{}_{}", scope.env_prefix(), name)
}

fn env_value<F>(source: &mut F, scope: GatewayEnvScope, name: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    source(&env_key(scope, name))
}

fn env_or<F>(source: &mut F, scope: GatewayEnvScope, name: &str, default: &str) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    env_value(source, scope, name).unwrap_or_else(|| default.to_string())
}

fn parse_env<F, T>(source: &mut F, scope: GatewayEnvScope, name: &str, default: &str) -> Result<T>
where
    F: FnMut(&str) -> Option<String>,
    T: FromStr,
    T::Err: std::fmt::Display + Send + Sync + 'static,
{
    let key = env_key(scope, name);
    let value = source(&key).unwrap_or_else(|| default.to_string());
    value.parse::<T>().map_err(|err| {
        ErrorBuilder::new(
            ErrorCode::ConfigurationError,
            format!("Invalid {key}: {value}"),
        )
        .details(err.to_string())
        .build_error()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_settings() {
        let settings = GatewaySettings::from_env().unwrap();
        assert_eq!(settings.server.port, 50050);
        assert_eq!(settings.auth.mode, AuthProviderMode::CoreJwt);
        assert!(settings.rate_limit.enabled);
        assert!(is_discovery_route_authority(
            &settings.grpc.message_ingest_service_url
        ));
        assert!(is_discovery_route_authority(
            &settings.grpc.message_orchestrator_service_url
        ));
    }

    #[test]
    fn auth_mode_parses_aliases() {
        assert_eq!(
            "core_jwt".parse::<AuthProviderMode>().unwrap(),
            AuthProviderMode::CoreJwt
        );
        assert_eq!(
            "external".parse::<AuthProviderMode>().unwrap(),
            AuthProviderMode::HttpHook
        );
    }

    #[test]
    fn gateway_env_scope_uses_service_specific_prefixes() {
        let env = HashMap::from([
            (
                "FLARE_API_GATEWAY_SERVER_PORT".to_string(),
                "51050".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_SERVER_PORT".to_string(),
                "51051".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_AUTH_MODE".to_string(),
                "http_hook".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_AUTH_HOOK_URL".to_string(),
                "https://admin-auth.example.com/validate".to_string(),
            ),
            (
                "FLARE_API_GATEWAY_AUTH_MODE".to_string(),
                "core_jwt".to_string(),
            ),
        ]);

        let admin =
            GatewaySettings::from_env_source(GatewayEnvScope::Admin, |key| env.get(key).cloned())
                .expect("admin settings");
        let api =
            GatewaySettings::from_env_source(GatewayEnvScope::Api, |key| env.get(key).cloned())
                .expect("api settings");

        assert_eq!(admin.server.port, 51051);
        assert_eq!(admin.auth.mode, AuthProviderMode::HttpHook);
        assert_eq!(
            admin.auth.hook_url.as_deref(),
            Some("https://admin-auth.example.com/validate")
        );
        assert_eq!(admin.tracing.service_name, "flare-admin-gateway");

        assert_eq!(api.server.port, 51050);
        assert_eq!(api.auth.mode, AuthProviderMode::CoreJwt);
        assert_eq!(api.auth.hook_url, None);
        assert_eq!(api.tracing.service_name, "flare-api-gateway");
    }
}
