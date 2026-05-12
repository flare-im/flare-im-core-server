use anyhow::{Context, Result};
use serde::Deserialize;

/// 网关配置
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    /// HTTP 服务器配置
    pub server: ServerConfig,
    /// gRPC 客户端配置
    pub grpc: GrpcConfig,
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

/// gRPC 客户端配置
#[derive(Debug, Clone, Deserialize)]
pub struct GrpcConfig {
    /// MediaService 地址
    pub media_service_url: String,
    /// MessageService 地址
    pub message_service_url: String,
    /// ConversationService 地址
    pub conversation_service_url: String,
    /// 连接超时(秒)
    pub connect_timeout_secs: u64,
    /// 请求超时(秒)
    pub request_timeout_secs: u64,
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

impl Settings {
    /// 从环境变量加载配置
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let server = ServerConfig {
            bind: std::env::var("SERVER_BIND").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("SERVER_PORT")
                .unwrap_or_else(|_| "50050".to_string())
                .parse()
                .context("Invalid SERVER_PORT")?,
            timeout_secs: std::env::var("SERVER_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("Invalid SERVER_TIMEOUT_SECS")?,
            max_body_size: std::env::var("SERVER_MAX_BODY_SIZE")
                .unwrap_or_else(|_| "10485760".to_string()) // 10MB
                .parse()
                .context("Invalid SERVER_MAX_BODY_SIZE")?,
        };

        let grpc = GrpcConfig {
            media_service_url: std::env::var("GRPC_MEDIA_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:60081".to_string()),
            message_service_url: std::env::var("GRPC_MESSAGE_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50052".to_string()),
            conversation_service_url: std::env::var("GRPC_CONVERSATION_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:50053".to_string()),
            connect_timeout_secs: std::env::var("GRPC_CONNECT_TIMEOUT_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("Invalid GRPC_CONNECT_TIMEOUT_SECS")?,
            request_timeout_secs: std::env::var("GRPC_REQUEST_TIMEOUT_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .context("Invalid GRPC_REQUEST_TIMEOUT_SECS")?,
        };

        let rate_limit = RateLimitConfig {
            enabled: std::env::var("RATE_LIMIT_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .context("Invalid RATE_LIMIT_ENABLED")?,
            requests_per_second: std::env::var("RATE_LIMIT_RPS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .context("Invalid RATE_LIMIT_RPS")?,
            burst_capacity: std::env::var("RATE_LIMIT_BURST")
                .unwrap_or_else(|_| "2000".to_string())
                .parse()
                .context("Invalid RATE_LIMIT_BURST")?,
        };

        let tracing = TracingConfig {
            enabled: std::env::var("TRACING_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .context("Invalid TRACING_ENABLED")?,
            service_name: std::env::var("TRACING_SERVICE_NAME")
                .unwrap_or_else(|_| "flare-gateway".to_string()),
            sample_rate: std::env::var("TRACING_SAMPLE_RATE")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse()
                .context("Invalid TRACING_SAMPLE_RATE")?,
        };

        let settings = Settings {
            server,
            grpc,
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
            anyhow::bail!("Server port cannot be 0");
        }

        // 验证超时
        if self.server.timeout_secs == 0 {
            anyhow::bail!("Server timeout cannot be 0");
        }

        // 验证 gRPC URL
        if !self.grpc.media_service_url.starts_with("http://")
            && !self.grpc.media_service_url.starts_with("https://")
        {
            anyhow::bail!("gRPC URL must start with http:// or https://");
        }

        // 验证限流配置
        if self.rate_limit.enabled {
            if self.rate_limit.requests_per_second == 0 {
                anyhow::bail!("Rate limit RPS cannot be 0 when enabled");
            }
        }

        // 验证追踪采样率
        if self.tracing.sample_rate < 0.0 || self.tracing.sample_rate > 1.0 {
            anyhow::bail!("Tracing sample rate must be between 0.0 and 1.0");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = Settings::from_env().unwrap();
        assert_eq!(settings.server.port, 50050);
        assert!(settings.rate_limit.enabled);
    }
}
