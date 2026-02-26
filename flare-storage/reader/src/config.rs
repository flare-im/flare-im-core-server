use anyhow::Result;
use flare_im_core::config::FlareAppConfig;
use std::env;

#[derive(Clone, Debug)]
pub struct StorageReaderConfig {
    pub redis_url: Option<String>,
    pub postgres_url: Option<String>,
    pub default_range_seconds: i64,
    pub max_page_size: i32,
    // PostgreSQL 连接池配置
    pub postgres_max_connections: u32,
    pub postgres_min_connections: u32,
    pub postgres_acquire_timeout_seconds: u64,
    pub postgres_idle_timeout_seconds: u64,
    pub postgres_max_lifetime_seconds: u64,
    // Redis 缓存配置（仅消息与查询索引 TTL）
    pub redis_message_cache_ttl_seconds: u64,
    pub redis_session_cache_ttl_seconds: u64,
}

impl StorageReaderConfig {
    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service_config = app.storage_reader_service();

        // 解析 Redis 配置引用（可选）
        let redis_url = env::var("STORAGE_REDIS_URL").ok().or_else(|| {
            if let Some(redis_name) = &service_config.redis {
                app.redis_profile(redis_name)
                    .map(|profile| profile.url.clone())
            } else {
                None
            }
        });

        // 解析 PostgreSQL 配置引用（必需）
        // 注意：StorageReaderServiceConfig 没有 postgres 字段，直接从环境变量或使用默认 postgres profile
        let postgres_url = env::var("STORAGE_POSTGRES_URL")
            .ok()
            .or_else(|| env::var("POSTGRES_URL").ok())
            .or_else(|| {
                app.postgres_profile("media")
                    .map(|profile| profile.url.clone())
            })
            .or_else(|| {
                app.postgres_profile("primary")
                    .map(|profile| profile.url.clone())
            });

        let default_range_seconds = env::var("STORAGE_READER_DEFAULT_RANGE_SECONDS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(7 * 24 * 3600);

        let max_page_size = env::var("STORAGE_READER_MAX_PAGE_SIZE")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .or_else(|| service_config.max_page_size.map(|v| v as i32))
            .unwrap_or(200);

        // PostgreSQL 连接池配置
        let postgres_max_connections = env::var("STORAGE_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(20); // 读侧可以支持更多连接

        let postgres_min_connections = env::var("STORAGE_POSTGRES_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(5);

        let postgres_acquire_timeout_seconds = env::var("STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);

        let postgres_idle_timeout_seconds = env::var("STORAGE_POSTGRES_IDLE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600); // 10 minutes

        let postgres_max_lifetime_seconds = env::var("STORAGE_POSTGRES_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1800); // 30 minutes

        // Redis 缓存配置
        let redis_message_cache_ttl_seconds = env::var("STORAGE_REDIS_MESSAGE_CACHE_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600); // 1 hour

        let redis_session_cache_ttl_seconds = env::var("STORAGE_REDIS_SESSION_CACHE_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1800); // 30 minutes

        Ok(Self {
            redis_url,
            postgres_url,
            default_range_seconds,
            max_page_size,
            postgres_max_connections,
            postgres_min_connections,
            postgres_acquire_timeout_seconds,
            postgres_idle_timeout_seconds,
            postgres_max_lifetime_seconds,
            redis_message_cache_ttl_seconds,
            redis_session_cache_ttl_seconds,
        })
    }

    /// 从环境变量加载（保留用于向后兼容，但不推荐使用）
    #[deprecated(note = "Use from_app_config instead")]
    pub fn from_env() -> Self {
        let redis_url = env::var("STORAGE_REDIS_URL").ok();
        let postgres_url = env::var("STORAGE_POSTGRES_URL").ok();
        let default_range_seconds = env::var("STORAGE_READER_DEFAULT_RANGE_SECONDS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(7 * 24 * 3600);
        let max_page_size = env::var("STORAGE_READER_MAX_PAGE_SIZE")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(200);

        Self {
            redis_url,
            postgres_url,
            default_range_seconds,
            max_page_size,
            postgres_max_connections: 20,
            postgres_min_connections: 5,
            postgres_acquire_timeout_seconds: 30,
            postgres_idle_timeout_seconds: 600,
            postgres_max_lifetime_seconds: 1800,
            redis_message_cache_ttl_seconds: 3600,
            redis_session_cache_ttl_seconds: 1800,
        }
    }
}
