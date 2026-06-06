//! Flare Capability 进程入口。
//!
//! 对外只暴露协议与通用适配装配入口；具体 RTC 后端实现类型不在公开 API 中出现。

use flare_capability::composition::{ApplicationBootstrap, CapabilityServiceConfig};
use flare_capability::domain::model::ExecutionMode;
use flare_im_core::{load_config, tracing::init_tracing_from_config};
use flare_server_core::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let app_config = load_config(Some("config"));
    init_tracing_from_config(Some(app_config.logging()));

    let cap_service = app_config.capability_service();
    let postgres_profile = cap_service
        .postgres
        .as_deref()
        .and_then(|name| app_config.postgres_profile(name))
        .or_else(|| app_config.postgres_profile("media"));
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| postgres_profile.map(|p| p.url.clone()))
        .or_else(|| Some("postgresql://flare:flare123@localhost:25432/flare2".to_string()));
    let postgres_max_connections = std::env::var("CAPABILITY_POSTGRES_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.max_connections))
        .unwrap_or(10);
    let postgres_min_connections = std::env::var("CAPABILITY_POSTGRES_MIN_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.min_connections))
        .unwrap_or(2);
    let postgres_acquire_timeout_seconds =
        std::env::var("CAPABILITY_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|p| p.acquire_timeout_seconds))
            .unwrap_or(10);
    let postgres_idle_timeout_seconds = std::env::var("CAPABILITY_POSTGRES_IDLE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.idle_timeout_seconds))
        .unwrap_or(300);
    let postgres_max_lifetime_seconds = std::env::var("CAPABILITY_POSTGRES_MAX_LIFETIME_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| postgres_profile.and_then(|p| p.max_lifetime_seconds))
        .unwrap_or(1800);

    let config_center_endpoint = std::env::var("CONFIG_CENTER_ENDPOINT")
        .ok()
        .or_else(|| Some("etcd://localhost:22379".to_string()));

    let tenant_id = std::env::var("TENANT_ID").ok();
    let config_file = std::env::var("CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/hooks.toml")));
    let runtime_config_file = std::env::var("CAPABILITY_RUNTIME_CONFIG_FILE")
        .ok()
        .map(std::path::PathBuf::from)
        .or_else(|| Some(std::path::PathBuf::from("config/services/capability.toml")));

    let config = CapabilityServiceConfig {
        config_file,
        runtime_config_file,
        database_url,
        postgres_max_connections,
        postgres_min_connections,
        postgres_acquire_timeout_seconds,
        postgres_idle_timeout_seconds,
        postgres_max_lifetime_seconds,
        config_center_endpoint,
        tenant_id,
        execution_mode: ExecutionMode::Sequential,
        refresh_interval_secs: 60,
    };

    tracing::info!(
        config_file = ?config.config_file,
        runtime_config_file = ?config.runtime_config_file,
        tenant_id = ?config.tenant_id,
        refresh_interval_secs = config.refresh_interval_secs,
        "Starting Flare Capability service"
    );
    ApplicationBootstrap::run(config).await
}
