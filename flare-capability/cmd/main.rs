//! Flare Capability 进程入口。
//!
//! 对外只暴露协议与通用适配装配入口；具体 RTC 后端实现类型不在公开 API 中出现。

use anyhow::Result;
use flare_capability::composition::{ApplicationBootstrap, CapabilityServiceConfig};
use flare_capability::domain::model::ExecutionMode;
use flare_im_core::{load_config, tracing::init_tracing_from_config};

#[tokio::main]
async fn main() -> Result<()> {
    let app_config = load_config(Some("config"));
    init_tracing_from_config(Some(app_config.logging()));

    let cap_service = app_config.capability_service();
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| {
            cap_service
                .postgres
                .as_deref()
                .and_then(|name| app_config.postgres_profile(name))
                .map(|p| p.url.clone())
        })
        .or_else(|| app_config.postgres_profile("media").map(|p| p.url.clone()))
        .or_else(|| Some("postgresql://flare:flare123@localhost:25432/flare2".to_string()));

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
