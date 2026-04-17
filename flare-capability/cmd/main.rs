//! # Flare Capability 服务入口
//!
//! Hook gRPC + 默认音视频能力插件（RTC/SFU 进程内实例、outbox、可选插件管理 HTTP）由 `ApplicationBootstrap` 启动。

use anyhow::Result;
use flare_capability::domain::model::ExecutionMode;
use flare_capability::service::bootstrap::{ApplicationBootstrap, CapabilityServiceConfig};
use flare_im_core::{load_config, tracing::init_tracing_from_config};

#[tokio::main]
async fn main() -> Result<()> {
    // 加载应用配置（用于统一日志初始化等）
    let app_config = load_config(Some("config"));

    // 从配置初始化日志系统（默认 debug 级别）
    init_tracing_from_config(Some(app_config.logging()));

    let cap_service = app_config.capability_service();
    // 数据库：DATABASE_URL > services.capability.postgres > postgres.media（flare2）> 内置默认
    let database_url = std::env::var("DATABASE_URL").ok().or_else(|| {
        cap_service
            .postgres
            .as_deref()
            .and_then(|name| app_config.postgres_profile(name))
            .map(|p| p.url.clone())
    }).or_else(|| {
        app_config
            .postgres_profile("media")
            .map(|p| p.url.clone())
    }).or_else(|| {
        Some("postgresql://flare:flare123@localhost:25432/flare2".to_string())
    });

    let config_center_endpoint = std::env::var("CONFIG_CENTER_ENDPOINT").ok().or_else(|| {
        // 默认使用 docker-compose 中的 etcd 配置
        Some("etcd://localhost:22379".to_string())
    });

    let tenant_id = std::env::var("TENANT_ID").ok();

    let config_file = std::env::var("CONFIG_FILE")
        .ok()
        .map(|s| std::path::PathBuf::from(s));

    let config = CapabilityServiceConfig {
        config_file,
        database_url,
        config_center_endpoint,
        tenant_id,
        execution_mode: ExecutionMode::Sequential,
        refresh_interval_secs: 60,
    };

    tracing::info!("Starting Flare Capability service with config: {:?}", config);

    // 启动应用
    ApplicationBootstrap::run(config).await
}
