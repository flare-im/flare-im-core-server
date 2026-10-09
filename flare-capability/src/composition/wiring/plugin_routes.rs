//! 插件登记的读模型装配：有库时接 `capability_plugin_endpoints`，启动载入、周期重读、健康落库、
//! TTL 清理；没有库时只在本进程。

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;

use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresPluginEndpointRepository;

/// TTL 清理的周期：清理只删超过 TTL（至少 5 分钟）的行，每分钟一次足够。
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub(super) fn plugin_route_book(db_pool: Option<&Arc<PgPool>>) -> PluginRouteBook {
    match db_pool {
        Some(pool) => PluginRouteBook::with_store(Arc::new(PostgresPluginEndpointRepository::new(
            Arc::clone(pool),
        ))),
        None => {
            tracing::warn!(
                "no database: plugin registrations (RegisterPluginEndpoint) are process-local; \
                 other instances do not see them and a restart loses them"
            );
            PluginRouteBook::new()
        }
    }
}

/// 载入库里的登记（失败则启动失败：表不在说明迁移没跑），再起后台任务按周期重读、按分钟清理。
pub(super) async fn load_and_maintain(
    routes: &Arc<PluginRouteBook>,
    runtime: &CapabilityRuntimeConfig,
) -> Result<()> {
    if !routes.is_persistent() {
        return Ok(());
    }
    let loaded = routes.reload().await.map_err(|error| {
        FlareError::system(format!(
            "cannot load plugin registrations from capability_plugin_endpoints (migration 0011 applied?): {error}"
        ))
    })?;
    tracing::info!(
        loaded,
        refresh_secs = runtime.plugin_route_refresh_interval.as_secs(),
        ttl_secs = runtime.plugin_route_ttl.as_secs(),
        "plugin registrations loaded from capability_plugin_endpoints"
    );
    let routes = Arc::clone(routes);
    let refresh_interval = runtime.plugin_route_refresh_interval;
    let ttl = runtime.plugin_route_ttl;
    tokio::spawn(async move {
        let mut refresh = tokio::time::interval(refresh_interval);
        let mut cleanup = tokio::time::interval(CLEANUP_INTERVAL);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick of each fires at once; the startup load just ran.
        refresh.tick().await;
        cleanup.tick().await;
        loop {
            tokio::select! {
                _ = refresh.tick() => {
                    if let Err(error) = routes.reload().await {
                        tracing::warn!(%error, "plugin registration refresh failed; keeping the last view");
                    }
                }
                _ = cleanup.tick() => match routes.cleanup_stale(ttl).await {
                    Ok(0) => {}
                    Ok(removed) => tracing::info!(removed, "removed plugin registrations not healthy within the TTL"),
                    Err(error) => tracing::warn!(%error, "plugin registration cleanup failed"),
                },
            }
        }
    });
    Ok(())
}
