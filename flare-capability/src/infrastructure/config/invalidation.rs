//! Hook 配置的跨实例失效通知（PostgreSQL `LISTEN/NOTIFY`）。
//!
//! 多个 Capability 实例共用一个库时，`CapabilityService/Administer` 的写入只落到收到请求的
//! 那个实例：它当场重载该租户（全局行变化时全量重载），其余实例以前要等定时刷新（默认
//! 60 s）才看到新增、修改、停用或删除的 Hook。现在写入的实例在 [`HOOK_CONFIG_CHANNEL`]
//! 上发一条通知（租户与发出实例），其余实例的监听任务收到后重载同一租户、或全量重载。
//!
//! 通知尽力而为：发不出去只记 WARN（写入已提交，定时刷新兜底）；监听连接断开重连后全量
//! 重载一次，补上断开期间漏掉的通知；通知解析不了也全量重载。监听连接取自配置仓储的连接池，
//! 不增加连接预算。

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::infrastructure::config::ConfigWatcher;

/// 通知频道。
pub const HOOK_CONFIG_CHANNEL: &str = "flare_hook_config_changed";

const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

/// 一条通知：哪个租户的行变了（`None` 为全局行），由哪个实例发出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HookConfigChange {
    tenant_id: Option<String>,
    origin: String,
}

/// 收到一条通知后该做的重载。
#[derive(Debug, PartialEq, Eq)]
enum Reload {
    /// 本实例发出的：写入时已经重载过。
    Skip,
    Tenant(String),
    All,
}

impl HookConfigChange {
    /// 解析不了的通知按全量重载处理（宁可多载一次，不漏）。
    fn reload_for(payload: &str, own_origin: &str) -> Reload {
        match serde_json::from_str::<HookConfigChange>(payload) {
            Ok(change) if change.origin == own_origin => Reload::Skip,
            Ok(HookConfigChange {
                tenant_id: Some(tenant),
                ..
            }) if !tenant.trim().is_empty() => Reload::Tenant(tenant),
            Ok(_) => Reload::All,
            Err(_) => {
                warn!(
                    channel = HOOK_CONFIG_CHANNEL,
                    "unparseable hook config change notification; reloading all hook config"
                );
                Reload::All
            }
        }
    }
}

/// 本实例的通知发送端与监听任务。
pub struct HookConfigInvalidation {
    pool: Arc<PgPool>,
    origin: String,
}

impl HookConfigInvalidation {
    /// `pool` 用配置仓储的连接池；每个实例取一个随机的发出方标识，用来跳过自己发的通知。
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            origin: uuid::Uuid::new_v4().simple().to_string(),
        }
    }

    /// 写入已提交后通知其余实例：`tenant_id` 为该行的租户，全局行为 `None`。
    pub async fn publish(&self, tenant_id: Option<&str>) -> Result<()> {
        let payload = serde_json::to_string(&HookConfigChange {
            tenant_id: tenant_id
                .filter(|tenant| !tenant.trim().is_empty())
                .map(str::to_string),
            origin: self.origin.clone(),
        })
        .map_err(|e| FlareError::system(format!("encode hook config change: {e}")))?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(HOOK_CONFIG_CHANNEL)
            .bind(payload)
            .execute(&*self.pool)
            .await
            .map_err(|e| FlareError::system(format!("notify hook config change: {e}")))?;
        Ok(())
    }

    /// 启动监听任务：收到其余实例的通知就重载 `watcher` 中对应的配置。返回的通道在第一次
    /// 订阅成功后收到 `()`（测试用来等订阅就绪）。
    pub fn spawn_listener(
        &self,
        watcher: Arc<ConfigWatcher>,
    ) -> (JoinHandle<()>, oneshot::Receiver<()>) {
        let (ready, subscribed) = oneshot::channel();
        let pool = Arc::clone(&self.pool);
        let origin = self.origin.clone();
        let task = tokio::spawn(listen(pool, origin, watcher, ready));
        (task, subscribed)
    }
}

async fn listen(
    pool: Arc<PgPool>,
    origin: String,
    watcher: Arc<ConfigWatcher>,
    ready: oneshot::Sender<()>,
) {
    let mut ready = Some(ready);
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut listener = match subscribe(&pool).await {
            Ok(listener) => listener,
            Err(error) => {
                warn!(
                    channel = HOOK_CONFIG_CHANNEL,
                    %error,
                    retry_in_secs = backoff.as_secs(),
                    "hook config change listener cannot subscribe; other instances' writes arrive with the periodic refresh"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        info!(
            channel = HOOK_CONFIG_CHANNEL,
            "hook config change listener subscribed"
        );
        match ready.take() {
            Some(ready) => {
                let _ = ready.send(());
            }
            // 重新订阅：断开期间的通知已经丢了，全量重载一次补上。
            None => reload_all(&watcher, "resubscribed").await,
        }
        loop {
            match listener.try_recv().await {
                Ok(Some(notification)) => {
                    match HookConfigChange::reload_for(notification.payload(), &origin) {
                        Reload::Skip => {}
                        Reload::Tenant(tenant) => {
                            info!(
                                channel = HOOK_CONFIG_CHANNEL,
                                tenant_id = %tenant,
                                "hook config changed on another instance; reloading tenant"
                            );
                            if let Err(error) = watcher.reload_tenant(&tenant).await {
                                warn!(
                                    tenant_id = %tenant,
                                    %error,
                                    "reload after another instance's hook config change failed; keeping the previous hooks"
                                );
                            }
                        }
                        Reload::All => reload_all(&watcher, "global change").await,
                    }
                }
                // 连接断了，已自动重连并重新 LISTEN；断开期间的通知丢了。
                Ok(None) => reload_all(&watcher, "connection re-established").await,
                Err(error) => {
                    warn!(
                        channel = HOOK_CONFIG_CHANNEL,
                        %error,
                        "hook config change listener lost its connection; resubscribing"
                    );
                    break;
                }
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

async fn subscribe(pool: &PgPool) -> std::result::Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(HOOK_CONFIG_CHANNEL).await?;
    Ok(listener)
}

async fn reload_all(watcher: &ConfigWatcher, reason: &'static str) {
    info!(
        channel = HOOK_CONFIG_CHANNEL,
        reason, "reloading all hook config after a change notification"
    );
    if let Err(error) = watcher.reload().await {
        warn!(%error, reason, "full hook config reload failed; keeping the previous hooks");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(tenant: Option<&str>, origin: &str) -> String {
        serde_json::to_string(&HookConfigChange {
            tenant_id: tenant.map(str::to_string),
            origin: origin.into(),
        })
        .unwrap()
    }

    #[test]
    fn notifications_map_to_the_reload_they_need() {
        assert_eq!(
            HookConfigChange::reload_for(&payload(Some("tenant-a"), "other"), "me"),
            Reload::Tenant("tenant-a".into())
        );
        assert_eq!(
            HookConfigChange::reload_for(&payload(None, "other"), "me"),
            Reload::All
        );
        assert_eq!(
            HookConfigChange::reload_for(&payload(Some(" "), "other"), "me"),
            Reload::All
        );
        // 自己发的跳过：写入时已经重载。
        assert_eq!(
            HookConfigChange::reload_for(&payload(Some("tenant-a"), "me"), "me"),
            Reload::Skip
        );
        // 解析不了的不漏：全量重载。
        assert_eq!(HookConfigChange::reload_for("tenant-a", "me"), Reload::All);
    }
}
