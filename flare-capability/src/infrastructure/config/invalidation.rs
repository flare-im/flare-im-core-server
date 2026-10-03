//! Hook 配置的跨实例失效通知（PostgreSQL `LISTEN/NOTIFY`）。
//!
//! 多个 Capability 实例共用一个库时，`CapabilityService/Administer` 的写入只落到收到请求的
//! 那个实例：它当场重载该租户（全局行变化时全量重载），其余实例以前要等定时刷新（默认
//! 60 s）才看到新增、修改、停用或删除的 Hook。现在写入的实例在 [`HOOK_CONFIG_CHANNEL`]
//! 上发一条通知（租户与发出实例），其余实例的监听任务收到后重载同一租户、或全量重载。
//!
//! 通知尽力而为：发不出去只记 WARN（写入已提交，定时刷新兜底）；监听连接断开重连后全量
//! 重载一次，补上断开期间漏掉的通知；通知解析不了也全量重载。
//!
//! `LISTEN` 用一条独立的连接（同一组连接参数、只放一条连接的专用连接池），不占配置仓储的
//! 连接池：以前监听长期占着仓储池里的一条连接，`CAPABILITY_POSTGRES_MAX_CONNECTIONS=1` 时
//! 仓储查询与发通知的 `pg_notify` 都要等到取连接超时。每个实例因此多用一条 PostgreSQL 连接。
//! 监听任务的句柄是 [`HookConfigListener`]：停机时 [`HookConfigListener::shutdown`] 停掉任务、
//! 关掉专用连接，句柄被丢弃时任务同样停止。

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::infrastructure::config::ConfigWatcher;

/// 通知频道。
pub const HOOK_CONFIG_CHANNEL: &str = "flare_hook_config_changed";

const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

/// 建立监听连接的超时（专用连接池的取连接超时）。
const LISTENER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 停机时等监听任务退出的上限，超过就强制中止。
const LISTENER_STOP_TIMEOUT: Duration = Duration::from_secs(5);

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

/// 本实例的通知发送端与监听连接。
pub struct HookConfigInvalidation {
    /// 配置仓储的连接池：发通知（一次短查询）。
    pool: Arc<PgPool>,
    /// `LISTEN` 专用：同一组连接参数、最多一条连接，按需建立（见模块说明）。
    listener_pool: PgPool,
    origin: String,
}

impl HookConfigInvalidation {
    /// `pool` 是配置仓储的连接池，只用来发通知；监听另开一条连接（连接参数取自 `pool`）。每个
    /// 实例取一个随机的发出方标识，用来跳过自己发的通知。
    pub fn new(pool: Arc<PgPool>) -> Self {
        let listener_pool = listener_pool_for(&pool);
        Self {
            pool,
            listener_pool,
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

    /// 启动监听任务：收到其余实例的通知就重载 `watcher` 中对应的配置。返回的句柄须保存到
    /// 停机（见 [`HookConfigListener`]）。
    pub fn spawn_listener(&self, watcher: Arc<ConfigWatcher>) -> HookConfigListener {
        let (ready, subscribed) = oneshot::channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let pool = self.listener_pool.clone();
        let origin = self.origin.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                () = listen(pool, origin, watcher, ready) => {}
                _ = stopped => {
                    info!(channel = HOOK_CONFIG_CHANNEL, "hook config change listener stopped");
                }
            }
        });
        HookConfigListener {
            task,
            stop: Some(stop),
            subscribed: Some(subscribed),
            listener_pool: self.listener_pool.clone(),
        }
    }
}

/// `LISTEN` 专用的连接池：与 `pool` 同一组连接参数，最多一条连接、按需建立，不设空闲与寿命
/// 回收（监听连接本就长期占用；断开时由 [`PgListener`] 重连）。
fn listener_pool_for(pool: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(LISTENER_CONNECT_TIMEOUT)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_lazy_with(pool.connect_options().as_ref().clone())
}

/// 运行中的监听任务。停机时调 [`Self::shutdown`]；句柄被丢弃时任务同样被中止。
pub struct HookConfigListener {
    task: JoinHandle<()>,
    stop: Option<oneshot::Sender<()>>,
    subscribed: Option<oneshot::Receiver<()>>,
    listener_pool: PgPool,
}

impl HookConfigListener {
    /// 等第一次订阅成功，最多等 `limit`（测试用来等订阅就绪）；已经等到过的再调直接返回 `true`。
    pub async fn wait_subscribed(&mut self, limit: Duration) -> bool {
        match self.subscribed.as_mut() {
            None => true,
            Some(subscribed) => match tokio::time::timeout(limit, subscribed).await {
                Ok(Ok(())) => {
                    self.subscribed = None;
                    true
                }
                Ok(Err(_)) | Err(_) => false,
            },
        }
    }

    /// 监听任务已经结束（停机后、或任务异常退出）。
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// 停机：通知任务退出（最多等 5 s，超时中止），再关掉专用监听连接。
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if tokio::time::timeout(LISTENER_STOP_TIMEOUT, &mut self.task)
            .await
            .is_err()
        {
            warn!(
                channel = HOOK_CONFIG_CHANNEL,
                "hook config change listener did not stop in time; aborting it"
            );
            self.task.abort();
        }
        self.listener_pool.close().await;
    }
}

impl Drop for HookConfigListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn listen(
    pool: PgPool,
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
        drop(listener);
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
    use crate::domain::model::HookConfig;
    use std::time::Instant;

    /// 监听用自己的连接池（最多一条连接），不是仓储的池；停机让监听任务立即结束（这里连不上
    /// 库，任务一直在重试订阅）。
    #[tokio::test]
    async fn the_listener_has_its_own_connection_and_stops_on_shutdown() {
        let repository_pool = Arc::new(
            PgPoolOptions::new()
                .max_connections(1)
                .connect_lazy("postgres://w13h@127.0.0.1:1/unreachable")
                .unwrap(),
        );
        let invalidation = HookConfigInvalidation::new(Arc::clone(&repository_pool));
        assert_eq!(
            invalidation.listener_pool.options().get_max_connections(),
            1
        );
        assert_eq!(
            invalidation.listener_pool.options().get_min_connections(),
            0
        );

        let watcher = Arc::new(ConfigWatcher::with_config(HookConfig::default()));
        let mut listener = invalidation.spawn_listener(watcher);
        assert!(!listener.wait_subscribed(Duration::from_millis(200)).await);
        assert!(!listener.is_finished());
        let started = Instant::now();
        listener.shutdown().await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown waited {:?}",
            started.elapsed()
        );
        assert!(invalidation.listener_pool.is_closed());
        assert!(!repository_pool.is_closed());
    }

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
