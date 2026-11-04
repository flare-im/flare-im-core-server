//! 网关宕机清扫器（单例）。
//!
//! 接入网关被 `kill -9` 或所在主机宕机时，它的连接不会走正常断开，Online 里的会话会一直挂到
//! TTL。网关每 5s 续一次 `gw:lease:{id}`（TTL 15s）；本清扫器每 5s 检查 `gw:known` 中每个网关
//! 的租约，缺失持续 6s（见 [`GatewaySweeperConfig::confirm_after`]）就分批读出 `gw:conns:{id}`，
//! 按正常断开的同一路径（[`OnlineCommandHandler`] 的注销 + presence 快照）移除这些连接，最后删掉
//! 索引、移出 `gw:known`。网关宕机到开始清扫至多 15s + 6s + 5s。
//!
//! 多个 Online 实例只有持有 Redis 单例锁的那个在扫（`SET NX PX` + 续期）。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_grpc_proto::signaling::online::LogoutRequest;
use flare_im_service_kit::gateway::lease::{
    LEASE_RENEW_INTERVAL, base_gateway_id, parse_connection_member,
};
use flare_im_service_kit::metrics::OnlineSweeperMetrics;
use flare_server_core::context::Context;
use flare_server_core::error::Result;
use futures_util::StreamExt;
use tracing::{debug, info, warn};

use crate::application::commands::LogoutCommand;
use crate::application::handlers::OnlineCommandHandler;
use crate::domain::repository::{
    ConversationRepository, GatewaySweepRepository, PresencePublisher, SubscriptionRepository,
};

/// 清扫节奏与批量。
#[derive(Debug, Clone)]
pub struct GatewaySweeperConfig {
    /// 检查周期。
    pub interval: Duration,
    /// 单例锁 TTL：持有者每个周期、每批都续期；持有者挂掉后别的实例最迟这么久接手。
    pub lock_ttl: Duration,
    /// 每批从 `gw:conns` 读多少成员。
    pub batch_size: usize,
    /// 一批里并发注销的连接数：一台网关可能有十万级连接，逐条串行清不完 30s 的目标。
    pub concurrency: usize,
    /// 租约缺失要持续多久才算宕机。一次看到缺失只记为嫌疑：Redis 中断超过租约 TTL 后恢复的
    /// 那一刻，所有网关的租约都已过期，活着的网关要等下一次续期（至多一个续期间隔）才写回；
    /// 这时立即清扫会把活连接全部当成死连接。取略大于网关续期间隔。
    pub confirm_after: Duration,
}

impl Default for GatewaySweeperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            lock_ttl: Duration::from_secs(15),
            batch_size: 200,
            concurrency: 32,
            confirm_after: LEASE_RENEW_INTERVAL + Duration::from_secs(1),
        }
    }
}

/// 一轮清扫的结果。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// 本实例是否持有单例锁（没持有就什么都没做）。
    pub leader: bool,
    /// 租约缺失的网关数。
    pub dead_gateways: usize,
    /// 移除的连接数。
    pub removed: u64,
    /// 清扫完成、已从 `gw:known` 移出的网关数。
    pub retired: usize,
}

#[derive(Debug, Default)]
struct GatewayOutcome {
    removed: u64,
    retired: bool,
    lost_lock: bool,
}

pub struct GatewaySweeper<G, CR, SR, PP>
where
    G: GatewaySweepRepository,
    CR: ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
{
    repository: Arc<G>,
    commands: Arc<OnlineCommandHandler<CR, SR, PP>>,
    metrics: Arc<OnlineSweeperMetrics>,
    /// 本实例的锁持有者标识。
    token: String,
    config: GatewaySweeperConfig,
    /// 租约缺失的网关第一次被看到缺失的时刻（见 [`GatewaySweeperConfig::confirm_after`]）。
    suspects: std::sync::Mutex<HashMap<String, Instant>>,
}

impl<G, CR, SR, PP> GatewaySweeper<G, CR, SR, PP>
where
    G: GatewaySweepRepository,
    CR: ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
{
    pub fn new(
        repository: Arc<G>,
        commands: Arc<OnlineCommandHandler<CR, SR, PP>>,
        metrics: Arc<OnlineSweeperMetrics>,
    ) -> Self {
        Self {
            repository,
            commands,
            metrics,
            token: uuid::Uuid::new_v4().to_string(),
            config: GatewaySweeperConfig::default(),
            suspects: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn with_config(mut self, config: GatewaySweeperConfig) -> Self {
        self.config = config;
        self
    }

    /// 周期清扫直到 `shutdown`；退出时释放自己持有的锁，别的实例下一轮即可接手。
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                // 停机优先：已经收到停机就不再开始新一轮
                biased;
                _ = &mut shutdown => break,
                _ = ticker.tick() => match self.sweep_once().await {
                    Ok(report) if report.removed > 0 || report.retired > 0 => info!(
                        dead_gateways = report.dead_gateways,
                        removed = report.removed,
                        retired = report.retired,
                        "gateway sweep removed connections of dead gateways"
                    ),
                    Ok(_) => {}
                    Err(error) => warn!(%error, "gateway sweep failed; retrying next round"),
                },
            }
        }
        if let Err(error) = self.repository.release_sweeper_lock(&self.token).await {
            debug!(%error, "gateway sweeper lock release failed; it expires on its own");
        }
    }

    /// 一轮清扫：持有锁才扫。
    pub async fn sweep_once(&self) -> Result<SweepReport> {
        let mut report = SweepReport::default();
        if !self
            .repository
            .hold_sweeper_lock(&self.token, self.config.lock_ttl)
            .await?
        {
            return Ok(report);
        }
        report.leader = true;
        let known = self.repository.known_gateways().await?;
        let present = self.repository.leases_present(&known).await?;
        let dead = self.confirmed_dead(&known, &present);
        for gateway_id in &dead {
            report.dead_gateways += 1;
            let outcome = self.sweep_gateway(gateway_id).await?;
            report.removed += outcome.removed;
            if outcome.retired {
                report.retired += 1;
            }
            if outcome.lost_lock {
                report.leader = false;
                break;
            }
        }
        Ok(report)
    }

    /// 租约缺失已持续 `confirm_after` 的网关。嫌疑表只留本轮仍缺失的网关：租约回来、或已不在
    /// `gw:known` 里的都清掉，下次缺失重新计时。
    fn confirmed_dead(&self, known: &[String], present: &[bool]) -> Vec<String> {
        let now = Instant::now();
        let Ok(mut suspects) = self.suspects.lock() else {
            return Vec::new();
        };
        let missing: Vec<&String> = known
            .iter()
            .zip(present)
            .filter_map(|(gateway_id, alive)| (!alive).then_some(gateway_id))
            .collect();
        suspects.retain(|gateway_id, _| missing.contains(&gateway_id));
        missing
            .into_iter()
            .filter(|gateway_id| {
                let since = *suspects.entry((*gateway_id).clone()).or_insert(now);
                now.duration_since(since) >= self.config.confirm_after
            })
            .cloned()
            .collect()
    }

    async fn sweep_gateway(&self, gateway_id: &str) -> Result<GatewayOutcome> {
        // 只清这一刻之前登记的会话：清扫途中网关带着同一 ID 回来，它的新连接登记在这之后，
        // 而且会写进同一个集合。
        let cutoff_ms = chrono::Utc::now().timestamp_millis();
        let owner_id = base_gateway_id(gateway_id);
        let mut outcome = GatewayOutcome::default();
        let mut cursor = 0u64;
        loop {
            // 每批之前确认仍持有锁、网关仍没回来：锁丢了就交给新的持有者，网关回来了就停手
            // （它启动时会把遗留连接转成孤儿网关，下一轮按孤儿清）。
            if !self
                .repository
                .hold_sweeper_lock(&self.token, self.config.lock_ttl)
                .await?
            {
                outcome.lost_lock = true;
                return Ok(outcome);
            }
            if self
                .repository
                .leases_present(&[gateway_id.to_string()])
                .await?
                .first()
                .copied()
                .unwrap_or(false)
            {
                info!(
                    gateway_id,
                    "gateway lease is back; stop sweeping its connections"
                );
                return Ok(outcome);
            }
            let (next, members) = self
                .repository
                .scan_gateway_connections(gateway_id, cursor, self.config.batch_size)
                .await?;
            let removed = futures_util::stream::iter(members)
                .map(|member| self.retire_member(gateway_id, owner_id, cutoff_ms, member))
                .buffer_unordered(self.config.concurrency.max(1))
                .fold(
                    0u64,
                    |total, removed| async move { total + u64::from(removed) },
                )
                .await;
            outcome.removed += removed;
            self.metrics.record_removed(owner_id, removed);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        outcome.retired = self.repository.retire_gateway(gateway_id).await?;
        if outcome.retired {
            info!(
                gateway_id,
                removed = outcome.removed,
                "dead gateway swept and retired"
            );
        }
        Ok(outcome)
    }

    /// 移除一条连接，返回是否真的移除了。
    async fn retire_member(
        &self,
        gateway_id: &str,
        owner_id: &str,
        cutoff_ms: i64,
        member: String,
    ) -> bool {
        let Some((tenant_id, user_id, session_field)) = parse_connection_member(&member) else {
            self.forget(gateway_id, &member).await;
            return false;
        };
        match self
            .repository
            .session_owner(tenant_id, user_id, session_field)
            .await
        {
            // 会话已经不在（正常断开与清扫交错、或会话已过期）：只清索引。
            Ok(None) => {
                self.forget(gateway_id, &member).await;
                false
            }
            // 不是这台网关登记的、或是网关回来之后才登记的：不动。
            Ok(Some(owner)) if owner.gateway_id != owner_id || owner.created_at_ms > cutoff_ms => {
                false
            }
            Ok(Some(_)) => {
                let ctx = Context::root()
                    .with_tenant_id(tenant_id)
                    .with_user_id(user_id);
                let command = LogoutCommand {
                    request: LogoutRequest {
                        user_id: user_id.to_string(),
                        conversation_id: session_field.to_string(),
                    },
                    ctx: ctx.clone(),
                };
                match self.commands.handle_gateway_lost(&ctx, command).await {
                    Ok(_) => true,
                    Err(error) => {
                        warn!(
                            gateway_id,
                            tenant_id,
                            %error,
                            "failed to remove a connection of a dead gateway; retrying next round"
                        );
                        false
                    }
                }
            }
            Err(error) => {
                warn!(gateway_id, %error, "session lookup failed during gateway sweep");
                false
            }
        }
    }

    async fn forget(&self, gateway_id: &str, member: &str) {
        if let Err(error) = self
            .repository
            .forget_gateway_connection(gateway_id, member)
            .await
        {
            debug!(gateway_id, %error, "stale gateway connection member not removed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use flare_grpc_proto::signaling::online::{
        DeviceConflictStrategy, PresenceEvent, UserPresenceEvent,
    };
    use flare_im_service_kit::gateway::lease::{connection_member, orphan_gateway_id};

    use super::*;
    use crate::domain::aggregate::Connection;
    use crate::domain::model::OnlineStatusRecord;
    use crate::domain::repository::{EvictedConnection, SessionOwner};
    use crate::domain::service::{OnlineStatusService, SubscriptionService};
    use crate::domain::value_object::{ConnectionId, DeviceId, UserId};

    type Sessions = Arc<Mutex<HashMap<(String, String, String), SessionOwner>>>;

    /// 会话表与网关索引的内存替身：`sessions` 与下面的会话仓储共享，注销会真的移除会话。
    #[derive(Default)]
    struct MemoryStore {
        sessions: Sessions,
        lock: Mutex<Option<String>>,
        known: Mutex<Vec<String>>,
        leases: Mutex<HashSet<String>>,
        conns: Mutex<HashMap<String, Vec<String>>>,
    }

    impl MemoryStore {
        fn add_session(&self, gateway_set: &str, owner: &str, tenant: &str, user: &str) -> String {
            self.add_session_at(gateway_set, owner, tenant, user, 1)
        }

        fn add_session_at(
            &self,
            gateway_set: &str,
            owner: &str,
            tenant: &str,
            user: &str,
            created_at_ms: i64,
        ) -> String {
            let field = uuid::Uuid::new_v4().to_string();
            self.sessions.lock().unwrap().insert(
                (tenant.into(), user.into(), field.clone()),
                SessionOwner {
                    gateway_id: owner.into(),
                    created_at_ms,
                },
            );
            self.conns
                .lock()
                .unwrap()
                .entry(gateway_set.into())
                .or_default()
                .push(connection_member(tenant, user, &field));
            field
        }

        fn session_count(&self) -> usize {
            self.sessions.lock().unwrap().len()
        }
    }

    impl GatewaySweepRepository for MemoryStore {
        async fn hold_sweeper_lock(&self, token: &str, _ttl: Duration) -> Result<bool> {
            let mut lock = self.lock.lock().unwrap();
            match lock.as_deref() {
                Some(holder) => Ok(holder == token),
                None => {
                    *lock = Some(token.to_string());
                    Ok(true)
                }
            }
        }
        async fn release_sweeper_lock(&self, token: &str) -> Result<()> {
            let mut lock = self.lock.lock().unwrap();
            if lock.as_deref() == Some(token) {
                *lock = None;
            }
            Ok(())
        }
        async fn known_gateways(&self) -> Result<Vec<String>> {
            Ok(self.known.lock().unwrap().clone())
        }
        async fn leases_present(&self, gateway_ids: &[String]) -> Result<Vec<bool>> {
            let leases = self.leases.lock().unwrap();
            Ok(gateway_ids.iter().map(|id| leases.contains(id)).collect())
        }
        async fn scan_gateway_connections(
            &self,
            gateway_id: &str,
            _cursor: u64,
            _count: usize,
        ) -> Result<(u64, Vec<String>)> {
            Ok((
                0,
                self.conns
                    .lock()
                    .unwrap()
                    .get(gateway_id)
                    .cloned()
                    .unwrap_or_default(),
            ))
        }
        async fn session_owner(
            &self,
            tenant_id: &str,
            user_id: &str,
            session_field: &str,
        ) -> Result<Option<SessionOwner>> {
            Ok(self
                .sessions
                .lock()
                .unwrap()
                .get(&(tenant_id.into(), user_id.into(), session_field.into()))
                .cloned())
        }
        async fn forget_gateway_connection(&self, gateway_id: &str, member: &str) -> Result<()> {
            if let Some(members) = self.conns.lock().unwrap().get_mut(gateway_id) {
                members.retain(|m| m != member);
            }
            Ok(())
        }
        async fn retire_gateway(&self, gateway_id: &str) -> Result<bool> {
            if self.leases.lock().unwrap().contains(gateway_id) {
                return Ok(false);
            }
            self.conns.lock().unwrap().remove(gateway_id);
            self.known.lock().unwrap().retain(|id| id != gateway_id);
            Ok(true)
        }
    }

    /// 会话仓储替身：注销移除共享会话表里的那条，在线查询按剩余会话回答。
    struct SessionRepository(Sessions);

    impl ConversationRepository for SessionRepository {
        async fn save_connection(&self, _connection: &Connection) -> Result<()> {
            Ok(())
        }
        async fn admit_connection(
            &self,
            _connection: &Connection,
            _gateway_connection_id: &str,
            _strategy: DeviceConflictStrategy,
            _max_devices: u32,
        ) -> Result<Vec<EvictedConnection>> {
            Ok(Vec::new())
        }
        async fn remove_connection(
            &self,
            tenant_id: &str,
            conversation_id: &ConnectionId,
            user_id: &UserId,
        ) -> Result<()> {
            self.0.lock().unwrap().remove(&(
                tenant_id.into(),
                user_id.as_str().into(),
                conversation_id.as_str().into(),
            ));
            Ok(())
        }
        async fn touch_connection(
            &self,
            _tenant_id: &str,
            _conversation_id: &ConnectionId,
            _user_id: &UserId,
        ) -> Result<()> {
            Ok(())
        }
        async fn fetch_statuses(
            &self,
            tenant_id: &str,
            user_ids: &[String],
        ) -> Result<HashMap<String, OnlineStatusRecord>> {
            let sessions = self.0.lock().unwrap();
            Ok(user_ids
                .iter()
                .filter_map(|user| {
                    sessions
                        .iter()
                        .find(|((tenant, owner, _), _)| tenant == tenant_id && owner == user)
                        .map(|(_, owner)| {
                            (
                                user.clone(),
                                OnlineStatusRecord {
                                    online: true,
                                    server_id: owner.gateway_id.clone(),
                                    gateway_id: Some(owner.gateway_id.clone()),
                                    cluster_id: None,
                                    last_seen: None,
                                    device_id: None,
                                    device_platform: None,
                                },
                            )
                        })
                })
                .collect())
        }
        async fn get_user_connections(
            &self,
            _tenant_id: &str,
            _user_id: &UserId,
        ) -> Result<Vec<Connection>> {
            Ok(Vec::new())
        }
        async fn remove_user_connections(
            &self,
            _tenant_id: &str,
            _user_id: &UserId,
            _device_ids: Option<&[DeviceId]>,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_connection_by_device(
            &self,
            _tenant_id: &str,
            _user_id: &UserId,
            _device_id: &DeviceId,
        ) -> Result<Option<Connection>> {
            Ok(None)
        }
        async fn list_user_connections(&self, _ctx: &Context) -> Result<Vec<Connection>> {
            Ok(Vec::new())
        }
        async fn scan_tenant_sessions(
            &self,
            _tenant_id: &str,
            _cursor: u64,
            _count: usize,
        ) -> Result<(u64, Vec<String>)> {
            Ok((0, Vec::new()))
        }
        async fn unindex_tenant_session(&self, _tenant_id: &str, _member: &str) -> Result<()> {
            Ok(())
        }
        async fn broadcast_user_kick(
            &self,
            _tenant_id: &str,
            _user_id: &str,
            _device_id: Option<&str>,
            _connection_id: Option<&str>,
            _reason: &str,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct NoSubscriptions;

    impl SubscriptionRepository for NoSubscriptions {
        async fn add_subscription(&self, _: &str, _: String, _: String) -> Result<()> {
            Ok(())
        }
        async fn remove_subscription(&self, _: &Context, _: &[String]) -> Result<()> {
            Ok(())
        }
        async fn get_user_subscriptions(
            &self,
            _: &Context,
        ) -> Result<Vec<(String, HashMap<String, String>)>> {
            Ok(Vec::new())
        }
        async fn get_topic_subscribers(&self, _: &str, _: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    /// 记下发布的 presence 事件。
    #[derive(Default)]
    struct RecordedPresence {
        presence: Mutex<Vec<(String, PresenceEvent)>>,
        user_presence: Mutex<Vec<(String, UserPresenceEvent)>>,
    }

    impl PresencePublisher for RecordedPresence {
        async fn publish_presence_event(
            &self,
            tenant_id: &str,
            event: PresenceEvent,
        ) -> Result<()> {
            self.presence
                .lock()
                .unwrap()
                .push((tenant_id.to_string(), event));
            Ok(())
        }
        async fn publish_user_presence_event(
            &self,
            tenant_id: &str,
            event: UserPresenceEvent,
        ) -> Result<()> {
            self.user_presence
                .lock()
                .unwrap()
                .push((tenant_id.to_string(), event));
            Ok(())
        }
    }

    type TestSweeper =
        GatewaySweeper<MemoryStore, SessionRepository, NoSubscriptions, RecordedPresence>;

    /// 租约一缺失就清扫（不等确认），用例只跑一轮。
    fn sweeper(store: &Arc<MemoryStore>, presence: &Arc<RecordedPresence>) -> TestSweeper {
        sweeper_confirming_after(store, presence, Duration::ZERO)
    }

    fn sweeper_confirming_after(
        store: &Arc<MemoryStore>,
        presence: &Arc<RecordedPresence>,
        confirm_after: Duration,
    ) -> TestSweeper {
        let commands = Arc::new(OnlineCommandHandler::new(
            Arc::new(OnlineStatusService::new(
                Arc::new(SessionRepository(store.sessions.clone())),
                "online-test".to_string(),
            )),
            Arc::new(SubscriptionService::new(
                Arc::new(NoSubscriptions),
                presence.clone(),
            )),
        ));
        GatewaySweeper::new(
            store.clone(),
            commands,
            Arc::new(OnlineSweeperMetrics::new()),
        )
        .with_config(GatewaySweeperConfig {
            confirm_after,
            ..GatewaySweeperConfig::default()
        })
    }

    /// 第一次看到租约缺失只记嫌疑（Redis 中断恢复时活网关还没来得及续期）；缺失持续到确认时长
    /// 才清扫，期间租约回来就撤销嫌疑、重新计时。
    #[tokio::test]
    async fn a_lease_missing_once_is_only_suspected_until_confirmed() {
        let store = Arc::new(MemoryStore::default());
        let presence = Arc::new(RecordedPresence::default());
        store.known.lock().unwrap().push("gw-1".to_string());
        store.add_session("gw-1", "gw-1", "tenant-a", "alice");
        let sweeper = sweeper_confirming_after(&store, &presence, Duration::from_millis(60));

        let report = sweeper.sweep_once().await.unwrap();
        assert_eq!((report.dead_gateways, report.removed), (0, 0));

        // 租约回来：嫌疑撤销。
        store.leases.lock().unwrap().insert("gw-1".to_string());
        sweeper.sweep_once().await.unwrap();
        store.leases.lock().unwrap().clear();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let report = sweeper.sweep_once().await.unwrap();
        assert_eq!(
            report.removed, 0,
            "the timer restarts after the lease came back"
        );
        assert_eq!(store.session_count(), 1);

        tokio::time::sleep(Duration::from_millis(80)).await;
        let report = sweeper.sweep_once().await.unwrap();
        assert_eq!((report.dead_gateways, report.removed), (1, 1));
        assert_eq!(store.session_count(), 0);
    }

    /// 租约缺失：这台网关的连接按正常断开移除，发出离线 presence（原因 `gateway_lost`），
    /// 索引删除、移出 gw:known；别的网关、以及清扫开始后才登记的会话不动。
    #[tokio::test]
    async fn connections_of_a_gateway_without_lease_are_removed_with_offline_events() {
        let store = Arc::new(MemoryStore::default());
        let presence = Arc::new(RecordedPresence::default());
        store
            .known
            .lock()
            .unwrap()
            .extend(["gw-dead".to_string(), "gw-live".to_string()]);
        store.leases.lock().unwrap().insert("gw-live".to_string());
        store.add_session("gw-dead", "gw-dead", "tenant-a", "alice");
        store.add_session("gw-dead", "gw-dead", "tenant-a", "bob");
        // 清扫开始之后才登记（网关带着同一 ID 回来）：不清。
        store.add_session_at("gw-dead", "gw-dead", "tenant-a", "late", i64::MAX);
        // 索引里的成员已无会话：只清索引。
        store
            .conns
            .lock()
            .unwrap()
            .get_mut("gw-dead")
            .unwrap()
            .push(connection_member(
                "tenant-a",
                "gone",
                &uuid::Uuid::new_v4().to_string(),
            ));
        store.add_session("gw-live", "gw-live", "tenant-a", "carol");

        let report = sweeper(&store, &presence).sweep_once().await.unwrap();

        assert!(report.leader);
        assert_eq!(report.dead_gateways, 1);
        assert_eq!(report.removed, 2);
        assert_eq!(report.retired, 1);
        let remaining: HashSet<String> = store
            .sessions
            .lock()
            .unwrap()
            .keys()
            .map(|(_, user, _)| user.clone())
            .collect();
        assert_eq!(
            remaining,
            HashSet::from(["late".to_string(), "carol".to_string()])
        );
        assert_eq!(*store.known.lock().unwrap(), vec!["gw-live".to_string()]);
        assert!(!store.conns.lock().unwrap().contains_key("gw-dead"));

        let events = presence.presence.lock().unwrap();
        let mut offline: Vec<&str> = events
            .iter()
            .map(|(tenant, event)| {
                assert_eq!(tenant, "tenant-a");
                assert_eq!(event.reason, "gateway_lost");
                assert!(!event.status.as_ref().unwrap().online);
                event.user_id.as_str()
            })
            .collect();
        offline.sort();
        assert_eq!(offline, vec!["alice", "bob"]);
        assert_eq!(presence.user_presence.lock().unwrap().len(), 2);
    }

    /// 租约在：什么都不动。
    #[tokio::test]
    async fn a_gateway_with_lease_is_left_alone() {
        let store = Arc::new(MemoryStore::default());
        let presence = Arc::new(RecordedPresence::default());
        store.known.lock().unwrap().push("gw-1".to_string());
        store.leases.lock().unwrap().insert("gw-1".to_string());
        store.add_session("gw-1", "gw-1", "tenant-a", "alice");

        let report = sweeper(&store, &presence).sweep_once().await.unwrap();

        assert!(report.leader);
        assert_eq!(report.dead_gateways, 0);
        assert_eq!(report.removed, 0);
        assert_eq!(store.session_count(), 1);
        assert!(presence.presence.lock().unwrap().is_empty());
        assert_eq!(*store.known.lock().unwrap(), vec!["gw-1".to_string()]);
    }

    /// 网关重启后把遗留连接转成孤儿网关：会话仍记在原网关名下，按原网关认领。
    #[tokio::test]
    async fn an_orphaned_connection_set_is_swept_under_its_real_gateway() {
        let store = Arc::new(MemoryStore::default());
        let presence = Arc::new(RecordedPresence::default());
        let orphan = orphan_gateway_id("gw-1", "b00t");
        store.known.lock().unwrap().push(orphan.clone());
        store.leases.lock().unwrap().insert("gw-1".to_string());
        store.add_session(&orphan, "gw-1", "tenant-a", "alice");

        let report = sweeper(&store, &presence).sweep_once().await.unwrap();

        assert_eq!(report.removed, 1);
        assert_eq!(report.retired, 1);
        assert_eq!(store.session_count(), 0);
        assert!(store.known.lock().unwrap().is_empty());
    }

    /// 单例锁：同一时刻只有一个实例在扫；持有者退出释放后，另一个实例接手。
    #[tokio::test]
    async fn only_the_lock_holder_sweeps() {
        let store = Arc::new(MemoryStore::default());
        let presence = Arc::new(RecordedPresence::default());
        store.known.lock().unwrap().push("gw-dead".to_string());
        store.add_session("gw-dead", "gw-dead", "tenant-a", "alice");
        let first = sweeper(&store, &presence);
        let second = sweeper(&store, &presence);

        assert!(
            store
                .hold_sweeper_lock(&first.token, Duration::from_secs(15))
                .await
                .unwrap()
        );
        let report = second.sweep_once().await.unwrap();
        assert!(!report.leader, "a second instance must not sweep");
        assert_eq!(store.session_count(), 1);

        first.run(async {}).await;
        assert!(
            store.lock.lock().unwrap().is_none(),
            "the holder releases on exit"
        );
        let report = second.sweep_once().await.unwrap();
        assert!(report.leader);
        assert_eq!(report.removed, 1);
        assert_eq!(store.session_count(), 0);
    }
}
