//! Evict online connections below a raised build floor.
//!
//! The version gate (`domain::service::client_build_gate`) runs at CONNECT. When an app's
//! `client_policy` later raises the floor (a higher `min_build`, a `min_build` that reaches its
//! effective time, or a new blocked range), connections already online keep running the old build.
//! This sweeps this node's authenticated connections, applies the same verdict, and closes each
//! one that CONNECT would now refuse, after writing the same typed refusal CONNECT sends:
//! `UPGRADE_REQUIRED` with `min_build` (`flare_core::common::link::upgrade_required_command`).
//!
//! Triggers:
//! - a `flare.access` / `flare.release` notice of the app (the projection of `client_policy`
//!   changed): the cached policy is dropped and the app's connections are swept;
//! - a periodic sweep of every app on this node (`FLARE_BUILD_FLOOR_SWEEP_SECS`, default 60,
//!   `0` = off). It covers floors that become effective by time and policy changes that move no
//!   watermark and so send no notice.
//!
//! Evictions are spread: the closes of one sweep are paced over
//! `FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS` (default 30) and at most
//! `FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE` per second (default 200), so a floor raise does not
//! send every old client back to CONNECT at the same instant. Each connection is judged again
//! with the then current policy right before it is closed: a floor lowered inside the window
//! spares the rest. Every close counts in `connections_evicted_total{reason}`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use flare_core::common::MessageParser;
use flare_core::common::link::upgrade_required_command;
use flare_core::common::protocol::{Reliability, frame_with_system_command};
use flare_core::server::connection::{ConnectionManagerTrait, TraitConnectionInfo};
use flare_core::transport::connection::Connection;
use flare_im_core_service_kit::metrics::AccessGatewayMetrics;
use flare_im_core_service_kit::tenant_notice::{ACCESS_TOPIC, ALL_TENANTS, RELEASE_TOPIC};
use flare_im_core_service_kit::tenant_runtime::TenantRuntimeCache;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::constants::{METADATA_KEY_APP_BUILD, METADATA_KEY_DISTRIBUTION, METADATA_KEY_TENANT_ID};
use crate::domain::service::client_build_gate::{
    LiveClientBuild, UpgradeRequired, connections_below_floor, reported_build,
};

/// Periodic sweep interval: `FLARE_BUILD_FLOOR_SWEEP_SECS`.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Window one sweep's closes are spread over: `FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS`.
pub const DEFAULT_EVICTION_WINDOW: Duration = Duration::from_secs(30);
/// Closes per second one node may cause: `FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE`.
pub const DEFAULT_EVICTIONS_PER_SECOND: u32 = 200;
/// The refusal frame waits at most this long; a peer that does not read cannot hold the close.
const REFUSAL_SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// How the closes of one sweep are spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionPacing {
    pub window: Duration,
    pub max_per_second: u32,
}

impl Default for EvictionPacing {
    fn default() -> Self {
        Self {
            window: DEFAULT_EVICTION_WINDOW,
            max_per_second: DEFAULT_EVICTIONS_PER_SECOND,
        }
    }
}

impl EvictionPacing {
    /// From `FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS` and `FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE`.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            window: env_u64("FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS")
                .map(Duration::from_secs)
                .unwrap_or(defaults.window),
            max_per_second: env_u64("FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE")
                .and_then(|rate| u32::try_from(rate).ok())
                .filter(|rate| *rate > 0)
                .unwrap_or(defaults.max_per_second),
        }
    }

    /// When each of `count` closes happens, from the start of the sweep: evenly over the window,
    /// stretched when needed so consecutive closes are at least `1 / max_per_second` apart.
    /// The first close is immediate.
    pub fn offsets(&self, count: usize) -> Vec<Duration> {
        if count == 0 {
            return Vec::new();
        }
        let rate = u128::from(self.max_per_second.max(1));
        let count = count as u128;
        let window_ms = self.window.as_millis();
        // Evenly over the window, but never closer together than 1/rate.
        (0..count)
            .map(|index| {
                let offset = (window_ms * index / count).max(index * 1_000 / rate);
                Duration::from_millis(u64::try_from(offset).unwrap_or(u64::MAX))
            })
            .collect()
    }
}

/// The periodic sweep interval from `FLARE_BUILD_FLOOR_SWEEP_SECS`; `None` when set to `0`.
pub fn sweep_interval_from_env() -> Option<Duration> {
    match env_u64("FLARE_BUILD_FLOOR_SWEEP_SECS") {
        Some(0) => None,
        Some(seconds) => Some(Duration::from_secs(seconds)),
        None => Some(DEFAULT_SWEEP_INTERVAL),
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

/// Whether a notice topic says the app's `client_policy` projection changed.
pub fn is_client_policy_topic(topic: &str) -> bool {
    topic == ACCESS_TOPIC || topic == RELEASE_TOPIC
}

/// The gate input of one live connection: authenticated, with a tenant and CONNECT device info.
/// `None` for connections the gate never judged (not authenticated, no device info).
pub fn live_client_build(info: &TraitConnectionInfo) -> Option<(String, LiveClientBuild)> {
    if !info.authenticated {
        return None;
    }
    let tenant_id = info.metadata.get(METADATA_KEY_TENANT_ID)?.clone();
    let device = info.device_info.as_ref()?;
    Some((
        tenant_id,
        LiveClientBuild {
            connection_id: info.connection_id.clone(),
            device_platform: device.platform.as_str().to_string(),
            distribution: device.metadata.get(METADATA_KEY_DISTRIBUTION).cloned(),
            app_build: reported_build(
                device
                    .metadata
                    .get(METADATA_KEY_APP_BUILD)
                    .map(String::as_str),
            ),
        },
    ))
}

/// Sweeps this node's connections against the current build floors and closes the ones below.
pub struct BuildFloorEvictor {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    tenant_runtime: TenantRuntimeCache,
    metrics: Arc<AccessGatewayMetrics>,
    pacing: EvictionPacing,
    /// Connections a sweep has scheduled and not yet handled; a later sweep skips them.
    scheduled: std::sync::Mutex<HashSet<String>>,
}

impl BuildFloorEvictor {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        tenant_runtime: TenantRuntimeCache,
        metrics: Arc<AccessGatewayMetrics>,
        pacing: EvictionPacing,
    ) -> Self {
        Self {
            connection_manager,
            tenant_runtime,
            metrics,
            pacing,
            scheduled: std::sync::Mutex::new(HashSet::new()),
        }
    }

    /// Start the periodic sweep (no-op without a tenant runtime database or with `interval` unset).
    pub fn spawn_periodic(self: &Arc<Self>, interval: Option<Duration>) {
        let Some(interval) = interval else {
            info!("build floor sweep disabled");
            return;
        };
        if !self.tenant_runtime.is_enabled() {
            return;
        }
        let evictor = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick is immediate; connections that just passed CONNECT need no sweep.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                evictor.sweep(ALL_TENANTS).await;
            }
        });
        info!(
            interval_secs = interval.as_secs(),
            "build floor sweep spawned"
        );
    }

    /// A notice of `tenant_id` (`*` = every app) on `topic`: when it is a `client_policy` topic,
    /// drop the cached policy and sweep that app's connections in the background.
    pub fn on_notice(self: &Arc<Self>, tenant_id: &str, topic: &str) {
        if !is_client_policy_topic(topic) || !self.tenant_runtime.is_enabled() {
            return;
        }
        let evictor = self.clone();
        let tenant_id = tenant_id.to_string();
        tokio::spawn(async move {
            evictor.sweep(&tenant_id).await;
        });
    }

    /// Sweep the connections of `tenant_id` (`*` = every app) on this node: schedule the paced
    /// close of each one CONNECT would now refuse. Returns how many were scheduled.
    pub async fn sweep(self: &Arc<Self>, tenant_id: &str) -> usize {
        let by_tenant = self.live_builds(tenant_id).await;
        let now = now_ms();
        let mut targets: Vec<(String, LiveClientBuild, UpgradeRequired)> = Vec::new();
        for (tenant, connections) in by_tenant {
            if tenant_id != ALL_TENANTS {
                self.tenant_runtime.invalidate_client_policy(&tenant).await;
            }
            let Some(policy) = self.tenant_runtime.client_policy(&tenant).await else {
                continue;
            };
            for (connection, verdict) in connections_below_floor(&policy, &connections, now) {
                targets.push((tenant.clone(), connection.clone(), verdict));
            }
        }
        let targets = self.claim(targets);
        if targets.is_empty() {
            return 0;
        }
        let offsets = self.pacing.offsets(targets.len());
        let scheduled = targets.len();
        info!(
            tenant_id = %tenant_id,
            connections = scheduled,
            spread_ms = offsets.last().map_or(0, |last| last.as_millis() as u64),
            "connections below the raised build floor scheduled for eviction"
        );
        for ((tenant, connection, _), offset) in targets.into_iter().zip(offsets) {
            let evictor = self.clone();
            tokio::spawn(async move {
                if !offset.is_zero() {
                    tokio::time::sleep(offset).await;
                }
                evictor.evict(&tenant, &connection).await;
                evictor.release(&connection.connection_id);
            });
        }
        scheduled
    }

    /// This node's live connections of `tenant_id` (`*` = every app), by tenant.
    async fn live_builds(&self, tenant_id: &str) -> BTreeMap<String, Vec<LiveClientBuild>> {
        let mut by_tenant: BTreeMap<String, Vec<LiveClientBuild>> = BTreeMap::new();
        for connection_id in self.connection_manager.list_connections().await {
            let Some((_, info)) = self.connection_manager.get_connection(&connection_id).await
            else {
                continue;
            };
            let Some((tenant, live)) = live_client_build(&info) else {
                continue;
            };
            if tenant_id == ALL_TENANTS || tenant == tenant_id {
                by_tenant.entry(tenant).or_default().push(live);
            }
        }
        by_tenant
    }

    /// Keep the targets no earlier sweep has scheduled, and mark them scheduled.
    fn claim<T>(
        &self,
        targets: Vec<(String, LiveClientBuild, T)>,
    ) -> Vec<(String, LiveClientBuild, T)> {
        let mut scheduled = self
            .scheduled
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets
            .into_iter()
            .filter(|(_, connection, _)| scheduled.insert(connection.connection_id.clone()))
            .collect()
    }

    fn release(&self, connection_id: &str) {
        self.scheduled
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(connection_id);
    }

    /// Judge the connection again with the current policy and, when it is still below the floor,
    /// write the refusal and close it. Returns whether it was closed.
    async fn evict(&self, tenant_id: &str, connection: &LiveClientBuild) -> bool {
        let Some((handle, info)) = self
            .connection_manager
            .get_connection(&connection.connection_id)
            .await
        else {
            return false;
        };
        let Some(policy) = self.tenant_runtime.client_policy(tenant_id).await else {
            return false;
        };
        let Some(verdict) = connection.verdict(&policy, now_ms()) else {
            debug!(connection_id = %connection.connection_id, "build floor lowered again; connection kept");
            return false;
        };
        send_upgrade_required(&handle, &info, verdict.min_build).await;
        let closed = self
            .connection_manager
            .remove_connection(&connection.connection_id)
            .await
            .is_ok();
        if closed {
            self.metrics
                .record_connection_evicted(verdict.reason.metric_label());
            info!(
                connection_id = %connection.connection_id,
                tenant_id = %tenant_id,
                app_build = connection.app_build,
                min_build = verdict.min_build,
                reason = verdict.reason.metric_label(),
                "online connection closed: client build must be upgraded"
            );
        }
        closed
    }
}

/// Write `UPGRADE_REQUIRED` with `min_build` in the connection's negotiated encoding, the same
/// typed refusal CONNECT sends. `release_hint` stays empty like at CONNECT: the gateway only has
/// the projected floor, the release notes are in the client's verified release manifest. Written
/// straight to the connection, not through the write queue the following close shuts.
async fn send_upgrade_required(
    connection: &Arc<Mutex<Box<dyn Connection>>>,
    info: &TraitConnectionInfo,
    min_build: u64,
) {
    let frame = frame_with_system_command(
        upgrade_required_command(min_build, None),
        Reliability::BestEffort,
    );
    let encoded = match &info.cached_parser {
        Some(parser) => parser.serialize(&frame),
        None => MessageParser::new(
            info.serialization_format,
            info.compression.clone(),
            info.encryption.clone(),
        )
        .serialize(&frame),
    };
    let data = match encoded {
        Ok(data) => data,
        Err(error) => {
            warn!(connection_id = %info.connection_id, %error, "UPGRADE_REQUIRED frame not encodable");
            return;
        }
    };
    let sent = tokio::time::timeout(REFUSAL_SEND_TIMEOUT, async {
        connection.lock().await.send(&data).await
    })
    .await;
    if !matches!(sent, Ok(Ok(()))) {
        debug!(connection_id = %info.connection_id, "UPGRADE_REQUIRED frame not delivered before close");
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use flare_core::common::DevicePlatform;
    use flare_core::common::device::DeviceInfo;
    use flare_im_core_contracts::domain::tenant::{
        ClientBuildPolicy, TenantClientPolicy, TenantRuntimeSnapshot,
    };
    use flare_im_core_service_kit::tenant_runtime::{
        TenantRuntimeCacheOptions, TenantRuntimeSource,
    };
    use std::collections::HashMap;

    #[test]
    fn closes_are_spread_over_the_window() {
        let pacing = EvictionPacing {
            window: Duration::from_secs(30),
            max_per_second: 200,
        };
        assert!(pacing.offsets(0).is_empty());
        assert_eq!(pacing.offsets(1), vec![Duration::ZERO]);
        assert_eq!(
            pacing.offsets(3),
            vec![
                Duration::ZERO,
                Duration::from_secs(10),
                Duration::from_secs(20)
            ]
        );
        let offsets = pacing.offsets(600);
        assert_eq!(offsets.len(), 600);
        assert_eq!(offsets[0], Duration::ZERO);
        assert!(offsets.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(*offsets.last().unwrap() < Duration::from_secs(30));
    }

    #[test]
    fn many_closes_stretch_the_window_to_respect_the_rate() {
        let pacing = EvictionPacing {
            window: Duration::from_secs(30),
            max_per_second: 100,
        };
        // 10 001 closes at 100/s need 100 s, more than the 30 s window
        let offsets = pacing.offsets(10_001);
        let last = *offsets.last().unwrap();
        assert!(last >= Duration::from_millis(99_900), "{last:?}");
        assert!(last <= Duration::from_secs(100), "{last:?}");
        // No second holds more than the rate allows (plus one for the boundary)
        let mut per_second: HashMap<u64, u32> = HashMap::new();
        for offset in &offsets {
            *per_second.entry(offset.as_secs()).or_default() += 1;
        }
        assert!(
            per_second.values().all(|count| *count <= 101),
            "{per_second:?}"
        );

        // A zero window still respects the rate; a zero rate counts as one per second
        let instant = EvictionPacing {
            window: Duration::ZERO,
            max_per_second: 0,
        };
        assert_eq!(
            instant.offsets(3),
            vec![
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(2)
            ]
        );
    }

    #[test]
    fn only_client_policy_topics_trigger_a_sweep() {
        assert!(is_client_policy_topic(ACCESS_TOPIC));
        assert!(is_client_policy_topic(RELEASE_TOPIC));
        assert!(!is_client_policy_topic(
            flare_im_core_service_kit::tenant_notice::PLUGIN_CATALOG_TOPIC
        ));
        assert!(!is_client_policy_topic("social.features"));
    }

    struct Silent {
        sent: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        last_active: flare_core::common::platform::MonotonicInstant,
    }

    #[async_trait]
    impl Connection for Silent {
        fn add_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}
        async fn send(&mut self, data: &[u8]) -> flare_core::common::error::Result<()> {
            self.sent.lock().unwrap().push(data.to_vec());
            Ok(())
        }
        async fn close(&mut self) -> flare_core::common::error::Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> flare_core::common::platform::MonotonicInstant {
            self.last_active
        }
        fn update_active_time(&mut self) {
            self.last_active = flare_core::common::platform::monotonic_now();
        }
    }

    /// One connection of `tenant_id`; `build` = `None` leaves `app_build` unreported. Returns what
    /// the connection was sent.
    async fn connect(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
        tenant_id: &str,
        platform: DevicePlatform,
        build: Option<u64>,
        authenticated: bool,
    ) -> Arc<std::sync::Mutex<Vec<Vec<u8>>>> {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        manager
            .add_connection(
                connection_id.into(),
                Box::new(Silent {
                    sent: sent.clone(),
                    last_active: flare_core::common::platform::monotonic_now(),
                }),
                Some("u1".into()),
                true,
            )
            .unwrap();
        let mut device = DeviceInfo::new(format!("device-{connection_id}"), platform);
        if let Some(build) = build {
            device = device.with_metadata(METADATA_KEY_APP_BUILD.to_string(), build.to_string());
        }
        manager
            .update_connection_negotiation(
                connection_id,
                Some(device),
                flare_core::common::protocol::SerializationFormat::Json,
                flare_core::common::CompressionAlgorithm::None,
                flare_core::common::EncryptionAlgorithm::None,
                Some("u1".into()),
                Some(HashMap::from([(
                    METADATA_KEY_TENANT_ID.to_string(),
                    tenant_id.to_string(),
                )])),
            )
            .unwrap();
        if authenticated {
            ConnectionManagerTrait::set_connection_authenticated(
                manager,
                connection_id,
                Some("u1".into()),
            )
            .await
            .unwrap();
        }
        sent
    }

    /// In-memory `client_policy` projection.
    struct Policies(std::sync::Mutex<HashMap<String, TenantClientPolicy>>);

    #[async_trait]
    impl TenantRuntimeSource for Policies {
        async fn load(&self, _: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(None)
        }
        async fn load_client_policy(
            &self,
            tenant_id: &str,
        ) -> Result<Option<TenantClientPolicy>, String> {
            Ok(self.0.lock().unwrap().get(tenant_id).cloned())
        }
    }

    fn android_floor(tenant_id: &str, min_build: u64) -> TenantClientPolicy {
        TenantClientPolicy {
            tenant_id: tenant_id.to_string(),
            build_policies: std::collections::BTreeMap::from([(
                "android".to_string(),
                ClientBuildPolicy {
                    min_build,
                    ..ClientBuildPolicy::default()
                },
            )]),
            ..TenantClientPolicy::default()
        }
    }

    async fn wait_until_removed(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
    ) {
        for _ in 0..200 {
            if ConnectionManagerTrait::get_connection(manager, connection_id)
                .await
                .is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{connection_id} was not evicted");
    }

    /// A raised floor closes the connections of that app below it, after the typed refusal, and
    /// leaves the rest of the app and the other apps alone.
    #[tokio::test]
    async fn a_raised_floor_closes_the_connections_below_it_after_the_refusal() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        let old = connect(
            &manager,
            "old",
            "app-a",
            DevicePlatform::Android,
            Some(150),
            true,
        )
        .await;
        let unreported = connect(
            &manager,
            "unreported",
            "app-a",
            DevicePlatform::Android,
            None,
            true,
        )
        .await;
        let current = connect(
            &manager,
            "current",
            "app-a",
            DevicePlatform::Android,
            Some(200),
            true,
        )
        .await;
        let anonymous = connect(
            &manager,
            "anonymous",
            "app-a",
            DevicePlatform::Android,
            Some(1),
            false,
        )
        .await;
        let other_app = connect(
            &manager,
            "other-app",
            "app-b",
            DevicePlatform::Android,
            Some(1),
            true,
        )
        .await;

        let policies = Arc::new(Policies(std::sync::Mutex::new(HashMap::from([
            // No floor yet: an unreported build counts as 0, so any floor would select it.
            ("app-a".to_string(), android_floor("app-a", 0)),
            ("app-b".to_string(), android_floor("app-b", 0)),
        ]))));
        let cache = TenantRuntimeCache::with_source(
            policies.clone(),
            TenantRuntimeCacheOptions {
                refresh_interval: Duration::from_secs(60),
                capacity: 16,
            },
        );
        let metrics = Arc::new(AccessGatewayMetrics::new());
        let evictor = Arc::new(BuildFloorEvictor::new(
            manager.clone() as Arc<dyn ConnectionManagerTrait>,
            cache,
            metrics.clone(),
            EvictionPacing {
                window: Duration::ZERO,
                max_per_second: 1_000,
            },
        ));

        // Nothing is below the current floor.
        assert_eq!(evictor.sweep("app-a").await, 0);

        // The floor goes up to 200: a notice-driven sweep re-reads the policy at once.
        policies
            .0
            .lock()
            .unwrap()
            .insert("app-a".to_string(), android_floor("app-a", 200));
        assert_eq!(evictor.sweep("app-a").await, 2);
        wait_until_removed(&manager, "old").await;
        wait_until_removed(&manager, "unreported").await;

        for sent in [&old, &unreported] {
            let frames = sent.lock().unwrap();
            assert_eq!(frames.len(), 1);
            let text = String::from_utf8_lossy(&frames[0]);
            assert!(text.contains("UPGRADE_REQUIRED"), "{text}");
        }
        for (id, sent) in [
            ("current", &current),
            ("anonymous", &anonymous),
            ("other-app", &other_app),
        ] {
            assert!(
                ConnectionManagerTrait::get_connection(manager.as_ref(), id)
                    .await
                    .is_some(),
                "{id}"
            );
            assert!(sent.lock().unwrap().is_empty(), "{id}");
        }
        assert_eq!(
            metrics
                .connections_evicted_total
                .with_label_values(&["below_min_build"])
                .get(),
            2
        );
    }

    /// A floor lowered before a scheduled close spares the connection.
    #[tokio::test]
    async fn a_floor_lowered_inside_the_window_spares_the_connection() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        let sent = connect(
            &manager,
            "old",
            "app-a",
            DevicePlatform::Android,
            Some(1),
            true,
        )
        .await;
        let policies = Arc::new(Policies(std::sync::Mutex::new(HashMap::from([(
            "app-a".to_string(),
            android_floor("app-a", 100),
        )]))));
        let cache = TenantRuntimeCache::with_source(
            policies.clone(),
            TenantRuntimeCacheOptions {
                refresh_interval: Duration::from_secs(60),
                capacity: 16,
            },
        );
        let metrics = Arc::new(AccessGatewayMetrics::new());
        let evictor = Arc::new(BuildFloorEvictor::new(
            manager.clone() as Arc<dyn ConnectionManagerTrait>,
            cache.clone(),
            metrics.clone(),
            EvictionPacing::default(),
        ));
        let connection = LiveClientBuild {
            connection_id: "old".to_string(),
            device_platform: "android".to_string(),
            distribution: None,
            app_build: 1,
        };
        policies
            .0
            .lock()
            .unwrap()
            .insert("app-a".to_string(), android_floor("app-a", 0));
        cache.invalidate_client_policy("app-a").await;
        assert!(!evictor.evict("app-a", &connection).await);
        assert!(
            ConnectionManagerTrait::get_connection(manager.as_ref(), "old")
                .await
                .is_some()
        );
        assert!(sent.lock().unwrap().is_empty());
        assert_eq!(
            metrics
                .connections_evicted_total
                .with_label_values(&["below_min_build"])
                .get(),
            0
        );
    }

    /// A connection already scheduled by one sweep is not scheduled again by the next.
    #[test]
    fn a_scheduled_connection_is_claimed_once() {
        let evictor = BuildFloorEvictor::new(
            Arc::new(flare_core::server::connection::ConnectionManager::new())
                as Arc<dyn ConnectionManagerTrait>,
            TenantRuntimeCache::disabled(),
            Arc::new(AccessGatewayMetrics::new()),
            EvictionPacing::default(),
        );
        let target = |id: &str| {
            (
                "app-a".to_string(),
                LiveClientBuild {
                    connection_id: id.to_string(),
                    device_platform: "android".to_string(),
                    distribution: None,
                    app_build: 1,
                },
                (),
            )
        };
        assert_eq!(evictor.claim(vec![target("a"), target("b")]).len(), 2);
        assert_eq!(evictor.claim(vec![target("a"), target("c")]).len(), 1);
        evictor.release("a");
        assert_eq!(evictor.claim(vec![target("a")]).len(), 1);
    }
}
