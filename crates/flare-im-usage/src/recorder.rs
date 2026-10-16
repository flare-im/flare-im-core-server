//! In-process window aggregation. Recording is one short lock and a hash lookup; the flusher
//! takes the window out under the same lock and builds events outside it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::event::{day_of_ms, now_ms, user_hash};
use crate::meter::{CONNECTION_MINUTES, CONNECTIONS_PEAK, Counter, PushOutcome};
use crate::metrics::{RecordDrop, metrics};
use crate::reporter::UsageLimits;

const MINUTE_MS: u64 = 60_000;

/// Cheap, cloneable handle the counting points hold. A disabled handle (no usage stream
/// configured, or tests) ignores every record.
#[derive(Clone, Default)]
pub struct UsageRecorder {
    inner: Option<Arc<Shared>>,
}

impl std::fmt::Debug for UsageRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageRecorder")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

pub(crate) struct Shared {
    pub(crate) producer: &'static str,
    pub(crate) limits: UsageLimits,
    pub(crate) state: Mutex<WindowState>,
}

impl UsageRecorder {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// A recorder that counts but is never flushed: for tests of counting points, read back
    /// with [`Self::live`].
    pub fn unpublished(producer: &'static str) -> Self {
        Self::enabled(Arc::new(Shared {
            producer,
            limits: UsageLimits::default(),
            state: Mutex::new(WindowState::default()),
        }))
    }

    pub(crate) fn enabled(shared: Arc<Shared>) -> Self {
        Self {
            inner: Some(shared),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// The broker accepted a message at ingest.
    pub fn message_accepted(&self, tenant: &str, category: &'static str) {
        self.count(tenant, Counter::MessagesAccepted { category }, 1);
    }

    /// The storage writer committed `count` new messages of the tenant.
    pub fn messages_persisted(&self, tenant: &str, count: u64) {
        self.count(tenant, Counter::MessagesPersisted, count);
    }

    /// One offline push delivery attempt finished with `outcome`.
    pub fn push_attempt(&self, tenant: &str, provider: &'static str, outcome: PushOutcome) {
        self.count(
            tenant,
            Counter::PushAttempts {
                provider,
                result: outcome.as_str(),
            },
            1,
        );
    }

    /// An upload completed and stored `bytes` new object bytes.
    pub fn media_stored(&self, tenant: &str, bytes: u64) {
        self.count(tenant, Counter::MediaBytesStored { op: "added" }, bytes);
    }

    /// An object of `bytes` was deleted from storage.
    pub fn media_removed(&self, tenant: &str, bytes: u64) {
        self.count(tenant, Counter::MediaBytesStored { op: "removed" }, bytes);
    }

    /// A download of `bytes` was signed or served.
    pub fn media_egress(&self, tenant: &str, bytes: u64) {
        self.count(tenant, Counter::MediaBytesEgress, bytes);
    }

    /// An authenticated public API request of `route_group`.
    pub fn api_call(&self, tenant: &str, route_group: &'static str) {
        self.count(tenant, Counter::ApiCalls { route_group }, 1);
    }

    /// An online session of the tenant registered on this instance. Pair every call with
    /// exactly one [`Self::connection_closed`].
    pub fn connection_opened(&self, tenant: &str) {
        self.with_valid_tenant(tenant, |_, state| {
            state.connection_opened(tenant, Instant::now());
        });
    }

    /// An online session registered with [`Self::connection_opened`] closed.
    pub fn connection_closed(&self, tenant: &str) {
        self.with_valid_tenant(tenant, |_, state| {
            state.connection_closed(tenant, Instant::now());
        });
    }

    /// The user came online; counted once per tenant, user and UTC day on this instance.
    pub fn user_active(&self, tenant: &str, user_id: &str) {
        if user_id.is_empty() {
            return;
        }
        let day = day_of_ms(now_ms());
        let hash = user_hash(tenant, user_id);
        self.with_valid_tenant(tenant, |shared, state| {
            if !state.user_active(tenant, day, hash, &shared.limits) {
                record_drop(shared.producer, RecordDrop::ActiveUserLimit);
            }
        });
    }

    /// The window in progress, without closing it: counters so far and each tenant's
    /// connection peak so far, sorted by tenant. This is the reading behind a live usage view
    /// (`GET /usage/live` in the design); a disabled recorder has none.
    pub fn live(&self) -> Vec<LiveReading> {
        match &self.inner {
            Some(shared) => shared.state.lock().live(),
            None => Vec::new(),
        }
    }

    fn count(&self, tenant: &str, counter: Counter, value: u64) {
        if value == 0 {
            return;
        }
        self.with_valid_tenant(tenant, |shared, state| {
            if !state.add(tenant, counter, value, &shared.limits) {
                record_drop(shared.producer, RecordDrop::CounterLimit);
            }
        });
    }

    fn with_valid_tenant(&self, tenant: &str, f: impl FnOnce(&Shared, &mut WindowState)) {
        let Some(shared) = &self.inner else {
            return;
        };
        if !flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant) {
            record_drop(shared.producer, RecordDrop::InvalidTenant);
            return;
        }
        let mut state = shared.state.lock();
        f(shared, &mut state);
    }
}

fn record_drop(producer: &str, reason: RecordDrop) {
    metrics()
        .records_dropped
        .with_label_values(&[producer, reason.as_str()])
        .inc();
}

/// One reading of the window in progress, see [`UsageRecorder::live`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveReading {
    pub app: String,
    pub meter: &'static str,
    pub dims: Vec<(&'static str, &'static str)>,
    pub value: u64,
}

/// One meter reading of a tenant in a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeterReading {
    pub(crate) meter: &'static str,
    pub(crate) dims: Vec<(&'static str, &'static str)>,
    pub(crate) value: u64,
}

/// What a tenant used in one window.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TenantWindow {
    pub(crate) meters: Vec<MeterReading>,
    /// Active-user hashes by UTC day (`yyyymmdd`).
    pub(crate) active_users: BTreeMap<u32, Vec<u64>>,
}

impl TenantWindow {
    fn is_empty(&self) -> bool {
        self.meters.is_empty() && self.active_users.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct WindowState {
    counters: HashMap<String, HashMap<Counter, u64>>,
    counter_series: usize,
    connections: HashMap<String, ConnectionGauge>,
    active: HashMap<String, ActiveUsers>,
    /// Hashes held in the per-day `seen` sets of all tenants.
    active_seen: usize,
    /// Hashes waiting for the next event.
    active_pending: usize,
}

struct ConnectionGauge {
    current: u64,
    peak: u64,
    since: Instant,
    /// Connected milliseconds not yet reported as whole minutes.
    connected_ms: u64,
}

impl ConnectionGauge {
    fn advance(&mut self, now: Instant) {
        let elapsed = u64::try_from(now.saturating_duration_since(self.since).as_millis())
            .unwrap_or(u64::MAX);
        self.connected_ms = self
            .connected_ms
            .saturating_add(self.current.saturating_mul(elapsed));
        self.since = now;
    }
}

#[derive(Default)]
struct ActiveUsers {
    day: u32,
    /// Users already reported for `day`; bounded by `max_tracked_active_users` overall.
    seen: HashSet<u64>,
    pending: BTreeMap<u32, Vec<u64>>,
}

impl WindowState {
    /// Adds to a counter; `false` when a new series would exceed the limit.
    pub(crate) fn add(
        &mut self,
        tenant: &str,
        counter: Counter,
        value: u64,
        limits: &UsageLimits,
    ) -> bool {
        if let Some(series) = self.counters.get_mut(tenant) {
            if let Some(total) = series.get_mut(&counter) {
                *total = total.saturating_add(value);
                return true;
            }
            if self.counter_series >= limits.max_counter_series {
                return false;
            }
            series.insert(counter, value);
            self.counter_series += 1;
            return true;
        }
        if self.counter_series >= limits.max_counter_series {
            return false;
        }
        self.counters
            .insert(tenant.to_owned(), HashMap::from([(counter, value)]));
        self.counter_series += 1;
        true
    }

    pub(crate) fn connection_opened(&mut self, tenant: &str, now: Instant) {
        // Once per session, so the key allocation does not matter here.
        let gauge = self
            .connections
            .entry(tenant.to_owned())
            .or_insert(ConnectionGauge {
                current: 0,
                peak: 0,
                since: now,
                connected_ms: 0,
            });
        gauge.advance(now);
        gauge.current += 1;
        gauge.peak = gauge.peak.max(gauge.current);
    }

    pub(crate) fn connection_closed(&mut self, tenant: &str, now: Instant) {
        // A close without an open (the gauge was already settled at zero) has nothing to undo.
        if let Some(gauge) = self.connections.get_mut(tenant) {
            gauge.advance(now);
            gauge.current = gauge.current.saturating_sub(1);
        }
    }

    /// Queues the user hash for the day unless already reported; `false` when the pending
    /// hashes are at their limit.
    pub(crate) fn user_active(
        &mut self,
        tenant: &str,
        day: u32,
        hash: u64,
        limits: &UsageLimits,
    ) -> bool {
        let users = self.active.entry(tenant.to_owned()).or_default();
        if day > users.day {
            self.active_seen -= users.seen.len();
            users.seen.clear();
            users.day = day;
        }
        // A record for an earlier day (clock step back) is queued but not deduplicated.
        let tracked = day == users.day;
        if tracked && users.seen.contains(&hash) {
            return true;
        }
        if self.active_pending >= limits.max_pending_active_users {
            return false;
        }
        users.pending.entry(day).or_default().push(hash);
        self.active_pending += 1;
        // Past the tracking limit a user may be reported again later; the consumer upserts by
        // `(tenant, day, user_hash)`, so a repeat costs bytes, not correctness.
        if tracked && self.active_seen < limits.max_tracked_active_users && users.seen.insert(hash)
        {
            self.active_seen += 1;
        }
        true
    }

    pub(crate) fn live(&self) -> Vec<LiveReading> {
        let mut readings: Vec<(&str, Counter, u64)> = self
            .counters
            .iter()
            .flat_map(|(app, series)| {
                series
                    .iter()
                    .map(move |(counter, value)| (app.as_str(), *counter, *value))
            })
            .collect();
        readings.sort_unstable_by_key(|(app, counter, _)| (*app, *counter));
        let mut live: Vec<LiveReading> = readings
            .into_iter()
            .map(|(app, counter, value)| LiveReading {
                app: app.to_owned(),
                meter: counter.meter(),
                dims: counter.dims(),
                value,
            })
            .collect();
        live.extend(
            self.connections
                .iter()
                .filter(|(_, gauge)| gauge.peak > 0)
                .map(|(app, gauge)| LiveReading {
                    app: app.clone(),
                    meter: CONNECTIONS_PEAK,
                    dims: Vec::new(),
                    value: gauge.peak,
                }),
        );
        live.sort_by(|a, b| a.app.cmp(&b.app));
        live
    }

    /// Takes everything the window counted. Connection gauges keep their current value; the
    /// peak restarts from it.
    pub(crate) fn take_window(&mut self, now: Instant) -> BTreeMap<String, TenantWindow> {
        let mut tenants: BTreeMap<String, TenantWindow> = BTreeMap::new();

        self.counter_series = 0;
        for (tenant, series) in std::mem::take(&mut self.counters) {
            let mut readings: Vec<_> = series.into_iter().collect();
            readings.sort_unstable_by_key(|(counter, _)| *counter);
            tenants
                .entry(tenant)
                .or_default()
                .meters
                .extend(readings.into_iter().map(|(counter, value)| MeterReading {
                    meter: counter.meter(),
                    dims: counter.dims(),
                    value,
                }));
        }

        self.connections.retain(|tenant, gauge| {
            gauge.advance(now);
            let idle = gauge.current == 0;
            let minutes = if idle {
                // Settle the remainder (rounded) so an idle tenant leaves no state behind.
                (gauge.connected_ms + MINUTE_MS / 2) / MINUTE_MS
            } else {
                gauge.connected_ms / MINUTE_MS
            };
            gauge.connected_ms -= (minutes * MINUTE_MS).min(gauge.connected_ms);
            let window = tenants.entry(tenant.clone()).or_default();
            if gauge.peak > 0 {
                window.meters.push(MeterReading {
                    meter: CONNECTIONS_PEAK,
                    dims: Vec::new(),
                    value: gauge.peak,
                });
            }
            if minutes > 0 {
                window.meters.push(MeterReading {
                    meter: CONNECTION_MINUTES,
                    dims: Vec::new(),
                    value: minutes,
                });
            }
            gauge.peak = gauge.current;
            !idle
        });

        self.active_pending = 0;
        let today = day_of_ms(now_ms());
        let mut seen_released = 0;
        self.active.retain(|tenant, users| {
            if !users.pending.is_empty() {
                tenants.entry(tenant.clone()).or_default().active_users =
                    std::mem::take(&mut users.pending);
            }
            // A tenant whose day is over keeps nothing: its next user starts a new day.
            let keep = users.day >= today;
            if !keep {
                seen_released += users.seen.len();
            }
            keep
        });
        self.active_seen -= seen_released;

        tenants.retain(|_, window| !window.is_empty());
        tenants
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn limits() -> UsageLimits {
        UsageLimits {
            max_counter_series: 4,
            max_tracked_active_users: 2,
            max_pending_active_users: 3,
            ..UsageLimits::default()
        }
    }

    fn meter(window: &TenantWindow, name: &str) -> Vec<(Vec<(&'static str, &'static str)>, u64)> {
        window
            .meters
            .iter()
            .filter(|reading| reading.meter == name)
            .map(|reading| (reading.dims.clone(), reading.value))
            .collect()
    }

    #[test]
    fn counters_aggregate_per_tenant_and_dimension_and_reset_per_window() {
        let limits = limits();
        let mut state = WindowState::default();
        let accepted = |category| Counter::MessagesAccepted { category };
        assert!(state.add("tenant-a", accepted("normal"), 1, &limits));
        assert!(state.add("tenant-a", accepted("normal"), 2, &limits));
        assert!(state.add("tenant-a", accepted("temporary"), 1, &limits));
        assert!(state.add("tenant-b", Counter::MessagesPersisted, 5, &limits));

        let window = state.take_window(Instant::now());
        assert_eq!(
            meter(&window["tenant-a"], "messages.accepted"),
            [
                (vec![("category", "normal")], 3),
                (vec![("category", "temporary")], 1)
            ]
        );
        assert_eq!(
            meter(&window["tenant-b"], "messages.persisted"),
            [(vec![], 5)]
        );
        assert!(state.take_window(Instant::now()).is_empty());
    }

    #[test]
    fn new_series_beyond_the_limit_are_refused_but_existing_ones_still_count() {
        let limits = limits();
        let mut state = WindowState::default();
        for tenant in ["t1", "t2", "t3", "t4"] {
            assert!(state.add(tenant, Counter::MessagesPersisted, 1, &limits));
        }
        assert!(!state.add("t5", Counter::MessagesPersisted, 1, &limits));
        assert!(!state.add("t1", Counter::MediaBytesEgress, 1, &limits));
        assert!(state.add("t1", Counter::MessagesPersisted, 1, &limits));
        let window = state.take_window(Instant::now());
        assert_eq!(window.len(), 4);
        assert_eq!(meter(&window["t1"], "messages.persisted"), [(vec![], 2)]);
        // The limit is per window.
        assert!(state.add("t5", Counter::MessagesPersisted, 1, &limits));
    }

    #[test]
    fn connection_peak_and_minutes_integrate_open_sessions() {
        let mut state = WindowState::default();
        let t0 = Instant::now();
        let at = |secs| t0 + Duration::from_secs(secs);
        state.connection_opened("tenant-a", at(0));
        state.connection_opened("tenant-a", at(30));
        state.connection_closed("tenant-a", at(60));
        state.connection_opened("tenant-a", at(70));
        state.connection_opened("tenant-a", at(80));

        // 0-30 s: 1, 30-60 s: 2, 60-70 s: 1, 70-80 s: 2, 80-110 s: 3 => 210 s: 3 min, 30 s left.
        let window = state.take_window(at(110));
        assert_eq!(
            meter(&window["tenant-a"], "connections.peak"),
            [(vec![], 3)]
        );
        assert_eq!(
            meter(&window["tenant-a"], "connection.minutes"),
            [(vec![], 3)]
        );

        // 30 s carried plus 3 sessions for 60 s = 210 s => 3 minutes, 30 s carried again.
        let window = state.take_window(at(170));
        assert_eq!(
            meter(&window["tenant-a"], "connections.peak"),
            [(vec![], 3)]
        );
        assert_eq!(
            meter(&window["tenant-a"], "connection.minutes"),
            [(vec![], 3)]
        );

        for _ in 0..3 {
            state.connection_closed("tenant-a", at(170));
        }
        // Idle: the 30 s remainder rounds to one minute and the gauge is released. The peak
        // is the 3 sessions open when the window started.
        let window = state.take_window(at(200));
        assert_eq!(
            meter(&window["tenant-a"], "connections.peak"),
            [(vec![], 3)]
        );
        assert_eq!(
            meter(&window["tenant-a"], "connection.minutes"),
            [(vec![], 1)]
        );
        assert!(state.connections.is_empty());
        assert!(state.take_window(at(260)).is_empty());
        // A close after the gauge was released does nothing.
        state.connection_closed("tenant-a", at(261));
        assert!(state.connections.is_empty());
    }

    #[test]
    fn active_users_are_reported_once_per_day_and_bounded() {
        let limits = limits();
        let mut state = WindowState::default();
        let day = day_of_ms(now_ms());
        assert!(state.user_active("tenant-a", day, 1, &limits));
        assert!(state.user_active("tenant-a", day, 1, &limits));
        assert!(state.user_active("tenant-a", day, 2, &limits));
        // Third distinct user: pending has room, the seen set does not.
        assert!(state.user_active("tenant-a", day, 3, &limits));
        // Pending is full now.
        assert!(!state.user_active("tenant-b", day, 4, &limits));

        let window = state.take_window(Instant::now());
        assert_eq!(window["tenant-a"].active_users[&day], [1, 2, 3]);
        assert!(!window.contains_key("tenant-b"));

        // Seen users stay silent; the untracked one is reported again (the consumer upserts).
        assert!(state.user_active("tenant-a", day, 1, &limits));
        assert!(state.user_active("tenant-a", day, 3, &limits));
        let window = state.take_window(Instant::now());
        assert_eq!(window["tenant-a"].active_users[&day], [3]);
    }

    #[test]
    fn a_new_day_resets_the_seen_set_and_old_days_are_released() {
        let limits = limits();
        let mut state = WindowState::default();
        let today = day_of_ms(now_ms());
        assert!(state.user_active("tenant-a", 20000101, 1, &limits));
        assert!(state.user_active("tenant-a", today, 1, &limits));
        let window = state.take_window(Instant::now());
        assert_eq!(window["tenant-a"].active_users[&20000101], [1]);
        assert_eq!(window["tenant-a"].active_users[&today], [1]);
        assert_eq!(state.active_seen, 1);

        assert!(state.user_active("tenant-b", 20000101, 9, &limits));
        let _ = state.take_window(Instant::now());
        // tenant-b's day is over: released with its seen set.
        assert!(!state.active.contains_key("tenant-b"));
        assert_eq!(state.active_seen, 1);
    }

    #[test]
    fn live_readings_show_the_window_in_progress_without_closing_it() {
        let limits = limits();
        let mut state = WindowState::default();
        assert!(state.add(
            "t2",
            Counter::ApiCalls {
                route_group: "messages"
            },
            2,
            &limits
        ));
        assert!(state.add("t1", Counter::MessagesPersisted, 1, &limits));
        state.connection_opened("t1", Instant::now());
        let live = state.live();
        let readings: Vec<_> = live
            .iter()
            .map(|r| (r.app.as_str(), r.meter, r.value))
            .collect();
        assert_eq!(
            readings,
            [
                ("t1", "messages.persisted", 1),
                ("t1", "connections.peak", 1),
                ("t2", "api.calls", 2)
            ]
        );
        assert_eq!(live[2].dims, [("route_group", "messages")]);
        // Still there for the flush.
        assert_eq!(state.take_window(Instant::now()).len(), 2);
    }

    #[test]
    fn a_disabled_recorder_ignores_everything() {
        let recorder = UsageRecorder::disabled();
        recorder.message_accepted("tenant-a", "normal");
        recorder.connection_opened("tenant-a");
        recorder.user_active("tenant-a", "user-1");
        assert!(!recorder.is_enabled());
        assert!(recorder.live().is_empty());
    }
}
