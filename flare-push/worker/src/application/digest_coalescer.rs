//! Throttling degrades to a digest.
//!
//! When a vendor throttles an application (HTTP 429, quota exhausted, or the local rate limit
//! is empty), or throttles one device's binding (APNs 429 is about one device token), and no
//! other binding of the device can take the task, retrying it would only feed
//! the storm. The coalescer holds the task instead: per (tenant, user, device) it keeps the
//! newest task per collapse key, the broker message is acknowledged, and once the vendor is
//! available again ONE task per device goes back to delivery: a DIGEST ("M new messages in N
//! chats") when more than one conversation is pending, otherwise the newest task of the one
//! pending conversation with its count.
//!
//! In memory by design. A worker that stops (or crashes) loses at most these held
//! notifications; the messages themselves are not lost, the app syncs them when it opens, and
//! the badge is corrected then. Persisting the hold would put a write on the delivery path for
//! a notification that is already late. The hold is bounded (devices, and collapse keys per
//! device); when it is full the task is not held and the broker redelivers it instead.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flare_im_contracts::Ctx;
use flare_im_contracts::domain::push_policy::device_digest_collapse_key;
use flare_proto::PushTaskEnvelope;
use flare_proto::common::{AlertClass, PushDisposition};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::application::vendor_governance::{BindingKey, VendorGovernance};
use crate::domain::offline_delivery::{OfflinePushExecutor, task_disposition};
use crate::domain::vendor_delivery::{Vendor, is_throttled_error};
use crate::infrastructure::vendor_http;
use crate::infrastructure::vendor_metrics;
use crate::infrastructure::vendor_trace::{VendorEvent, VendorOutcome, VendorTrace};

/// Marks a task the coalescer built. Delivery does not hold such a task again when the vendor
/// is still throttled: it answers "throttled" and the coalescer keeps its own held state.
pub const COALESCED_TASK_HEADER: &str = "x-flare-push-coalesced";

/// How often held devices are checked for a vendor that is available again.
const FLUSH_TICK: Duration = Duration::from_millis(250);
/// Devices handed back to delivery at once.
const FLUSH_CONCURRENCY: usize = 16;
/// Devices handed back per tick, so that a vendor coming back does not get the whole hold in
/// one burst (the rate limit would throttle it again anyway).
const MAX_FLUSH_PER_TICK: usize = 1_024;
/// The same deadline the consumer gives one task.
const FLUSH_DEADLINE: Duration = Duration::from_secs(30);
/// After a flush that failed (not for throttling): doubled per failed flush (5, 10, 20, 40 s),
/// which outlasts a vendor breaker's open period.
const FAILED_FLUSH_BACKOFF: Duration = Duration::from_secs(5);
/// Failed flushes before the held notifications are given up.
const MAX_FAILED_FLUSHES: u8 = 5;
/// Recheck of a still-throttled vendor whose throttle mark is already gone.
const STILL_THROTTLED_RECHECK: Duration = Duration::from_secs(1);

/// Bounds of the hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoalescerLimits {
    pub max_devices: usize,
    pub max_keys_per_device: usize,
}

impl Default for CoalescerLimits {
    fn default() -> Self {
        Self {
            max_devices: 10_000,
            max_keys_per_device: 64,
        }
    }
}

/// The hold is full: the task was not held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoalescerFull;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeviceKey {
    tenant_id: String,
    user_id: String,
    device_id: String,
}

impl DeviceKey {
    fn of(task: &PushTaskEnvelope) -> Self {
        Self {
            tenant_id: task.tenant_id.clone(),
            user_id: task.user_id.clone(),
            device_id: task.device_id.clone(),
        }
    }
}

/// The newest held task of one collapse key.
#[derive(Debug, Clone)]
struct HeldTask {
    task: PushTaskEnvelope,
    arrival: u64,
    /// A task held under this key would have rung (loud or digest) and no clear came after it.
    rang: bool,
    /// The latest expiry among those ringing tasks.
    ring_expire_at: Option<i64>,
}

impl HeldTask {
    fn new(task: PushTaskEnvelope, arrival: u64) -> Self {
        let rang = rings(&task);
        Self {
            ring_expire_at: if rang { expiry(&task) } else { None },
            task,
            arrival,
            rang,
        }
    }

    /// A quiet refresh whose loud predecessor never reached the device must still ring once.
    fn upgrades(&self) -> bool {
        self.rang && task_disposition(&self.task) == Some(PushDisposition::Quiet)
    }

    fn expire_at(&self) -> Option<i64> {
        if self.upgrades() {
            later(expiry(&self.task), self.ring_expire_at)
        } else {
            expiry(&self.task)
        }
    }

    fn expired(&self, now_ms: i64) -> bool {
        self.expire_at().is_some_and(|at| at <= now_ms)
    }

    fn remember_ring(&mut self, task: &PushTaskEnvelope) {
        self.rang = true;
        self.ring_expire_at = later(self.ring_expire_at, expiry(task));
    }
}

/// Everything held for one device.
struct HeldDevice {
    /// The context of the newest held task (tenant-attested, carries the trace id).
    ctx: Ctx,
    /// The vendor that throttled most recently.
    vendor: Vendor,
    /// The binding that vendor throttled on its own, when it throttled a binding rather than
    /// the application.
    binding: Option<BindingKey>,
    ready_at: Instant,
    failed_flushes: u8,
    slots: HashMap<String, HeldTask>,
}

impl HeldDevice {
    fn new(ctx: Ctx, vendor: Vendor, binding: Option<BindingKey>, ready_at: Instant) -> Self {
        Self {
            ctx,
            vendor,
            binding,
            ready_at,
            failed_flushes: 0,
            slots: HashMap::new(),
        }
    }

    /// Hold `task` under its collapse key: it replaces the held one unless it represents an
    /// older message (a redelivery overtaken by a newer task). A clear forgets that the key
    /// rang; a loud task remembers it even when a newer task is held.
    fn hold(
        &mut self,
        task: PushTaskEnvelope,
        arrival: u64,
        max_keys: usize,
    ) -> Result<(), CoalescerFull> {
        let full = self.slots.len() >= max_keys;
        let held = match self.slots.entry(slot_key(&task)) {
            Entry::Vacant(vacant) => {
                if full {
                    return Err(CoalescerFull);
                }
                vacant.insert(HeldTask::new(task, arrival));
                return Ok(());
            }
            Entry::Occupied(occupied) => occupied.into_mut(),
        };
        let clears = task_disposition(&task) == Some(PushDisposition::Clear);
        if supersedes(&task, &held.task) {
            if clears {
                held.rang = false;
                held.ring_expire_at = None;
            } else if rings(&task) {
                held.remember_ring(&task);
            }
            held.task = task;
            held.arrival = arrival;
        } else if rings(&task) && task_disposition(&held.task) != Some(PushDisposition::Clear) {
            held.remember_ring(&task);
        }
        Ok(())
    }

    /// Put back what a failed flush took out, under anything held since: those are newer.
    fn merge_back(&mut self, restored: HeldDevice, max_keys: usize) {
        self.failed_flushes = self.failed_flushes.max(restored.failed_flushes);
        for (key, older) in restored.slots {
            let full = self.slots.len() >= max_keys;
            match self.slots.entry(key) {
                Entry::Occupied(mut occupied) => {
                    let newer = occupied.get_mut();
                    if older.rang && task_disposition(&newer.task) != Some(PushDisposition::Clear) {
                        newer.rang = true;
                        newer.ring_expire_at = later(newer.ring_expire_at, older.ring_expire_at);
                    }
                }
                Entry::Vacant(vacant) => {
                    if !full {
                        vacant.insert(older);
                    }
                }
            }
        }
    }

    /// Remove the held tasks whose expiry passed and return them (for the trace).
    fn drop_expired(&mut self, now_ms: i64) -> Vec<PushTaskEnvelope> {
        let expired: Vec<String> = self
            .slots
            .iter()
            .filter(|(_, held)| held.expired(now_ms))
            .map(|(key, _)| key.clone())
            .collect();
        expired
            .into_iter()
            .filter_map(|key| self.slots.remove(&key))
            .map(|held| held.task)
            .collect()
    }

    /// The one task this device gets. `None` when nothing is held.
    fn merged(&self, device: &DeviceKey) -> Option<PushTaskEnvelope> {
        // The badge is absolute: the newest one known is the right one.
        let badge = self
            .slots
            .values()
            .filter(|held| held.task.badge.is_some())
            .max_by_key(|held| held.arrival)
            .and_then(|held| held.task.badge);
        let alerts: Vec<&HeldTask> = self
            .slots
            .values()
            .filter(|held| shows(&held.task))
            .collect();
        let Some(newest_alert) = alerts.iter().copied().max_by_key(|held| held.arrival) else {
            // Only clears and badge updates: the newest says the most.
            let newest = self.slots.values().max_by_key(|held| held.arrival)?;
            let mut task = newest.task.clone();
            task.badge = badge.or(task.badge);
            return Some(coalesced(task));
        };

        let mut conversation_ids = HashSet::new();
        let mut notices = 0u32;
        let mut messages = 0u32;
        for held in alerts.iter().filter(|held| !is_digest(&held.task)) {
            let conversation = held.task.conversation_id.trim();
            if conversation.is_empty() {
                notices = notices.saturating_add(1);
            } else {
                conversation_ids.insert(conversation);
            }
            // Counts are per collapse key and cumulative: the held (newest) one stands for the
            // whole key; across keys they add up.
            messages = messages.saturating_add(held.task.count.max(1));
        }
        let mut conversations = u32::try_from(conversation_ids.len())
            .unwrap_or(u32::MAX)
            .saturating_add(notices);
        // A digest the router already made overlaps the conversation updates held with it; take
        // the larger figures instead of adding them up.
        if let Some(digest) = alerts
            .iter()
            .filter(|held| is_digest(&held.task))
            .max_by_key(|held| held.arrival)
        {
            conversations = conversations.max(digest.task.digest_conversations);
            messages = messages.max(digest.task.count);
        }
        if conversations > 1 {
            return Some(self.digest(
                device,
                &alerts,
                newest_alert,
                conversations,
                messages,
                badge,
            ));
        }
        let mut task = newest_alert.task.clone();
        if newest_alert.upgrades() {
            task.disposition = PushDisposition::Loud as i32;
            task.expire_at = newest_alert.expire_at();
        }
        task.badge = badge.or(task.badge);
        Some(coalesced(task))
    }

    fn digest(
        &self,
        device: &DeviceKey,
        alerts: &[&HeldTask],
        newest: &HeldTask,
        conversations: u32,
        messages: u32,
        badge: Option<u32>,
    ) -> PushTaskEnvelope {
        // The payload decides whether the digest may ring (the message's own sound setting):
        // take it from the newest task that would have rung.
        let ringing = alerts
            .iter()
            .copied()
            .filter(|held| held.rang)
            .max_by_key(|held| held.arrival)
            .unwrap_or(newest);
        let collapse_key =
            device_digest_collapse_key(&device.tenant_id, &device.user_id, &device.device_id);
        coalesced(PushTaskEnvelope {
            user_id: device.user_id.clone(),
            message_id: newest.task.message_id.clone(),
            // A digest spans conversations: a tap opens the conversation list.
            conversation_id: String::new(),
            tenant_id: device.tenant_id.clone(),
            priority: alerts
                .iter()
                .map(|held| held.task.priority)
                .max()
                .unwrap_or(newest.task.priority),
            expire_at: alerts.iter().filter_map(|held| held.expire_at()).max(),
            push_payload: ringing.task.push_payload.clone(),
            headers: newest.task.headers.clone(),
            payload_kind: ringing.task.payload_kind,
            device_id: device.device_id.clone(),
            idempotency_key: self.digest_identity(device),
            thread_key: collapse_key.clone(),
            collapse_key,
            disposition: PushDisposition::Digest as i32,
            alert_class: AlertClass::Normal as i32,
            count: messages,
            badge,
            // Seqs of different conversations do not compare.
            conversation_seq: 0,
            preview: newest.task.preview,
            digest_conversations: conversations,
        })
    }

    /// A stable identity of this digest: the same held tasks give the same key (receipts and
    /// Getui's request id deduplicate on it), any other set a different one.
    fn digest_identity(&self, device: &DeviceKey) -> String {
        let mut parts: Vec<(&str, &str)> = self
            .slots
            .iter()
            .map(|(key, held)| {
                let identity = if held.task.idempotency_key.is_empty() {
                    held.task.message_id.as_str()
                } else {
                    held.task.idempotency_key.as_str()
                };
                (key.as_str(), identity)
            })
            .collect();
        parts.sort_unstable();
        let mut hash = Sha256::new();
        for part in [
            "flare.push.coalesced-digest",
            device.tenant_id.as_str(),
            device.user_id.as_str(),
            device.device_id.as_str(),
        ]
        .into_iter()
        .chain(
            parts
                .into_iter()
                .flat_map(|(key, identity)| [key, identity]),
        ) {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        hex::encode(hash.finalize())
    }
}

/// Holds tasks of throttled vendors and hands one task per device back to delivery.
pub struct DigestCoalescer {
    held: Mutex<HashMap<DeviceKey, HeldDevice>>,
    limits: CoalescerLimits,
    arrivals: AtomicU64,
    governance: Arc<VendorGovernance>,
    trace: VendorTrace,
    /// Where held tasks go back: the worker's offline delivery. Weak, because that delivery
    /// holds this coalescer.
    target: OnceLock<Weak<dyn OfflinePushExecutor>>,
}

impl DigestCoalescer {
    pub fn new(
        limits: CoalescerLimits,
        governance: Arc<VendorGovernance>,
        trace: VendorTrace,
    ) -> Self {
        Self {
            held: Mutex::new(HashMap::new()),
            limits: CoalescerLimits {
                max_devices: limits.max_devices.max(1),
                max_keys_per_device: limits.max_keys_per_device.max(1),
            },
            arrivals: AtomicU64::new(0),
            governance,
            trace,
            target: OnceLock::new(),
        }
    }

    /// Wire the delivery held tasks go back to. Set once; later calls are ignored.
    pub fn set_target(&self, target: Weak<dyn OfflinePushExecutor>) {
        let _ = self.target.set(target);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<DeviceKey, HeldDevice>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Devices with held tasks.
    pub fn held_devices(&self) -> usize {
        self.lock().len()
    }

    /// Hold `task` because `vendor` throttled it (the application, or only `binding` when the
    /// vendor throttled that binding); try again from `ready_at`. `Err`: the hold is full, the
    /// caller lets the broker redeliver the task.
    pub fn absorb(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        vendor: Vendor,
        ready_at: Instant,
        binding: Option<BindingKey>,
    ) -> Result<(), CoalescerFull> {
        let key = DeviceKey::of(task);
        let arrival = self.arrivals.fetch_add(1, Ordering::Relaxed);
        {
            let mut held = self.lock();
            if !held.contains_key(&key) && held.len() >= self.limits.max_devices {
                return Err(CoalescerFull);
            }
            let device = held
                .entry(key)
                .or_insert_with(|| HeldDevice::new(ctx.clone(), vendor, binding, ready_at));
            device.hold(task.clone(), arrival, self.limits.max_keys_per_device)?;
            device.ctx = ctx.clone();
            device.vendor = vendor;
            device.binding = binding;
            device.ready_at = device.ready_at.min(ready_at);
        }
        vendor_metrics::record_coalesced(vendor);
        self.trace.record(
            task,
            VendorEvent::new(Some(vendor), VendorOutcome::Coalesced),
        );
        Ok(())
    }

    /// Hand held devices back to delivery until shutdown. What is still held at shutdown is
    /// lost (see the module comment).
    pub async fn run(
        self: Arc<Self>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tick = tokio::time::interval(FLUSH_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => self.flush_ready(Instant::now()).await,
            }
        }
        let left = self.held_devices();
        if left > 0 {
            tracing::warn!(
                devices = left,
                "push worker stopping with held notifications of throttled vendors; they are dropped, the messages sync when the app opens"
            );
        }
        Ok(())
    }

    /// Hand back every device whose vendor should be available by `now`.
    pub async fn flush_ready(&self, now: Instant) {
        let ready = self.take_ready(now);
        if ready.is_empty() {
            return;
        }
        let Some(target) = self.target.get().and_then(Weak::upgrade) else {
            for (key, device) in ready {
                self.restore(key, device, now + FAILED_FLUSH_BACKOFF);
            }
            return;
        };
        futures::stream::iter(ready)
            .for_each_concurrent(FLUSH_CONCURRENCY, |(key, device)| {
                let target = target.clone();
                async move { self.flush_device(target.as_ref(), key, device).await }
            })
            .await;
    }

    fn take_ready(&self, now: Instant) -> Vec<(DeviceKey, HeldDevice)> {
        let mut held = self.lock();
        let ready: Vec<DeviceKey> = held
            .iter()
            .filter(|(_, device)| device.ready_at <= now)
            .map(|(key, _)| key.clone())
            .take(MAX_FLUSH_PER_TICK)
            .collect();
        ready
            .into_iter()
            .filter_map(|key| held.remove(&key).map(|device| (key, device)))
            .collect()
    }

    async fn flush_device(
        &self,
        target: &dyn OfflinePushExecutor,
        key: DeviceKey,
        mut device: HeldDevice,
    ) {
        for task in device.drop_expired(now_ms()) {
            vendor_metrics::record_digest_flush("expired");
            self.trace.record(
                &task,
                VendorEvent::new(Some(device.vendor), VendorOutcome::Expired),
            );
        }
        let Some(task) = device.merged(&key) else {
            return;
        };
        let result = tokio::time::timeout(FLUSH_DEADLINE, target.deliver(&device.ctx, &task))
            .await
            .unwrap_or_else(|_| {
                Err(vendor_http::failure(
                    "held push flush deadline; acceptance unknown",
                ))
            });
        match result {
            Ok(()) => vendor_metrics::record_digest_flush("sent"),
            Err(error) if is_throttled_error(&error) => {
                // Still throttled: keep waiting, this is not a failed flush.
                vendor_metrics::record_digest_flush("waiting");
                let now = Instant::now();
                let binding_until = device.binding.and_then(|binding| {
                    self.governance
                        .binding_throttled_until(device.vendor, binding, now)
                });
                let until = self
                    .governance
                    .throttled_until(device.vendor, &key.tenant_id, now)
                    .max(binding_until)
                    .unwrap_or(now + STILL_THROTTLED_RECHECK);
                self.restore(key, device, until.max(now + FLUSH_TICK));
            }
            Err(error)
                if error.is_retryable() && device.failed_flushes + 1 < MAX_FAILED_FLUSHES =>
            {
                vendor_metrics::record_digest_flush("retry");
                let backoff = FAILED_FLUSH_BACKOFF * (1u32 << device.failed_flushes.min(4));
                device.failed_flushes += 1;
                self.restore(key, device, Instant::now() + backoff);
            }
            Err(error) => {
                vendor_metrics::record_digest_flush("dropped");
                tracing::warn!(
                    error = %error,
                    tenant_id = %key.tenant_id,
                    user_id = %key.user_id,
                    device_id = %key.device_id,
                    held = device.slots.len(),
                    "held push notifications dropped after failed flushes; the messages sync when the app opens"
                );
            }
        }
    }

    fn restore(&self, key: DeviceKey, mut device: HeldDevice, ready_at: Instant) {
        let mut held = self.lock();
        if let Some(current) = held.get_mut(&key) {
            current.ready_at = current.ready_at.max(ready_at);
            current.merge_back(device, self.limits.max_keys_per_device);
            return;
        }
        if held.len() >= self.limits.max_devices {
            vendor_metrics::record_digest_flush("dropped");
            return;
        }
        device.ready_at = ready_at;
        held.insert(key, device);
    }
}

/// The collapse key a task is held under. A task without one (a notice outside any
/// conversation) never replaces another: it gets a key of its own.
fn slot_key(task: &PushTaskEnvelope) -> String {
    if !task.collapse_key.is_empty() {
        task.collapse_key.clone()
    } else if !task.idempotency_key.is_empty() {
        format!("task:{}", task.idempotency_key)
    } else {
        format!("message:{}", task.message_id)
    }
}

/// Whether `incoming` replaces `held` under the same key: unless it represents an older message
/// of the conversation.
fn supersedes(incoming: &PushTaskEnvelope, held: &PushTaskEnvelope) -> bool {
    incoming.conversation_seq == 0
        || held.conversation_seq == 0
        || incoming.conversation_seq >= held.conversation_seq
}

fn rings(task: &PushTaskEnvelope) -> bool {
    matches!(
        task_disposition(task),
        Some(PushDisposition::Loud | PushDisposition::Digest)
    )
}

/// A notification the user sees (or one that replaces it).
fn shows(task: &PushTaskEnvelope) -> bool {
    matches!(
        task_disposition(task),
        Some(
            PushDisposition::Loud
                | PushDisposition::Quiet
                | PushDisposition::RecallReplace
                | PushDisposition::Digest
        )
    )
}

fn is_digest(task: &PushTaskEnvelope) -> bool {
    task_disposition(task) == Some(PushDisposition::Digest)
}

fn expiry(task: &PushTaskEnvelope) -> Option<i64> {
    task.expire_at.filter(|at| *at > 0)
}

fn later(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn coalesced(mut task: PushTaskEnvelope) -> PushTaskEnvelope {
    task.headers
        .insert(COALESCED_TASK_HEADER.to_string(), "1".to_string());
    task
}

/// Whether delivery is handling a task the coalescer built.
pub fn is_coalesced_task(task: &PushTaskEnvelope) -> bool {
    task.headers.contains_key(COALESCED_TASK_HEADER)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::FlareError;

    const C1: &str = "c:11111111111111111111111111111111";
    const C2: &str = "c:22222222222222222222222222222222";

    fn task(
        conversation: &str,
        collapse: &str,
        disposition: PushDisposition,
        seq: u64,
        count: u32,
    ) -> PushTaskEnvelope {
        PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
            conversation_id: conversation.into(),
            message_id: format!("{conversation}-{seq}"),
            collapse_key: collapse.into(),
            thread_key: collapse.into(),
            disposition: disposition as i32,
            alert_class: AlertClass::Normal as i32,
            conversation_seq: seq,
            count,
            idempotency_key: format!("{collapse}-{seq}-{}", disposition as i32),
            expire_at: Some(i64::MAX),
            ..Default::default()
        }
    }

    fn coalescer(limits: CoalescerLimits) -> DigestCoalescer {
        DigestCoalescer::new(
            limits,
            Arc::new(VendorGovernance::with_defaults()),
            VendorTrace::disabled(),
        )
    }

    fn held_task(coalescer: &DigestCoalescer) -> Option<PushTaskEnvelope> {
        let held = coalescer.lock();
        let key = DeviceKey {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
        };
        held.get(&key).and_then(|device| device.merged(&key))
    }

    fn absorb(coalescer: &DigestCoalescer, task: &PushTaskEnvelope) -> Result<(), CoalescerFull> {
        coalescer.absorb(&Ctx::default(), task, Vendor::Fcm, Instant::now(), None)
    }

    #[test]
    fn several_conversations_become_one_digest_with_the_summed_counts() {
        let coalescer = coalescer(CoalescerLimits::default());
        let mut first = task("conv-1", C1, PushDisposition::Loud, 10, 1);
        first.badge = Some(4);
        absorb(&coalescer, &first).unwrap();
        // The quiet refresh of the same conversation replaces the loud one: its count is
        // cumulative (two messages since the notification opened).
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Quiet, 11, 2),
        )
        .unwrap();
        let mut second = task("conv-2", C2, PushDisposition::Loud, 3, 1);
        second.badge = Some(6);
        absorb(&coalescer, &second).unwrap();
        let before = vendor_metrics::coalesced_count(Vendor::Fcm);
        assert!(before >= 3);

        let digest = held_task(&coalescer).expect("one task for the device");
        assert_eq!(task_disposition(&digest), Some(PushDisposition::Digest));
        assert_eq!(digest.digest_conversations, 2);
        assert_eq!(digest.count, 3);
        assert_eq!(digest.badge, Some(6), "the newest absolute badge");
        assert!(digest.conversation_id.is_empty());
        assert_eq!(
            digest.collapse_key,
            device_digest_collapse_key("t1", "u1", "d1")
        );
        assert_eq!(digest.thread_key, digest.collapse_key);
        assert_eq!(digest.conversation_seq, 0);
        assert!(is_coalesced_task(&digest));
        // The same held set always names the same digest.
        assert_eq!(
            held_task(&coalescer).unwrap().idempotency_key,
            digest.idempotency_key
        );
        assert_eq!(coalescer.held_devices(), 1);
    }

    #[test]
    fn one_conversation_gives_its_newest_task_and_still_rings() {
        let coalescer = coalescer(CoalescerLimits::default());
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Loud, 10, 1),
        )
        .unwrap();
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Quiet, 12, 3),
        )
        .unwrap();
        // A redelivered older task does not replace the newer one.
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Quiet, 11, 2),
        )
        .unwrap();
        let merged = held_task(&coalescer).unwrap();
        assert_eq!(merged.conversation_seq, 12);
        assert_eq!(merged.count, 3);
        assert_eq!(
            task_disposition(&merged),
            Some(PushDisposition::Loud),
            "the loud task never reached the device"
        );
        assert_eq!(merged.collapse_key, C1);
        assert!(is_coalesced_task(&merged));
    }

    #[test]
    fn a_clear_after_the_alert_leaves_only_the_clear() {
        let coalescer = coalescer(CoalescerLimits::default());
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Loud, 10, 1),
        )
        .unwrap();
        let mut clear = task("conv-1", C1, PushDisposition::Clear, 10, 0);
        clear.badge = Some(0);
        absorb(&coalescer, &clear).unwrap();
        let merged = held_task(&coalescer).unwrap();
        assert_eq!(task_disposition(&merged), Some(PushDisposition::Clear));
        assert_eq!(merged.badge, Some(0));
        // A quiet refresh after the clear does not ring: the clear forgot the loud one.
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Quiet, 11, 1),
        )
        .unwrap();
        let merged = held_task(&coalescer).unwrap();
        assert_eq!(task_disposition(&merged), Some(PushDisposition::Quiet));
    }

    #[test]
    fn a_recall_replacement_is_never_turned_into_a_loud_alert() {
        let coalescer = coalescer(CoalescerLimits::default());
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Loud, 10, 1),
        )
        .unwrap();
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::RecallReplace, 10, 1),
        )
        .unwrap();
        let merged = held_task(&coalescer).unwrap();
        assert_eq!(
            task_disposition(&merged),
            Some(PushDisposition::RecallReplace)
        );
    }

    #[test]
    fn the_hold_is_bounded() {
        let coalescer = coalescer(CoalescerLimits {
            max_devices: 1,
            max_keys_per_device: 1,
        });
        absorb(&coalescer, &task("conv-1", C1, PushDisposition::Loud, 1, 1)).unwrap();
        // The same key is replaced, not added.
        absorb(
            &coalescer,
            &task("conv-1", C1, PushDisposition::Quiet, 2, 2),
        )
        .unwrap();
        assert_eq!(
            absorb(&coalescer, &task("conv-2", C2, PushDisposition::Loud, 1, 1)),
            Err(CoalescerFull)
        );
        let mut other_device = task("conv-1", C1, PushDisposition::Loud, 1, 1);
        other_device.device_id = "d2".into();
        assert_eq!(absorb(&coalescer, &other_device), Err(CoalescerFull));
        assert_eq!(coalescer.held_devices(), 1);
    }

    #[test]
    fn expired_tasks_are_dropped_and_an_upgrade_keeps_the_loud_expiry() {
        let mut device = HeldDevice::new(Ctx::default(), Vendor::Apns, None, Instant::now());
        let mut quiet_only = task("conv-1", C1, PushDisposition::Quiet, 5, 1);
        quiet_only.expire_at = Some(1_000);
        device.hold(quiet_only, 0, 64).unwrap();
        let mut loud = task("conv-2", C2, PushDisposition::Loud, 7, 1);
        loud.expire_at = Some(9_000);
        device.hold(loud, 1, 64).unwrap();
        let mut refresh = task("conv-2", C2, PushDisposition::Quiet, 8, 2);
        refresh.expire_at = Some(2_000);
        device.hold(refresh, 2, 64).unwrap();

        let expired = device.drop_expired(5_000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conversation_id, "conv-1");
        let key = DeviceKey {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
        };
        let merged = device.merged(&key).unwrap();
        assert_eq!(task_disposition(&merged), Some(PushDisposition::Loud));
        assert_eq!(merged.expire_at, Some(9_000));
        assert!(device.drop_expired(10_000).len() == 1);
        assert!(device.merged(&key).is_none());
    }

    struct Target {
        delivered: Mutex<Vec<PushTaskEnvelope>>,
        throttled: bool,
    }

    #[async_trait::async_trait]
    impl OfflinePushExecutor for Target {
        async fn deliver(&self, _: &Ctx, task: &PushTaskEnvelope) -> Result<(), FlareError> {
            self.delivered.lock().unwrap().push(task.clone());
            if self.throttled {
                Err(crate::domain::vendor_delivery::throttled_error())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn a_ready_device_is_handed_back_once_and_kept_while_still_throttled() {
        let coalescer = coalescer(CoalescerLimits::default());
        let target = Arc::new(Target {
            delivered: Mutex::new(Vec::new()),
            throttled: false,
        });
        let target_dyn: Arc<dyn OfflinePushExecutor> = target.clone();
        coalescer.set_target(Arc::downgrade(&target_dyn));
        let later = Instant::now() + Duration::from_secs(60);
        coalescer
            .absorb(
                &Ctx::default(),
                &task("conv-1", C1, PushDisposition::Loud, 1, 1),
                Vendor::Getui,
                later,
                None,
            )
            .unwrap();
        coalescer.flush_ready(Instant::now()).await;
        assert!(target.delivered.lock().unwrap().is_empty(), "not ready yet");
        coalescer.flush_ready(later).await;
        assert_eq!(target.delivered.lock().unwrap().len(), 1);
        assert_eq!(coalescer.held_devices(), 0);

        let throttled = Arc::new(Target {
            delivered: Mutex::new(Vec::new()),
            throttled: true,
        });
        let throttled_dyn: Arc<dyn OfflinePushExecutor> = throttled.clone();
        let waiting = super::DigestCoalescer::new(
            CoalescerLimits::default(),
            Arc::new(VendorGovernance::with_defaults()),
            VendorTrace::disabled(),
        );
        waiting.set_target(Arc::downgrade(&throttled_dyn));
        waiting
            .absorb(
                &Ctx::default(),
                &task("conv-1", C1, PushDisposition::Loud, 1, 1),
                Vendor::Getui,
                Instant::now(),
                None,
            )
            .unwrap();
        for _ in 0..(MAX_FAILED_FLUSHES + 2) {
            waiting
                .flush_ready(Instant::now() + Duration::from_secs(3_600))
                .await;
        }
        assert_eq!(
            waiting.held_devices(),
            1,
            "a vendor that is still throttled is waited for, not given up"
        );
        assert_eq!(
            throttled.delivered.lock().unwrap().len(),
            usize::from(MAX_FAILED_FLUSHES + 2)
        );
    }

    /// A device held for its own binding's throttle mark (APNs 429) waits for that mark, not
    /// for an application mark that does not exist.
    #[tokio::test]
    async fn a_device_held_for_its_binding_waits_for_the_binding_mark() {
        let governance = Arc::new(VendorGovernance::with_defaults());
        let binding = BindingKey::of("t1", "apns-token");
        let start = Instant::now();
        governance.mark_binding_throttled(Vendor::Apns, binding, start + Duration::from_secs(60));
        let coalescer = DigestCoalescer::new(
            CoalescerLimits::default(),
            governance.clone(),
            VendorTrace::disabled(),
        );
        let target = Arc::new(Target {
            delivered: Mutex::new(Vec::new()),
            throttled: true,
        });
        let target_dyn: Arc<dyn OfflinePushExecutor> = target.clone();
        coalescer.set_target(Arc::downgrade(&target_dyn));
        coalescer
            .absorb(
                &Ctx::default(),
                &task("conv-1", C1, PushDisposition::Loud, 1, 1),
                Vendor::Apns,
                start,
                Some(binding),
            )
            .unwrap();
        coalescer.flush_ready(start).await;
        assert_eq!(target.delivered.lock().unwrap().len(), 1);
        // Still throttled: the next try is at the binding's mark, not a second later.
        coalescer.flush_ready(start + Duration::from_secs(30)).await;
        assert_eq!(target.delivered.lock().unwrap().len(), 1);
        assert_eq!(coalescer.held_devices(), 1);
    }
}
