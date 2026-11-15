//! 进程内的闸门状态：推送 Redis 不可用时的近似，也是测试里的存储替身。
//!
//! 语义与 Redis 那份逐项相同（比较版本后写入、到期队列领取即移除、推迟记录累加、角标表、
//! 会话波次），只是状态按实例各自保存：多实例部署下同一台设备、同一个会话的状态分散在各实例，
//! 噪声上界近似成立，每个新会话仍然至少提示一次。条目有上限，超出时先清过期的，还不够就整体
//! 清空——它本来就是软状态。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use flare_server_core::error::FlareError;

use crate::domain::model::{
    BadgeBase, BadgeChange, BadgeValue, DelayedAlert, DeviceRecord, DueClaim, DueEntry, DueKind,
    GateDevice, GateSlotKey, GateSnapshot, GateStoreOrigin, SlotRecord, WaveDue, WaveKey,
    WaveState, raw_version, tier_code,
};
use crate::domain::repository::{
    BadgeWrite, CommitOutcome, DueChange, GateCommit, NotifyGateStore, VersionedWrite, WaveCommit,
    WaveMessageWrite, WaveSnapshot, WaveStore,
};

use super::redis_gate_store::BADGE_CONVERSATION_CAP;

/// 缺省最多保存多少个槽位（设备、推迟记录、展示索引、角标表各自同样的上限）。
pub const DEFAULT_MEMORY_CAPACITY: usize = 100_000;

struct Stored<T> {
    value: T,
    expires_at: Instant,
}

impl<T> Stored<T> {
    fn new(value: T, ttl_ms: i64) -> Self {
        Self {
            value,
            expires_at: Instant::now() + Duration::from_millis(ttl_ms.max(1) as u64),
        }
    }

    fn live(&self, now: Instant) -> bool {
        self.expires_at > now
    }
}

/// 角标表：基线加各会话的贡献。
#[derive(Default)]
struct Ledger {
    base: i64,
    sum: i64,
    adopted_ms: i64,
    contributions: HashMap<String, i64>,
}

impl Ledger {
    fn apply(&mut self, conversation_id: &str, write: &BadgeWrite) -> BadgeValue {
        if let Some(BadgeBase { count, at_ms }) = write.base
            && at_ms > self.adopted_ms
        {
            *self = Ledger {
                base: i64::from(count),
                adopted_ms: at_ms,
                ..Ledger::default()
            };
        }
        let mut changed = false;
        match write.change {
            BadgeChange::Add(times) => {
                let times = i64::from(times.max(1));
                if self.contributions.contains_key(conversation_id)
                    || self.contributions.len() < BADGE_CONVERSATION_CAP as usize
                {
                    *self
                        .contributions
                        .entry(conversation_id.to_string())
                        .or_default() += times;
                    self.sum += times;
                } else {
                    self.base += times;
                }
                changed = true;
            }
            BadgeChange::Remove | BadgeChange::Decrement => {
                let held = self
                    .contributions
                    .get(conversation_id)
                    .copied()
                    .unwrap_or(0);
                if held > 0 {
                    let take = if write.change == BadgeChange::Decrement {
                        1
                    } else {
                        held
                    };
                    if take >= held {
                        self.contributions.remove(conversation_id);
                    } else if let Some(left) = self.contributions.get_mut(conversation_id) {
                        *left -= take;
                    }
                    self.sum -= take;
                    changed = true;
                }
            }
            BadgeChange::Get => {}
        }
        BadgeValue {
            count: u32::try_from((self.base + self.sum).max(0)).unwrap_or(u32::MAX),
            changed,
        }
    }
}

#[derive(Default)]
struct MemoryState {
    slots: HashMap<GateSlotKey, Stored<Vec<u8>>>,
    devices: HashMap<GateDevice, Stored<Vec<u8>>>,
    /// 到期队列：按（到期时刻，槽位，种类）排序；`due_index` 记每个（种类，槽位）当前的到期时刻。
    due: BTreeSet<(i64, GateSlotKey, DueKind)>,
    due_index: HashMap<(DueKind, GateSlotKey), i64>,
    delays: HashMap<GateSlotKey, Stored<DelayedAlert>>,
    shown: HashMap<(String, String, String), Stored<HashSet<GateDevice>>>,
    badges: HashMap<GateDevice, Stored<Ledger>>,
    /// 会话波次：版本、状态与最新一条消息的正文。
    waves: HashMap<WaveKey, Stored<WaveSnapshot>>,
    /// 波次的到期队列与每个会话当前的到期时刻。
    wave_due: BTreeSet<(i64, WaveKey)>,
    wave_due_index: HashMap<WaveKey, i64>,
}

impl MemoryState {
    fn schedule_wave(&mut self, key: &WaveKey, due_ms: i64) {
        if let Some(previous) = self.wave_due_index.insert(key.clone(), due_ms) {
            self.wave_due.remove(&(previous, key.clone()));
        }
        self.wave_due.insert((due_ms, key.clone()));
    }

    fn schedule(&mut self, kind: DueKind, key: &GateSlotKey, due_ms: i64) {
        if let Some(previous) = self.due_index.insert((kind, key.clone()), due_ms) {
            self.due.remove(&(previous, key.clone(), kind));
        }
        self.due.insert((due_ms, key.clone(), kind));
    }

    fn unschedule(&mut self, kind: DueKind, key: &GateSlotKey) {
        if let Some(previous) = self.due_index.remove(&(kind, key.clone())) {
            self.due.remove(&(previous, key.clone(), kind));
        }
    }

    fn version_matches(stored: Option<&Stored<Vec<u8>>>, now: Instant, expect: &[u8; 8]) -> bool {
        let value = stored
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.as_slice());
        raw_version(value) == *expect
    }

    fn badge(
        &mut self,
        device: &GateDevice,
        conversation_id: &str,
        write: &BadgeWrite,
        now: Instant,
    ) -> BadgeValue {
        let entry = self.badges.entry(device.clone()).or_insert_with(|| {
            Stored::new(Ledger::default(), super::redis_gate_store::BADGE_TTL_MS)
        });
        if !entry.live(now) {
            *entry = Stored::new(Ledger::default(), super::redis_gate_store::BADGE_TTL_MS);
        }
        let value = entry.value.apply(conversation_id, write);
        entry.expires_at =
            now + Duration::from_millis(super::redis_gate_store::BADGE_TTL_MS as u64);
        value
    }

    /// 超出上限时先清过期的，还不够就整体清空。
    fn prune(&mut self, capacity: usize, now: Instant) {
        if self.slots.len() > capacity {
            self.slots.retain(|_, stored| stored.live(now));
            if self.slots.len() > capacity {
                self.slots.clear();
            }
        }
        if self.devices.len() > capacity {
            self.devices.retain(|_, stored| stored.live(now));
            if self.devices.len() > capacity {
                self.devices.clear();
            }
        }
        if self.delays.len() > capacity {
            self.delays.retain(|_, stored| stored.live(now));
            if self.delays.len() > capacity {
                self.delays.clear();
            }
        }
        if self.shown.len() > capacity {
            self.shown.retain(|_, stored| stored.live(now));
            if self.shown.len() > capacity {
                self.shown.clear();
            }
        }
        if self.badges.len() > capacity {
            self.badges.retain(|_, stored| stored.live(now));
            if self.badges.len() > capacity {
                self.badges.clear();
            }
        }
        if self.due.len() > capacity {
            self.due.clear();
            self.due_index.clear();
        }
        if self.waves.len() > capacity {
            self.waves.retain(|_, stored| stored.live(now));
            if self.waves.len() > capacity {
                self.waves.clear();
            }
        }
        if self.wave_due.len() > capacity {
            self.wave_due.clear();
            self.wave_due_index.clear();
        }
    }
}

/// 进程内闸门状态。
pub struct MemoryGateStore {
    state: Mutex<MemoryState>,
    capacity: usize,
    origin: GateStoreOrigin,
}

impl MemoryGateStore {
    /// Redis 不可用时的兜底：领取出的到期条目标成 [`GateStoreOrigin::Fallback`]。
    pub fn fallback() -> Self {
        Self::with_capacity(DEFAULT_MEMORY_CAPACITY, GateStoreOrigin::Fallback)
    }

    /// 当作主存储用（测试）。
    pub fn primary() -> Self {
        Self::with_capacity(DEFAULT_MEMORY_CAPACITY, GateStoreOrigin::Primary)
    }

    pub fn with_capacity(capacity: usize, origin: GateStoreOrigin) -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
            capacity: capacity.max(1),
            origin,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 撤掉一台设备的全部到期条目（测试里回放「设备回到前台」用）。
    pub fn clear_device_due(&self, device: &GateDevice) {
        let mut state = self.lock();
        let doomed: Vec<(DueKind, GateSlotKey)> = state
            .due_index
            .keys()
            .filter(|(_, key)| key.device == *device)
            .cloned()
            .collect();
        for (kind, key) in doomed {
            state.unschedule(kind, &key);
        }
    }
}

#[async_trait::async_trait]
impl NotifyGateStore for MemoryGateStore {
    async fn load(&self, key: &GateSlotKey) -> Result<GateSnapshot, FlareError> {
        let now = Instant::now();
        let state = self.lock();
        let slot = state
            .slots
            .get(key)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.as_slice());
        let device = state
            .devices
            .get(&key.device)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.as_slice());
        Ok(GateSnapshot {
            slot: SlotRecord::decode(slot),
            slot_version: raw_version(slot),
            device: DeviceRecord::decode(device),
            device_version: raw_version(device),
        })
    }

    async fn commit(&self, commit: &GateCommit) -> Result<CommitOutcome, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        let key = &commit.key;
        if let Some(VersionedWrite { expect, .. }) = &commit.slot
            && !MemoryState::version_matches(state.slots.get(key), now, expect)
        {
            return Ok(CommitOutcome::Conflict);
        }
        if let Some(VersionedWrite { expect, .. }) = &commit.device
            && !MemoryState::version_matches(state.devices.get(&key.device), now, expect)
        {
            return Ok(CommitOutcome::Conflict);
        }
        if let Some(value) = commit.slot.as_ref().and_then(|write| write.value.clone()) {
            state
                .slots
                .insert(key.clone(), Stored::new(value, commit.slot_ttl_ms));
        }
        if let Some(value) = commit.device.as_ref().and_then(|write| write.value.clone()) {
            state
                .devices
                .insert(key.device.clone(), Stored::new(value, commit.device_ttl_ms));
        }
        match commit.due {
            DueChange::None => {}
            DueChange::Schedule(due_ms) => state.schedule(DueKind::Tail, key, due_ms),
            DueChange::Cancel => state.unschedule(DueKind::Tail, key),
        }
        if let Some(message_id) = &commit.shown_message_id {
            let index = (
                key.device.tenant_id.clone(),
                key.conversation_id.clone(),
                message_id.clone(),
            );
            let entry = state
                .shown
                .entry(index)
                .or_insert_with(|| Stored::new(HashSet::new(), commit.slot_ttl_ms));
            if !entry.live(now) {
                entry.value.clear();
            }
            entry.value.insert(key.device.clone());
            entry.expires_at = now + Duration::from_millis(commit.slot_ttl_ms.max(1) as u64);
        }
        let badge = commit
            .badge
            .as_ref()
            .map(|write| state.badge(&key.device, &key.conversation_id, write, now));
        state.prune(self.capacity, now);
        Ok(CommitOutcome::Applied { badge })
    }

    async fn defer_alert(
        &self,
        alert: &DelayedAlert,
        ttl_ms: i64,
        badge: Option<BadgeWrite>,
    ) -> Result<Option<BadgeValue>, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        let key = &alert.key;
        let existing = state
            .delays
            .get(key)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.clone());
        let merged = match existing {
            None => DelayedAlert {
                count: alert.count.max(1),
                ..alert.clone()
            },
            Some(mut held) => {
                if tier_code(alert.tier) > tier_code(held.tier) {
                    held.tier = alert.tier;
                }
                if alert.latest.seq >= held.latest.seq {
                    held.latest = alert.latest.clone();
                }
                held.count = held.count.saturating_add(alert.count.max(1));
                held
            }
        };
        let due_ms = merged.due_ms;
        state
            .delays
            .insert(key.clone(), Stored::new(merged, ttl_ms));
        if !state.due_index.contains_key(&(DueKind::Delay, key.clone())) {
            state.schedule(DueKind::Delay, key, due_ms);
        }
        let value = badge
            .as_ref()
            .map(|write| state.badge(&key.device, &key.conversation_id, write, now));
        state.prune(self.capacity, now);
        Ok(value)
    }

    async fn take_delayed(&self, key: &GateSlotKey) -> Result<Option<DelayedAlert>, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        Ok(state
            .delays
            .remove(key)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value))
    }

    async fn cancel_delayed(&self, key: &GateSlotKey, read_seq: u64) -> Result<bool, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        let cancel = state
            .delays
            .get(key)
            .filter(|stored| stored.live(now))
            .is_some_and(|stored| read_seq == 0 || read_seq >= stored.value.latest.seq);
        if cancel {
            state.delays.remove(key);
            state.unschedule(DueKind::Delay, key);
        }
        Ok(cancel)
    }

    async fn claim_due(&self, now_ms: i64, limit: usize) -> Result<Vec<DueClaim>, FlareError> {
        let mut state = self.lock();
        let mut out = Vec::new();
        while out.len() < limit.max(1) {
            let slot_due = state.due.first().map(|(due_ms, _, _)| *due_ms);
            let wave_due = state.wave_due.first().map(|(due_ms, _)| *due_ms);
            // 两个队列按到期时刻合并；同一时刻槽位在前。
            let take_wave = match (slot_due, wave_due) {
                (Some(slot), Some(wave)) => wave < slot,
                (None, Some(_)) => true,
                _ => false,
            };
            if take_wave {
                let Some((due_ms, key)) = state.wave_due.first().cloned() else {
                    break;
                };
                if due_ms > now_ms {
                    break;
                }
                state.wave_due.remove(&(due_ms, key.clone()));
                state.wave_due_index.remove(&key);
                out.push(DueClaim::Wave(WaveDue {
                    due_ms,
                    key,
                    origin: self.origin,
                }));
                continue;
            }
            let Some((due_ms, key, kind)) = state.due.first().cloned() else {
                break;
            };
            if due_ms > now_ms {
                break;
            }
            state.due.remove(&(due_ms, key.clone(), kind));
            state.due_index.remove(&(kind, key.clone()));
            out.push(DueClaim::Slot(DueEntry {
                due_ms,
                key,
                kind,
                origin: self.origin,
            }));
        }
        Ok(out)
    }

    async fn shown_devices(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Vec<GateDevice>, FlareError> {
        let now = Instant::now();
        let state = self.lock();
        let index = (
            tenant_id.to_string(),
            conversation_id.to_string(),
            message_id.to_string(),
        );
        let mut devices: Vec<GateDevice> = state
            .shown
            .get(&index)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.iter().cloned().collect())
            .unwrap_or_default();
        devices.sort();
        Ok(devices)
    }

    async fn badge(
        &self,
        device: &GateDevice,
        conversation_id: &str,
        badge: BadgeWrite,
    ) -> Result<BadgeValue, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        let value = state.badge(device, conversation_id, &badge, now);
        state.prune(self.capacity, now);
        Ok(value)
    }
}

#[async_trait::async_trait]
impl WaveStore for MemoryGateStore {
    async fn load_wave(&self, key: &WaveKey) -> Result<WaveSnapshot, FlareError> {
        let now = Instant::now();
        let state = self.lock();
        Ok(state
            .waves
            .get(key)
            .filter(|stored| stored.live(now))
            .map(|stored| stored.value.clone())
            .unwrap_or_default())
    }

    async fn commit_wave(&self, commit: &WaveCommit) -> Result<bool, FlareError> {
        let now = Instant::now();
        let mut state = self.lock();
        let current = state
            .waves
            .get(&commit.key)
            .filter(|stored| stored.live(now))
            .map(|stored| (stored.value.version, stored.value.message.clone()));
        let (version, message) = current.unwrap_or((0, None));
        if version != commit.expect {
            return Ok(false);
        }
        let message = match &commit.message {
            WaveMessageWrite::Keep => message,
            WaveMessageWrite::Set(bytes) => Some(bytes.clone()),
            WaveMessageWrite::Clear => None,
        };
        let stored = WaveSnapshot {
            state: WaveState::clone(&commit.state),
            version: commit.expect.wrapping_add(1),
            message,
        };
        state
            .waves
            .insert(commit.key.clone(), Stored::new(stored, commit.ttl_ms));
        if let Some(due_ms) = commit.due_ms {
            state.schedule_wave(&commit.key, due_ms);
        }
        state.prune(self.capacity, now);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::SlotMessage;
    use flare_notify_rules::{ConversationKind, Tier};

    fn key(conversation: &str) -> GateSlotKey {
        GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), conversation)
    }

    fn write(expect: [u8; 8], value: &[u8]) -> VersionedWrite {
        VersionedWrite {
            expect,
            value: Some(value.to_vec()),
        }
    }

    fn versioned(version: u64) -> Vec<u8> {
        let mut value = vec![1u8; 9];
        value[1..9].copy_from_slice(&version.to_le_bytes());
        value
    }

    #[tokio::test]
    async fn a_stale_version_is_a_conflict_and_writes_nothing() {
        let store = MemoryGateStore::primary();
        let mut commit = GateCommit::new(key("c1"));
        commit.slot = Some(write([0; 8], &versioned(1)));
        commit.slot_ttl_ms = 60_000;
        assert!(matches!(
            store.commit(&commit).await.unwrap(),
            CommitOutcome::Applied { badge: None }
        ));
        // 还拿着版本 0 写：冲突，到期登记也不生效。
        commit.slot = Some(write([0; 8], &versioned(2)));
        commit.due = DueChange::Schedule(5);
        assert_eq!(
            store.commit(&commit).await.unwrap(),
            CommitOutcome::Conflict
        );
        assert!(store.claim_due(10, 10).await.unwrap().is_empty());
        let snapshot = store.load(&key("c1")).await.unwrap();
        assert_eq!(snapshot.slot_version, 1u64.to_le_bytes());
    }

    #[tokio::test]
    async fn due_entries_come_out_in_time_then_conversation_order_once() {
        let store = MemoryGateStore::primary();
        {
            let mut state = store.lock();
            state.schedule(DueKind::Tail, &key("b"), 20);
            state.schedule(DueKind::Tail, &key("a"), 20);
            state.schedule(DueKind::Tail, &key("c"), 10);
            state.schedule(DueKind::Tail, &key("c"), 30);
        }
        let claimed: Vec<(i64, String)> = store
            .claim_due(25, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|claim| match claim {
                DueClaim::Slot(entry) => (entry.due_ms, entry.key.conversation_id),
                DueClaim::Wave(wave) => (wave.due_ms, wave.key.conversation_id),
            })
            .collect();
        assert_eq!(claimed, vec![(20, "a".into()), (20, "b".into())]);
        assert_eq!(store.claim_due(100, 10).await.unwrap().len(), 1);
        assert!(store.claim_due(100, 10).await.unwrap().is_empty());
    }

    /// 波次与槽位共用一个到期队列：按到期时刻交错领取，波次的版本比较与正文改动和 Redis 一致。
    #[tokio::test]
    async fn waves_share_the_due_queue_and_compare_versions() {
        let store = MemoryGateStore::primary();
        let wave = WaveKey::new("t1", "g-1");
        let commit = |expect: u64, message: WaveMessageWrite, due_ms: Option<i64>| WaveCommit {
            key: wave.clone(),
            expect,
            state: WaveState {
                pending: 2,
                ..WaveState::default()
            },
            message,
            ttl_ms: 60_000,
            due_ms,
        };
        assert!(
            store
                .commit_wave(&commit(0, WaveMessageWrite::Set(b"m".to_vec()), Some(15)))
                .await
                .unwrap()
        );
        assert!(
            !store
                .commit_wave(&commit(0, WaveMessageWrite::Keep, None))
                .await
                .unwrap(),
            "还拿着版本 0 写：冲突"
        );
        let snapshot = store.load_wave(&wave).await.unwrap();
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.state.pending, 2);
        assert_eq!(snapshot.message.as_deref(), Some(&b"m"[..]));
        assert!(
            store
                .commit_wave(&commit(1, WaveMessageWrite::Keep, Some(25)))
                .await
                .unwrap()
        );
        assert_eq!(
            store.load_wave(&wave).await.unwrap().message.as_deref(),
            Some(&b"m"[..]),
            "Keep 不动正文"
        );

        store.lock().schedule(DueKind::Tail, &key("c"), 20);
        let claimed: Vec<i64> = store
            .claim_due(100, 10)
            .await
            .unwrap()
            .iter()
            .map(DueClaim::due_ms)
            .collect();
        assert_eq!(claimed, vec![20, 25], "同一会话重复登记只改到期时刻");
        assert!(
            store
                .commit_wave(&commit(2, WaveMessageWrite::Clear, None))
                .await
                .unwrap()
        );
        assert_eq!(store.load_wave(&wave).await.unwrap().message, None);
    }

    #[tokio::test]
    async fn delayed_alerts_accumulate_and_a_read_cancels_them() {
        let store = MemoryGateStore::primary();
        let alert = |seq: u64, tier: Tier| DelayedAlert {
            key: key("c1"),
            kind: ConversationKind::Group,
            tier,
            latest: SlotMessage {
                message_id: format!("m-{seq}"),
                seq,
                alert_class: 3,
            },
            count: 1,
            created_ms: 1_000,
            due_ms: 16_000,
        };
        store
            .defer_alert(&alert(5, Tier::Normal), 60_000, None)
            .await
            .unwrap();
        store
            .defer_alert(&alert(6, Tier::Priority), 60_000, None)
            .await
            .unwrap();
        // 已读到 5：最新的 6 还没读，不取消。
        assert!(!store.cancel_delayed(&key("c1"), 5).await.unwrap());
        let due = store.claim_due(16_000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(matches!(&due[0], DueClaim::Slot(entry) if entry.kind == DueKind::Delay));
        let held = store.take_delayed(&key("c1")).await.unwrap().unwrap();
        assert_eq!(
            (held.count, held.tier, held.latest.seq),
            (2, Tier::Priority, 6)
        );
        assert!(store.take_delayed(&key("c1")).await.unwrap().is_none());

        store
            .defer_alert(&alert(7, Tier::Normal), 60_000, None)
            .await
            .unwrap();
        assert!(store.cancel_delayed(&key("c1"), 7).await.unwrap());
        assert!(store.claim_due(i64::MAX, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_badge_is_base_plus_contributions_and_a_newer_base_resets_it() {
        let store = MemoryGateStore::primary();
        let device = GateDevice::new("t1", "u1", "phone");
        let base = Some(BadgeBase {
            count: 4,
            at_ms: 100,
        });
        let change = |change, base| BadgeWrite { change, base };
        let add = store
            .badge(&device, "c1", change(BadgeChange::Add(1), base))
            .await
            .unwrap();
        assert_eq!(add.count, 5);
        store
            .badge(&device, "c2", change(BadgeChange::Add(1), base))
            .await
            .unwrap();
        store
            .badge(&device, "c2", change(BadgeChange::Add(1), base))
            .await
            .unwrap();
        let decremented = store
            .badge(&device, "c2", change(BadgeChange::Decrement, None))
            .await
            .unwrap();
        assert_eq!(
            decremented,
            BadgeValue {
                count: 6,
                changed: true
            }
        );
        let removed = store
            .badge(&device, "c2", change(BadgeChange::Remove, None))
            .await
            .unwrap();
        assert_eq!(
            removed,
            BadgeValue {
                count: 5,
                changed: true
            }
        );
        let nothing = store
            .badge(&device, "c2", change(BadgeChange::Remove, None))
            .await
            .unwrap();
        assert!(!nothing.changed, "没有贡献的会话被读，角标不变");
        // 设备重新报了基线：之前的贡献都在新基线里了。
        let reset = store
            .badge(
                &device,
                "c1",
                change(
                    BadgeChange::Get,
                    Some(BadgeBase {
                        count: 2,
                        at_ms: 200,
                    }),
                ),
            )
            .await
            .unwrap();
        assert_eq!(reset.count, 2);
    }

    #[tokio::test]
    async fn shown_devices_are_indexed_per_message() {
        let store = MemoryGateStore::primary();
        let mut commit = GateCommit::new(key("c1"));
        commit.slot_ttl_ms = 60_000;
        commit.shown_message_id = Some("m-1".into());
        store.commit(&commit).await.unwrap();
        assert_eq!(
            store.shown_devices("t1", "c1", "m-1").await.unwrap(),
            vec![GateDevice::new("t1", "u1", "phone")]
        );
        assert!(
            store
                .shown_devices("t1", "c1", "m-2")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
