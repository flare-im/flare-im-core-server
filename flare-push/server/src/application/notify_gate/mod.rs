//! 防打扰闸门（应用服务）：读出槽位与设备状态，在进程里跑 `flare-notify-rules`，再比较版本后写回。
//!
//! - 规则只有那一份实现。这里不判断任何「该不该响」，只负责状态的读写与并发：版本对不上就
//!   重读重算，至多 [`CAS_ATTEMPTS`] 次；还是写不进去就放行（按会话的第一条处理，`fail_open`）。
//! - 设备状态是这台设备所有会话共用的一份，同一台设备的读改写在进程内先排队（按设备分段的锁），
//!   多个会话同时来消息时不会在进程里互相冲突到放行；比较后写入只用来挡另一个实例同时写。
//! - 推送 Redis 不可用时改用进程内的闸门（同一份规则，状态按实例各自保存），每个新会话仍然至少
//!   提示一次；两边都不可用才放行。
//! - 设备「回到前台」是惰性生效的：Online 写下标记，闸门下一次碰到这台设备时发现标记比自己
//!   采用过的新，先对设备做一次 `on_active`（作废全部槽位、退出摘要模式）再继续。

use std::collections::HashMap;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_service_kit::metrics::PushServerMetrics;
use flare_notify_rules::{
    ConversationKind, ConversationTag, Decision, DigestAlert, DueAction, Gate, GateInput,
    PreviewMode, ReadOutcome, SlotAction, Tier, UserPrefs, fail_open,
};
use flare_server_core::error::FlareError;

use crate::domain::model::{
    BadgeChange, BadgeValue, DelayedAlert, DeviceRecord, DeviceSignals, DueClaim, DueEntry,
    DueKind, GateDevice, GateSlotKey, GateSnapshot, GateStoreOrigin, NotifyPolicy, SlotMessage,
    SlotSide, kind_code,
};
use crate::domain::notify_inputs::decision_label;
use crate::domain::repository::{
    BadgeWrite, CommitOutcome, DeviceSignalRepository, DueChange, GateCommit, NotifyGateStore,
    UserNotifySettingsRepository, VersionedWrite,
};

/// 比较后写入的尝试次数。进程内同一台设备的读改写已经排队，冲突只来自另一个实例同时写这台
/// 设备；每次冲突后稍等再重读（见 [`CAS_BACKOFF`]），给对方把写入做完。
pub const CAS_ATTEMPTS: usize = 5;

/// 第 n 次冲突后等 n 倍这么久再重读。
const CAS_BACKOFF: std::time::Duration = std::time::Duration::from_millis(2);

/// 按设备分段的锁有多少段。段数只影响不相干的设备碰在同一段上的概率，锁只在一次读改写期间持有。
const DEVICE_LOCK_STRIPES: usize = 256;

/// 槽位值比规则的空闲过期多留多久：规则自己惰性过期，存储过期只是回收空间，必须更晚。
const SLOT_TTL_MARGIN_MS: i64 = 10 * 60 * 1000;

/// 设备值保留多久。设备一天没有任何推送，预算与摘要模式早已回到初始状态。
const DEVICE_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// 推迟记录比推迟时长多留多久：到期循环晚到也取得到。
const DELAY_TTL_MARGIN_MS: i64 = 10 * 60 * 1000;

/// 一批（推迟到期的提示、会话波次）最多按多少条进闸门。条数再多设备上也只显示「99+」，
/// 这只是不让一批的计算随条数无界增长。
const MAX_BATCH_ADMITS: u32 = 1_000;

/// 一次闸门判定的请求。
#[derive(Debug, Clone)]
pub struct AdmitRequest {
    pub key: GateSlotKey,
    pub kind: ConversationKind,
    pub tier: Tier,
    /// 按几条消息进闸门：平时是 1，推迟到期的一批、会话波次的一批是这期间来的条数。
    pub times: u32,
    /// 这次要展示的那条（一批时是其中最新的一条）。
    pub message: SlotMessage,
    pub signals: DeviceSignals,
    /// 顺带改角标表：计入未读的消息为 `Add`，只要当前值为 `Get`。
    pub badge: Option<BadgeChange>,
}

/// 一次闸门判定的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admission {
    pub slot: SlotAction,
    pub digest: Option<DigestAlert>,
    /// 角标绝对值（请求带了角标改动且写入成功时）。
    pub badge: Option<u32>,
    /// 状态用不上，按会话第一条放行的。
    pub fail_open: bool,
}

/// 尾部刷新到期的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailVerdict {
    pub action: DueAction,
    /// 槽位的尾巴：尾部刷新要展示其中最新的一条。
    pub side: SlotSide,
    pub badge: Option<u32>,
}

/// 会话被读之后这台设备的修正。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadVerdict {
    pub outcome: ReadOutcome,
    /// 推迟的提示被取消了。
    pub delay_cancelled: bool,
    pub badge: Option<BadgeValue>,
    /// 被读之前设备上展示着的那条（决策链把修正记在它上面）；状态用不上时为空。
    pub shown: SlotMessage,
}

/// 撤回了设备上正在展示的那条。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecallVerdict {
    /// 撤回后通知代表的条数。
    pub count: u32,
    /// 被撤回的那条。
    pub shown: SlotMessage,
    pub badge: Option<u32>,
}

/// 一次读改写的计划：要写的内容（`None` 表示什么都不用写）与规则给出的结果。
struct Planned<R> {
    commit: Option<GateCommit>,
    result: R,
}

/// 一次读改写的结局。
enum Updated<R> {
    Applied(R, Option<BadgeValue>),
    Skipped(R),
    /// 比较后写入一直冲突，重试用完。
    Contended,
}

/// 设备回到前台的标记比闸门采用过的新：先对设备做一次 `on_active`。返回是否做了。
fn apply_active(gate: &Gate, device: &mut DeviceRecord, active_at_ms: i64) -> bool {
    if active_at_ms > device.active_applied_ms {
        gate.on_active(&mut device.state);
        device.active_applied_ms = active_at_ms;
        true
    } else {
        false
    }
}

fn slot_ttl_ms(gate: &Gate) -> i64 {
    gate.config()
        .slot_idle_expiry_ms
        .saturating_add(SLOT_TTL_MARGIN_MS)
}

fn slot_label(action: SlotAction) -> &'static str {
    match action {
        SlotAction::Loud { .. } => "loud",
        SlotAction::Quiet { .. } => "quiet",
        SlotAction::Defer { .. } => "defer",
        SlotAction::CountOnly { .. } => "count_only",
    }
}

/// 防打扰闸门。
pub struct NotifyGate {
    store: Arc<dyn NotifyGateStore>,
    fallback: Arc<dyn NotifyGateStore>,
    signals: Option<Arc<dyn DeviceSignalRepository>>,
    settings: Option<Arc<dyn UserNotifySettingsRepository>>,
    policy: Arc<NotifyPolicy>,
    metrics: Option<Arc<PushServerMetrics>>,
    /// 同一台设备的读改写在进程内排队，按设备分段。
    device_locks: Box<[tokio::sync::Mutex<()>]>,
}

impl NotifyGate {
    pub fn new(
        store: Arc<dyn NotifyGateStore>,
        fallback: Arc<dyn NotifyGateStore>,
        policy: Arc<NotifyPolicy>,
    ) -> Self {
        Self {
            store,
            fallback,
            signals: None,
            settings: None,
            policy,
            metrics: None,
            device_locks: (0..DEVICE_LOCK_STRIPES)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect(),
        }
    }

    /// 这台设备所在的那一段锁。
    fn device_lock(&self, device: &GateDevice) -> &tokio::sync::Mutex<()> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        device.hash(&mut hasher);
        &self.device_locks[(hasher.finish() % self.device_locks.len() as u64) as usize]
    }

    pub fn with_signals(mut self, signals: Arc<dyn DeviceSignalRepository>) -> Self {
        self.signals = Some(signals);
        self
    }

    pub fn with_settings(mut self, settings: Arc<dyn UserNotifySettingsRepository>) -> Self {
        self.settings = Some(settings);
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<PushServerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn metric(&self, record: impl FnOnce(&PushServerMetrics)) {
        if let Some(metrics) = &self.metrics {
            record(metrics);
        }
    }

    fn gate(&self, tenant_id: &str) -> Gate {
        self.policy.of(tenant_id).gate
    }

    fn store_of(&self, origin: GateStoreOrigin) -> &dyn NotifyGateStore {
        match origin {
            GateStoreOrigin::Primary => self.store.as_ref(),
            GateStoreOrigin::Fallback => self.fallback.as_ref(),
        }
    }

    /// 接收者实际的预览：用户设置与租户强制隐藏取更严的。
    pub fn preview(&self, tenant_id: &str, user: PreviewMode) -> PreviewMode {
        self.policy.preview(tenant_id, user)
    }

    pub fn record_decision(&self, decision: Decision) {
        self.metric(|metrics| metrics.record_notify_decision(decision_label(decision)));
    }

    pub fn record_due(&self, kind: DueKind, result: &str) {
        self.metric(|metrics| metrics.record_due_entry(kind.label(), result));
    }

    pub fn record_correction(&self, kind: &str) {
        self.metric(|metrics| metrics.record_correction(kind));
    }

    /// 这批用户的用户级通知设置。读不到的用户（以及读取失败时的全部用户）用默认值。
    pub async fn user_prefs(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> HashMap<String, UserPrefs> {
        let Some(settings) = self.settings.as_ref() else {
            return HashMap::new();
        };
        match settings.settings_for(ctx, tenant_id, user_ids).await {
            Ok(prefs) => prefs,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    users = user_ids.len(),
                    "user notification settings unavailable; using defaults"
                );
                HashMap::new()
            }
        }
    }

    /// 这批设备的信号。读不到时当作没有信号：不知道设备回过前台，角标没有新基线。
    pub async fn device_signals(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        devices: &[(String, String)],
    ) -> HashMap<(String, String), DeviceSignals> {
        let Some(signals) = self.signals.as_ref() else {
            return HashMap::new();
        };
        if devices.is_empty() {
            return HashMap::new();
        }
        match signals.device_signals(ctx, tenant_id, devices).await {
            Ok(signals) => signals,
            Err(error) => {
                tracing::warn!(error = %error, "device signals unavailable");
                HashMap::new()
            }
        }
    }

    /// 在一个存储上做一次读改写：读、算、比较后写，冲突就稍等重来。整个过程持有这台设备的那段锁，
    /// 进程内同一台设备的读改写一个接一个做。
    async fn update<R, P>(
        &self,
        store: &dyn NotifyGateStore,
        key: &GateSlotKey,
        plan: &P,
    ) -> Result<Updated<R>, FlareError>
    where
        P: Fn(GateSnapshot) -> Planned<R> + Sync,
        R: Send,
    {
        let _serial = self.device_lock(&key.device).lock().await;
        for attempt in 1..=CAS_ATTEMPTS {
            let snapshot = store.load(key).await?;
            let planned = plan(snapshot);
            let Some(commit) = planned.commit else {
                return Ok(Updated::Skipped(planned.result));
            };
            match store.commit(&commit).await? {
                CommitOutcome::Applied { badge } => {
                    return Ok(Updated::Applied(planned.result, badge));
                }
                CommitOutcome::Conflict => {
                    self.metric(|metrics| metrics.record_gate_cas_conflict());
                    if attempt < CAS_ATTEMPTS {
                        tokio::time::sleep(CAS_BACKOFF * attempt as u32).await;
                    }
                }
            }
        }
        Ok(Updated::Contended)
    }

    /// 先在推送 Redis 上做；它不可用就改在进程内的闸门上做。
    async fn update_with_fallback<R, P>(
        &self,
        key: &GateSlotKey,
        plan: &P,
    ) -> Result<Updated<R>, FlareError>
    where
        P: Fn(GateSnapshot) -> Planned<R> + Sync,
        R: Send,
    {
        match self.update(self.store.as_ref(), key, plan).await {
            Ok(updated) => Ok(updated),
            Err(error) => {
                tracing::warn!(error = %error, "push gate store unavailable; using the in-process gate");
                self.metric(|metrics| metrics.record_gate_fallback());
                self.update(self.fallback.as_ref(), key, plan).await
            }
        }
    }

    /// 一条（或推迟到期的一批）消息进闸门。
    pub async fn admit(&self, now_ms: i64, request: &AdmitRequest) -> Admission {
        let gate = self.gate(&request.key.device.tenant_id);
        let times = request.times.clamp(1, MAX_BATCH_ADMITS);
        let input = GateInput {
            kind: request.kind,
            tier: request.tier,
            tag: ConversationTag::of(request.key.conversation_id.as_bytes()),
        };
        let plan = |snapshot: GateSnapshot| {
            let mut slot = snapshot.slot;
            let mut device = snapshot.device;
            // 同一条消息再进一次（broker 重投了整条记录）：不再计数、不再改角标，只把通知按当前
            // 条数静默更新一次——上一次的任务可能正是因为没发出去才重投的。
            let redelivered = times == 1
                && !request.message.message_id.is_empty()
                && slot.state.is_open()
                && slot.side.latest.message_id == request.message.message_id;
            if redelivered {
                return Planned {
                    commit: None,
                    result: (
                        SlotAction::Quiet {
                            count: slot.state.count(),
                        },
                        None,
                    ),
                };
            }
            apply_active(&gate, &mut device, request.signals.active_at_ms);

            let mut loud = false;
            let mut quiet = false;
            let mut last = None;
            let mut digest = None;
            for _ in 0..times {
                let admitted = gate.admit(now_ms, input, &mut slot.state, &mut device.state);
                match admitted.slot {
                    SlotAction::Loud { .. } => loud = true,
                    SlotAction::Quiet { .. } => quiet = true,
                    SlotAction::Defer { .. } | SlotAction::CountOnly { .. } => {}
                }
                last = Some(admitted.slot);
                if admitted.digest.is_some() {
                    digest = admitted.digest;
                }
            }
            // 一批消息同时进闸门：只要有一条该发就发一次，内容是最新的一条，条数是全部。
            let count = slot.state.count();
            let action = if loud {
                SlotAction::Loud { count }
            } else if quiet {
                SlotAction::Quiet { count }
            } else {
                last.unwrap_or(SlotAction::CountOnly { count })
            };

            slot.side.conversation_id = request.key.conversation_id.clone();
            slot.side.kind = kind_code(request.kind);
            if request.message.seq == 0 || request.message.seq >= slot.side.latest.seq {
                slot.side.latest = request.message.clone();
            }
            let sends = action.sends();
            if sends {
                slot.side.shown = request.message.clone();
                slot.side.shown_generation = device.state.generation();
                slot.side.shown_at_ms = now_ms;
            }

            let mut commit = GateCommit::new(request.key.clone());
            commit.slot = Some(VersionedWrite {
                expect: snapshot.slot_version,
                value: Some(slot.encode()),
            });
            commit.device = Some(VersionedWrite {
                expect: snapshot.device_version,
                value: Some(device.encode()),
            });
            commit.slot_ttl_ms = slot_ttl_ms(&gate);
            commit.device_ttl_ms = DEVICE_TTL_MS;
            commit.due = slot
                .state
                .due_ms()
                .map_or(DueChange::None, DueChange::Schedule);
            commit.shown_message_id = sends
                .then(|| request.message.message_id.clone())
                .filter(|message_id| !message_id.is_empty());
            commit.badge = request.badge.map(|change| BadgeWrite {
                change,
                base: request.signals.badge_base,
            });
            Planned {
                commit: Some(commit),
                result: (action, digest),
            }
        };

        let (slot, digest, badge) = match self.update_with_fallback(&request.key, &plan).await {
            Ok(Updated::Applied((slot, digest), badge)) => (slot, digest, badge),
            Ok(Updated::Skipped((slot, digest))) => (slot, digest, None),
            Ok(Updated::Contended) => {
                self.metric(|metrics| {
                    metrics.record_gate_fail_open("conflict");
                    metrics.record_gate_outcome("fail_open");
                });
                return Admission {
                    slot: fail_open(request.tier),
                    digest: None,
                    badge: None,
                    fail_open: true,
                };
            }
            Err(error) => {
                tracing::warn!(error = %error, "push gate unusable; letting the message through");
                self.metric(|metrics| {
                    metrics.record_gate_fail_open("store");
                    metrics.record_gate_outcome("fail_open");
                });
                return Admission {
                    slot: fail_open(request.tier),
                    digest: None,
                    badge: None,
                    fail_open: true,
                };
            }
        };
        self.metric(|metrics| {
            metrics.record_gate_outcome(slot_label(slot));
            if digest.is_some() {
                metrics.record_gate_outcome("digest");
            }
        });
        Admission {
            slot,
            digest,
            badge: badge.map(|badge| badge.count),
            fail_open: false,
        }
    }

    /// 尾部刷新到期。在登记它的那个存储上处理；冲突重试用完或存储不可用时放弃这次刷新
    /// （通知暂时显示旧的内容与计数，下一条消息或打开应用时纠正）。
    pub async fn tail_due(
        &self,
        now_ms: i64,
        entry: &DueEntry,
        signals: DeviceSignals,
        with_badge: bool,
    ) -> Option<TailVerdict> {
        let gate = self.gate(&entry.key.device.tenant_id);
        let plan = |snapshot: GateSnapshot| {
            let mut slot = snapshot.slot;
            let mut device = snapshot.device;
            let device_reset = apply_active(&gate, &mut device, signals.active_at_ms);
            let slot_before = slot.state.version;
            let device_before = device.state.version;
            let action = gate.on_due(now_ms, &mut slot.state, &mut device.state);
            let tail = matches!(action, DueAction::Tail { .. });
            if tail {
                slot.side.shown = slot.side.latest.clone();
                slot.side.shown_generation = device.state.generation();
                slot.side.shown_at_ms = now_ms;
            }
            let mut commit = GateCommit::new(entry.key.clone());
            commit.slot = Some(VersionedWrite {
                expect: snapshot.slot_version,
                value: (tail || slot.state.version != slot_before).then(|| slot.encode()),
            });
            commit.device = Some(VersionedWrite {
                expect: snapshot.device_version,
                value: (device_reset || device.state.version != device_before)
                    .then(|| device.encode()),
            });
            commit.slot_ttl_ms = slot_ttl_ms(&gate);
            commit.device_ttl_ms = DEVICE_TTL_MS;
            commit.due = match action {
                DueAction::Defer { due_ms } => DueChange::Schedule(due_ms),
                DueAction::Tail { .. } | DueAction::Nothing => DueChange::None,
            };
            commit.shown_message_id = tail
                .then(|| slot.side.latest.message_id.clone())
                .filter(|message_id| !message_id.is_empty());
            commit.badge = (tail && with_badge).then_some(BadgeWrite {
                change: BadgeChange::Get,
                base: signals.badge_base,
            });
            Planned {
                commit: Some(commit),
                result: (action, slot.side.clone()),
            }
        };
        let store = self.store_of(entry.origin);
        let verdict = match self.update(store, &entry.key, &plan).await {
            Ok(Updated::Applied((action, side), badge)) => TailVerdict {
                action,
                side,
                badge: badge.map(|badge| badge.count),
            },
            Ok(Updated::Skipped((action, side))) => TailVerdict {
                action,
                side,
                badge: None,
            },
            Ok(Updated::Contended) => {
                self.metric(|metrics| metrics.record_gate_fail_open("conflict"));
                return None;
            }
            Err(error) => {
                tracing::warn!(error = %error, "tail refresh dropped: gate store unavailable");
                return None;
            }
        };
        self.metric(|metrics| {
            metrics.record_gate_outcome(match verdict.action {
                DueAction::Tail { .. } => "tail",
                DueAction::Defer { .. } => "due_defer",
                DueAction::Nothing => "due_nothing",
            })
        });
        Some(verdict)
    }

    /// 会话在别处被读到 `read_seq`：取消这台设备上推迟的提示，关闭它的槽位，去掉角标里这个会话的贡献。
    /// 状态用不上时只当没有可修正的：那条通知留到设备打开应用时清。
    pub async fn read(
        &self,
        now_ms: i64,
        key: &GateSlotKey,
        read_seq: u64,
        signals: DeviceSignals,
        with_badge: bool,
    ) -> ReadVerdict {
        let mut delay_cancelled = false;
        for store in [self.store.as_ref(), self.fallback.as_ref()] {
            match store.cancel_delayed(key, read_seq).await {
                Ok(cancelled) => delay_cancelled |= cancelled,
                Err(error) => {
                    tracing::debug!(error = %error, "delayed alert not cancelled on read");
                }
            }
        }
        let gate = self.gate(&key.device.tenant_id);
        let plan = |snapshot: GateSnapshot| {
            let mut slot = snapshot.slot;
            let mut device = snapshot.device;
            let device_reset = apply_active(&gate, &mut device, signals.active_at_ms);
            let was_open = slot.state.is_open();
            let shown = slot.side.shown.clone();
            let outcome = gate.on_read(now_ms, &mut slot.state, &device.state);
            if !was_open && !device_reset && !with_badge {
                return Planned {
                    commit: None,
                    result: (outcome, shown),
                };
            }
            let mut commit = GateCommit::new(key.clone());
            commit.slot = was_open.then(|| VersionedWrite {
                expect: snapshot.slot_version,
                value: Some(slot.encode()),
            });
            commit.device = device_reset.then(|| VersionedWrite {
                expect: snapshot.device_version,
                value: Some(device.encode()),
            });
            commit.slot_ttl_ms = slot_ttl_ms(&gate);
            commit.device_ttl_ms = DEVICE_TTL_MS;
            commit.due = if outcome.cancel_tail {
                DueChange::Cancel
            } else {
                DueChange::None
            };
            commit.badge = with_badge.then_some(BadgeWrite {
                change: BadgeChange::Remove,
                base: signals.badge_base,
            });
            Planned {
                commit: Some(commit),
                result: (outcome, shown),
            }
        };
        let ((outcome, shown), badge) = match self.update_with_fallback(key, &plan).await {
            Ok(Updated::Applied(result, badge)) => (result, badge),
            Ok(Updated::Skipped(result)) => (result, None),
            Ok(Updated::Contended) | Err(_) => {
                ((ReadOutcome::default(), SlotMessage::default()), None)
            }
        };
        ReadVerdict {
            outcome,
            delay_cancelled,
            badge,
            shown,
        }
    }

    /// 消息 `message_id` 被撤回：这台设备上正在展示的就是它时，把展示记录清掉、角标减一，
    /// 返回撤回后的条数，由调用方发撤回替换。不是它（已经换成更新的一条、槽位已关、设备回过前台）
    /// 时返回 `None`。
    pub async fn recall(
        &self,
        now_ms: i64,
        key: &GateSlotKey,
        message_id: &str,
        with_badge: bool,
    ) -> Option<RecallVerdict> {
        if message_id.is_empty() {
            return None;
        }
        let gate = self.gate(&key.device.tenant_id);
        let expiry_ms = gate.config().slot_idle_expiry_ms;
        let plan = |snapshot: GateSnapshot| {
            let mut slot = snapshot.slot;
            let device = snapshot.device;
            let showing = slot.state.is_open()
                && slot.side.shown.message_id == message_id
                && slot.side.shown_generation == device.state.generation()
                && now_ms.saturating_sub(slot.side.shown_at_ms) < expiry_ms;
            if !showing {
                return Planned {
                    commit: None,
                    result: None,
                };
            }
            let count = slot.state.count().saturating_sub(1);
            let shown = std::mem::take(&mut slot.side.shown);
            // 只改了尾巴，版本照样加一：并发的写入据此发现冲突，不会把展示记录写回去。
            slot.state.version = slot.state.version.wrapping_add(1);
            let mut commit = GateCommit::new(key.clone());
            commit.slot = Some(VersionedWrite {
                expect: snapshot.slot_version,
                value: Some(slot.encode()),
            });
            commit.slot_ttl_ms = slot_ttl_ms(&gate);
            commit.device_ttl_ms = DEVICE_TTL_MS;
            commit.badge = with_badge.then_some(BadgeWrite {
                change: BadgeChange::Decrement,
                base: None,
            });
            Planned {
                commit: Some(commit),
                result: Some((count, shown)),
            }
        };
        match self.update_with_fallback(key, &plan).await {
            Ok(Updated::Applied(Some((count, shown)), badge)) => Some(RecallVerdict {
                count,
                shown,
                badge: badge.map(|badge| badge.count),
            }),
            _ => None,
        }
    }

    /// 登记一条推迟的提示。存储不可用时报错，调用方去掉推迟立即进闸门：宁可早提示，不丢提示。
    pub async fn defer(
        &self,
        alert: &DelayedAlert,
        delay_ms: i64,
        badge: Option<BadgeWrite>,
    ) -> Result<Option<u32>, FlareError> {
        let ttl_ms = delay_ms.max(0).saturating_add(DELAY_TTL_MARGIN_MS);
        let badge = self.store.defer_alert(alert, ttl_ms, badge).await?;
        Ok(badge.map(|badge| badge.count))
    }

    /// 取走到期的推迟提示；已被取消时为 `None`。
    pub async fn take_delayed(&self, entry: &DueEntry) -> Option<DelayedAlert> {
        match self.store_of(entry.origin).take_delayed(&entry.key).await {
            Ok(alert) => alert,
            Err(error) => {
                tracing::warn!(error = %error, "delayed alert dropped: gate store unavailable");
                None
            }
        }
    }

    /// 领取到期条目（槽位的尾部刷新与推迟提示、会话的波次）：先推送 Redis，再进程内闸门，
    /// 按到期时刻排序。
    pub async fn claim_due(&self, now_ms: i64, limit: usize) -> Vec<DueClaim> {
        let mut entries = match self.store.claim_due(now_ms, limit).await {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(error = %error, "due entries not claimed from push redis");
                Vec::new()
            }
        };
        if let Ok(fallback) = self.fallback.claim_due(now_ms, limit).await {
            entries.extend(fallback);
        }
        entries.sort();
        entries
    }

    /// 单独改一次角标表（多端「只更新角标」）。存储不可用时改在进程内做。
    pub async fn badge(
        &self,
        device: &GateDevice,
        conversation_id: &str,
        badge: BadgeWrite,
    ) -> Option<u32> {
        let value = match self.store.badge(device, conversation_id, badge).await {
            Ok(value) => Some(value),
            Err(_) => self
                .fallback
                .badge(device, conversation_id, badge)
                .await
                .ok(),
        };
        value.map(|value| value.count)
    }

    /// 最近展示过这条消息的设备（两个存储合并去重）。
    pub async fn shown_devices(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Vec<GateDevice> {
        let mut devices = Vec::new();
        for store in [self.store.as_ref(), self.fallback.as_ref()] {
            match store
                .shown_devices(tenant_id, conversation_id, message_id)
                .await
            {
                Ok(found) => devices.extend(found),
                Err(error) => {
                    tracing::debug!(error = %error, "shown index not readable");
                }
            }
        }
        devices.sort();
        devices.dedup();
        devices
    }
}

#[cfg(test)]
mod tests;
