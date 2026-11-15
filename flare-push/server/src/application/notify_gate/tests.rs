//! 用规则 crate 的向量（`flare-notify-rules/tests/vectors/gate.json`）回放服务端的接线：
//! 状态经存储编码、比较后写回，尾部刷新经到期队列领取，「回到前台」经设备信号惰性生效。
//! 回放规则与 crate 的 README「Vectors」一节相同，得出的日志或汇总必须与文件里的期望一致。

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use flare_notify_rules::{ConversationKind, Gate, GateConfig, SlotAction, Tier};
use serde_json::{Map, Value, json};

use super::*;
use crate::config::GateOverride;
use crate::domain::model::{GateDevice, NotifyPolicy, TenantNotifyPolicy};
use crate::infrastructure::notify::MemoryGateStore;

const TENANT: &str = "t-vectors";

fn load_gate_vectors() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../flare-protocol/flare-notify-rules/tests/vectors/gate.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("missing `{key}` in {value}"))
}

fn int(value: &Value, key: &str) -> i64 {
    value[key]
        .as_i64()
        .unwrap_or_else(|| panic!("missing `{key}` in {value}"))
}

fn tier(name: &str) -> Tier {
    match name {
        "quiet" => Tier::Quiet,
        "normal" => Tier::Normal,
        "priority" => Tier::Priority,
        other => panic!("unknown tier `{other}`"),
    }
}

fn kind(name: &str) -> ConversationKind {
    match name {
        "direct" => ConversationKind::Direct,
        "group" => ConversationKind::Group,
        other => panic!("unknown kind `{other}`"),
    }
}

/// 场景的参数覆盖走服务端的租户覆盖：字段名相同，同一份解析与校验。
fn scenario_gate(config: &Value) -> Gate {
    let config = if config.is_null() {
        json!({})
    } else {
        config.clone()
    };
    let overrides: GateOverride =
        serde_json::from_value(config).expect("vector config parses as a tenant override");
    Gate::new(
        overrides
            .apply(GateConfig::default())
            .expect("vector config validates"),
    )
}

/// 展开 `admit_series`，按 `at_ms` 稳定排序。
fn expand(events: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for event in events {
        if text(event, "op") == "admit_series" {
            let start = int(event, "start_ms");
            let every = int(event, "every_ms");
            for i in 0..int(event, "count") {
                out.push(json!({
                    "at_ms": start + i * every,
                    "op": "admit",
                    "conversation": event["conversation"],
                    "kind": event["kind"],
                    "tier": event["tier"],
                }));
            }
        } else {
            out.push(event.clone());
        }
    }
    out.sort_by_key(|event| int(event, "at_ms"));
    out
}

struct ServerReplay {
    gate: NotifyGate,
    store: Arc<MemoryGateStore>,
    device: GateDevice,
    /// Online 写的「回到前台」标记：每次 `active` 加一。
    active_marker: i64,
    seq: u64,
    log: Vec<Value>,
}

impl ServerReplay {
    fn new(scenario: &Value) -> Self {
        let mut tenants = HashMap::new();
        tenants.insert(
            TENANT.to_string(),
            TenantNotifyPolicy {
                gate: scenario_gate(&scenario["config"]),
                ..TenantNotifyPolicy::default()
            },
        );
        let store = Arc::new(MemoryGateStore::primary());
        let gate = NotifyGate::new(
            store.clone(),
            Arc::new(MemoryGateStore::fallback()),
            Arc::new(NotifyPolicy::new(tenants)),
        );
        Self {
            gate,
            store,
            device: GateDevice::new(TENANT, "u1", "phone"),
            active_marker: 0,
            seq: 0,
            log: Vec::new(),
        }
    }

    fn signals(&self) -> DeviceSignals {
        DeviceSignals {
            active_at_ms: self.active_marker,
            badge_base: None,
        }
    }

    fn key(&self, conversation: &str) -> GateSlotKey {
        GateSlotKey::new(self.device.clone(), conversation)
    }

    /// 按到期时刻（同一时刻按会话名）领取不晚于 `limit` 的条目，在到期时刻处理。
    async fn run_dues(&mut self, limit: Option<i64>) {
        loop {
            let bound = limit.unwrap_or(i64::MAX);
            let Some(claim) = self.gate.claim_due(bound, 1).await.into_iter().next() else {
                break;
            };
            let DueClaim::Slot(entry) = claim else {
                panic!("the gate replay registers no conversation waves");
            };
            let verdict = self
                .gate
                .tail_due(entry.due_ms, &entry, self.signals(), false)
                .await
                .expect("the in-memory store never contends in a replay");
            let mut log = json!({
                "at_ms": entry.due_ms,
                "op": "due",
                "conversation": entry.key.conversation_id,
            });
            match verdict.action {
                DueAction::Tail { count } => {
                    log["due"] = json!("tail");
                    log["count"] = json!(count);
                }
                DueAction::Defer { due_ms } => {
                    log["due"] = json!("defer");
                    log["due_ms"] = json!(due_ms);
                }
                DueAction::Nothing => log["due"] = json!("nothing"),
            }
            self.log.push(log);
        }
    }

    async fn apply(&mut self, event: &Value) {
        let at = int(event, "at_ms");
        self.run_dues(Some(at)).await;
        match text(event, "op") {
            "admit" => {
                let conversation = text(event, "conversation").to_owned();
                self.seq += 1;
                let request = AdmitRequest {
                    key: self.key(&conversation),
                    kind: kind(text(event, "kind")),
                    tier: tier(text(event, "tier")),
                    times: 1,
                    message: SlotMessage {
                        message_id: format!("m-{}", self.seq),
                        seq: self.seq,
                        alert_class: 3,
                    },
                    signals: self.signals(),
                    badge: None,
                };
                let admission = self.gate.admit(at, &request).await;
                assert!(!admission.fail_open);
                let (slot, due_ms, count) = match admission.slot {
                    SlotAction::Loud { count } => ("loud", None, count),
                    SlotAction::Quiet { count } => ("quiet", None, count),
                    SlotAction::Defer { due_ms, count } => ("defer", Some(due_ms), count),
                    SlotAction::CountOnly { count } => ("count_only", None, count),
                };
                let mut entry = json!({
                    "at_ms": at,
                    "op": "admit",
                    "conversation": conversation,
                    "slot": slot,
                });
                if let Some(due_ms) = due_ms {
                    entry["due_ms"] = json!(due_ms);
                }
                entry["count"] = json!(count);
                if let Some(digest) = admission.digest {
                    entry["digest"] = json!({
                        "conversations": digest.conversations,
                        "messages": digest.messages,
                    });
                }
                self.log.push(entry);
            }
            "read" => {
                let conversation = text(event, "conversation").to_owned();
                let verdict = self
                    .gate
                    .read(at, &self.key(&conversation), 0, self.signals(), false)
                    .await;
                self.log.push(json!({
                    "at_ms": at,
                    "op": "read",
                    "conversation": conversation,
                    "clear": verdict.outcome.clear,
                    "cancel_tail": verdict.outcome.cancel_tail,
                }));
            }
            "active" => {
                // Online 写下标记；闸门下一次碰到这台设备时惰性生效。回放规则里 `active` 清空
                // 到期队列，这里撤掉这台设备的登记，与之对应。
                self.active_marker += 1;
                self.store.clear_device_due(&self.device);
                self.log.push(json!({"at_ms": at, "op": "active"}));
            }
            other => panic!("unknown event op `{other}`"),
        }
    }
}

async fn replay(scenario: &Value) -> Vec<Value> {
    let mut run = ServerReplay::new(scenario);
    let events = scenario["events"].as_array().expect("events");
    for event in expand(events) {
        run.apply(&event).await;
    }
    run.run_dues(None).await;
    run.log
}

const TOTAL_KEYS: [&str; 13] = [
    "admit",
    "loud",
    "quiet",
    "defer",
    "count_only",
    "digest",
    "tail",
    "due_defer",
    "due_nothing",
    "read",
    "active",
    "vendor_calls",
    "rings",
];

fn max_in_window(times: &[i64], window_ms: i64) -> u64 {
    let mut best = 0;
    let mut start = 0;
    for (end, &at) in times.iter().enumerate() {
        while at - times[start] >= window_ms {
            start += 1;
        }
        best = best.max(end - start + 1);
    }
    best as u64
}

/// 大场景的汇总，口径与 crate 的 README 相同。
fn summary(log: &[Value], window_ms: i64) -> Value {
    let mut totals: BTreeMap<&str, u64> = TOTAL_KEYS.iter().map(|key| (*key, 0)).collect();
    let mut bump = |key: &str| {
        *totals
            .get_mut(key)
            .unwrap_or_else(|| panic!("unknown total `{key}`")) += 1;
    };
    let mut ring_times = Vec::new();
    let mut last_count = Map::new();
    let mut digests = Vec::new();
    for entry in log {
        let at = int(entry, "at_ms");
        match text(entry, "op") {
            "admit" => {
                bump("admit");
                let slot = text(entry, "slot");
                bump(slot);
                if slot == "loud" || slot == "quiet" {
                    bump("vendor_calls");
                    last_count.insert(
                        text(entry, "conversation").to_owned(),
                        entry["count"].clone(),
                    );
                }
                if slot == "loud" {
                    bump("rings");
                    ring_times.push(at);
                }
                if let Some(digest) = entry.get("digest") {
                    bump("digest");
                    bump("vendor_calls");
                    bump("rings");
                    ring_times.push(at);
                    digests.push(json!({
                        "at_ms": at,
                        "conversations": digest["conversations"],
                        "messages": digest["messages"],
                    }));
                }
            }
            "due" => match text(entry, "due") {
                "tail" => {
                    bump("tail");
                    bump("vendor_calls");
                    last_count.insert(
                        text(entry, "conversation").to_owned(),
                        entry["count"].clone(),
                    );
                }
                "defer" => bump("due_defer"),
                _ => bump("due_nothing"),
            },
            "read" => bump("read"),
            "active" => bump("active"),
            other => panic!("unknown log op `{other}`"),
        }
    }
    let totals: Map<String, Value> = TOTAL_KEYS
        .iter()
        .map(|key| (key.to_string(), json!(totals[key])))
        .collect();
    json!({
        "totals": totals,
        "ring_times_ms": ring_times,
        "last_count": last_count,
        "max_rings_in_window": {
            "window_ms": window_ms,
            "max": max_in_window(&ring_times, window_ms),
        },
        "digests": digests,
    })
}

#[tokio::test]
async fn the_rule_vectors_replay_through_the_server_gate() {
    let vectors = load_gate_vectors();
    assert_eq!(vectors["format"], 1);
    let scenarios = vectors["scenarios"].as_array().expect("scenarios");
    assert!(!scenarios.is_empty());
    for scenario in scenarios {
        let name = text(scenario, "name");
        let log = replay(scenario).await;
        let expect = scenario["expect"].as_object().expect("expect");
        if let Some(Value::Array(want)) = expect.get("log") {
            for (i, (got, want)) in log.iter().zip(want).enumerate() {
                assert_eq!(got, want, "scenario `{name}` log[{i}]");
            }
            assert_eq!(log.len(), want.len(), "scenario `{name}` log length");
            continue;
        }
        let window_ms = expect
            .get("max_rings_in_window")
            .and_then(|window| window["window_ms"].as_i64())
            .unwrap_or(60_000);
        let got = summary(&log, window_ms);
        for (key, want) in expect {
            assert_eq!(got.get(key), Some(want), "scenario `{name}` {key}");
        }
    }
}

/// 存储写不进去（一直冲突）时放行：按会话的第一条处理，普通与优先档位响铃。
#[tokio::test]
async fn contention_fails_open() {
    struct AlwaysConflicts(MemoryGateStore);

    #[async_trait::async_trait]
    impl NotifyGateStore for AlwaysConflicts {
        async fn load(&self, key: &GateSlotKey) -> Result<GateSnapshot, FlareError> {
            self.0.load(key).await
        }
        async fn commit(&self, _commit: &GateCommit) -> Result<CommitOutcome, FlareError> {
            Ok(CommitOutcome::Conflict)
        }
        async fn defer_alert(
            &self,
            alert: &DelayedAlert,
            ttl_ms: i64,
            badge: Option<BadgeWrite>,
        ) -> Result<Option<BadgeValue>, FlareError> {
            self.0.defer_alert(alert, ttl_ms, badge).await
        }
        async fn take_delayed(
            &self,
            key: &GateSlotKey,
        ) -> Result<Option<DelayedAlert>, FlareError> {
            self.0.take_delayed(key).await
        }
        async fn cancel_delayed(
            &self,
            key: &GateSlotKey,
            read_seq: u64,
        ) -> Result<bool, FlareError> {
            self.0.cancel_delayed(key, read_seq).await
        }
        async fn claim_due(&self, now_ms: i64, limit: usize) -> Result<Vec<DueClaim>, FlareError> {
            self.0.claim_due(now_ms, limit).await
        }
        async fn shown_devices(
            &self,
            tenant_id: &str,
            conversation_id: &str,
            message_id: &str,
        ) -> Result<Vec<GateDevice>, FlareError> {
            self.0
                .shown_devices(tenant_id, conversation_id, message_id)
                .await
        }
        async fn badge(
            &self,
            device: &GateDevice,
            conversation_id: &str,
            badge: BadgeWrite,
        ) -> Result<BadgeValue, FlareError> {
            self.0.badge(device, conversation_id, badge).await
        }
    }

    let gate = NotifyGate::new(
        Arc::new(AlwaysConflicts(MemoryGateStore::primary())),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let request = AdmitRequest {
        key: GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "c1"),
        kind: ConversationKind::Group,
        tier: Tier::Normal,
        times: 1,
        message: SlotMessage::default(),
        signals: DeviceSignals::default(),
        badge: Some(BadgeChange::Add(1)),
    };
    for _ in 0..3 {
        let admission = gate.admit(1_000, &request).await;
        assert!(admission.fail_open);
        assert_eq!(admission.slot, SlotAction::Loud { count: 1 });
        assert_eq!(admission.badge, None);
    }
}

/// 同一台设备的多个会话同时进闸门（设备状态是共用的一份）：进程内按设备排队，不互相冲突到放行，
/// 设备预算照常生效——第一条响铃，紧接着的其他会话在设备最小响铃间隔内静默。
#[tokio::test]
async fn concurrent_conversations_of_one_device_do_not_contend() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Interleaving {
        inner: MemoryGateStore,
        conflicts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl NotifyGateStore for Interleaving {
        async fn load(&self, key: &GateSlotKey) -> Result<GateSnapshot, FlareError> {
            let snapshot = self.inner.load(key).await;
            // 读完先让出执行权：不排队时别的会话会在读与写之间读到同一个版本。
            tokio::task::yield_now().await;
            snapshot
        }
        async fn commit(&self, commit: &GateCommit) -> Result<CommitOutcome, FlareError> {
            let outcome = self.inner.commit(commit).await?;
            if matches!(outcome, CommitOutcome::Conflict) {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        }
        async fn defer_alert(
            &self,
            alert: &DelayedAlert,
            ttl_ms: i64,
            badge: Option<BadgeWrite>,
        ) -> Result<Option<BadgeValue>, FlareError> {
            self.inner.defer_alert(alert, ttl_ms, badge).await
        }
        async fn take_delayed(
            &self,
            key: &GateSlotKey,
        ) -> Result<Option<DelayedAlert>, FlareError> {
            self.inner.take_delayed(key).await
        }
        async fn cancel_delayed(
            &self,
            key: &GateSlotKey,
            read_seq: u64,
        ) -> Result<bool, FlareError> {
            self.inner.cancel_delayed(key, read_seq).await
        }
        async fn claim_due(&self, now_ms: i64, limit: usize) -> Result<Vec<DueClaim>, FlareError> {
            self.inner.claim_due(now_ms, limit).await
        }
        async fn shown_devices(
            &self,
            tenant_id: &str,
            conversation_id: &str,
            message_id: &str,
        ) -> Result<Vec<GateDevice>, FlareError> {
            self.inner
                .shown_devices(tenant_id, conversation_id, message_id)
                .await
        }
        async fn badge(
            &self,
            device: &GateDevice,
            conversation_id: &str,
            badge: BadgeWrite,
        ) -> Result<BadgeValue, FlareError> {
            self.inner.badge(device, conversation_id, badge).await
        }
    }

    let store = Arc::new(Interleaving {
        inner: MemoryGateStore::primary(),
        conflicts: AtomicUsize::new(0),
    });
    let gate = NotifyGate::new(
        store.clone(),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let requests: Vec<AdmitRequest> = (0..8)
        .map(|i| AdmitRequest {
            key: GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), format!("c{i}")),
            kind: ConversationKind::Direct,
            tier: Tier::Normal,
            times: 1,
            message: SlotMessage::default(),
            signals: DeviceSignals::default(),
            badge: None,
        })
        .collect();
    let admissions =
        futures::future::join_all(requests.iter().map(|request| gate.admit(1_000, request))).await;

    assert_eq!(store.conflicts.load(Ordering::SeqCst), 0);
    assert!(
        admissions.iter().all(|admission| !admission.fail_open),
        "{admissions:?}"
    );
    let loud = admissions
        .iter()
        .filter(|admission| matches!(admission.slot, SlotAction::Loud { .. }))
        .count();
    assert_eq!(loud, 1, "{admissions:?}");
}

/// 推送 Redis 不可用时改用进程内闸门：第一条照常响，紧跟着的同一会话被合并。
#[tokio::test]
async fn an_unavailable_store_falls_back_to_the_in_process_gate() {
    struct Down;

    fn down() -> FlareError {
        FlareError::localized(
            flare_server_core::error::ErrorCode::ServiceUnavailable,
            "push redis down",
        )
    }

    #[async_trait::async_trait]
    impl NotifyGateStore for Down {
        async fn load(&self, _key: &GateSlotKey) -> Result<GateSnapshot, FlareError> {
            Err(down())
        }
        async fn commit(&self, _commit: &GateCommit) -> Result<CommitOutcome, FlareError> {
            Err(down())
        }
        async fn defer_alert(
            &self,
            _alert: &DelayedAlert,
            _ttl_ms: i64,
            _badge: Option<BadgeWrite>,
        ) -> Result<Option<BadgeValue>, FlareError> {
            Err(down())
        }
        async fn take_delayed(
            &self,
            _key: &GateSlotKey,
        ) -> Result<Option<DelayedAlert>, FlareError> {
            Err(down())
        }
        async fn cancel_delayed(
            &self,
            _key: &GateSlotKey,
            _read_seq: u64,
        ) -> Result<bool, FlareError> {
            Err(down())
        }
        async fn claim_due(
            &self,
            _now_ms: i64,
            _limit: usize,
        ) -> Result<Vec<DueClaim>, FlareError> {
            Err(down())
        }
        async fn shown_devices(
            &self,
            _tenant_id: &str,
            _conversation_id: &str,
            _message_id: &str,
        ) -> Result<Vec<GateDevice>, FlareError> {
            Err(down())
        }
        async fn badge(
            &self,
            _device: &GateDevice,
            _conversation_id: &str,
            _badge: BadgeWrite,
        ) -> Result<BadgeValue, FlareError> {
            Err(down())
        }
    }

    let gate = NotifyGate::new(
        Arc::new(Down),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let request = |seq: u64| AdmitRequest {
        key: GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "c1"),
        kind: ConversationKind::Group,
        tier: Tier::Normal,
        times: 1,
        message: SlotMessage {
            message_id: format!("m-{seq}"),
            seq,
            alert_class: 3,
        },
        signals: DeviceSignals::default(),
        badge: None,
    };
    let first = gate.admit(1_000, &request(1)).await;
    assert!(!first.fail_open);
    assert_eq!(first.slot, SlotAction::Loud { count: 1 });
    let second = gate.admit(1_100, &request(2)).await;
    assert!(matches!(second.slot, SlotAction::Defer { count: 2, .. }));
    // 尾部刷新登记在进程内闸门上，到期循环从那里领取。
    let due = gate.claim_due(i64::MAX, 10).await;
    assert_eq!(due.len(), 1);
    assert!(matches!(&due[0], DueClaim::Slot(entry) if entry.origin == GateStoreOrigin::Fallback));
}

/// 一批推迟到期的消息一起进闸门：发一次，条数是全部，展示的是最新一条。
#[tokio::test]
async fn a_delayed_batch_is_sent_once_with_the_full_count() {
    let store = Arc::new(MemoryGateStore::primary());
    let gate = NotifyGate::new(
        store.clone(),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let key = GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "c1");
    let admission = gate
        .admit(
            20_000,
            &AdmitRequest {
                key: key.clone(),
                kind: ConversationKind::Group,
                tier: Tier::Normal,
                times: 4,
                message: SlotMessage {
                    message_id: "m-4".into(),
                    seq: 4,
                    alert_class: 3,
                },
                signals: DeviceSignals::default(),
                badge: None,
            },
        )
        .await;
    assert_eq!(admission.slot, SlotAction::Loud { count: 4 });
    // 展示的是 m-4：撤回它要发撤回替换，条数减一。
    let recalled = gate
        .recall(21_000, &key, "m-4", false)
        .await
        .expect("shown");
    assert_eq!(recalled.count, 3);
    assert_eq!(recalled.shown.message_id, "m-4");
    assert!(
        gate.recall(21_500, &key, "m-4", false).await.is_none(),
        "只修正一次"
    );
    assert!(gate.recall(21_500, &key, "m-1", false).await.is_none());
}

/// 设备回到前台之后，之前的通知已被清掉：读到这个会话不再发清除，撤回也不再修正。
#[tokio::test]
async fn a_device_back_in_the_foreground_needs_no_corrections() {
    let gate = NotifyGate::new(
        Arc::new(MemoryGateStore::primary()),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let key = GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "c1");
    let request = AdmitRequest {
        key: key.clone(),
        kind: ConversationKind::Direct,
        tier: Tier::Normal,
        times: 1,
        message: SlotMessage {
            message_id: "m-1".into(),
            seq: 1,
            alert_class: 3,
        },
        signals: DeviceSignals::default(),
        badge: None,
    };
    gate.admit(1_000, &request).await;
    let back = DeviceSignals {
        active_at_ms: 2_000,
        badge_base: None,
    };
    let read = gate.read(3_000, &key, 1, back, false).await;
    assert!(!read.outcome.clear);

    gate.admit(4_000, &request).await;
    let read = gate.read(5_000, &key, 1, back, false).await;
    assert!(read.outcome.clear, "回到前台之后新推的通知照常清除");
}
