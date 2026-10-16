//! 每个 (租户, Hook) 一个熔断器。
//!
//! 关闭时每次调用都发出；同一个租户的同一条 Hook 在 `failure_window` 内连续 `failure_threshold` 次
//! 调用失败或超时，熔断打开 `open_for`：期间不调用 Hook，按 Hook 的错误策略处理（必需的 Hook 与
//! 不可用同样拒绝，可选的跳过）。到期后半开，只放一次探测：探测成功关闭，失败再打开一个窗口；探测
//! 未结束时其余调用仍短路。Hook 给出结论（放行或拒绝）都算健康。
//!
//! 只为最近失败过的 Hook 留状态：成功即删，表的大小随正在失败的 Hook 数而不随租户数增长；另有
//! 上限 [`MAX_TRACKED_CIRCUITS`]，满了先清掉连续失败已过窗口的条目，仍满时不再追踪新的 Hook
//! （照常调用，不熔断）。日志里的租户与 Hook 名一律转义。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::domain::model::LoggableName;

/// 探测未结束时，其余调用的重试提示。
pub const PROBE_RETRY_AFTER: Duration = Duration::from_secs(1);

/// 同时追踪的熔断器上限。
pub const MAX_TRACKED_CIRCUITS: usize = 100_000;

/// 熔断参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookCircuitPolicy {
    pub enabled: bool,
    /// 连续失败多少次打开（至少 1）。
    pub failure_threshold: u32,
    /// 连续失败须落在这个窗口里：距这一串第一次失败超过窗口，重新计数。
    pub failure_window: Duration,
    /// 打开多久后半开。
    pub open_for: Duration,
}

impl Default for HookCircuitPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            failure_window: Duration::from_secs(30),
            open_for: Duration::from_secs(20),
        }
    }
}

/// 熔断器的键：租户、Hook 类型与名称。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HookCircuitKey {
    pub tenant_id: String,
    pub hook_type: String,
    pub hook_name: String,
}

/// 状态变化（日志已在这里记，调用方据此记指标）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookCircuitTransition {
    /// 连续失败达到阈值，打开。
    Opened,
    /// 半开探测失败，再打开一个窗口。
    Reopened,
    /// 半开探测成功，关闭。
    Closed,
}

impl HookCircuitTransition {
    /// 指标标签取值（固定）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Reopened => "reopened",
            Self::Closed => "closed",
        }
    }
}

/// 一次调用能否发出。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookCircuitAdmission {
    /// 发出；`probe` 为半开时的那一次探测，结果决定开合。
    Admitted { probe: bool },
    /// 熔断打开（或探测未结束），不调用；`retry_after` 为大约还要多久。
    Rejected { retry_after: Duration },
}

/// 调用结果对熔断的意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookCircuitResult {
    /// Hook 给出了结论。
    Healthy,
    /// 调用失败或超时。
    Unhealthy,
    /// 说明不了 Hook 的健康（调用方放弃了这次调用）。
    Neutral,
}

#[derive(Debug)]
struct Circuit {
    consecutive_failures: u32,
    streak_started: Instant,
    /// 打开时为窗口结束时刻；过了这个时刻即半开。
    open_until: Option<Instant>,
    probing: bool,
    failed_probes: u32,
}

/// 全部 (租户, Hook) 的熔断器。
pub struct HookCircuitBreakers {
    policy: HookCircuitPolicy,
    circuits: Mutex<HashMap<HookCircuitKey, Circuit>>,
    max_tracked: usize,
}

impl HookCircuitBreakers {
    pub fn new(policy: HookCircuitPolicy) -> Self {
        Self::with_capacity_limit(policy, MAX_TRACKED_CIRCUITS)
    }

    fn with_capacity_limit(policy: HookCircuitPolicy, max_tracked: usize) -> Self {
        Self {
            policy: HookCircuitPolicy {
                failure_threshold: policy.failure_threshold.max(1),
                ..policy
            },
            circuits: Mutex::new(HashMap::new()),
            max_tracked: max_tracked.max(1),
        }
    }

    pub fn policy(&self) -> &HookCircuitPolicy {
        &self.policy
    }

    pub fn admit(&self, key: &HookCircuitKey) -> HookCircuitAdmission {
        self.admit_at(key, Instant::now())
    }

    pub(crate) fn admit_at(&self, key: &HookCircuitKey, now: Instant) -> HookCircuitAdmission {
        let admitted = HookCircuitAdmission::Admitted { probe: false };
        if !self.policy.enabled {
            return admitted;
        }
        let mut circuits = self.lock();
        let Some(circuit) = circuits.get_mut(key) else {
            return admitted;
        };
        match circuit.open_until {
            None => admitted,
            Some(until) if now < until => HookCircuitAdmission::Rejected {
                retry_after: until - now,
            },
            Some(_) if circuit.probing => HookCircuitAdmission::Rejected {
                retry_after: PROBE_RETRY_AFTER,
            },
            Some(_) => {
                circuit.probing = true;
                HookCircuitAdmission::Admitted { probe: true }
            }
        }
    }

    /// 记一次已发出调用的结果；状态变了时记日志并返回变化。
    pub fn settle(
        &self,
        key: &HookCircuitKey,
        probe: bool,
        result: HookCircuitResult,
    ) -> Option<HookCircuitTransition> {
        self.settle_at(key, probe, result, Instant::now())
    }

    pub(crate) fn settle_at(
        &self,
        key: &HookCircuitKey,
        probe: bool,
        result: HookCircuitResult,
        now: Instant,
    ) -> Option<HookCircuitTransition> {
        if !self.policy.enabled {
            return None;
        }
        let mut circuits = self.lock();
        match result {
            HookCircuitResult::Neutral => {
                if probe && let Some(circuit) = circuits.get_mut(key) {
                    circuit.probing = false;
                }
                None
            }
            HookCircuitResult::Healthy => {
                let circuit = circuits.get(key)?;
                if probe {
                    let failed_probes = circuit.failed_probes;
                    circuits.remove(key);
                    tracing::info!(
                        tenant_id = %LoggableName(&key.tenant_id),
                        hook_type = %LoggableName(&key.hook_type),
                        hook = %LoggableName(&key.hook_name),
                        failed_probes,
                        "hook circuit closed after a successful probe"
                    );
                    return Some(HookCircuitTransition::Closed);
                }
                // 打开前就已发出的调用不关也不延长熔断，只由探测决定。
                if circuit.open_until.is_none() {
                    circuits.remove(key);
                }
                None
            }
            HookCircuitResult::Unhealthy if probe => {
                let circuit = circuits.get_mut(key)?;
                circuit.probing = false;
                circuit.open_until = Some(now + self.policy.open_for);
                circuit.failed_probes = circuit.failed_probes.saturating_add(1);
                // 长时间不可达时每个窗口探一次：只记第 1、2、4、8… 次失败。
                if circuit.failed_probes.is_power_of_two() {
                    tracing::warn!(
                        tenant_id = %LoggableName(&key.tenant_id),
                        hook_type = %LoggableName(&key.hook_type),
                        hook = %LoggableName(&key.hook_name),
                        failed_probes = circuit.failed_probes,
                        open_ms = self.policy.open_for.as_millis() as u64,
                        "hook circuit probe failed; circuit reopened"
                    );
                }
                Some(HookCircuitTransition::Reopened)
            }
            HookCircuitResult::Unhealthy => {
                if !circuits.contains_key(key) && !self.make_room(&mut circuits, now) {
                    return None;
                }
                let circuit = circuits.entry(key.clone()).or_insert(Circuit {
                    consecutive_failures: 0,
                    streak_started: now,
                    open_until: None,
                    probing: false,
                    failed_probes: 0,
                });
                if circuit.open_until.is_some() {
                    return None;
                }
                if now.saturating_duration_since(circuit.streak_started)
                    > self.policy.failure_window
                {
                    circuit.consecutive_failures = 0;
                    circuit.streak_started = now;
                }
                circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
                if circuit.consecutive_failures < self.policy.failure_threshold {
                    return None;
                }
                circuit.consecutive_failures = 0;
                circuit.failed_probes = 0;
                circuit.open_until = Some(now + self.policy.open_for);
                tracing::warn!(
                    tenant_id = %LoggableName(&key.tenant_id),
                    hook_type = %LoggableName(&key.hook_type),
                    hook = %LoggableName(&key.hook_name),
                    failures = self.policy.failure_threshold,
                    window_ms = self.policy.failure_window.as_millis() as u64,
                    open_ms = self.policy.open_for.as_millis() as u64,
                    "hook circuit opened"
                );
                Some(HookCircuitTransition::Opened)
            }
        }
    }

    /// 表满时清掉连续失败已过窗口、没有打开的条目；仍满返回 `false`（这次失败不追踪）。
    fn make_room(&self, circuits: &mut HashMap<HookCircuitKey, Circuit>, now: Instant) -> bool {
        if circuits.len() < self.max_tracked {
            return true;
        }
        let window = self.policy.failure_window;
        circuits.retain(|_, circuit| {
            circuit.open_until.is_some()
                || now.saturating_duration_since(circuit.streak_started) <= window
        });
        if circuits.len() < self.max_tracked {
            return true;
        }
        tracing::warn!(
            tracked = circuits.len(),
            "hook circuit table is full; a failing hook is not tracked"
        );
        false
    }

    /// 正在追踪的熔断器数（测试与排障）。
    pub fn tracked(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<HookCircuitKey, Circuit>> {
        // 状态是几个在锁内整体更新的字段，别处的 panic 不会留下写了一半的状态。
        self.circuits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(30);
    const OPEN: Duration = Duration::from_secs(20);

    fn policy(threshold: u32) -> HookCircuitPolicy {
        HookCircuitPolicy {
            enabled: true,
            failure_threshold: threshold,
            failure_window: WINDOW,
            open_for: OPEN,
        }
    }

    fn key(tenant: &str, name: &str) -> HookCircuitKey {
        HookCircuitKey {
            tenant_id: tenant.into(),
            hook_type: "pre_send".into(),
            hook_name: name.into(),
        }
    }

    fn admitted(admission: HookCircuitAdmission) -> Option<bool> {
        match admission {
            HookCircuitAdmission::Admitted { probe } => Some(probe),
            HookCircuitAdmission::Rejected { .. } => None,
        }
    }

    fn fail(
        breakers: &HookCircuitBreakers,
        key: &HookCircuitKey,
        now: Instant,
    ) -> Option<HookCircuitTransition> {
        let probe = admitted(breakers.admit_at(key, now)).expect("admitted");
        breakers.settle_at(key, probe, HookCircuitResult::Unhealthy, now)
    }

    /// 连续失败达到阈值才打开；打开期间短路并给出剩余时间；到期半开只放一次探测，探测成功关闭。
    #[test]
    fn opens_after_consecutive_failures_then_probes_and_closes() {
        let breakers = HookCircuitBreakers::new(policy(3));
        let hook = key("t1", "dlp");
        let now = Instant::now();
        assert_eq!(fail(&breakers, &hook, now), None);
        assert_eq!(fail(&breakers, &hook, now), None);
        assert_eq!(
            fail(&breakers, &hook, now),
            Some(HookCircuitTransition::Opened)
        );

        let later = now + Duration::from_secs(5);
        assert_eq!(
            breakers.admit_at(&hook, later),
            HookCircuitAdmission::Rejected {
                retry_after: OPEN - Duration::from_secs(5)
            }
        );

        let expired = now + OPEN;
        assert_eq!(admitted(breakers.admit_at(&hook, expired)), Some(true));
        assert_eq!(
            breakers.admit_at(&hook, expired),
            HookCircuitAdmission::Rejected {
                retry_after: PROBE_RETRY_AFTER
            },
            "only one probe while it is outstanding"
        );
        assert_eq!(
            breakers.settle_at(&hook, true, HookCircuitResult::Healthy, expired),
            Some(HookCircuitTransition::Closed)
        );
        assert_eq!(admitted(breakers.admit_at(&hook, expired)), Some(false));
        assert_eq!(breakers.tracked(), 0, "a closed circuit keeps no state");
    }

    /// 一次成功清零连续失败；距这一串第一次失败超过窗口的失败重新计数。
    #[test]
    fn a_success_or_an_expired_window_resets_the_streak() {
        let breakers = HookCircuitBreakers::new(policy(3));
        let hook = key("t1", "dlp");
        let now = Instant::now();
        fail(&breakers, &hook, now);
        fail(&breakers, &hook, now);
        assert_eq!(
            breakers.settle_at(&hook, false, HookCircuitResult::Healthy, now),
            None
        );
        assert_eq!(breakers.tracked(), 0);
        fail(&breakers, &hook, now);
        fail(&breakers, &hook, now);
        let outside = now + WINDOW + Duration::from_millis(1);
        assert_eq!(fail(&breakers, &hook, outside), None, "streak restarted");
        assert_eq!(fail(&breakers, &hook, outside), None);
        assert_eq!(
            fail(&breakers, &hook, outside),
            Some(HookCircuitTransition::Opened)
        );
    }

    /// 探测失败再打开一个窗口；放弃的探测（未结算就丢弃）不卡住半开。
    #[test]
    fn a_failed_probe_reopens_and_an_abandoned_probe_is_released() {
        let breakers = HookCircuitBreakers::new(policy(1));
        let hook = key("t1", "dlp");
        let now = Instant::now();
        assert_eq!(
            fail(&breakers, &hook, now),
            Some(HookCircuitTransition::Opened)
        );
        let expired = now + OPEN;
        assert_eq!(
            fail(&breakers, &hook, expired),
            Some(HookCircuitTransition::Reopened)
        );
        assert!(admitted(breakers.admit_at(&hook, expired)).is_none());

        let again = expired + OPEN;
        assert_eq!(admitted(breakers.admit_at(&hook, again)), Some(true));
        breakers.settle_at(&hook, true, HookCircuitResult::Neutral, again);
        assert_eq!(
            admitted(breakers.admit_at(&hook, again)),
            Some(true),
            "the abandoned probe is released"
        );
    }

    /// 打开前已发出的调用（迟到的结果）不关闭也不延长熔断。
    #[test]
    fn stragglers_admitted_before_opening_do_not_change_an_open_circuit() {
        let breakers = HookCircuitBreakers::new(policy(1));
        let hook = key("t1", "dlp");
        let now = Instant::now();
        let straggler = admitted(breakers.admit_at(&hook, now)).unwrap();
        assert_eq!(
            fail(&breakers, &hook, now),
            Some(HookCircuitTransition::Opened)
        );
        let later = now + Duration::from_secs(5);
        assert_eq!(
            breakers.settle_at(&hook, straggler, HookCircuitResult::Healthy, later),
            None
        );
        assert_eq!(
            breakers.settle_at(&hook, straggler, HookCircuitResult::Unhealthy, later),
            None
        );
        assert_eq!(
            breakers.admit_at(&hook, later),
            HookCircuitAdmission::Rejected {
                retry_after: OPEN - Duration::from_secs(5)
            }
        );
    }

    /// 熔断按 (租户, Hook) 隔离：一个租户的 Hook 打开不影响别的租户或别的 Hook。
    #[test]
    fn circuits_are_per_tenant_and_per_hook() {
        let breakers = HookCircuitBreakers::new(policy(1));
        let now = Instant::now();
        fail(&breakers, &key("t1", "dlp"), now);
        assert!(admitted(breakers.admit_at(&key("t1", "dlp"), now)).is_none());
        assert_eq!(
            admitted(breakers.admit_at(&key("t2", "dlp"), now)),
            Some(false)
        );
        assert_eq!(
            admitted(breakers.admit_at(&key("t1", "audit"), now)),
            Some(false)
        );
        let other_type = HookCircuitKey {
            hook_type: "post_send".into(),
            ..key("t1", "dlp")
        };
        assert_eq!(admitted(breakers.admit_at(&other_type, now)), Some(false));
    }

    /// 关掉熔断：永远发出，不留状态。
    #[test]
    fn a_disabled_policy_never_rejects() {
        let breakers = HookCircuitBreakers::new(HookCircuitPolicy {
            enabled: false,
            ..policy(1)
        });
        let hook = key("t1", "dlp");
        let now = Instant::now();
        for _ in 0..10 {
            assert_eq!(fail(&breakers, &hook, now), None);
        }
        assert_eq!(breakers.tracked(), 0);
    }

    /// 表满时先清过期条目；仍满时新的失败 Hook 不追踪（照常调用），已打开的不被清掉。
    #[test]
    fn the_table_is_bounded_and_never_drops_open_circuits() {
        let breakers = HookCircuitBreakers::with_capacity_limit(policy(2), 2);
        let now = Instant::now();
        fail(&breakers, &key("t1", "a"), now);
        fail(&breakers, &key("t1", "a"), now);
        fail(&breakers, &key("t1", "b"), now);
        assert_eq!(breakers.tracked(), 2);
        fail(&breakers, &key("t1", "c"), now);
        assert_eq!(breakers.tracked(), 2, "full: c is not tracked");
        assert_eq!(
            admitted(breakers.admit_at(&key("t1", "c"), now)),
            Some(false)
        );

        let later = now + WINDOW + Duration::from_secs(1);
        fail(&breakers, &key("t1", "c"), later);
        assert_eq!(breakers.tracked(), 2, "b expired and made room for c");
        assert!(
            admitted(breakers.admit_at(&key("t1", "a"), now + Duration::from_secs(1))).is_none(),
            "a is open and kept"
        );
    }

    /// 熔断日志里的租户与 Hook 名转义，伪造不了日志行。
    #[test]
    fn transition_logs_escape_names() {
        let breakers = HookCircuitBreakers::new(policy(1));
        let hook = HookCircuitKey {
            tenant_id: "t\nWARN forged".into(),
            hook_type: "pre_send".into(),
            hook_name: "dlp\nERROR forged".into(),
        };
        let capture = crate::test_logs::LogCapture::start(tracing::Level::INFO);
        let now = Instant::now();
        fail(&breakers, &hook, now);
        let expired = now + OPEN;
        let probe = admitted(breakers.admit_at(&hook, expired)).unwrap();
        breakers.settle_at(&hook, probe, HookCircuitResult::Healthy, expired);
        let output = capture.finish();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "{output}");
        for line in lines {
            assert!(line.contains("tenant_id=t\\nWARN forged"), "{line}");
            assert!(line.contains("hook=dlp\\nERROR forged"), "{line}");
        }
    }
}
