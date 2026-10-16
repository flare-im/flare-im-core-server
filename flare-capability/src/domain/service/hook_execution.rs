//! Hook 执行守卫：每次调用 Hook 前过 (租户, Hook) 熔断，调用后结算熔断并交出执行记录。
//!
//! 记录交给 [`HookExecutionObserver`]（基础设施实现：有界缓冲、批量写库、指标）。观察者的方法都
//! 不阻塞、不等待：消息链路上只多一次加锁入队，写库在后台。

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use flare_server_core::context::Context;

use crate::domain::model::{
    HookExecutionClass, HookExecutionOutcome, HookExecutionRecord, MAX_RECORDED_FIELD_CHARS,
    bounded,
};
use crate::domain::service::hook_circuit::{
    HookCircuitAdmission, HookCircuitBreakers, HookCircuitKey, HookCircuitPolicy,
    HookCircuitResult, HookCircuitTransition,
};

/// 执行记录与熔断事件的去处。实现不得阻塞调用方。
pub trait HookExecutionObserver: Send + Sync {
    /// 这一类结果这次要不要记：要记时返回这一行代表的执行次数（全量记录的为 1）。不记时调用方
    /// 不构造记录。
    fn sample(&self, class: HookExecutionClass) -> Option<u32>;

    /// 交出一条记录（入队即返回）。
    fn record(&self, record: HookExecutionRecord);

    /// 熔断状态变化（日志已由熔断器记过）。
    fn circuit_transition(&self, _tenant_id: &str, _transition: HookCircuitTransition) {}

    /// 一次调用因熔断打开被短路。
    fn circuit_rejected(&self, _tenant_id: &str) {}
}

/// 熔断器与执行记录的组合，挂在每个执行计划上。
pub struct HookExecutionGuard {
    circuits: HookCircuitBreakers,
    observer: Arc<dyn HookExecutionObserver>,
}

/// 一次调用要记的 Hook 身份。
#[derive(Debug, Clone, Copy)]
pub struct HookIdentity<'a> {
    pub hook_type: &'a str,
    pub hook_name: &'a str,
    pub hook_version: Option<&'a str>,
}

impl HookExecutionGuard {
    pub fn new(policy: HookCircuitPolicy, observer: Arc<dyn HookExecutionObserver>) -> Self {
        Self {
            circuits: HookCircuitBreakers::new(policy),
            observer,
        }
    }

    pub fn circuits(&self) -> &HookCircuitBreakers {
        &self.circuits
    }

    /// 开始一次调用：没有租户的上下文不熔断、不记录（返回 `None`）。
    pub fn begin<'a>(&'a self, ctx: &Context, hook: HookIdentity<'_>) -> Option<HookCall<'a>> {
        let tenant_id = ctx.tenant_id().filter(|tenant| !tenant.is_empty())?;
        let key = HookCircuitKey {
            tenant_id: tenant_id.to_string(),
            hook_type: hook.hook_type.to_string(),
            hook_name: hook.hook_name.to_string(),
        };
        let admission = self.circuits.admit(&key);
        if matches!(admission, HookCircuitAdmission::Rejected { .. }) {
            self.observer.circuit_rejected(&key.tenant_id);
        }
        Some(HookCall {
            guard: self,
            key,
            hook_version: hook.hook_version.map(str::to_string),
            request_id: non_empty(ctx.request_id()),
            trace_id: non_empty(ctx.trace_id()),
            admission,
            started: Instant::now(),
            executed_at: SystemTime::now(),
            settled: false,
        })
    }
}

fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| bounded(value))
}

/// 一次进行中的调用。丢弃未结算的调用（调用方放弃了）按不影响熔断处理，探测名额随之释放。
pub struct HookCall<'a> {
    guard: &'a HookExecutionGuard,
    key: HookCircuitKey,
    hook_version: Option<String>,
    request_id: Option<String>,
    trace_id: Option<String>,
    admission: HookCircuitAdmission,
    started: Instant,
    executed_at: SystemTime,
    settled: bool,
}

impl HookCall<'_> {
    /// 熔断打开（或探测未结束），这次不调用 Hook。
    pub fn short_circuited(&self) -> bool {
        matches!(self.admission, HookCircuitAdmission::Rejected { .. })
    }

    /// 结束这次调用：结算熔断（短路的调用不结算），按类别采样后交出记录。
    pub fn finish(mut self, outcome: HookExecutionOutcome, message_id: Option<&str>) {
        self.settled = true;
        let latency = match self.admission {
            HookCircuitAdmission::Admitted { probe } => {
                let result = if outcome.answered() {
                    HookCircuitResult::Healthy
                } else {
                    HookCircuitResult::Unhealthy
                };
                if let Some(transition) = self.guard.circuits.settle(&self.key, probe, result) {
                    self.guard
                        .observer
                        .circuit_transition(&self.key.tenant_id, transition);
                }
                self.started.elapsed()
            }
            HookCircuitAdmission::Rejected { .. } => Duration::ZERO,
        };
        let Some(sample_weight) = self.guard.observer.sample(outcome.class()) else {
            return;
        };
        let key = std::mem::replace(
            &mut self.key,
            HookCircuitKey {
                tenant_id: String::new(),
                hook_type: String::new(),
                hook_name: String::new(),
            },
        );
        self.guard.observer.record(HookExecutionRecord {
            execution_id: uuid::Uuid::new_v4().to_string(),
            tenant_id: key.tenant_id,
            hook_type: key.hook_type,
            hook_name: key.hook_name,
            hook_version: self.hook_version.take().map(|v| bounded(&v)),
            message_id: message_id
                .filter(|id| !id.is_empty())
                .map(|id| id.chars().take(MAX_RECORDED_FIELD_CHARS).collect()),
            request_id: self.request_id.take(),
            trace_id: self.trace_id.take(),
            outcome,
            latency,
            executed_at: self.executed_at,
            sample_weight,
        });
    }
}

impl Drop for HookCall<'_> {
    fn drop(&mut self) {
        if !self.settled
            && let HookCircuitAdmission::Admitted { probe: true } = self.admission
        {
            self.guard
                .circuits
                .settle(&self.key, true, HookCircuitResult::Neutral);
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    use super::*;
    use crate::domain::model::HookExecutionClass;

    /// 记下所有记录与熔断事件；`sample_every` 为采样类的 N（0 表示不记采样类）。
    #[derive(Default)]
    pub(crate) struct RecordingObserver {
        pub sample_every: u32,
        pub records: Mutex<Vec<HookExecutionRecord>>,
        pub transitions: Mutex<Vec<(String, HookCircuitTransition)>>,
        pub rejected: Mutex<Vec<String>>,
    }

    impl RecordingObserver {
        pub(crate) fn recording_everything() -> Self {
            Self {
                sample_every: 1,
                ..Default::default()
            }
        }

        pub(crate) fn records(&self) -> Vec<HookExecutionRecord> {
            self.records.lock().unwrap().clone()
        }
    }

    impl HookExecutionObserver for RecordingObserver {
        fn sample(&self, class: HookExecutionClass) -> Option<u32> {
            match class {
                HookExecutionClass::Always => Some(1),
                HookExecutionClass::Sampled if self.sample_every == 0 => None,
                HookExecutionClass::Sampled => Some(self.sample_every),
            }
        }

        fn record(&self, record: HookExecutionRecord) {
            self.records.lock().unwrap().push(record);
        }

        fn circuit_transition(&self, tenant_id: &str, transition: HookCircuitTransition) {
            self.transitions
                .lock()
                .unwrap()
                .push((tenant_id.to_string(), transition));
        }

        fn circuit_rejected(&self, tenant_id: &str) {
            self.rejected.lock().unwrap().push(tenant_id.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::RecordingObserver;
    use super::*;

    fn guard(threshold: u32, observer: Arc<RecordingObserver>) -> HookExecutionGuard {
        HookExecutionGuard::new(
            HookCircuitPolicy {
                enabled: true,
                failure_threshold: threshold,
                failure_window: Duration::from_secs(30),
                open_for: Duration::from_secs(60),
            },
            observer,
        )
    }

    const HOOK: HookIdentity<'static> = HookIdentity {
        hook_type: "pre_send",
        hook_name: "dlp",
        hook_version: Some("v3"),
    };

    fn ctx(tenant: &str) -> Context {
        Context::with_request_id("req-1")
            .with_trace_id("trace-1")
            .with_tenant_id(tenant)
    }

    /// 一次调用的记录带租户、Hook、版本、消息号、请求号与追踪号，不带下游原文。
    #[test]
    fn a_finished_call_hands_over_one_record() {
        let observer = Arc::new(RecordingObserver::recording_everything());
        let guard = guard(5, Arc::clone(&observer));
        let call = guard.begin(&ctx("t1"), HOOK).unwrap();
        assert!(!call.short_circuited());
        call.finish(
            HookExecutionOutcome::Failed {
                code: "SERVICE_UNAVAILABLE",
            },
            Some("m1"),
        );
        let records = observer.records();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.tenant_id, "t1");
        assert_eq!(record.hook_id(), "pre_send:dlp");
        assert_eq!(record.hook_version.as_deref(), Some("v3"));
        assert_eq!(record.message_id.as_deref(), Some("m1"));
        assert_eq!(record.request_id.as_deref(), Some("req-1"));
        assert_eq!(record.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(record.sample_weight, 1);
        assert!(!record.outcome.answered());
    }

    /// 没有租户的上下文不熔断也不记录。
    #[test]
    fn calls_without_a_tenant_are_not_guarded() {
        let observer = Arc::new(RecordingObserver::recording_everything());
        let guard = guard(1, Arc::clone(&observer));
        assert!(guard.begin(&Context::with_request_id("r"), HOOK).is_none());
        assert!(
            guard
                .begin(&Context::with_request_id("r").with_tenant_id(""), HOOK)
                .is_none()
        );
    }

    /// 连续失败打开熔断后调用被短路，短路按采样类记录（耗时 0），观察者收到打开与短路事件。
    #[test]
    fn failures_open_the_circuit_and_short_circuits_are_recorded_as_sampled() {
        let observer = Arc::new(RecordingObserver {
            sample_every: 10,
            ..Default::default()
        });
        let guard = guard(2, Arc::clone(&observer));
        for _ in 0..2 {
            guard
                .begin(&ctx("t1"), HOOK)
                .unwrap()
                .finish(HookExecutionOutcome::TimedOut, None);
        }
        let call = guard.begin(&ctx("t1"), HOOK).unwrap();
        assert!(call.short_circuited());
        call.finish(HookExecutionOutcome::CircuitOpen, Some("m3"));
        assert_eq!(
            *observer.transitions.lock().unwrap(),
            vec![("t1".to_string(), HookCircuitTransition::Opened)]
        );
        assert_eq!(*observer.rejected.lock().unwrap(), vec!["t1".to_string()]);
        let records = observer.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].outcome, HookExecutionOutcome::CircuitOpen);
        assert_eq!(records[2].latency, Duration::ZERO);
        assert_eq!(records[2].sample_weight, 10);
        assert!(
            guard
                .begin(&ctx("t2"), HOOK)
                .is_some_and(|c| !c.short_circuited()),
            "another tenant is not affected"
        );
    }

    /// 放行按采样类：观察者不要时不构造记录；拒绝全量。
    #[test]
    fn sampled_out_successes_build_no_record() {
        let observer = Arc::new(RecordingObserver::default());
        let guard = guard(5, Arc::clone(&observer));
        guard
            .begin(&ctx("t1"), HOOK)
            .unwrap()
            .finish(HookExecutionOutcome::Allowed, None);
        assert!(observer.records().is_empty());
        guard.begin(&ctx("t1"), HOOK).unwrap().finish(
            HookExecutionOutcome::Rejected {
                reason: "SPAM".into(),
            },
            None,
        );
        assert_eq!(observer.records().len(), 1);
    }

    /// 半开探测被丢弃（调用方放弃）时释放探测名额，下一次调用成为新的探测。
    #[test]
    fn a_dropped_probe_releases_the_half_open_slot() {
        let observer = Arc::new(RecordingObserver::default());
        let guard = HookExecutionGuard::new(
            HookCircuitPolicy {
                enabled: true,
                failure_threshold: 1,
                failure_window: Duration::from_secs(30),
                open_for: Duration::ZERO,
            },
            observer,
        );
        guard
            .begin(&ctx("t1"), HOOK)
            .unwrap()
            .finish(HookExecutionOutcome::TimedOut, None);
        let probe = guard.begin(&ctx("t1"), HOOK).unwrap();
        assert!(!probe.short_circuited());
        assert!(guard.begin(&ctx("t1"), HOOK).unwrap().short_circuited());
        drop(probe);
        let next = guard.begin(&ctx("t1"), HOOK).unwrap();
        assert!(!next.short_circuited());
        next.finish(HookExecutionOutcome::Allowed, None);
        assert_eq!(guard.circuits().tracked(), 0, "closed again");
    }
}
