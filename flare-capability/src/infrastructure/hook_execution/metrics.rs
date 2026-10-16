//! Hook 执行证据与熔断的指标。标签取值固定或经 `tenant_label`（关闭租户标签时聚合）；Hook 名不进
//! 标签（租户可登记任意名字，会放大时序基数），只进日志与 `hook_executions`。

use std::sync::Arc;

use flare_im_service_kit::metrics::{REGISTRY, tenant_label};
use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts};

use crate::domain::model::HookExecutionClass;
use crate::domain::service::HookCircuitTransition;

/// 记录被丢弃的原因（指标标签取值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// 缓冲满。
    BufferFull,
    /// 写库失败或超时。
    WriteFailed,
    /// 停机时没来得及写。
    Shutdown,
}

impl DropReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BufferFull => "buffer_full",
            Self::WriteFailed => "write_failed",
            Self::Shutdown => "shutdown",
        }
    }
}

pub struct HookExecutionMetrics {
    /// 入队的记录数（采样后）。
    pub queued: IntCounter,
    /// 写进库的行数。
    pub written: IntCounter,
    /// 丢弃的记录数，按原因与类别。
    pub dropped: IntCounterVec,
    /// 缓冲里待写的记录数。
    pub buffered: IntGauge,
    /// 批量写入次数，按结果。
    pub flushes: IntCounterVec,
    /// 保留期清理删掉的行数。
    pub expired: IntCounter,
    /// 熔断状态变化，按租户与变化。
    pub circuit_transitions: IntCounterVec,
    /// 因熔断打开被短路的调用，按租户。
    pub short_circuits: IntCounterVec,
    /// 当前打开（含半开）的熔断器数。
    pub circuits_open: IntGauge,
}

static REGISTERED: Lazy<Arc<HookExecutionMetrics>> = Lazy::new(|| {
    let metrics = HookExecutionMetrics::new();
    metrics.register();
    Arc::new(metrics)
});

impl HookExecutionMetrics {
    /// 未登记到任何注册表的一份（测试与组件内计数）。
    pub fn new() -> Self {
        Self {
            queued: IntCounter::new(
                "capability_hook_execution_records_queued_total",
                "Hook execution records queued for writing after sampling",
            )
            .expect("valid metric"),
            written: IntCounter::new(
                "capability_hook_execution_records_written_total",
                "Hook execution rows written to hook_executions",
            )
            .expect("valid metric"),
            dropped: IntCounterVec::new(
                Opts::new(
                    "capability_hook_execution_records_dropped_total",
                    "Hook execution records dropped before reaching hook_executions",
                ),
                &["reason", "class"],
            )
            .expect("valid metric"),
            buffered: IntGauge::new(
                "capability_hook_execution_records_buffered",
                "Hook execution records waiting in the write buffer",
            )
            .expect("valid metric"),
            flushes: IntCounterVec::new(
                Opts::new(
                    "capability_hook_execution_flushes_total",
                    "Batched hook execution writes by result",
                ),
                &["result"],
            )
            .expect("valid metric"),
            expired: IntCounter::new(
                "capability_hook_execution_records_expired_total",
                "Hook execution rows deleted by the retention sweep",
            )
            .expect("valid metric"),
            circuit_transitions: IntCounterVec::new(
                Opts::new(
                    "capability_hook_circuit_transitions_total",
                    "Per (tenant, hook) circuit breaker transitions",
                ),
                &["tenant_id", "transition"],
            )
            .expect("valid metric"),
            short_circuits: IntCounterVec::new(
                Opts::new(
                    "capability_hook_circuit_short_circuits_total",
                    "Hook calls not made because the circuit of the (tenant, hook) was open",
                ),
                &["tenant_id"],
            )
            .expect("valid metric"),
            circuits_open: IntGauge::new(
                "capability_hook_circuits_open",
                "Per (tenant, hook) circuit breakers currently open or half-open",
            )
            .expect("valid metric"),
        }
    }

    /// 进程里登记到共享注册表（`/metrics` 出口）的那一份。
    pub fn registered() -> Arc<Self> {
        Arc::clone(&REGISTERED)
    }

    fn register(&self) {
        let _ = REGISTRY.register(Box::new(self.queued.clone()));
        let _ = REGISTRY.register(Box::new(self.written.clone()));
        let _ = REGISTRY.register(Box::new(self.dropped.clone()));
        let _ = REGISTRY.register(Box::new(self.buffered.clone()));
        let _ = REGISTRY.register(Box::new(self.flushes.clone()));
        let _ = REGISTRY.register(Box::new(self.expired.clone()));
        let _ = REGISTRY.register(Box::new(self.circuit_transitions.clone()));
        let _ = REGISTRY.register(Box::new(self.short_circuits.clone()));
        let _ = REGISTRY.register(Box::new(self.circuits_open.clone()));
    }

    pub fn record_dropped(&self, reason: DropReason, class: HookExecutionClass, count: u64) {
        if count > 0 {
            self.dropped
                .with_label_values(&[reason.as_str(), class.as_str()])
                .inc_by(count);
        }
    }

    pub fn dropped_count(&self, reason: DropReason, class: HookExecutionClass) -> u64 {
        self.dropped
            .with_label_values(&[reason.as_str(), class.as_str()])
            .get()
    }

    pub fn record_transition(&self, tenant_id: &str, transition: HookCircuitTransition) {
        self.circuit_transitions
            .with_label_values(&[tenant_label(tenant_id), transition.as_str()])
            .inc();
        match transition {
            HookCircuitTransition::Opened => self.circuits_open.inc(),
            HookCircuitTransition::Closed => self.circuits_open.dec(),
            HookCircuitTransition::Reopened => {}
        }
    }

    pub fn record_short_circuit(&self, tenant_id: &str) {
        self.short_circuits
            .with_label_values(&[tenant_label(tenant_id)])
            .inc();
    }
}

impl Default for HookExecutionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 进程里那一份登记在共享注册表上：`/metrics`（共享运行时挂载的出口）输出里有它们。
    #[test]
    fn the_registered_metrics_are_served_by_the_shared_endpoint() {
        let metrics = HookExecutionMetrics::registered();
        metrics.record_short_circuit("t-served");
        let text = flare_im_service_kit::metrics::encode_prometheus_text().unwrap();
        for name in [
            "capability_hook_execution_records_queued_total",
            "capability_hook_execution_records_buffered",
            "capability_hook_circuits_open",
            "capability_hook_circuit_short_circuits_total",
        ] {
            assert!(text.contains(name), "{name} missing from /metrics");
        }
        assert!(Arc::ptr_eq(&metrics, &HookExecutionMetrics::registered()));
    }
}
