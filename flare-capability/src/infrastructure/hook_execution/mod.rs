//! Hook 执行证据的写入路径：采样、有界缓冲、后台批量写库与保留期清理。
//!
//! 消息链路上只做 [`HookExecutionBuffer::record`]：一次加锁入队，从不等待。缓冲满时先丢采样类
//! （放行、熔断短路）给全量类（拒绝、失败、超时）腾位置，全是全量类时丢新来的；每次丢弃都计入
//! `capability_hook_execution_records_dropped_total`。写库失败的一批同样丢弃计数，不回队（回队会在
//! 库不可用时把缓冲变成无界重试）。停机时写入任务写完后关闭缓冲，之后交来的记录直接计为停机
//! 丢弃。后台任务见 [`tasks`]。

pub mod metrics;
pub mod tasks;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use rand::Rng;
use tokio::sync::Notify;

use crate::domain::model::{HookExecutionClass, HookExecutionRecord};
use crate::domain::service::{HookCircuitTransition, HookExecutionObserver};

pub use metrics::{DropReason, HookExecutionMetrics};
pub use tasks::{HookExecutionTasks, run_retention, run_writer};

/// 成功采样比例换算成 1/N 的 N：0 表示不记成功；比例四舍五入到最近的 1/N（例如 0.3 记作 1/3），
/// 每条留下的记录代表 N 次执行，统计因此无偏。
pub fn sample_every(ratio: f64) -> u32 {
    if !ratio.is_finite() || ratio <= 0.0 {
        return 0;
    }
    if ratio >= 1.0 {
        return 1;
    }
    let every = (1.0 / ratio).round();
    if every >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        (every as u32).max(1)
    }
}

/// 采样器：给定 N，这次是否留下（概率 1/N）。
pub type SampleChooser = Arc<dyn Fn(u32) -> bool + Send + Sync>;

fn random_one_in(every: u32) -> bool {
    every <= 1 || rand::thread_rng().gen_range(0..every) == 0
}

#[derive(Default)]
struct Pending {
    always: VecDeque<HookExecutionRecord>,
    sampled: VecDeque<HookExecutionRecord>,
    /// 写入任务已停：不再入队。
    closed: bool,
}

impl Pending {
    fn len(&self) -> usize {
        self.always.len() + self.sampled.len()
    }
}

/// 缓冲参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookExecutionBufferSettings {
    /// 缓冲上限（条）。
    pub capacity: usize,
    /// 一批写多少条；缓冲里攒够一批就唤醒写入任务。
    pub batch_size: usize,
    /// 成功（与熔断短路）每 N 次留 1 次；0 为不留。
    pub sample_every: u32,
}

/// 有界缓冲：[`HookExecutionObserver`] 的实现，写入任务从这里取批。
pub struct HookExecutionBuffer {
    settings: HookExecutionBufferSettings,
    pending: Mutex<Pending>,
    wake: Notify,
    chooser: SampleChooser,
    metrics: Arc<HookExecutionMetrics>,
}

impl HookExecutionBuffer {
    pub fn new(settings: HookExecutionBufferSettings, metrics: Arc<HookExecutionMetrics>) -> Self {
        Self::with_chooser(settings, metrics, Arc::new(random_one_in))
    }

    pub fn with_chooser(
        settings: HookExecutionBufferSettings,
        metrics: Arc<HookExecutionMetrics>,
        chooser: SampleChooser,
    ) -> Self {
        let capacity = settings.capacity.max(1);
        Self {
            settings: HookExecutionBufferSettings {
                capacity,
                batch_size: settings.batch_size.clamp(1, capacity),
                sample_every: settings.sample_every,
            },
            pending: Mutex::new(Pending::default()),
            wake: Notify::new(),
            chooser,
            metrics,
        }
    }

    pub fn settings(&self) -> HookExecutionBufferSettings {
        self.settings
    }

    pub fn metrics(&self) -> &Arc<HookExecutionMetrics> {
        &self.metrics
    }

    /// 缓冲里的记录数。
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 取至多一批：全量类在前。
    pub fn take_batch(&self) -> Vec<HookExecutionRecord> {
        let mut pending = self.lock();
        let mut batch = Vec::with_capacity(self.settings.batch_size.min(pending.len()));
        while batch.len() < self.settings.batch_size {
            match pending.always.pop_front() {
                Some(record) => batch.push(record),
                None => break,
            }
        }
        while batch.len() < self.settings.batch_size {
            match pending.sampled.pop_front() {
                Some(record) => batch.push(record),
                None => break,
            }
        }
        self.metrics.buffered.set(pending.len() as i64);
        batch
    }

    /// 写入任务停下时关闭缓冲：没写进去的全部记录清空并按类别计为丢弃，返回条数；之后交来的记录
    /// 不再入队，直接按同一原因计为丢弃（停机时仍在收尾的请求也会交记录）。
    pub fn close(&self, reason: DropReason) -> u64 {
        let mut pending = self.lock();
        pending.closed = true;
        let always = pending.always.len() as u64;
        let sampled = pending.sampled.len() as u64;
        pending.always.clear();
        pending.sampled.clear();
        self.metrics.buffered.set(0);
        drop(pending);
        self.metrics
            .record_dropped(reason, HookExecutionClass::Always, always);
        self.metrics
            .record_dropped(reason, HookExecutionClass::Sampled, sampled);
        always + sampled
    }

    /// 等到缓冲攒够一批（或别处唤醒）。
    pub async fn wait_for_batch(&self) {
        self.wake.notified().await;
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        // 两个队列各自整体增删，别处的 panic 不会留下写了一半的状态。
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl HookExecutionObserver for HookExecutionBuffer {
    fn sample(&self, class: HookExecutionClass) -> Option<u32> {
        match class {
            HookExecutionClass::Always => Some(1),
            HookExecutionClass::Sampled => {
                let every = self.settings.sample_every;
                (every > 0 && (self.chooser)(every)).then_some(every)
            }
        }
    }

    fn record(&self, record: HookExecutionRecord) {
        let class = record.outcome.class();
        let mut pending = self.lock();
        if pending.closed {
            drop(pending);
            self.metrics.record_dropped(DropReason::Shutdown, class, 1);
            return;
        }
        if pending.len() >= self.settings.capacity {
            let evicted = match class {
                HookExecutionClass::Always => pending.sampled.pop_front().is_some(),
                HookExecutionClass::Sampled => false,
            };
            if !evicted {
                drop(pending);
                self.metrics
                    .record_dropped(DropReason::BufferFull, class, 1);
                return;
            }
            self.metrics
                .record_dropped(DropReason::BufferFull, HookExecutionClass::Sampled, 1);
        }
        match class {
            HookExecutionClass::Always => pending.always.push_back(record),
            HookExecutionClass::Sampled => pending.sampled.push_back(record),
        }
        let len = pending.len();
        drop(pending);
        self.metrics.queued.inc();
        self.metrics.buffered.set(len as i64);
        if len >= self.settings.batch_size {
            self.wake.notify_one();
        }
    }

    fn circuit_transition(&self, tenant_id: &str, transition: HookCircuitTransition) {
        self.metrics.record_transition(tenant_id, transition);
    }

    fn circuit_rejected(&self, tenant_id: &str) {
        self.metrics.record_short_circuit(tenant_id);
    }
}

/// 没有库时只计熔断指标、不留记录。
pub struct MetricsOnlyHookExecutions {
    metrics: Arc<HookExecutionMetrics>,
}

impl MetricsOnlyHookExecutions {
    pub fn new(metrics: Arc<HookExecutionMetrics>) -> Self {
        Self { metrics }
    }
}

impl HookExecutionObserver for MetricsOnlyHookExecutions {
    fn sample(&self, _class: HookExecutionClass) -> Option<u32> {
        None
    }

    fn record(&self, _record: HookExecutionRecord) {}

    fn circuit_transition(&self, tenant_id: &str, transition: HookCircuitTransition) {
        self.metrics.record_transition(tenant_id, transition);
    }

    fn circuit_rejected(&self, tenant_id: &str) {
        self.metrics.record_short_circuit(tenant_id);
    }
}

#[cfg(test)]
pub(crate) mod test_records {
    use std::time::{Duration, SystemTime};

    use crate::domain::model::{HookExecutionOutcome, HookExecutionRecord};

    pub(crate) fn record(
        tenant: &str,
        name: &str,
        outcome: HookExecutionOutcome,
        executed_at: SystemTime,
    ) -> HookExecutionRecord {
        HookExecutionRecord {
            execution_id: uuid::Uuid::new_v4().to_string(),
            tenant_id: tenant.into(),
            hook_type: "pre_send".into(),
            hook_name: name.into(),
            hook_version: Some("v1".into()),
            message_id: Some(format!("m-{}", uuid::Uuid::new_v4().simple())),
            request_id: Some("req".into()),
            trace_id: None,
            outcome,
            latency: Duration::from_millis(12),
            executed_at,
            sample_weight: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::SystemTime;

    use super::test_records::record;
    use super::*;
    use crate::domain::model::HookExecutionOutcome;

    fn buffer(capacity: usize, batch_size: usize, sample_every: u32) -> HookExecutionBuffer {
        HookExecutionBuffer::new(
            HookExecutionBufferSettings {
                capacity,
                batch_size,
                sample_every,
            },
            Arc::new(HookExecutionMetrics::new()),
        )
    }

    fn allowed() -> HookExecutionRecord {
        record(
            "t1",
            "dlp",
            HookExecutionOutcome::Allowed,
            SystemTime::now(),
        )
    }

    fn failed() -> HookExecutionRecord {
        record(
            "t1",
            "dlp",
            HookExecutionOutcome::TimedOut,
            SystemTime::now(),
        )
    }

    #[test]
    fn sample_ratios_round_to_one_in_n() {
        assert_eq!(sample_every(0.0), 0);
        assert_eq!(sample_every(-1.0), 0);
        assert_eq!(sample_every(f64::NAN), 0);
        assert_eq!(sample_every(1.0), 1);
        assert_eq!(sample_every(2.0), 1);
        assert_eq!(sample_every(0.01), 100);
        assert_eq!(sample_every(0.3), 3);
        assert_eq!(sample_every(0.5), 2);
        assert_eq!(sample_every(1e-12), u32::MAX);
    }

    /// 全量类每次都记、权重 1；采样类按 1/N 留下、权重 N；N 为 0 时不记采样类。
    #[test]
    fn failures_are_always_kept_and_successes_are_sampled_with_their_weight() {
        let seen = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&seen);
        // 确定的采样器：每 N 次留第 N 次。
        let chooser: SampleChooser =
            Arc::new(move |every| counter.fetch_add(1, Ordering::Relaxed) % every == every - 1);
        let buffer = HookExecutionBuffer::with_chooser(
            HookExecutionBufferSettings {
                capacity: 100,
                batch_size: 10,
                sample_every: 4,
            },
            Arc::new(HookExecutionMetrics::new()),
            chooser,
        );
        assert_eq!(buffer.sample(HookExecutionClass::Always), Some(1));
        let kept: Vec<Option<u32>> = (0..8)
            .map(|_| buffer.sample(HookExecutionClass::Sampled))
            .collect();
        assert_eq!(
            kept,
            vec![None, None, None, Some(4), None, None, None, Some(4)]
        );

        let never = buffer_with_every(0);
        assert_eq!(never.sample(HookExecutionClass::Sampled), None);
        assert_eq!(never.sample(HookExecutionClass::Always), Some(1));
        let all = buffer_with_every(1);
        assert_eq!(all.sample(HookExecutionClass::Sampled), Some(1));
    }

    fn buffer_with_every(every: u32) -> HookExecutionBuffer {
        buffer(10, 5, every)
    }

    /// 随机采样器的留下比例接近 1/N。
    #[test]
    fn the_random_sampler_keeps_about_one_in_n() {
        let buffer = buffer(10, 5, 10);
        let kept = (0..20_000)
            .filter(|_| buffer.sample(HookExecutionClass::Sampled).is_some())
            .count();
        assert!((1_500..2_500).contains(&kept), "{kept}");
    }

    /// 批按全量类在前、至多 batch_size 条；攒够一批时唤醒写入任务。
    #[tokio::test]
    async fn batches_put_failures_first_and_wake_the_writer_when_full() {
        let buffer = Arc::new(buffer(100, 3, 1));
        buffer.record(allowed());
        buffer.record(failed());
        assert_eq!(buffer.len(), 2);
        let waiter = {
            let buffer = Arc::clone(&buffer);
            tokio::spawn(async move { buffer.wait_for_batch().await })
        };
        tokio::task::yield_now().await;
        buffer.record(allowed());
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("woken when a batch is ready")
            .unwrap();
        buffer.record(failed());
        let batch = buffer.take_batch();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].outcome, HookExecutionOutcome::TimedOut);
        assert_eq!(batch[1].outcome, HookExecutionOutcome::TimedOut);
        assert_eq!(batch[2].outcome, HookExecutionOutcome::Allowed);
        assert_eq!(buffer.take_batch().len(), 1);
        assert!(buffer.take_batch().is_empty());
        assert_eq!(buffer.metrics().queued.get(), 4);
        assert_eq!(buffer.metrics().buffered.get(), 0);
    }

    /// 缓冲满：采样类直接丢；全量类挤掉最旧的采样类；全是全量类时丢新来的。每次丢弃都计数。
    #[test]
    fn back_pressure_drops_sampled_records_first_and_counts_every_drop() {
        let buffer = buffer(3, 3, 1);
        buffer.record(allowed());
        buffer.record(allowed());
        buffer.record(failed());
        buffer.record(allowed());
        let metrics = buffer.metrics();
        assert_eq!(
            metrics.dropped_count(DropReason::BufferFull, HookExecutionClass::Sampled),
            1,
            "an incoming success is dropped when full"
        );
        buffer.record(failed());
        buffer.record(failed());
        assert_eq!(buffer.len(), 3);
        assert_eq!(
            metrics.dropped_count(DropReason::BufferFull, HookExecutionClass::Sampled),
            3,
            "failures evicted both buffered successes"
        );
        buffer.record(failed());
        assert_eq!(
            metrics.dropped_count(DropReason::BufferFull, HookExecutionClass::Always),
            1,
            "a buffer full of failures drops the incoming one"
        );
        assert!(
            buffer
                .take_batch()
                .iter()
                .all(|record| record.outcome == HookExecutionOutcome::TimedOut)
        );
        buffer.record(allowed());
        buffer.record(failed());
        assert_eq!(buffer.close(DropReason::Shutdown), 2);
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Sampled),
            1
        );
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Always),
            1
        );
        assert!(buffer.is_empty());

        // 关闭后交来的记录不入队，直接计为停机丢弃。
        let queued = metrics.queued.get();
        buffer.record(failed());
        buffer.record(allowed());
        assert!(buffer.is_empty());
        assert_eq!(metrics.queued.get(), queued);
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Always),
            2
        );
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Sampled),
            2
        );
    }

    /// 熔断事件进指标：打开加一、关闭减一；短路按租户计。
    #[test]
    fn circuit_events_feed_the_metrics() {
        let buffer = buffer(10, 5, 1);
        buffer.circuit_transition("t1", HookCircuitTransition::Opened);
        buffer.circuit_transition("t1", HookCircuitTransition::Reopened);
        buffer.circuit_rejected("t1");
        let metrics = buffer.metrics();
        assert_eq!(metrics.circuits_open.get(), 1);
        buffer.circuit_transition("t1", HookCircuitTransition::Closed);
        assert_eq!(metrics.circuits_open.get(), 0);
        assert_eq!(
            metrics
                .circuit_transitions
                .with_label_values(&["t1", "opened"])
                .get(),
            1
        );
        assert_eq!(metrics.short_circuits.with_label_values(&["t1"]).get(), 1);
    }
}
