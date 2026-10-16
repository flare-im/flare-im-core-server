//! 后台任务：把缓冲按批写进 `hook_executions`，按保留期分批删除过期行。两者都随进程停机结束；
//! 停机时写入任务在有限时间内把缓冲写完，写不完的计为丢弃。每条入队的记录要么写进库，要么计入
//! 丢弃指标：超时的一批整批计数，停机后才到的记录直接计数（见 [`HookExecutionBuffer::close`]）。

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::oneshot;

use super::{DropReason, HookExecutionBuffer, HookExecutionMetrics};
use crate::domain::model::HookExecutionRecord;
use crate::domain::repository::HookExecutionRepository;

/// 一批写库的时限：超过即放弃这一批（计为丢弃），不让慢库拖住缓冲。
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// 停机时写完缓冲的总时限（每批的时限截到这里为止）。
pub const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
/// 写库连续失败时 WARN 的最小间隔，期间的失败次数在下一条里补记。
const WRITE_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// 一轮清理至多删几批：一轮删不完的留到下一轮。
const MAX_BATCHES_PER_SWEEP: u32 = 1_000;
/// 两批删除之间的间隔，给其他写入让出行锁与 IO。
const SWEEP_PAUSE: Duration = Duration::from_millis(50);

/// 写入与清理任务的参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookExecutionTasks {
    pub flush_interval: Duration,
    pub retention: Duration,
    pub retention_sweep_interval: Duration,
    pub retention_delete_batch: u32,
}

/// 写库失败的日志节流（写入任务独占，不加锁）。
#[derive(Default)]
struct FailureLog {
    last: Option<Instant>,
    suppressed: u64,
}

impl FailureLog {
    fn failed(&mut self, rows: usize, reason: &str) {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < WRITE_FAILURE_LOG_INTERVAL)
        {
            self.suppressed += 1;
            return;
        }
        tracing::warn!(
            rows,
            reason,
            suppressed_failures = self.suppressed,
            "hook execution records dropped: batch write failed"
        );
        self.last = Some(now);
        self.suppressed = 0;
    }
}

/// 写一批，至多等 `time_limit`。写不进去的整批计为丢弃：出错按 `DropReason::WriteFailed`，超时按
/// `timeout_reason`（平时为 `WriteFailed`，被停机总时限截断时为 `Shutdown`）。
async fn write_batch(
    repository: &dyn HookExecutionRepository,
    metrics: &HookExecutionMetrics,
    batch: Vec<HookExecutionRecord>,
    failures: &mut FailureLog,
    time_limit: Duration,
    timeout_reason: DropReason,
) {
    let (reason, dropped_as) =
        match tokio::time::timeout(time_limit, repository.insert_batch(&batch)).await {
            Ok(Ok(written)) => {
                metrics.written.inc_by(written);
                metrics.flushes.with_label_values(&["ok"]).inc();
                return;
            }
            Ok(Err(_)) => ("storage error", DropReason::WriteFailed),
            Err(_) => ("timed out", timeout_reason),
        };
    metrics.flushes.with_label_values(&["error"]).inc();
    for record in &batch {
        metrics.record_dropped(dropped_as, record.outcome.class(), 1);
    }
    failures.failed(batch.len(), reason);
}

/// 把缓冲里现有的记录按批写完（最后一批不满即停，余下的等下一次唤醒）。
async fn flush(
    buffer: &HookExecutionBuffer,
    repository: &dyn HookExecutionRepository,
    failures: &mut FailureLog,
) {
    let batch_size = buffer.settings().batch_size;
    loop {
        let batch = buffer.take_batch();
        if batch.is_empty() {
            return;
        }
        let full = batch.len() >= batch_size;
        write_batch(
            repository,
            buffer.metrics(),
            batch,
            failures,
            WRITE_TIMEOUT,
            DropReason::WriteFailed,
        )
        .await;
        if !full {
            return;
        }
    }
}

/// 写入任务：攒够一批或每过 `flush_interval` 写一次；收到停机信号后在
/// [`SHUTDOWN_FLUSH_TIMEOUT`] 内按批写完缓冲（被总时限截断的那一批与剩下的计为停机丢弃），然后关闭
/// 缓冲：停机期间仍在收尾的请求再交来的记录直接计为停机丢弃。
pub async fn run_writer(
    buffer: Arc<HookExecutionBuffer>,
    repository: Arc<dyn HookExecutionRepository>,
    flush_interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(flush_interval.max(Duration::from_millis(10)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut failures = FailureLog::default();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = buffer.wait_for_batch() => {}
            _ = ticker.tick() => {}
        }
        flush(&buffer, repository.as_ref(), &mut failures).await;
    }
    // 每批单独限时、截到总时限为止：整体套一个超时会把正在写的一批连同它的计数一起取消（这批已
    // 离开缓冲，既不算写入也不算丢弃）。
    let deadline = tokio::time::Instant::now() + SHUTDOWN_FLUSH_TIMEOUT;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let batch = buffer.take_batch();
        if batch.is_empty() {
            break;
        }
        let (time_limit, timeout_reason) = if left < WRITE_TIMEOUT {
            (left, DropReason::Shutdown)
        } else {
            (WRITE_TIMEOUT, DropReason::WriteFailed)
        };
        write_batch(
            repository.as_ref(),
            buffer.metrics(),
            batch,
            &mut failures,
            time_limit,
            timeout_reason,
        )
        .await;
    }
    let remaining = buffer.close(DropReason::Shutdown);
    if remaining > 0 {
        tracing::warn!(
            remaining,
            "hook execution buffer not fully written before shutdown"
        );
    }
}

/// 一轮清理的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sweep {
    /// 删掉的行数。
    pub deleted: u64,
    /// 这一轮里收到了停机信号：调用方随即结束，不能再等这个信号（oneshot 完成后再 poll 会 panic）。
    pub shutdown: bool,
}

/// 一轮清理：删掉执行时间早于 `now - retention` 的行，每批至多 `delete_batch` 行；两批之间收到
/// 停机信号即停（结果里 `shutdown` 为真）。
pub async fn sweep_expired(
    repository: &dyn HookExecutionRepository,
    metrics: &HookExecutionMetrics,
    retention: Duration,
    delete_batch: u32,
    shutdown: &mut oneshot::Receiver<()>,
) -> Sweep {
    let mut sweep = Sweep {
        deleted: 0,
        shutdown: false,
    };
    let Some(cutoff) = SystemTime::now().checked_sub(retention) else {
        return sweep;
    };
    let delete_batch = delete_batch.max(1);
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES_PER_SWEEP {
        match repository
            .delete_executed_before(cutoff, delete_batch)
            .await
        {
            Ok(rows) => {
                deleted += rows;
                metrics.expired.inc_by(rows);
                if rows < u64::from(delete_batch) {
                    break;
                }
            }
            Err(error) => {
                tracing::warn!(%error, deleted, "hook execution retention sweep failed; retrying next round");
                break;
            }
        }
        tokio::select! {
            _ = &mut *shutdown => {
                sweep.shutdown = true;
                break;
            }
            _ = tokio::time::sleep(SWEEP_PAUSE) => {}
        }
    }
    if deleted > 0 {
        tracing::info!(
            deleted,
            retention_secs = retention.as_secs(),
            "expired hook execution records deleted"
        );
    }
    sweep.deleted = deleted;
    sweep
}

/// 清理任务：每过 `sweep_interval` 清一轮（启动后先等一个间隔）；停机信号不论落在等待中还是一轮
/// 清理中，都在这一轮结束后返回。
pub async fn run_retention(
    repository: Arc<dyn HookExecutionRepository>,
    metrics: Arc<HookExecutionMetrics>,
    settings: HookExecutionTasks,
    mut shutdown: oneshot::Receiver<()>,
) {
    let interval = settings
        .retention_sweep_interval
        .max(Duration::from_secs(1));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = ticker.tick() => {}
        }
        let sweep = sweep_expired(
            repository.as_ref(),
            &metrics,
            settings.retention,
            settings.retention_delete_batch,
            &mut shutdown,
        )
        .await;
        if sweep.shutdown {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use super::*;
    use crate::domain::model::{
        HookExecutionClass, HookExecutionFilter, HookExecutionOutcome, HookExecutionScope,
        HookExecutionStatistics, HookExecutionTimeRange, StoredHookExecution,
    };
    use crate::domain::service::HookExecutionObserver;
    use crate::infrastructure::hook_execution::HookExecutionBufferSettings;
    use crate::infrastructure::hook_execution::test_records::record;
    use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

    /// 记下写入的批；`fail` 打开时写入报错，`hang` 打开时写入不返回；保留期按条数模拟。
    #[derive(Default)]
    struct FakeRepository {
        batches: Mutex<Vec<Vec<HookExecutionRecord>>>,
        fail: AtomicBool,
        hang: AtomicBool,
        expired_rows: AtomicU32,
        delete_calls: AtomicU32,
    }

    #[async_trait::async_trait]
    impl HookExecutionRepository for FakeRepository {
        async fn insert_batch(&self, records: &[HookExecutionRecord]) -> Result<u64> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(ErrorBuilder::new(ErrorCode::DatabaseError, "down").build_error());
            }
            if self.hang.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            self.batches.lock().unwrap().push(records.to_vec());
            Ok(records.len() as u64)
        }

        async fn delete_executed_before(&self, _cutoff: SystemTime, limit: u32) -> Result<u64> {
            self.delete_calls.fetch_add(1, Ordering::Relaxed);
            let left = self.expired_rows.load(Ordering::Relaxed);
            let take = left.min(limit);
            self.expired_rows.fetch_sub(take, Ordering::Relaxed);
            Ok(u64::from(take))
        }

        async fn statistics(
            &self,
            _scope: &HookExecutionScope,
            _range: &HookExecutionTimeRange,
        ) -> Result<HookExecutionStatistics> {
            Ok(HookExecutionStatistics::default())
        }

        async fn query(&self, _filter: &HookExecutionFilter) -> Result<Vec<StoredHookExecution>> {
            Ok(Vec::new())
        }
    }

    fn buffer(batch_size: usize) -> Arc<HookExecutionBuffer> {
        Arc::new(HookExecutionBuffer::new(
            HookExecutionBufferSettings {
                capacity: 100,
                batch_size,
                sample_every: 1,
            },
            Arc::new(HookExecutionMetrics::new()),
        ))
    }

    fn failed() -> HookExecutionRecord {
        record(
            "t1",
            "dlp",
            HookExecutionOutcome::TimedOut,
            SystemTime::now(),
        )
    }

    /// 攒够一批立即写；不满一批等定时器；停机时写完余下的。
    #[tokio::test]
    async fn the_writer_flushes_full_batches_on_the_interval_and_at_shutdown() {
        let buffer = buffer(2);
        let repository = Arc::new(FakeRepository::default());
        let (stop, shutdown) = oneshot::channel();
        let writer = tokio::spawn(run_writer(
            Arc::clone(&buffer),
            repository.clone(),
            Duration::from_secs(3600),
            shutdown,
        ));
        // 第一次 tick 立即触发；之后定时器一小时一次，只有攒够一批会写。
        tokio::time::sleep(Duration::from_millis(50)).await;
        buffer.record(failed());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(repository.batches.lock().unwrap().is_empty());
        buffer.record(failed());
        wait_until(|| repository.batches.lock().unwrap().len() == 1).await;
        assert_eq!(repository.batches.lock().unwrap()[0].len(), 2);

        buffer.record(failed());
        stop.send(()).unwrap();
        writer.await.unwrap();
        let batches = repository.batches.lock().unwrap();
        assert_eq!(batches.len(), 2, "the partial batch is written at shutdown");
        assert_eq!(buffer.metrics().written.get(), 3);
        assert_eq!(buffer.metrics().flushes.with_label_values(&["ok"]).get(), 2);
    }

    /// 停机时写库卡住：这一批的时限截到停机总时限，到点整批计为停机丢弃（以前整体超时把正在写的
    /// 一批连同计数一起取消，这批既不算写入也不算丢弃）；之后交来的记录也计入。
    #[tokio::test(start_paused = true)]
    async fn a_write_cut_off_at_shutdown_is_counted_as_dropped() {
        let buffer = buffer(100);
        let repository = Arc::new(FakeRepository::default());
        repository.hang.store(true, Ordering::Relaxed);
        let (stop, shutdown) = oneshot::channel();
        let writer = tokio::spawn(run_writer(
            Arc::clone(&buffer),
            repository.clone(),
            Duration::from_secs(3600),
            shutdown,
        ));
        // 第一次 tick 立即触发（缓冲还空）；5 条不满一批，不唤醒写入任务。
        tokio::time::sleep(Duration::from_millis(50)).await;
        for _ in 0..5 {
            buffer.record(failed());
        }
        let started = tokio::time::Instant::now();
        stop.send(()).unwrap();
        writer.await.unwrap();
        let waited = started.elapsed();
        assert!(
            (SHUTDOWN_FLUSH_TIMEOUT..WRITE_TIMEOUT).contains(&waited),
            "{waited:?}"
        );
        let metrics = buffer.metrics();
        assert_eq!(metrics.written.get(), 0);
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Always),
            5
        );
        assert_eq!(
            metrics.dropped_count(DropReason::WriteFailed, HookExecutionClass::Always),
            0
        );
        buffer.record(failed());
        assert_eq!(
            metrics.dropped_count(DropReason::Shutdown, HookExecutionClass::Always),
            6,
            "records arriving after the writer stopped are counted"
        );
        assert!(buffer.is_empty());
    }

    /// 写库失败的一批丢弃并按类别计数，不回队；之后恢复照常写。
    #[tokio::test]
    async fn failed_writes_are_counted_as_dropped_and_not_retried() {
        let buffer = buffer(2);
        let repository = Arc::new(FakeRepository::default());
        repository.fail.store(true, Ordering::Relaxed);
        let mut failures = FailureLog::default();
        buffer.record(failed());
        buffer.record(record(
            "t1",
            "dlp",
            HookExecutionOutcome::Allowed,
            SystemTime::now(),
        ));
        flush(&buffer, repository.as_ref(), &mut failures).await;
        let metrics = buffer.metrics();
        assert_eq!(
            metrics.dropped_count(DropReason::WriteFailed, HookExecutionClass::Always),
            1
        );
        assert_eq!(
            metrics.dropped_count(DropReason::WriteFailed, HookExecutionClass::Sampled),
            1
        );
        assert!(buffer.is_empty());
        repository.fail.store(false, Ordering::Relaxed);
        buffer.record(failed());
        flush(&buffer, repository.as_ref(), &mut failures).await;
        assert_eq!(metrics.written.get(), 1);
        assert_eq!(metrics.flushes.with_label_values(&["error"]).get(), 1);
    }

    /// 一轮清理分批删到不满一批为止，删除数进指标。
    #[tokio::test]
    async fn a_sweep_deletes_in_batches_until_a_short_batch() {
        let repository = FakeRepository::default();
        repository.expired_rows.store(25, Ordering::Relaxed);
        let metrics = HookExecutionMetrics::new();
        let (_stop, mut shutdown) = oneshot::channel::<()>();
        let sweep = sweep_expired(
            &repository,
            &metrics,
            Duration::from_secs(7 * 86_400),
            10,
            &mut shutdown,
        )
        .await;
        assert_eq!(
            sweep,
            Sweep {
                deleted: 25,
                shutdown: false
            }
        );
        assert_eq!(repository.delete_calls.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.expired.get(), 25);
    }

    /// 停机信号落在一轮多批清理的中途：这一轮停下，清理任务随即结束，不再去等已经收到的信号
    /// （再 poll 已完成的 oneshot 会 panic）。
    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_multi_batch_sweep_ends_the_retention_task() {
        let repository = Arc::new(FakeRepository::default());
        repository.expired_rows.store(1_000_000, Ordering::Relaxed);
        let (stop, shutdown) = oneshot::channel();
        let task = tokio::spawn(run_retention(
            repository.clone(),
            Arc::new(HookExecutionMetrics::new()),
            HookExecutionTasks {
                flush_interval: Duration::from_secs(1),
                retention: Duration::from_secs(86_400),
                retention_sweep_interval: Duration::from_secs(1),
                retention_delete_batch: 10,
            },
            shutdown,
        ));
        wait_until(|| repository.delete_calls.load(Ordering::Relaxed) >= 3).await;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the retention task ends at shutdown")
            .expect("the retention task does not panic");
        let calls = repository.delete_calls.load(Ordering::Relaxed);
        assert!(
            calls < MAX_BATCHES_PER_SWEEP,
            "the sweep stopped early: {calls}"
        );
    }

    /// 一轮清理收到停机信号即停，并告诉调用方。
    #[tokio::test]
    async fn a_sweep_stops_at_shutdown_and_says_so() {
        let repository = FakeRepository::default();
        repository.expired_rows.store(100, Ordering::Relaxed);
        let metrics = HookExecutionMetrics::new();
        let (stop, mut shutdown) = oneshot::channel::<()>();
        stop.send(()).unwrap();
        let sweep = sweep_expired(
            &repository,
            &metrics,
            Duration::from_secs(86_400),
            10,
            &mut shutdown,
        )
        .await;
        assert_eq!(
            sweep,
            Sweep {
                deleted: 10,
                shutdown: true
            }
        );
        assert_eq!(repository.delete_calls.load(Ordering::Relaxed), 1);
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "condition not reached");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
