//! 后台任务：把缓冲按批交给 [`HookExecutionSink`]。随进程停机结束；停机时在有限时间内把缓冲写完，
//! 写不完的计为丢弃。每条入队的记录要么写进去，要么计入丢弃指标：超时的一批整批计数，停机后才到的
//! 记录直接计数（见 [`HookExecutionBuffer::close`]）。保留期清理归 Capability。

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_server_core::error::Result;
use tokio::sync::oneshot;

use super::buffer::HookExecutionBuffer;
use super::metrics::{DropReason, HookExecutionMetrics};
use super::record::HookExecutionRecord;

/// 执行记录的去处。Ingest 的实现批量写 `hook_executions`。
#[async_trait]
pub trait HookExecutionSink: Send + Sync {
    /// 写一批，返回写入的行数。
    async fn write_batch(&self, records: &[HookExecutionRecord]) -> Result<u64>;
}

/// 一批写库的时限：超过即放弃这一批（计为丢弃），不让慢库拖住缓冲。
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// 停机时写完缓冲的总时限（每批的时限截到这里为止）。
pub const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
/// 写库连续失败时 WARN 的最小间隔，期间的失败次数在下一条里补记。
const WRITE_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

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
    sink: &dyn HookExecutionSink,
    metrics: &HookExecutionMetrics,
    batch: Vec<HookExecutionRecord>,
    failures: &mut FailureLog,
    time_limit: Duration,
    timeout_reason: DropReason,
) {
    let (reason, dropped_as) =
        match tokio::time::timeout(time_limit, sink.write_batch(&batch)).await {
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
    sink: &dyn HookExecutionSink,
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
            sink,
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
    sink: Arc<dyn HookExecutionSink>,
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
        flush(&buffer, sink.as_ref(), &mut failures).await;
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
            sink.as_ref(),
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::SystemTime;

    use super::*;
    use crate::hooks::execution::buffer::{HookExecutionBufferSettings, test_records::record};
    use crate::hooks::execution::guard::HookExecutionObserver;
    use crate::hooks::execution::record::{HookExecutionClass, HookExecutionOutcome};
    use flare_server_core::error::{ErrorBuilder, ErrorCode};

    /// 记下写入的批；`fail` 打开时写入报错，`hang` 打开时写入不返回。
    #[derive(Default)]
    struct FakeSink {
        batches: Mutex<Vec<Vec<HookExecutionRecord>>>,
        fail: AtomicBool,
        hang: AtomicBool,
    }

    #[async_trait]
    impl HookExecutionSink for FakeSink {
        async fn write_batch(&self, records: &[HookExecutionRecord]) -> Result<u64> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(ErrorBuilder::new(ErrorCode::DatabaseError, "down").build_error());
            }
            if self.hang.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            self.batches.lock().unwrap().push(records.to_vec());
            Ok(records.len() as u64)
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
        let sink = Arc::new(FakeSink::default());
        let (stop, shutdown) = oneshot::channel();
        let writer = tokio::spawn(run_writer(
            Arc::clone(&buffer),
            sink.clone(),
            Duration::from_secs(3600),
            shutdown,
        ));
        // 第一次 tick 立即触发；之后定时器一小时一次，只有攒够一批会写。
        tokio::time::sleep(Duration::from_millis(50)).await;
        buffer.record(failed());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(sink.batches.lock().unwrap().is_empty());
        buffer.record(failed());
        wait_until(|| sink.batches.lock().unwrap().len() == 1).await;
        assert_eq!(sink.batches.lock().unwrap()[0].len(), 2);

        buffer.record(failed());
        stop.send(()).unwrap();
        writer.await.unwrap();
        let batches = sink.batches.lock().unwrap();
        assert_eq!(batches.len(), 2, "the partial batch is written at shutdown");
        assert_eq!(buffer.metrics().written.get(), 3);
        assert_eq!(buffer.metrics().flushes.with_label_values(&["ok"]).get(), 2);
    }

    /// 停机时写库卡住：这一批的时限截到停机总时限，到点整批计为停机丢弃（以前整体超时把正在写的
    /// 一批连同计数一起取消，这批既不算写入也不算丢弃）；之后交来的记录也计入。
    #[tokio::test(start_paused = true)]
    async fn a_write_cut_off_at_shutdown_is_counted_as_dropped() {
        let buffer = buffer(100);
        let sink = Arc::new(FakeSink::default());
        sink.hang.store(true, Ordering::Relaxed);
        let (stop, shutdown) = oneshot::channel();
        let writer = tokio::spawn(run_writer(
            Arc::clone(&buffer),
            sink.clone(),
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
        let sink = Arc::new(FakeSink::default());
        sink.fail.store(true, Ordering::Relaxed);
        let mut failures = FailureLog::default();
        buffer.record(failed());
        buffer.record(record(
            "t1",
            "dlp",
            HookExecutionOutcome::Allowed,
            SystemTime::now(),
        ));
        flush(&buffer, sink.as_ref(), &mut failures).await;
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
        sink.fail.store(false, Ordering::Relaxed);
        buffer.record(failed());
        flush(&buffer, sink.as_ref(), &mut failures).await;
        assert_eq!(metrics.written.get(), 1);
        assert_eq!(metrics.flushes.with_label_values(&["error"]).get(), 1);
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "condition not reached");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
