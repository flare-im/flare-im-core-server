//! 发送幂等账本（`message_idempotency_ledger`）的保留期清理任务，做法与 Capability 清理 Hook 执行记录相同。
//!
//! 每过一个间隔清一轮：删掉入账早于保留期的行，每批至多 [`DELETE_BATCH`] 行、一条独立语句（锁定后按主键删，
//! 见 `PostgresMessageStore`），批间停顿 [`SWEEP_PAUSE`] 给消息写入让出 IO；一轮至多 [`MAX_BATCHES_PER_SWEEP`]
//! 批，删不完的留到下一轮。多个 Writer 实例同时清理时各自跳过对方已锁的行。删除数与失败计入
//! `storage_writer_persist_total{path="idempotency_ledger_retention"}`。

use std::sync::Arc;
use std::time::Duration;

use flare_im_service_kit::metrics::StorageWriterMetrics;
use tokio::sync::oneshot;

use crate::domain::repository::IdempotencyLedgerRetention;

/// 账本保留期：覆盖客户端离线后带同一 `client_msg_id` 重发的时间（客户端发件箱的自动续发上限远小于它）。
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
/// 每批至多删的行数。
pub const DELETE_BATCH: u32 = 10_000;
/// 两轮清理之间的间隔。
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// 两批删除之间的停顿。
const SWEEP_PAUSE: Duration = Duration::from_millis(50);
/// 一轮至多删几批。
const MAX_BATCHES_PER_SWEEP: u32 = 1_000;

const METRIC_PATH: &str = "idempotency_ledger_retention";

/// 清理节奏。生产用 [`Default`]（上面的常量），用例缩短间隔。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSchedule {
    pub retention: Duration,
    pub sweep_interval: Duration,
    pub delete_batch: u32,
}

impl Default for RetentionSchedule {
    fn default() -> Self {
        Self {
            retention: RETENTION,
            sweep_interval: SWEEP_INTERVAL,
            delete_batch: DELETE_BATCH,
        }
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

/// 一轮清理：删掉入账早于 `now - retention` 的行，每批至多 `delete_batch` 行，删到不满一批为止；
/// 两批之间收到停机信号即停（结果里 `shutdown` 为真）。出错只记一次，留到下一轮重试。
pub async fn sweep_expired<R: IdempotencyLedgerRetention>(
    repository: &R,
    metrics: Option<&StorageWriterMetrics>,
    retention: Duration,
    delete_batch: u32,
    shutdown: &mut oneshot::Receiver<()>,
) -> Sweep {
    let mut sweep = Sweep {
        deleted: 0,
        shutdown: false,
    };
    let Some(cutoff) = chrono::Duration::from_std(retention)
        .ok()
        .and_then(|retention| chrono::Utc::now().checked_sub_signed(retention))
    else {
        return sweep;
    };
    let delete_batch = delete_batch.max(1);
    for _ in 0..MAX_BATCHES_PER_SWEEP {
        match repository.delete_created_before(cutoff, delete_batch).await {
            Ok(rows) => {
                sweep.deleted += rows;
                if let Some(metrics) = metrics {
                    metrics
                        .storage_persist_total
                        .with_label_values(&[METRIC_PATH, "deleted"])
                        .inc_by(rows);
                }
                if rows < u64::from(delete_batch) {
                    break;
                }
            }
            Err(error) => {
                if let Some(metrics) = metrics {
                    metrics.record_storage_persist(METRIC_PATH, "error");
                }
                tracing::warn!(
                    %error,
                    deleted = sweep.deleted,
                    "message idempotency ledger retention sweep failed; retrying next round"
                );
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
    if sweep.deleted > 0 {
        tracing::info!(
            deleted = sweep.deleted,
            retention_secs = retention.as_secs(),
            "expired message idempotency ledger rows deleted"
        );
    }
    sweep
}

/// 清理任务：启动后先等一个间隔，之后每个间隔清一轮；停机信号不论落在等待中还是一轮清理中，都在这一轮
/// 结束后返回。
pub async fn run_retention<R: IdempotencyLedgerRetention>(
    repository: Arc<R>,
    metrics: Option<Arc<StorageWriterMetrics>>,
    schedule: RetentionSchedule,
    mut shutdown: oneshot::Receiver<()>,
) {
    let interval = schedule.sweep_interval.max(Duration::from_secs(1));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = ticker.tick() => {}
        }
        let sweep = sweep_expired(
            repository.as_ref(),
            metrics.as_deref(),
            schedule.retention,
            schedule.delete_batch,
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
    use flare_server_core::error::{FlareError, Result};

    /// 过期行按条数模拟；`fail` 打开时删除报错。
    #[derive(Default)]
    struct FakeLedger {
        expired_rows: AtomicU32,
        delete_calls: AtomicU32,
        fail: AtomicBool,
        cutoffs: Mutex<Vec<chrono::DateTime<chrono::Utc>>>,
    }

    impl IdempotencyLedgerRetention for FakeLedger {
        async fn delete_created_before(
            &self,
            cutoff: chrono::DateTime<chrono::Utc>,
            limit: u32,
        ) -> Result<u64> {
            self.delete_calls.fetch_add(1, Ordering::Relaxed);
            self.cutoffs.lock().unwrap().push(cutoff);
            if self.fail.load(Ordering::Relaxed) {
                return Err(FlareError::system("ledger unavailable"));
            }
            let left = self.expired_rows.load(Ordering::Relaxed);
            let take = left.min(limit);
            self.expired_rows.fetch_sub(take, Ordering::Relaxed);
            Ok(u64::from(take))
        }
    }

    fn deleted(metrics: &StorageWriterMetrics) -> u64 {
        metrics
            .storage_persist_total
            .with_label_values(&[METRIC_PATH, "deleted"])
            .get()
    }

    /// 一轮清理分批删到不满一批为止，截止时间是此刻减保留期，删除数进指标。
    #[tokio::test]
    async fn a_sweep_deletes_in_bounded_batches_until_a_short_batch() {
        let ledger = FakeLedger::default();
        ledger.expired_rows.store(25, Ordering::Relaxed);
        let metrics = StorageWriterMetrics::new();
        let before = deleted(&metrics);
        let (_stop, mut shutdown) = oneshot::channel::<()>();
        let started = chrono::Utc::now();
        let sweep = sweep_expired(&ledger, Some(&metrics), RETENTION, 10, &mut shutdown).await;
        assert_eq!(
            sweep,
            Sweep {
                deleted: 25,
                shutdown: false
            }
        );
        assert_eq!(ledger.delete_calls.load(Ordering::Relaxed), 3);
        assert_eq!(deleted(&metrics) - before, 25);
        let cutoff = ledger.cutoffs.lock().unwrap()[0];
        let age = started - cutoff;
        assert!(
            (age.num_seconds() - 7 * 24 * 3600).abs() <= 5,
            "cutoff is seven days back: {age}"
        );
    }

    /// 删除失败：这一轮停下，留到下一轮，失败计数。
    #[tokio::test]
    async fn a_failed_batch_ends_the_round_and_is_counted() {
        let ledger = FakeLedger::default();
        ledger.expired_rows.store(25, Ordering::Relaxed);
        ledger.fail.store(true, Ordering::Relaxed);
        let metrics = StorageWriterMetrics::new();
        let errors = || {
            metrics
                .storage_persist_total
                .with_label_values(&[METRIC_PATH, "error"])
                .get()
        };
        let before = errors();
        let (_stop, mut shutdown) = oneshot::channel::<()>();
        let sweep = sweep_expired(&ledger, Some(&metrics), RETENTION, 10, &mut shutdown).await;
        assert_eq!(sweep.deleted, 0);
        assert_eq!(ledger.delete_calls.load(Ordering::Relaxed), 1);
        assert_eq!(errors() - before, 1);
    }

    /// 停机信号落在多批清理的中途：这一轮停下，任务随即结束。
    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_multi_batch_sweep_ends_the_task() {
        let ledger = Arc::new(FakeLedger::default());
        ledger.expired_rows.store(1_000_000, Ordering::Relaxed);
        let (stop, shutdown) = oneshot::channel();
        let task = tokio::spawn(run_retention(
            ledger.clone(),
            None,
            RetentionSchedule {
                retention: RETENTION,
                sweep_interval: Duration::from_secs(1),
                delete_batch: 10,
            },
            shutdown,
        ));
        while ledger.delete_calls.load(Ordering::Relaxed) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the retention task ends at shutdown")
            .expect("the retention task does not panic");
        assert!(ledger.delete_calls.load(Ordering::Relaxed) < MAX_BATCHES_PER_SWEEP);
    }
}
