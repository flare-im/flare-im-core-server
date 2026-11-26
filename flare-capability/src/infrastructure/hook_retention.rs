//! `hook_executions` 的保留期清理：按保留期分批删除过期行，随进程停机结束。写入在 Ingest，统计、查询
//! 和清理留在控制面。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use flare_im_core_service_kit::metrics::REGISTRY;
use once_cell::sync::Lazy;
use prometheus::IntCounter;
use tokio::sync::oneshot;

use crate::domain::repository::HookExecutionRepository;

/// 一轮清理至多删几批：一轮删不完的留到下一轮。
const MAX_BATCHES_PER_SWEEP: u32 = 1_000;
/// 两批删除之间的间隔，给其他写入让出行锁与 IO。
const SWEEP_PAUSE: Duration = Duration::from_millis(50);

/// 清理任务的参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookRetentionTask {
    pub retention: Duration,
    pub sweep_interval: Duration,
    pub delete_batch: u32,
}

/// 保留期清理的指标。
pub struct HookRetentionMetrics {
    /// 删掉的行数。
    pub expired: IntCounter,
}

static REGISTERED: Lazy<Arc<HookRetentionMetrics>> = Lazy::new(|| {
    let metrics = HookRetentionMetrics::new();
    let _ = REGISTRY.register(Box::new(metrics.expired.clone()));
    Arc::new(metrics)
});

impl HookRetentionMetrics {
    /// 未登记到任何注册表的一份（测试用）。
    pub fn new() -> Self {
        Self {
            expired: IntCounter::new(
                "capability_hook_execution_records_expired_total",
                "Hook execution rows deleted by the retention sweep",
            )
            .expect("valid metric"),
        }
    }

    /// 进程里登记到共享注册表（`/metrics` 出口）的那一份。
    pub fn registered() -> Arc<Self> {
        Arc::clone(&REGISTERED)
    }
}

impl Default for HookRetentionMetrics {
    fn default() -> Self {
        Self::new()
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
    metrics: &HookRetentionMetrics,
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
    metrics: Arc<HookRetentionMetrics>,
    settings: HookRetentionTask,
    mut shutdown: oneshot::Receiver<()>,
) {
    let interval = settings.sweep_interval.max(Duration::from_secs(1));
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
            settings.delete_batch,
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
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant;

    use super::*;
    use crate::domain::model::{
        HookExecutionFilter, HookExecutionScope, HookExecutionStatistics, HookExecutionTimeRange,
        StoredHookExecution,
    };
    use flare_server_core::error::Result;

    /// 保留期按条数模拟。
    #[derive(Default)]
    struct FakeRepository {
        expired_rows: AtomicU32,
        delete_calls: AtomicU32,
    }

    #[async_trait::async_trait]
    impl HookExecutionRepository for FakeRepository {
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

    /// 一轮清理分批删到不满一批为止，删除数进指标。
    #[tokio::test]
    async fn a_sweep_deletes_in_batches_until_a_short_batch() {
        let repository = FakeRepository::default();
        repository.expired_rows.store(25, Ordering::Relaxed);
        let metrics = HookRetentionMetrics::new();
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
            Arc::new(HookRetentionMetrics::new()),
            HookRetentionTask {
                retention: Duration::from_secs(86_400),
                sweep_interval: Duration::from_secs(1),
                delete_batch: 10,
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
        let metrics = HookRetentionMetrics::new();
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
