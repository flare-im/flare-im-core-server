//! 推送绑定吊销的派发。
//!
//! 踢线、身份会话吊销、被别的设备顶下线时，领域服务把吊销请求交给 [`RevocationQueue`]（不等待、
//! 不失败），派发任务（[`RevocationDispatcher`]）经 [`PushBindingRevoker`] 送到推送侧。结束会话的
//! 操作因此不等推送代理，也不因它失败。
//!
//! 与网关失联交接的上报不同，这里丢一次就意味着一台已经被踢的设备继续收到带预览的推送，所以
//! 失败要重试：按退避间隔重试到上限为止，最终失败的逐条记错误日志并计数。队列与重试都在进程内，
//! 进程退出时还没送出的请求不会保留。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, warn};

use crate::domain::push_binding::{BindingRevocation, BindingRevocationSink, PushBindingRevoker};

/// 派发的上限与重试节奏。
#[derive(Debug, Clone)]
pub struct RevocationDispatchConfig {
    /// 队列容量。踢整个租户时请求成批进来；推送代理长时间不可达时队列会满，之后的请求丢弃并计数。
    pub queue_capacity: usize,
    /// 同时在途的调用数。
    pub concurrency: usize,
    /// 同时处理中的请求数，含正在等下一次重试的。等待重试的不占在途调用的名额，推送代理不可达
    /// 时可以有比并发数多得多的请求在退避里等它恢复。
    pub in_progress_limit: usize,
    /// 单次调用的时限，覆盖解析通道、建连与调用本身。
    pub call_timeout: Duration,
    /// 每次失败之后、下一次尝试之前等多久；总尝试次数是它的长度加一。
    pub retry_delays: Vec<Duration>,
    /// 停机时给还没了结的请求的宽限，须小于运行时等待任务退出的时间。
    pub shutdown_grace: Duration,
}

impl Default for RevocationDispatchConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 16_384,
            concurrency: 32,
            in_progress_limit: 1_024,
            call_timeout: Duration::from_secs(2),
            // 六次尝试，间隔合计 62s：推送代理重启、滚动发布这类分钟级以内的中断能等过去。
            retry_delays: [2, 4, 8, 16, 32]
                .into_iter()
                .map(Duration::from_secs)
                .collect(),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

/// 吊销请求的累计计数（进程内）。
#[derive(Debug, Default)]
pub struct RevocationStats {
    queued: AtomicU64,
    revoked: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
}

impl RevocationStats {
    /// 已入队的请求数。
    pub fn queued(&self) -> u64 {
        self.queued.load(Ordering::Relaxed)
    }

    /// 推送侧已执行的请求数（含本来就没有绑定的）。
    pub fn revoked(&self) -> u64 {
        self.revoked.load(Ordering::Relaxed)
    }

    /// 失败后重试的次数。
    pub fn retried(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }

    /// 重试到上限仍然失败的请求数。
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// 没能入队（队列已满或派发任务已退出）而丢弃的请求数。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 已入队、还没了结的请求数（在队列里、在途或在等重试）。
    pub fn pending(&self) -> u64 {
        self.queued()
            .saturating_sub(self.revoked())
            .saturating_sub(self.failed())
    }
}

/// 队列的写入端：领域服务经 [`BindingRevocationSink`] 提交。
pub struct RevocationQueue {
    sender: mpsc::Sender<BindingRevocation>,
    stats: Arc<RevocationStats>,
}

impl RevocationQueue {
    pub fn stats(&self) -> &RevocationStats {
        &self.stats
    }
}

impl BindingRevocationSink for RevocationQueue {
    fn submit(&self, revocation: BindingRevocation) {
        let (revocation, cause) = match self.sender.try_send(revocation) {
            Ok(()) => {
                self.stats.queued.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(TrySendError::Full(revocation)) => (revocation, "queue full"),
            Err(TrySendError::Closed(revocation)) => (revocation, "dispatcher stopped"),
        };
        let dropped = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        // 队列只会在成批踢线（踢整个租户）撞上推送代理不可达时写满，那时会连续丢弃上万条：计数
        // 照常累加，日志按 2 的幂次记。
        if dropped.is_power_of_two() {
            error!(
                tenant_id = %revocation.tenant_id,
                reason = revocation.reason.as_str(),
                cause,
                dropped_total = dropped,
                "push binding revocation dropped before it was sent: the device may keep receiving pushes"
            );
        }
    }
}

/// 队列的读取端：把吊销请求送到推送侧，失败按退避重试。
pub struct RevocationDispatcher<R: PushBindingRevoker> {
    revoker: Arc<R>,
    receiver: mpsc::Receiver<BindingRevocation>,
    stats: Arc<RevocationStats>,
    config: RevocationDispatchConfig,
}

/// 建一对队列与派发器。
pub fn revocation_channel<R: PushBindingRevoker>(
    revoker: Arc<R>,
    config: RevocationDispatchConfig,
) -> (RevocationQueue, RevocationDispatcher<R>) {
    let (sender, receiver) = mpsc::channel(config.queue_capacity.max(1));
    let stats = Arc::new(RevocationStats::default());
    (
        RevocationQueue {
            sender,
            stats: stats.clone(),
        },
        RevocationDispatcher {
            revoker,
            receiver,
            stats,
            config,
        },
    )
}

impl<R: PushBindingRevoker> RevocationDispatcher<R> {
    pub fn stats(&self) -> Arc<RevocationStats> {
        self.stats.clone()
    }

    /// 派发直到 `shutdown`。写入端由领域服务长期持有，队列不会自己关闭；停机后只再等已入队的
    /// 请求了结（都了结就立刻退出），到宽限仍没了结的放弃并记错误日志。
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let Self {
            revoker,
            receiver,
            stats,
            config,
        } = self;
        let revoker: &R = &revoker;
        let stats: &RevocationStats = &stats;
        let config = &config;
        let calls = Semaphore::new(config.concurrency.max(1));
        let calls = &calls;
        let work = ReceiverStream::new(receiver).for_each_concurrent(
            config.in_progress_limit.max(1),
            |revocation| async move {
                settle(revoker, calls, stats, config, revocation).await;
            },
        );
        tokio::pin!(work);
        tokio::pin!(shutdown);
        tokio::select! {
            _ = &mut work => return,
            _ = &mut shutdown => {}
        }
        let settled = async {
            while stats.pending() > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            _ = &mut work => {}
            _ = settled => {}
            _ = tokio::time::sleep(config.shutdown_grace) => {}
        }
        let abandoned = stats.pending();
        if abandoned > 0 {
            error!(
                abandoned,
                revoked_total = stats.revoked(),
                failed_total = stats.failed(),
                dropped_total = stats.dropped(),
                "push binding revocations abandoned at shutdown: those devices may keep receiving pushes"
            );
        }
    }
}

/// 了结一条吊销请求：限时调用，失败按退避重试到上限。结果只进计数与日志。每次尝试发的是同一份
/// 请求，其中的登记时刻上限是提交时定下的：重试拖得再久，也只吊销那一刻之前登记的绑定。
async fn settle<R: PushBindingRevoker>(
    revoker: &R,
    calls: &Semaphore,
    stats: &RevocationStats,
    config: &RevocationDispatchConfig,
    revocation: BindingRevocation,
) {
    let attempts = config.retry_delays.len() + 1;
    let mut failure = String::new();
    for attempt in 1..=attempts {
        let outcome = {
            // 名额只管调用本身：拿到才发起，调用结束（或超时）就还回去。
            let _permit = calls.acquire().await;
            tokio::time::timeout(config.call_timeout, revoker.revoke(&revocation)).await
        };
        match outcome {
            Ok(Ok(revoked)) => {
                stats.revoked.fetch_add(1, Ordering::Relaxed);
                debug!(
                    tenant_id = %revocation.tenant_id,
                    user_id = %revocation.user_id,
                    device_id = revocation.device_id.as_deref().unwrap_or("*"),
                    reason = revocation.reason.as_str(),
                    revoked,
                    attempt,
                    "push bindings revoked"
                );
                return;
            }
            Ok(Err(error)) => failure = error.to_string(),
            Err(_) => failure = format!("no answer within {}ms", config.call_timeout.as_millis()),
        }
        let Some(delay) = config.retry_delays.get(attempt - 1) else {
            break;
        };
        stats.retried.fetch_add(1, Ordering::Relaxed);
        warn!(
            tenant_id = %revocation.tenant_id,
            user_id = %revocation.user_id,
            device_id = revocation.device_id.as_deref().unwrap_or("*"),
            reason = revocation.reason.as_str(),
            error = %failure,
            attempt,
            attempts,
            retry_in_ms = delay.as_millis() as u64,
            "push binding revocation failed; retrying"
        );
        tokio::time::sleep(*delay).await;
    }
    let failed = stats.failed.fetch_add(1, Ordering::Relaxed) + 1;
    error!(
        tenant_id = %revocation.tenant_id,
        user_id = %revocation.user_id,
        device_id = revocation.device_id.as_deref().unwrap_or("*"),
        reason = revocation.reason.as_str(),
        error = %failure,
        attempts,
        failed_total = failed,
        "push bindings were not revoked: the device may keep receiving pushes"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use flare_server_core::error::{FlareError, Result};

    use super::*;
    use crate::domain::push_binding::BindingRevocationReason;

    /// 推送侧的替身：前 `failures` 次调用失败（`hang` 时是不应答），之后成功。记下每次调用带来的
    /// 登记时刻上限、成功收到的请求与同时在途的峰值。
    struct Proxy {
        failures: usize,
        hang: bool,
        calls: AtomicUsize,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        cutoffs: Mutex<Vec<i64>>,
        seen: Mutex<Vec<BindingRevocation>>,
    }

    impl Proxy {
        fn failing(failures: usize) -> Arc<Self> {
            Arc::new(Self {
                failures,
                hang: false,
                calls: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                cutoffs: Mutex::new(Vec::new()),
                seen: Mutex::new(Vec::new()),
            })
        }

        fn silent() -> Arc<Self> {
            Arc::new(Self {
                failures: usize::MAX,
                hang: true,
                calls: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                cutoffs: Mutex::new(Vec::new()),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    impl PushBindingRevoker for Proxy {
        async fn revoke(&self, revocation: &BindingRevocation) -> Result<u32> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.cutoffs
                .lock()
                .unwrap()
                .push(revocation.registered_before_ms);
            if call < self.failures {
                if self.hang {
                    return std::future::pending().await;
                }
                return Err(FlareError::system("push proxy unavailable"));
            }
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(in_flight, Ordering::SeqCst);
            // 让出执行，别的调用才有机会同时在途。
            tokio::time::sleep(Duration::from_millis(2)).await;
            self.seen.lock().unwrap().push(revocation.clone());
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(1)
        }
    }

    /// 踢线发生的时刻（用例里的固定值）：远早于用例运行的「现在」。
    const KICKED_AT_MS: i64 = 1_700_000_000_000;

    fn kicked(device: usize) -> BindingRevocation {
        BindingRevocation::device(
            "tenant-a",
            "alice",
            &format!("device-{device}"),
            BindingRevocationReason::Kicked,
            KICKED_AT_MS,
        )
    }

    /// 六次尝试，间隔几毫秒：用例不用真等一分钟。
    fn config() -> RevocationDispatchConfig {
        RevocationDispatchConfig {
            queue_capacity: 1_024,
            concurrency: 4,
            in_progress_limit: 64,
            call_timeout: Duration::from_millis(20),
            retry_delays: vec![Duration::from_millis(3); 5],
            shutdown_grace: Duration::from_secs(5),
        }
    }

    /// 提交后停机：派发在宽限内把已入队的了结完就退出。
    async fn settle_all<R: PushBindingRevoker>(dispatcher: RevocationDispatcher<R>) {
        tokio::time::timeout(Duration::from_secs(10), dispatcher.run(async {}))
            .await
            .expect("the dispatcher stops once everything queued is settled");
    }

    /// 缺省节奏：六次尝试，间隔合计约一分钟。
    #[test]
    fn the_default_schedule_is_six_attempts_over_about_a_minute() {
        let config = RevocationDispatchConfig::default();
        assert_eq!(config.retry_delays.len() + 1, 6);
        let waits: Duration = config.retry_delays.iter().sum();
        assert_eq!(waits, Duration::from_secs(62));
        assert!(config.retry_delays.windows(2).all(|pair| pair[0] < pair[1]));
    }

    /// 入队的请求全部送到推送侧；同时在途的调用不超过并发上限。
    #[tokio::test]
    async fn queued_revocations_are_sent_within_the_concurrency_limit() {
        let proxy = Proxy::failing(0);
        let (queue, dispatcher) = revocation_channel(proxy.clone(), config());
        let stats = dispatcher.stats();
        for device in 0..40 {
            queue.submit(kicked(device));
        }
        assert_eq!(stats.queued(), 40);

        settle_all(dispatcher).await;

        assert_eq!(
            (stats.revoked(), stats.failed(), stats.dropped()),
            (40, 0, 0)
        );
        assert_eq!(stats.pending(), 0);
        assert_eq!(proxy.seen.lock().unwrap().len(), 40);
        let peak = proxy.peak.load(Ordering::SeqCst);
        assert!((2..=4).contains(&peak), "peak in flight was {peak}");
    }

    /// 推送代理一时不可达：失败的请求按退避重试，恢复后送达，不算失败。每一次重试带的都是提交时
    /// 定下的那个时刻，不是重试时的「现在」：设备在这期间重新登录后登记的绑定不会被清掉。
    #[tokio::test]
    async fn a_failed_revocation_is_retried_with_its_original_cutoff() {
        let proxy = Proxy::failing(3);
        let (queue, dispatcher) = revocation_channel(proxy.clone(), config());
        let stats = dispatcher.stats();
        queue.submit(kicked(1));

        settle_all(dispatcher).await;

        assert_eq!(proxy.calls.load(Ordering::SeqCst), 4);
        assert_eq!(
            (stats.revoked(), stats.retried(), stats.failed()),
            (1, 3, 0)
        );
        assert_eq!(*proxy.cutoffs.lock().unwrap(), vec![KICKED_AT_MS; 4]);
        assert_eq!(*proxy.seen.lock().unwrap(), vec![kicked(1)]);
    }

    /// 六次尝试都失败（被拒绝，或一直不应答）：记为最终失败，不再重试。
    #[tokio::test]
    async fn a_revocation_that_keeps_failing_is_given_up_after_six_attempts() {
        for proxy in [Proxy::failing(usize::MAX), Proxy::silent()] {
            let (queue, dispatcher) = revocation_channel(proxy.clone(), config());
            let stats = dispatcher.stats();
            queue.submit(kicked(1));

            settle_all(dispatcher).await;

            assert_eq!(proxy.calls.load(Ordering::SeqCst), 6);
            assert_eq!(
                (stats.revoked(), stats.retried(), stats.failed()),
                (0, 5, 1)
            );
            assert_eq!(stats.pending(), 0);
        }
    }

    /// 提交不等待：队列满了或派发任务已退出，请求被丢弃并计数。
    #[tokio::test]
    async fn a_full_or_closed_queue_drops_instead_of_waiting() {
        let (queue, dispatcher) = revocation_channel(
            Proxy::failing(0),
            RevocationDispatchConfig {
                queue_capacity: 2,
                ..config()
            },
        );
        queue.submit(kicked(1));
        queue.submit(kicked(2));
        queue.submit(kicked(3));
        assert_eq!((queue.stats().queued(), queue.stats().dropped()), (2, 1));

        drop(dispatcher);
        queue.submit(kicked(4));
        assert_eq!((queue.stats().queued(), queue.stats().dropped()), (2, 2));
    }

    /// 停机时还在等重试的请求到宽限就放弃，派发不会一直挂着。
    #[tokio::test]
    async fn shutdown_gives_up_on_revocations_still_waiting_to_retry() {
        let proxy = Proxy::failing(usize::MAX);
        let (queue, dispatcher) = revocation_channel(
            proxy,
            RevocationDispatchConfig {
                retry_delays: vec![Duration::from_secs(60); 5],
                shutdown_grace: Duration::from_millis(50),
                ..config()
            },
        );
        let stats = dispatcher.stats();
        queue.submit(kicked(1));

        settle_all(dispatcher).await;

        assert_eq!((stats.revoked(), stats.failed()), (0, 0));
        assert_eq!(stats.pending(), 1);
    }
}
