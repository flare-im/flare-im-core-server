//! 网关失联交接的上报派发。
//!
//! 清扫器每移除一条失联网关上的会话，就把那台设备的交接放进这里的有界队列（[`HandoffQueue`]）；
//! 派发任务（[`HandoffDispatcher`]）从队列取出，经 [`DeviceHandoffReporter`] 送到推送通道。两边
//! 只通过队列相连：推送代理慢或不可达时清扫照常进行，入队不等待，队列满了就丢弃并计数；派发的
//! 在途数量与单次耗时都有上限。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

use crate::domain::device_handoff::{DeviceHandoffReporter, GatewayLostHandoff};

/// 派发的上限。
#[derive(Debug, Clone)]
pub struct HandoffDispatchConfig {
    /// 队列容量。一台网关可能有十万级连接，清扫比上报快时差额先攒在队列里；推送代理长时间不可达
    /// 时队列会满，之后的交接直接丢弃，内存占用不随清扫量增长。
    pub queue_capacity: usize,
    /// 同时在途的上报数。
    pub concurrency: usize,
    /// 单次上报的时限，覆盖解析通道、建连与调用本身。
    pub call_timeout: Duration,
    /// 停机时给已入队交接的宽限，须小于运行时等待任务退出的时间。
    pub shutdown_grace: Duration,
}

impl Default for HandoffDispatchConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 65_536,
            concurrency: 64,
            call_timeout: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(3),
        }
    }
}

/// 交接上报的累计计数（进程内）。
#[derive(Debug, Default)]
pub struct HandoffStats {
    reported: AtomicU64,
    failed: AtomicU64,
    dropped: AtomicU64,
}

impl HandoffStats {
    /// 推送侧已收下的交接数。
    pub fn reported(&self) -> u64 {
        self.reported.load(Ordering::Relaxed)
    }

    /// 上报失败（不可达、被拒绝、超时）的交接数。
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// 没能入队（队列已满或派发任务已退出）而丢弃的交接数。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// 队列的写入端，交给清扫器。只有这一份：清扫器退出后队列关闭，派发任务送完剩余的就结束。
pub struct HandoffQueue {
    sender: mpsc::Sender<GatewayLostHandoff>,
    stats: Arc<HandoffStats>,
}

impl HandoffQueue {
    /// 入队一次交接，不等待。队列已满或派发任务已退出时丢弃并计数，返回 `false`。
    pub fn offer(&self, handoff: GatewayLostHandoff) -> bool {
        let (handoff, reason) = match self.sender.try_send(handoff) {
            Ok(()) => return true,
            Err(TrySendError::Full(handoff)) => (handoff, "queue full"),
            Err(TrySendError::Closed(handoff)) => (handoff, "dispatcher stopped"),
        };
        let dropped = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        // 队列满时一台网关的清扫会连续丢弃上万次：计数照常累加，日志按 2 的幂次记。
        if dropped.is_power_of_two() {
            warn!(
                tenant_id = %handoff.tenant_id,
                gateway_id = %handoff.gateway_id,
                reason,
                dropped_total = dropped,
                "device handoff dropped before reporting"
            );
        }
        false
    }

    pub fn stats(&self) -> &HandoffStats {
        &self.stats
    }
}

/// 队列的读取端：把交接送到推送通道。
pub struct HandoffDispatcher<H: DeviceHandoffReporter> {
    reporter: Arc<H>,
    receiver: mpsc::Receiver<GatewayLostHandoff>,
    stats: Arc<HandoffStats>,
    config: HandoffDispatchConfig,
}

/// 建一对队列与派发器。
pub fn handoff_channel<H: DeviceHandoffReporter>(
    reporter: Arc<H>,
    config: HandoffDispatchConfig,
) -> (HandoffQueue, HandoffDispatcher<H>) {
    let (sender, receiver) = mpsc::channel(config.queue_capacity.max(1));
    let stats = Arc::new(HandoffStats::default());
    (
        HandoffQueue {
            sender,
            stats: stats.clone(),
        },
        HandoffDispatcher {
            reporter,
            receiver,
            stats,
            config,
        },
    )
}

impl<H: DeviceHandoffReporter> HandoffDispatcher<H> {
    pub fn stats(&self) -> Arc<HandoffStats> {
        self.stats.clone()
    }

    /// 派发直到队列关闭（清扫器已退出）。收到 `shutdown` 后不立即停：已入队的交接对应的会话已经
    /// 被移除，不送出去这些设备就收不到补提示，所以再给一个短宽限，到时仍没送完的才放弃。
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let Self {
            reporter,
            receiver,
            stats,
            config,
        } = self;
        let reporter: &H = &reporter;
        let stats: &HandoffStats = &stats;
        let call_timeout = config.call_timeout;
        let deliveries = ReceiverStream::new(receiver).for_each_concurrent(
            config.concurrency.max(1),
            |handoff| async move {
                deliver(reporter, stats, call_timeout, handoff).await;
            },
        );
        tokio::pin!(deliveries);
        tokio::pin!(shutdown);
        tokio::select! {
            _ = &mut deliveries => return,
            _ = &mut shutdown => {}
        }
        if tokio::time::timeout(config.shutdown_grace, &mut deliveries)
            .await
            .is_err()
        {
            warn!(
                reported_total = stats.reported(),
                failed_total = stats.failed(),
                dropped_total = stats.dropped(),
                "device handoff reporting stopped with handoffs still queued"
            );
        }
    }
}

/// 上报一次交接：限时，结果只进计数与日志，不向清扫器回传。
async fn deliver<H: DeviceHandoffReporter>(
    reporter: &H,
    stats: &HandoffStats,
    call_timeout: Duration,
    handoff: GatewayLostHandoff,
) {
    let failure = match tokio::time::timeout(call_timeout, reporter.report(&handoff)).await {
        Ok(Ok(())) => {
            stats.reported.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Ok(Err(error)) => error.to_string(),
        Err(_) => format!("no answer within {}ms", call_timeout.as_millis()),
    };
    let failed = stats.failed.fetch_add(1, Ordering::Relaxed) + 1;
    // 推送代理不可用时一台网关的清扫会连续失败上万次：计数照常累加，日志按 2 的幂次记。
    if failed.is_power_of_two() {
        warn!(
            tenant_id = %handoff.tenant_id,
            gateway_id = %handoff.gateway_id,
            error = %failure,
            failed_total = failed,
            reported_total = stats.reported(),
            "device handoff report failed"
        );
    } else {
        debug!(
            tenant_id = %handoff.tenant_id,
            gateway_id = %handoff.gateway_id,
            user_id = %handoff.user_id,
            device_id = %handoff.device_id,
            error = %failure,
            "device handoff report failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use flare_server_core::error::{FlareError, Result};

    use super::*;

    #[derive(Clone, Copy)]
    enum Answer {
        Accept,
        Reject,
        Hang,
    }

    /// 推送通道的替身：记下收到的交接与同时在途的峰值。
    struct Recording {
        answer: Answer,
        seen: Mutex<Vec<GatewayLostHandoff>>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl Recording {
        fn answering(answer: Answer) -> Arc<Self> {
            Arc::new(Self {
                answer,
                seen: Mutex::new(Vec::new()),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            })
        }
    }

    impl DeviceHandoffReporter for Recording {
        async fn report(&self, handoff: &GatewayLostHandoff) -> Result<()> {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(in_flight, Ordering::SeqCst);
            let outcome = match self.answer {
                Answer::Accept => {
                    // 让出执行，别的上报才有机会同时在途。
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    self.seen.lock().unwrap().push(handoff.clone());
                    Ok(())
                }
                Answer::Reject => Err(FlareError::system("push proxy rejected the handoff")),
                Answer::Hang => std::future::pending().await,
            };
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            outcome
        }
    }

    fn handoff(session: usize) -> GatewayLostHandoff {
        GatewayLostHandoff {
            tenant_id: "tenant-a".to_string(),
            user_id: format!("user-{session}"),
            device_id: "phone".to_string(),
            session_id: format!("session-{session}"),
            gateway_id: "gw-dead".to_string(),
            since_ms: 1_000,
            occurred_at_ms: 2_000,
        }
    }

    fn config(concurrency: usize, call_timeout: Duration) -> HandoffDispatchConfig {
        HandoffDispatchConfig {
            queue_capacity: 1_024,
            concurrency,
            call_timeout,
            shutdown_grace: Duration::from_millis(200),
        }
    }

    /// 入队的交接全部送到推送通道，同时在途的不超过并发上限；队列关闭后派发结束。
    #[tokio::test]
    async fn queued_handoffs_are_reported_within_the_concurrency_limit() {
        let reporter = Recording::answering(Answer::Accept);
        let (queue, dispatcher) =
            handoff_channel(reporter.clone(), config(4, Duration::from_secs(5)));
        let stats = dispatcher.stats();
        for session in 0..40 {
            assert!(queue.offer(handoff(session)));
        }
        drop(queue);

        dispatcher.run(std::future::pending()).await;

        assert_eq!(stats.reported(), 40);
        assert_eq!((stats.failed(), stats.dropped()), (0, 0));
        let mut sessions: Vec<String> = reporter
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|handoff| handoff.session_id.clone())
            .collect();
        sessions.sort();
        sessions.dedup();
        assert_eq!(sessions.len(), 40);
        let peak = reporter.peak.load(Ordering::SeqCst);
        assert!((2..=4).contains(&peak), "peak in flight was {peak}");
    }

    /// 被拒绝与超时的上报只计数，不重试、不卡住后面的交接。
    #[tokio::test]
    async fn rejected_and_unanswered_reports_are_counted_as_failed() {
        for answer in [Answer::Reject, Answer::Hang] {
            let reporter = Recording::answering(answer);
            let (queue, dispatcher) =
                handoff_channel(reporter, config(8, Duration::from_millis(20)));
            let stats = dispatcher.stats();
            for session in 0..5 {
                assert!(queue.offer(handoff(session)));
            }
            drop(queue);

            tokio::time::timeout(
                Duration::from_secs(5),
                dispatcher.run(std::future::pending()),
            )
            .await
            .expect("unanswered reports must not hold the dispatcher");

            assert_eq!(stats.failed(), 5);
            assert_eq!(stats.reported(), 0);
        }
    }

    /// 入队不等待：队列满了或派发任务已退出，交接被丢弃并计数。
    #[tokio::test]
    async fn a_full_or_closed_queue_drops_instead_of_waiting() {
        let (queue, dispatcher) = handoff_channel(
            Recording::answering(Answer::Accept),
            HandoffDispatchConfig {
                queue_capacity: 2,
                ..config(1, Duration::from_secs(5))
            },
        );
        assert!(queue.offer(handoff(1)));
        assert!(queue.offer(handoff(2)));
        assert!(!queue.offer(handoff(3)));
        assert_eq!(queue.stats().dropped(), 1);

        drop(dispatcher);
        assert!(!queue.offer(handoff(4)));
        assert_eq!(queue.stats().dropped(), 2);
        assert_eq!(queue.stats().reported(), 0);
    }

    /// 停机后已入队的交接仍在宽限内送出；清扫器还没退出（队列没关）也不会一直等下去。
    #[tokio::test]
    async fn shutdown_drains_what_is_already_queued() {
        let reporter = Recording::answering(Answer::Accept);
        let (queue, dispatcher) =
            handoff_channel(reporter.clone(), config(4, Duration::from_secs(5)));
        let stats = dispatcher.stats();
        for session in 0..8 {
            assert!(queue.offer(handoff(session)));
        }

        tokio::time::timeout(Duration::from_secs(5), dispatcher.run(async {}))
            .await
            .expect("the dispatcher stops once the grace is over");

        assert_eq!(stats.reported(), 8);
        drop(queue);
    }
}
