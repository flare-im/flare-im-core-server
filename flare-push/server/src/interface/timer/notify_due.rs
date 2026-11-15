//! 到期循环：定时领取闸门到期队列里的条目，交给路由补发尾部刷新、推迟的提示，或枚举群会话
//! 波次里攒下的消息。
//!
//! 每个实例都轮询全部分片；领取在存储里原子完成（领取即移除），同一条目只会被一个实例处理。
//! 领取后处理失败（下游不可用、进程退出）这一条就丢了：闸门与波次是软状态，通知暂时显示旧的
//! 内容或计数、或这一批没有提示，打开应用时同步纠正。

use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::Ctx;
use flare_server_core::error::FlareError;
use futures::stream::{self, StreamExt};
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::application::{NotifyGate, PushRouterHandler};

/// 为到期条目所属的租户签一份服务身份上下文：领取出的条目不是消息记录，没有上游的上下文可以
/// 重新背书，读会话服务与消息时要以推送服务自己的身份、限定在这个租户内。
pub type DueContextFactory = Arc<dyn Fn(&str) -> Result<Ctx, FlareError> + Send + Sync>;

/// 一轮里同时处理多少条。
const DUE_CONCURRENCY: usize = 32;

pub struct NotifyDueLoop {
    router: Arc<PushRouterHandler>,
    gate: Arc<NotifyGate>,
    contexts: DueContextFactory,
    interval: Duration,
    batch: usize,
}

impl NotifyDueLoop {
    pub fn new(
        router: Arc<PushRouterHandler>,
        gate: Arc<NotifyGate>,
        contexts: DueContextFactory,
        interval: Duration,
        batch: usize,
    ) -> Self {
        Self {
            router,
            gate,
            contexts,
            interval: interval.max(Duration::from_millis(10)),
            batch: batch.max(1),
        }
    }

    /// 一直跑到收到关闭信号。
    pub async fn run(self, mut shutdown: oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = ticker.tick() => self.tick().await,
            }
        }
        tracing::info!("notify due loop stopped");
    }

    /// 领取一轮到期条目并处理。
    pub async fn tick(&self) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let entries = self.gate.claim_due(now_ms, self.batch).await;
        if entries.is_empty() {
            return;
        }
        stream::iter(entries)
            .for_each_concurrent(DUE_CONCURRENCY, |entry| async move {
                let tenant_id = entry.tenant_id().to_string();
                let ctx = match (self.contexts)(&tenant_id) {
                    Ok(ctx) => ctx,
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            tenant_id = %tenant_id,
                            "due entry dropped: no service context for its tenant"
                        );
                        return;
                    }
                };
                if let Err(error) = self.router.handle_notify_due(&ctx, entry).await {
                    tracing::warn!(error = %error, tenant_id = %tenant_id, "due entry not delivered");
                }
            })
            .await;
    }
}
