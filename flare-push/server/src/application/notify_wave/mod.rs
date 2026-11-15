//! 会话波次（应用服务）：读出会话的波次状态，按领域规则（[`WaveState`]）算出这条消息是现在枚举
//! 还是累加进正在进行的间隔，比较版本后写回。
//!
//! - 冲突就重读重算，至多 [`CAS_ATTEMPTS`] 次；推送 Redis 不可用时改用进程内的状态（与闸门共用
//!   同一个进程内存储，到期条目由同一个到期循环领取）。都用不上时放行：这一条当场枚举，只推它
//!   自己（[`WaveBatch::single`]）。
//! - 波次只约束服务端枚举接收者的成本；设备上响不响、合不合并仍由闸门决定。

use std::sync::Arc;

use flare_im_service_kit::metrics::PushServerMetrics;
use flare_server_core::error::FlareError;

use crate::domain::model::{
    Arrived, GateStoreOrigin, MentionAll, NotifyPolicy, WaveArrival, WaveBatch, WaveDue,
    WaveDueOutcome, WaveKey, wave_state_ttl_ms,
};
use crate::domain::repository::{WaveCommit, WaveMessageWrite, WaveSnapshot, WaveStore};

/// 比较后写入的尝试次数。冲突只在同一个会话的消息被几个实例同时处理时出现。
pub const CAS_ATTEMPTS: usize = 3;

/// 波次里存的最新一条消息正文的上限。更大的不存，到期枚举时用通用文案。
pub const MAX_STORED_MESSAGE_BYTES: usize = 16 * 1024;

/// 一次枚举要推的内容：批次，与它展示的那条消息的正文（协议 `Message` 的编码；没存或太大时为空，
/// 用通用文案）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaveRelease {
    pub batch: WaveBatch,
    pub message: Option<Vec<u8>>,
}

/// 一条消息进波次之后。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaveStep {
    /// 累加进了正在进行的间隔，到期时一起枚举。
    Held,
    /// 现在枚举。
    Flush(WaveRelease),
}

impl WaveStep {
    /// 这次枚举里 @所有人 的范围；累加时为 `No`。
    pub fn mention_all(&self) -> MentionAll {
        match self {
            Self::Held => MentionAll::No,
            Self::Flush(release) => release.batch.mention_all,
        }
    }
}

/// 一次读改写的结局。
enum Updated<R> {
    Applied(R),
    /// 比较后写入一直冲突，重试用完。
    Contended,
}

/// 指标里的事件：当场枚举、间隔里开始累加、累加。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrivalEvent {
    Immediate,
    Started,
    Held,
}

pub struct NotifyWaves {
    store: Arc<dyn WaveStore>,
    fallback: Arc<dyn WaveStore>,
    policy: Arc<NotifyPolicy>,
    metrics: Option<Arc<PushServerMetrics>>,
}

impl NotifyWaves {
    /// `fallback` 必须与闸门的进程内存储是同一个实例：登记在那里的波次到期条目由闸门的到期领取
    /// 一起领出来。
    pub fn new(
        store: Arc<dyn WaveStore>,
        fallback: Arc<dyn WaveStore>,
        policy: Arc<NotifyPolicy>,
    ) -> Self {
        Self {
            store,
            fallback,
            policy,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<PushServerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn metric(&self, record: impl FnOnce(&PushServerMetrics)) {
        if let Some(metrics) = &self.metrics {
            record(metrics);
        }
    }

    fn store_of(&self, origin: GateStoreOrigin) -> &dyn WaveStore {
        match origin {
            GateStoreOrigin::Primary => self.store.as_ref(),
            GateStoreOrigin::Fallback => self.fallback.as_ref(),
        }
    }

    /// 这个租户在超过成员上限的群里，两次全员 @所有人 波次之间的最短间隔（毫秒）。
    pub fn all_mention_interval_ms(&self, tenant_id: &str) -> i64 {
        self.policy.all_mention_interval_ms(tenant_id)
    }

    fn ttl_ms(&self, key: &WaveKey) -> i64 {
        wave_state_ttl_ms(self.all_mention_interval_ms(&key.tenant_id))
    }

    /// 在一个存储上做一次读改写：读、算、比较后写，冲突就重来。
    async fn update<R, P>(
        &self,
        store: &dyn WaveStore,
        key: &WaveKey,
        plan: &P,
    ) -> Result<Updated<R>, FlareError>
    where
        P: Fn(WaveSnapshot) -> (Option<WaveCommit>, R) + Sync,
        R: Send,
    {
        for _ in 0..CAS_ATTEMPTS {
            let snapshot = store.load_wave(key).await?;
            let (commit, result) = plan(snapshot);
            let Some(commit) = commit else {
                return Ok(Updated::Applied(result));
            };
            if store.commit_wave(&commit).await? {
                return Ok(Updated::Applied(result));
            }
        }
        Ok(Updated::Contended)
    }

    /// 先在推送 Redis 上做；它不可用就改在进程内的状态上做。
    async fn update_with_fallback<R, P>(
        &self,
        key: &WaveKey,
        plan: &P,
    ) -> Result<Updated<R>, FlareError>
    where
        P: Fn(WaveSnapshot) -> (Option<WaveCommit>, R) + Sync,
        R: Send,
    {
        match self.update(self.store.as_ref(), key, plan).await {
            Ok(updated) => Ok(updated),
            Err(error) => {
                tracing::warn!(error = %error, "wave store unavailable; using the in-process waves");
                self.update(self.fallback.as_ref(), key, plan).await
            }
        }
    }

    /// 一条消息进波次。`message` 是这条消息的正文（协议 `Message` 的编码），累加时存下来，
    /// 到期枚举时展示。
    pub async fn arrive(
        &self,
        now_ms: i64,
        key: &WaveKey,
        arrival: &WaveArrival<'_>,
        message: &[u8],
    ) -> WaveStep {
        let ttl_ms = self.ttl_ms(key);
        let storable = !message.is_empty() && message.len() <= MAX_STORED_MESSAGE_BYTES;
        let plan = |snapshot: WaveSnapshot| {
            let version = snapshot.version;
            let stored = snapshot.message;
            let mut state = snapshot.state;
            let (write, due_ms, step, event) = match state.arrive(now_ms, arrival) {
                Arrived::Flush {
                    batch,
                    newest_is_arrival,
                } => {
                    let shown = if newest_is_arrival {
                        (!message.is_empty()).then(|| message.to_vec())
                    } else {
                        stored
                    };
                    (
                        WaveMessageWrite::Clear,
                        None,
                        WaveStep::Flush(WaveRelease {
                            batch,
                            message: shown,
                        }),
                        ArrivalEvent::Immediate,
                    )
                }
                Arrived::Held {
                    due_ms,
                    first,
                    newest_is_arrival,
                } => {
                    let write = if !newest_is_arrival {
                        WaveMessageWrite::Keep
                    } else if storable {
                        WaveMessageWrite::Set(message.to_vec())
                    } else {
                        WaveMessageWrite::Clear
                    };
                    let event = if first {
                        ArrivalEvent::Started
                    } else {
                        ArrivalEvent::Held
                    };
                    (write, Some(due_ms), WaveStep::Held, event)
                }
            };
            let commit = WaveCommit {
                key: key.clone(),
                expect: version,
                state,
                message: write,
                ttl_ms,
                due_ms,
            };
            (Some(commit), (step, event))
        };
        let (step, event) = match self.update_with_fallback(key, &plan).await {
            Ok(Updated::Applied(result)) => result,
            Ok(Updated::Contended) => return self.fail_open(arrival, message, None),
            Err(error) => return self.fail_open(arrival, message, Some(error)),
        };
        self.metric(|metrics| match event {
            ArrivalEvent::Immediate => metrics.record_wave("immediate"),
            ArrivalEvent::Started => {
                metrics.record_wave("started");
                metrics.record_wave("held");
            }
            ArrivalEvent::Held => metrics.record_wave("held"),
        });
        step
    }

    /// 状态用不上：这一条当场枚举，只推它自己。
    fn fail_open(
        &self,
        arrival: &WaveArrival<'_>,
        message: &[u8],
        error: Option<FlareError>,
    ) -> WaveStep {
        match error {
            Some(error) => {
                tracing::warn!(error = %error, "wave state unusable; enumerating this message now");
            }
            None => tracing::debug!("wave state contended; enumerating this message now"),
        }
        self.metric(|metrics| metrics.record_wave("fail_open"));
        WaveStep::Flush(WaveRelease {
            batch: WaveBatch::single(arrival),
            message: (!message.is_empty()).then(|| message.to_vec()),
        })
    }

    /// 波次到期：有累加就取出这一批并开始下一个间隔。在登记它的那个存储上处理；冲突重试用完说明
    /// 同时有别的写入（新来的消息会重新登记到期，或已经当场枚举），存储不可用时放弃这一批
    /// （设备打开应用时同步可见）。
    pub async fn due(&self, now_ms: i64, due: &WaveDue) -> Option<WaveRelease> {
        let ttl_ms = self.ttl_ms(&due.key);
        let plan = |snapshot: WaveSnapshot| {
            let version = snapshot.version;
            let stored = snapshot.message;
            let mut state = snapshot.state;
            match state.due(now_ms) {
                WaveDueOutcome::Nothing => (None, None),
                WaveDueOutcome::Reschedule(due_ms) => (
                    Some(WaveCommit {
                        key: due.key.clone(),
                        expect: version,
                        state,
                        message: WaveMessageWrite::Keep,
                        ttl_ms,
                        due_ms: Some(due_ms),
                    }),
                    None,
                ),
                WaveDueOutcome::Flush(batch) => (
                    Some(WaveCommit {
                        key: due.key.clone(),
                        expect: version,
                        state,
                        message: WaveMessageWrite::Clear,
                        ttl_ms,
                        due_ms: None,
                    }),
                    Some(WaveRelease {
                        batch,
                        message: stored,
                    }),
                ),
            }
        };
        match self
            .update(self.store_of(due.origin), &due.key, &plan)
            .await
        {
            Ok(Updated::Applied(Some(release))) => {
                self.metric(|metrics| metrics.record_wave("fired"));
                Some(release)
            }
            Ok(Updated::Applied(None)) => None,
            Ok(Updated::Contended) => {
                tracing::debug!("wave due entry contended; a concurrent message took it over");
                None
            }
            Err(error) => {
                tracing::warn!(error = %error, "wave due entry dropped: wave store unavailable");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests;
