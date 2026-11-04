//! 何时回查发送幂等账本（[`crate::domain::repository::IdempotencyLedger`]）。
//!
//! 只有 Redis 去重窗口兜不住的重发才值得一次 PG 点查。判据是客户端创建时间 `Message.created_at`：客户端
//! 组装消息时写入，本地发件箱重发时原样带回；摄入要到幂等判定之后（[`super::MessageSubmission`] 准备阶段）
//! 才用服务端时间覆盖它。首发的 Redis 结果在「首发时刻 + 结果 TTL」过期，而首发不早于创建，所以 Redis 未命中
//! 的真重发，创建时间必然早于「现在 − 结果 TTL」。新消息的创建时间就在眼前，永远走不到 PG。

use std::time::Duration;

/// 客户端时钟余量：客户端时钟比服务端快不超过这么多时，过窗重发仍会回查。
pub const CLIENT_CLOCK_MARGIN: Duration = Duration::from_secs(5 * 60);

/// 回查账本的原因，也是指标与日志里的标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookbackReason {
    /// 客户端创建时间早于 Redis 结果窗口：首发结果可能已从 Redis 过期。
    StaleClientTime,
    // 扩展点：租户 dataEpoch 在 24 小时内变化过（容灾切换后 Redis 不是同一份）时，不论创建时间都要回查。
    // dataEpoch 落地后在这里加一个变体，并在 `LedgerLookbackPolicy::reason` 里先判这一支。
}

impl LookbackReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StaleClientTime => "stale_client_time",
        }
    }
}

/// 回查策略：由 Redis 结果 TTL 推出「旧发送」的界线。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerLookbackPolicy {
    redis_result_ttl: Duration,
}

impl LedgerLookbackPolicy {
    pub fn new(redis_result_ttl: Duration) -> Self {
        Self { redis_result_ttl }
    }

    /// 这次发送是否回查账本；`None` 就不访问 PG。
    ///
    /// 客户端创建时间早于 `now − (结果 TTL − 时钟余量)` 才回查。创建时间为 0 或负（调用方没给）不回查：
    /// 没有证据说明它是旧发送。HTTP 发送入口把创建时间填成网关收到请求的时刻，因而也不回查。
    pub fn reason(&self, client_created_at_ms: i64, now_ms: i64) -> Option<LookbackReason> {
        if client_created_at_ms <= 0 {
            return None;
        }
        let window_ms = i64::try_from(
            self.redis_result_ttl
                .saturating_sub(CLIENT_CLOCK_MARGIN)
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        (client_created_at_ms < now_ms.saturating_sub(window_ms))
            .then_some(LookbackReason::StaleClientTime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000_000;
    const MINUTE: i64 = 60_000;

    #[test]
    fn only_sends_older_than_the_redis_window_minus_the_margin_look_back() {
        let policy = LedgerLookbackPolicy::new(Duration::from_secs(3_600));
        // 结果 TTL 1 小时、余量 5 分钟：创建早于 55 分钟前才回查。
        assert_eq!(
            policy.reason(NOW - 120 * MINUTE, NOW),
            Some(LookbackReason::StaleClientTime)
        );
        assert_eq!(
            policy.reason(NOW - 55 * MINUTE - 1, NOW),
            Some(LookbackReason::StaleClientTime)
        );
        assert_eq!(policy.reason(NOW - 55 * MINUTE, NOW), None);
        assert_eq!(policy.reason(NOW - MINUTE, NOW), None);
        assert_eq!(policy.reason(NOW, NOW), None);
        // 客户端时钟偏快：不回查。
        assert_eq!(policy.reason(NOW + 10 * MINUTE, NOW), None);
    }

    #[test]
    fn a_send_without_a_client_time_never_looks_back() {
        let policy = LedgerLookbackPolicy::new(Duration::from_secs(3_600));
        assert_eq!(policy.reason(0, NOW), None);
        assert_eq!(policy.reason(-1, NOW), None);
    }
}
