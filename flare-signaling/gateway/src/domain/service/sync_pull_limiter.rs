//! Sync 拉取限流：tenant + user 双令牌桶。
//!
//! 租户阈值先取控制面投影的 `quota.core.sync_pull_qps`（调用方每次传入；`0` = 未投影 / 不限），
//! 再回落全局配置。投影配额是硬上限：速率与突发都取该值。两个作用域各自独立：
//! 某一作用域配置为 0 只关闭该作用域，不影响另一个。

use std::collections::HashMap;
use std::time::Instant;

use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SyncPullRateLimitConfig {
    pub enabled: bool,
    pub user_requests_per_second: u32,
    pub user_burst: u32,
    pub tenant_requests_per_second: u32,
    pub tenant_burst: u32,
}

impl Default for SyncPullRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            user_requests_per_second: 20,
            user_burst: 60,
            tenant_requests_per_second: 2_000,
            tenant_burst: 5_000,
        }
    }
}

pub struct SyncPullLimiter {
    config: SyncPullRateLimitConfig,
    state: Mutex<LimiterState>,
}

#[derive(Default)]
struct LimiterState {
    users: HashMap<String, Bucket>,
    tenants: HashMap<String, Bucket>,
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn new(burst: u32, now: Instant) -> Self {
        Self {
            tokens: burst as f64,
            last_refill: now,
        }
    }

    fn refill(&mut self, rate_per_second: u32, burst: u32, now: Instant) {
        if rate_per_second == 0 || burst == 0 {
            self.tokens = burst as f64;
            self.last_refill = now;
            return;
        }
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        // `min(burst)` 也把配额收紧后的旧桶立刻夹到新上限。
        self.tokens = (self.tokens + elapsed * rate_per_second as f64).min(burst as f64);
        self.last_refill = now;
    }

    fn can_take(&self) -> bool {
        self.tokens >= 1.0
    }

    fn take(&mut self) {
        self.tokens -= 1.0;
    }
}

impl SyncPullLimiter {
    pub fn new(config: SyncPullRateLimitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(LimiterState::default()),
        }
    }

    /// 租户作用域的有效 (rate, burst)：投影配额优先，`0` 回落全局。
    fn tenant_limits(&self, tenant_sync_pull_qps: u32) -> (u32, u32) {
        if tenant_sync_pull_qps > 0 {
            (tenant_sync_pull_qps, tenant_sync_pull_qps)
        } else {
            (
                self.config.tenant_requests_per_second,
                self.config.tenant_burst,
            )
        }
    }

    /// `tenant_sync_pull_qps`：该租户投影里的 `quota.core.sync_pull_qps`；`0` = 用全局配置。
    pub async fn try_acquire(
        &self,
        tenant_id: &str,
        user_id: &str,
        tenant_sync_pull_qps: u32,
    ) -> bool {
        if !self.config.enabled {
            return true;
        }
        let (tenant_rate, tenant_burst) = self.tenant_limits(tenant_sync_pull_qps);
        let user_limited = self.config.user_requests_per_second > 0 && self.config.user_burst > 0;
        let tenant_limited = tenant_rate > 0 && tenant_burst > 0;
        if !user_limited && !tenant_limited {
            return true;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;

        let tenant_can_take = if tenant_limited {
            let tenant_bucket = state
                .tenants
                .entry(tenant_id.to_string())
                .or_insert_with(|| Bucket::new(tenant_burst, now));
            tenant_bucket.refill(tenant_rate, tenant_burst, now);
            tenant_bucket.can_take()
        } else {
            true
        };

        let user_key = format!("{tenant_id}\x1f{user_id}");
        let user_can_take = if user_limited {
            let user_bucket = state
                .users
                .entry(user_key.clone())
                .or_insert_with(|| Bucket::new(self.config.user_burst, now));
            user_bucket.refill(
                self.config.user_requests_per_second,
                self.config.user_burst,
                now,
            );
            user_bucket.can_take()
        } else {
            true
        };

        if !tenant_can_take || !user_can_take {
            return false;
        }
        if tenant_limited && let Some(tenant_bucket) = state.tenants.get_mut(tenant_id) {
            tenant_bucket.take();
        }
        if user_limited && let Some(user_bucket) = state.users.get_mut(&user_key) {
            user_bucket.take();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn denies_after_user_burst_is_exhausted() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig {
            enabled: true,
            user_requests_per_second: 1,
            user_burst: 2,
            tenant_requests_per_second: 100,
            tenant_burst: 100,
        });

        assert!(limiter.try_acquire("t1", "u1", 0).await);
        assert!(limiter.try_acquire("t1", "u1", 0).await);
        assert!(!limiter.try_acquire("t1", "u1", 0).await);
        assert!(limiter.try_acquire("t1", "u2", 0).await);
    }

    #[tokio::test]
    async fn denies_after_tenant_burst_is_exhausted() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig {
            enabled: true,
            user_requests_per_second: 100,
            user_burst: 100,
            tenant_requests_per_second: 1,
            tenant_burst: 2,
        });

        assert!(limiter.try_acquire("t1", "u1", 0).await);
        assert!(limiter.try_acquire("t1", "u2", 0).await);
        assert!(!limiter.try_acquire("t1", "u3", 0).await);
    }

    #[tokio::test]
    async fn disabled_limiter_allows_requests() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig {
            enabled: false,
            user_requests_per_second: 1,
            user_burst: 1,
            tenant_requests_per_second: 1,
            tenant_burst: 1,
        });

        assert!(limiter.try_acquire("t1", "u1", 0).await);
        assert!(limiter.try_acquire("t1", "u1", 0).await);
    }

    #[tokio::test]
    async fn tenant_quota_overrides_global_tenant_threshold() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig {
            enabled: true,
            user_requests_per_second: 100,
            user_burst: 100,
            tenant_requests_per_second: 1_000,
            tenant_burst: 1_000,
        });

        // 投影配额 2/s → 突发 2，第三次拒绝；不同租户互不影响。
        assert!(limiter.try_acquire("t1", "u1", 2).await);
        assert!(limiter.try_acquire("t1", "u2", 2).await);
        assert!(!limiter.try_acquire("t1", "u3", 2).await);
        assert!(limiter.try_acquire("t2", "u1", 0).await);
    }

    #[tokio::test]
    async fn tenant_quota_applies_even_when_global_tenant_limit_is_off() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig {
            enabled: true,
            user_requests_per_second: 100,
            user_burst: 100,
            tenant_requests_per_second: 0,
            tenant_burst: 0,
        });

        assert!(limiter.try_acquire("t1", "u1", 1).await);
        assert!(!limiter.try_acquire("t1", "u2", 1).await);
        // 无配额 + 全局关闭 = 租户维度不限。
        assert!(limiter.try_acquire("t2", "u1", 0).await);
        assert!(limiter.try_acquire("t2", "u2", 0).await);
    }

    #[tokio::test]
    async fn shrinking_quota_clamps_existing_bucket() {
        let limiter = SyncPullLimiter::new(SyncPullRateLimitConfig::default());
        assert!(limiter.try_acquire("t1", "u1", 100).await);
        // 配额从 100 收紧到 1：旧桶立刻夹到 1，且刚才已消费 → 需要等补充。
        assert!(limiter.try_acquire("t1", "u2", 1).await);
        assert!(!limiter.try_acquire("t1", "u3", 1).await);
    }
}
