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

    pub async fn try_acquire(&self, tenant_id: &str, user_id: &str) -> bool {
        if !self.config.enabled {
            return true;
        }
        if self.config.user_requests_per_second == 0
            || self.config.user_burst == 0
            || self.config.tenant_requests_per_second == 0
            || self.config.tenant_burst == 0
        {
            return true;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;
        let tenant_can_take = {
            let tenant_bucket = state
                .tenants
                .entry(tenant_id.to_string())
                .or_insert_with(|| Bucket::new(self.config.tenant_burst, now));
            tenant_bucket.refill(
                self.config.tenant_requests_per_second,
                self.config.tenant_burst,
                now,
            );
            tenant_bucket.can_take()
        };

        let user_key = format!("{tenant_id}\x1f{user_id}");
        let user_can_take = {
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
        };

        if !tenant_can_take || !user_can_take {
            return false;
        }
        let Some(tenant_bucket) = state.tenants.get_mut(tenant_id) else {
            return true;
        };
        tenant_bucket.take();
        let Some(user_bucket) = state.users.get_mut(&user_key) else {
            return true;
        };
        user_bucket.take();
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

        assert!(limiter.try_acquire("t1", "u1").await);
        assert!(limiter.try_acquire("t1", "u1").await);
        assert!(!limiter.try_acquire("t1", "u1").await);
        assert!(limiter.try_acquire("t1", "u2").await);
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

        assert!(limiter.try_acquire("t1", "u1").await);
        assert!(limiter.try_acquire("t1", "u2").await);
        assert!(!limiter.try_acquire("t1", "u3").await);
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

        assert!(limiter.try_acquire("t1", "u1").await);
        assert!(limiter.try_acquire("t1", "u1").await);
    }
}
