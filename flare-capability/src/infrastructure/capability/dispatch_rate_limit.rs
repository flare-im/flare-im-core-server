//! 按自然分钟桶的 `Dispatch` 限流（租户 + 用户维度，防滥用）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

/// 每分钟每个 key 最多 `max` 次成功 `check_and_record`。
pub struct DispatchRateLimiter {
    max: u32,
    /// key -> (minute_bucket, count)
    state: DashMap<String, (u64, u32)>,
    denied_total: AtomicU64,
}

impl DispatchRateLimiter {
    pub fn new(max_per_minute: u32) -> Self {
        Self {
            max: max_per_minute,
            state: DashMap::new(),
            denied_total: AtomicU64::new(0),
        }
    }

    pub fn denied_total(&self) -> u64 {
        self.denied_total.load(Ordering::Relaxed)
    }

    /// 超过配额返回 `false`。
    pub fn check_and_record(&self, tenant_id: &str, user_id: &str) -> bool {
        let bucket = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60)
            .unwrap_or(0);
        let key = format!("{tenant_id}\x1f{user_id}");
        let mut entry = self.state.entry(key).or_insert((bucket, 0));
        if entry.0 != bucket {
            *entry = (bucket, 0);
        }
        if entry.1 >= self.max {
            self.denied_total.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        entry.1 += 1;
        true
    }
}
