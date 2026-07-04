//! Ingest-side send rate limiting.
//!
//! This is a per-process fixed-window guard for immediate backpressure at the
//! send boundary. The keys are tenant-scoped so the same sender/conversation id
//! in different tenants cannot interfere with each other.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use flare_proto::common::Message;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

const DEFAULT_WINDOW_MS: u64 = 1000;
const DEFAULT_MAX_TRACKED_KEYS: usize = 200_000;
const MIN_MAX_TRACKED_KEYS: usize = 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendRateLimitConfig {
    pub enabled: bool,
    /// Tenant-wide accepted send requests per window. `0` disables this scope.
    pub tenant_per_second: u32,
    /// Sender limit scoped as `(tenant, sender)`. `0` disables this scope.
    pub tenant_sender_per_second: u32,
    /// Conversation limit scoped as `(tenant, conversation)`. `0` disables this scope.
    pub tenant_conversation_per_second: u32,
    /// Fixed-window size in milliseconds.
    pub window_ms: u64,
    /// Maximum in-memory keys retained by this process.
    pub max_tracked_keys: usize,
}

impl SendRateLimitConfig {
    pub fn is_effective(&self) -> bool {
        self.enabled
            && (self.tenant_per_second > 0
                || self.tenant_sender_per_second > 0
                || self.tenant_conversation_per_second > 0)
    }

    fn normalized(mut self) -> Self {
        if self.window_ms == 0 {
            self.window_ms = DEFAULT_WINDOW_MS;
        }
        if self.max_tracked_keys == 0 {
            self.max_tracked_keys = DEFAULT_MAX_TRACKED_KEYS;
        }
        self.max_tracked_keys = self.max_tracked_keys.max(MIN_MAX_TRACKED_KEYS);
        self
    }
}

#[derive(Debug)]
pub struct SendRateLimiter {
    config: SendRateLimitConfig,
    state: Mutex<HashMap<String, WindowCounter>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WindowCounter {
    window_start_ms: u64,
    count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RateLimitScope {
    Tenant,
    TenantSender,
    TenantConversation,
}

impl RateLimitScope {
    fn label(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::TenantSender => "tenant_sender",
            Self::TenantConversation => "tenant_conversation",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RateLimitCheck {
    key: String,
    scope: RateLimitScope,
    limit: u32,
}

impl SendRateLimiter {
    pub fn new(config: SendRateLimitConfig) -> Self {
        Self {
            config: config.normalized(),
            state: Mutex::new(HashMap::new()),
        }
    }

    pub fn check(&self, tenant_id: &str, message: &Message) -> Result<()> {
        if !self.config.is_effective() {
            return Ok(());
        }
        self.check_at_millis(Self::now_millis(), tenant_id, message)
    }

    fn check_at_millis(&self, now_ms: u64, tenant_id: &str, message: &Message) -> Result<()> {
        let checks = self.build_checks(tenant_id, message);
        if checks.is_empty() {
            return Ok(());
        }

        let window_ms = self.config.window_ms.max(1);
        let window_start_ms = now_ms / window_ms * window_ms;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::trim_state(&mut state, now_ms, window_ms, self.config.max_tracked_keys);

        for check in &checks {
            let current = state
                .get(&check.key)
                .filter(|counter| counter.window_start_ms == window_start_ms)
                .map(|counter| counter.count)
                .unwrap_or(0);
            if current >= check.limit {
                return Err(rate_limit_error(check.scope, check.limit, window_ms));
            }
        }

        for check in checks {
            let counter = state.entry(check.key).or_insert(WindowCounter {
                window_start_ms,
                count: 0,
            });
            if counter.window_start_ms != window_start_ms {
                counter.window_start_ms = window_start_ms;
                counter.count = 0;
            }
            counter.count = counter.count.saturating_add(1);
        }

        Self::trim_state(&mut state, now_ms, window_ms, self.config.max_tracked_keys);
        Ok(())
    }

    fn build_checks(&self, tenant_id: &str, message: &Message) -> Vec<RateLimitCheck> {
        if !self.config.is_effective() {
            return Vec::new();
        }

        let tenant = non_empty_or(tenant_id, "0");
        let sender = non_empty_or(&message.sender_id, "_");
        let conversation = non_empty_or(&message.conversation_id, "_");
        let mut checks = Vec::with_capacity(3);

        if self.config.tenant_per_second > 0 {
            checks.push(RateLimitCheck {
                key: format!("tenant:{tenant}"),
                scope: RateLimitScope::Tenant,
                limit: self.config.tenant_per_second,
            });
        }
        if self.config.tenant_sender_per_second > 0 {
            checks.push(RateLimitCheck {
                key: format!("tenant_sender:{tenant}:{sender}"),
                scope: RateLimitScope::TenantSender,
                limit: self.config.tenant_sender_per_second,
            });
        }
        if self.config.tenant_conversation_per_second > 0 {
            checks.push(RateLimitCheck {
                key: format!("tenant_conversation:{tenant}:{conversation}"),
                scope: RateLimitScope::TenantConversation,
                limit: self.config.tenant_conversation_per_second,
            });
        }

        checks
    }

    fn trim_state(
        state: &mut HashMap<String, WindowCounter>,
        now_ms: u64,
        window_ms: u64,
        max_tracked_keys: usize,
    ) {
        if state.len() <= max_tracked_keys {
            return;
        }

        let oldest_live_window = now_ms.saturating_sub(window_ms.saturating_mul(2));
        state.retain(|_, counter| counter.window_start_ms >= oldest_live_window);

        if state.len() <= max_tracked_keys {
            return;
        }

        let remove_count = state.len().saturating_sub(max_tracked_keys);
        let keys = state.keys().take(remove_count).cloned().collect::<Vec<_>>();
        for key in keys {
            state.remove(&key);
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
            .unwrap_or_default()
    }
}

fn non_empty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback
    } else {
        trimmed
    }
}

fn rate_limit_error(
    scope: RateLimitScope,
    limit: u32,
    window_ms: u64,
) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::MessageRateLimitExceeded,
        "message send rate limit exceeded",
    )
    .param("scope", scope.label())
    .param("limit_per_window", limit.to_string())
    .param("window_ms", window_ms.to_string())
    .build_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(sender_id: &str, conversation_id: &str) -> Message {
        Message {
            sender_id: sender_id.to_string(),
            conversation_id: conversation_id.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn disabled_limiter_allows_requests() {
        let limiter = SendRateLimiter::new(SendRateLimitConfig {
            enabled: false,
            tenant_sender_per_second: 1,
            ..Default::default()
        });
        let msg = message("u1", "c1");

        limiter.check_at_millis(0, "tenant-a", &msg).unwrap();
        limiter.check_at_millis(0, "tenant-a", &msg).unwrap();
    }

    #[test]
    fn tenant_sender_limit_rejects_third_request_in_window() {
        let limiter = SendRateLimiter::new(SendRateLimitConfig {
            enabled: true,
            tenant_sender_per_second: 2,
            window_ms: 1000,
            ..Default::default()
        });
        let msg = message("u1", "c1");

        limiter.check_at_millis(10, "tenant-a", &msg).unwrap();
        limiter.check_at_millis(20, "tenant-a", &msg).unwrap();
        let err = limiter
            .check_at_millis(30, "tenant-a", &msg)
            .expect_err("third request should be rate limited");

        assert_eq!(err.code(), Some(ErrorCode::MessageRateLimitExceeded));
        assert!(err.reason().contains("rate limit"));
    }

    #[test]
    fn conversation_limit_is_tenant_scoped() {
        let limiter = SendRateLimiter::new(SendRateLimitConfig {
            enabled: true,
            tenant_conversation_per_second: 1,
            window_ms: 1000,
            ..Default::default()
        });
        let first = message("u1", "c1");
        let second = message("u2", "c1");

        limiter.check_at_millis(10, "tenant-a", &first).unwrap();
        assert!(limiter.check_at_millis(20, "tenant-a", &second).is_err());
        limiter.check_at_millis(20, "tenant-b", &second).unwrap();
    }

    #[test]
    fn window_reset_allows_new_requests() {
        let limiter = SendRateLimiter::new(SendRateLimitConfig {
            enabled: true,
            tenant_per_second: 1,
            window_ms: 1000,
            ..Default::default()
        });
        let msg = message("u1", "c1");

        limiter.check_at_millis(999, "tenant-a", &msg).unwrap();
        assert!(limiter.check_at_millis(999, "tenant-a", &msg).is_err());
        limiter.check_at_millis(1000, "tenant-a", &msg).unwrap();
    }
}
