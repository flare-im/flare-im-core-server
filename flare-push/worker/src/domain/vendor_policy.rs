//! Vendor governance as plain state machines: a rolling-window circuit breaker and a token
//! bucket, plus the per-vendor limits (`[services.push_worker.vendors.<vendor>]`). No clocks
//! and no locks here: callers pass the time in, which keeps the transitions testable.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::domain::vendor_delivery::Vendor;

/// Breaker settings of one vendor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BreakerConfig {
    /// Open when at least this share of the calls in the window failed.
    pub failure_ratio: f64,
    /// How many of the most recent calls the window holds.
    pub window: usize,
    /// No verdict on fewer calls than this.
    pub min_calls: usize,
    /// How long the breaker stays open before letting probes through.
    pub open_for: Duration,
    /// Probes allowed at once while half-open; this many successes close the breaker.
    pub half_open_probes: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_ratio: 0.5,
            window: 20,
            min_calls: 10,
            open_for: Duration::from_millis(30_000),
            half_open_probes: 3,
        }
    }
}

impl BreakerConfig {
    /// Values a breaker can work with: a ratio in (0, 1], a window of at least one call, a
    /// minimum that fits the window, at least one probe.
    pub fn sanitized(self) -> Self {
        let window = self.window.max(1);
        Self {
            failure_ratio: if self.failure_ratio.is_finite() && self.failure_ratio > 0.0 {
                self.failure_ratio.min(1.0)
            } else {
                Self::default().failure_ratio
            },
            window,
            min_calls: self.min_calls.clamp(1, window),
            open_for: self.open_for.max(Duration::from_millis(1)),
            half_open_probes: self.half_open_probes.max(1),
        }
    }
}

/// Limits of one vendor: concurrency across the worker, rate per application credential, and
/// the breaker.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VendorLimits {
    pub max_concurrency: usize,
    pub rate_per_sec: u32,
    pub burst: u32,
    pub breaker: BreakerConfig,
}

impl VendorLimits {
    /// The defaults: APNs and FCM take far more than Getui's REST API.
    pub fn defaults(vendor: Vendor) -> Self {
        let (max_concurrency, rate_per_sec) = match vendor {
            Vendor::Apns => (256, 2_000),
            Vendor::Fcm => (256, 1_000),
            Vendor::Getui => (64, 300),
        };
        Self {
            max_concurrency,
            rate_per_sec,
            burst: rate_per_sec,
            breaker: BreakerConfig::default(),
        }
    }

    pub fn sanitized(self) -> Self {
        Self {
            max_concurrency: self.max_concurrency.max(1),
            rate_per_sec: self.rate_per_sec.max(1),
            burst: self.burst.max(1),
            breaker: self.breaker.sanitized(),
        }
    }
}

/// Breaker state; the gauge value of `flare_push_vendor_breaker_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    HalfOpen,
    Open,
}

impl BreakerState {
    pub const fn gauge(self) -> i64 {
        match self {
            Self::Closed => 0,
            Self::HalfOpen => 1,
            Self::Open => 2,
        }
    }
}

/// How a call the breaker let through ended, as far as the vendor's health goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerSignal {
    Success,
    Failure,
    /// Says nothing about the vendor (throttled, refused credential, local failure).
    Neutral,
}

/// How the breaker let a call through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerPass {
    Normal,
    Probe,
}

/// Rolling-window breaker: closed until the share of failures among the last `window` calls
/// (at least `min_calls` of them) reaches `failure_ratio`; then open for `open_for`; then
/// half-open, letting `half_open_probes` calls through at a time. That many successes close it,
/// one failure opens it again.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: BreakerConfig,
    state: BreakerState,
    /// `true` for a failure, oldest first.
    outcomes: VecDeque<bool>,
    failures: usize,
    opened_at: Option<Instant>,
    probes_in_flight: u32,
    probes_succeeded: u32,
}

impl CircuitBreaker {
    pub fn new(config: BreakerConfig) -> Self {
        let config = config.sanitized();
        Self {
            outcomes: VecDeque::with_capacity(config.window),
            config,
            state: BreakerState::Closed,
            failures: 0,
            opened_at: None,
            probes_in_flight: 0,
            probes_succeeded: 0,
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }

    /// Ask to make a call at `now`. `None`: open, or half-open with every probe slot taken.
    pub fn try_pass(&mut self, now: Instant) -> Option<BreakerPass> {
        if self.state == BreakerState::Open {
            let elapsed = self
                .opened_at
                .map_or(Duration::MAX, |at| now.saturating_duration_since(at));
            if elapsed < self.config.open_for {
                return None;
            }
            self.state = BreakerState::HalfOpen;
            self.probes_in_flight = 0;
            self.probes_succeeded = 0;
        }
        match self.state {
            BreakerState::Closed => Some(BreakerPass::Normal),
            BreakerState::HalfOpen if self.probes_in_flight < self.config.half_open_probes => {
                self.probes_in_flight += 1;
                Some(BreakerPass::Probe)
            }
            BreakerState::HalfOpen | BreakerState::Open => None,
        }
    }

    /// Report how a call let through by [`Self::try_pass`] ended. A call that started in an
    /// earlier phase (a normal call finishing after the breaker opened, a probe finishing after
    /// the breaker closed or reopened) does not move the current phase.
    pub fn finish(&mut self, now: Instant, pass: BreakerPass, signal: BreakerSignal) {
        match pass {
            BreakerPass::Probe => {
                if self.state != BreakerState::HalfOpen {
                    return;
                }
                self.probes_in_flight = self.probes_in_flight.saturating_sub(1);
                match signal {
                    BreakerSignal::Failure => self.trip(now),
                    BreakerSignal::Success => {
                        self.probes_succeeded += 1;
                        if self.probes_succeeded >= self.config.half_open_probes {
                            self.close();
                        }
                    }
                    BreakerSignal::Neutral => {}
                }
            }
            BreakerPass::Normal => {
                if self.state != BreakerState::Closed {
                    return;
                }
                let failed = match signal {
                    BreakerSignal::Success => false,
                    BreakerSignal::Failure => true,
                    BreakerSignal::Neutral => return,
                };
                self.outcomes.push_back(failed);
                if failed {
                    self.failures += 1;
                }
                while self.outcomes.len() > self.config.window {
                    if self.outcomes.pop_front() == Some(true) {
                        self.failures -= 1;
                    }
                }
                let calls = self.outcomes.len();
                if calls >= self.config.min_calls
                    && self.failures as f64 >= self.config.failure_ratio * calls as f64
                {
                    self.trip(now);
                }
            }
        }
    }

    fn trip(&mut self, now: Instant) {
        self.state = BreakerState::Open;
        self.opened_at = Some(now);
        self.reset_counts();
    }

    fn close(&mut self) {
        self.state = BreakerState::Closed;
        self.opened_at = None;
        self.reset_counts();
    }

    fn reset_counts(&mut self) {
        self.outcomes.clear();
        self.failures = 0;
        self.probes_in_flight = 0;
        self.probes_succeeded = 0;
    }
}

/// Token bucket: `rate` tokens a second, at most `burst` saved up. A caller may reserve a token
/// that becomes available within `max_wait` and wait for it, so short bursts are smoothed
/// instead of refused.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    rate: f64,
    burst: f64,
    /// Below zero while reservations are outstanding.
    tokens: f64,
    refilled_at: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: u32, burst: u32, now: Instant) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            rate: f64::from(rate_per_sec.max(1)),
            burst,
            tokens: burst,
            refilled_at: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.refilled_at)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
            self.refilled_at = now;
        }
    }

    /// Take one token at `now`. `Ok(wait)`: taken, usable after `wait` (zero when a token was
    /// there). `Err(wait)`: none within `max_wait`; nothing was taken, and the next token for
    /// this caller is `wait` away.
    pub fn reserve(&mut self, now: Instant, max_wait: Duration) -> Result<Duration, Duration> {
        self.refill(now);
        let after = self.tokens - 1.0;
        if after >= 0.0 {
            self.tokens = after;
            return Ok(Duration::ZERO);
        }
        let wait = Duration::from_secs_f64(-after / self.rate);
        if wait <= max_wait {
            self.tokens = after;
            Ok(wait)
        } else {
            Err(wait)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BreakerConfig {
        BreakerConfig {
            failure_ratio: 0.5,
            window: 20,
            min_calls: 10,
            open_for: Duration::from_secs(30),
            half_open_probes: 3,
        }
    }

    fn call(breaker: &mut CircuitBreaker, now: Instant, signal: BreakerSignal) {
        let pass = breaker.try_pass(now).expect("let through");
        breaker.finish(now, pass, signal);
    }

    #[test]
    fn the_breaker_needs_the_minimum_calls_before_it_opens() {
        let now = Instant::now();
        let mut breaker = CircuitBreaker::new(config());
        for _ in 0..9 {
            call(&mut breaker, now, BreakerSignal::Failure);
        }
        assert_eq!(
            breaker.state(),
            BreakerState::Closed,
            "nine calls are no verdict"
        );
        call(&mut breaker, now, BreakerSignal::Failure);
        assert_eq!(breaker.state(), BreakerState::Open);
        assert!(breaker.try_pass(now).is_none());
    }

    #[test]
    fn the_breaker_opens_at_the_failure_ratio_of_the_rolling_window() {
        let now = Instant::now();
        let mut breaker = CircuitBreaker::new(config());
        // Twenty successes fill the window; failures then push successes out one by one.
        for _ in 0..20 {
            call(&mut breaker, now, BreakerSignal::Success);
        }
        for _ in 0..9 {
            call(&mut breaker, now, BreakerSignal::Failure);
        }
        assert_eq!(breaker.state(), BreakerState::Closed, "9 of 20 failed");
        call(&mut breaker, now, BreakerSignal::Failure);
        assert_eq!(breaker.state(), BreakerState::Open, "10 of 20 failed");
    }

    #[test]
    fn neutral_answers_do_not_count() {
        let now = Instant::now();
        let mut breaker = CircuitBreaker::new(config());
        for _ in 0..50 {
            call(&mut breaker, now, BreakerSignal::Neutral);
        }
        for _ in 0..9 {
            call(&mut breaker, now, BreakerSignal::Failure);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
    }

    fn opened(now: Instant) -> CircuitBreaker {
        let mut breaker = CircuitBreaker::new(config());
        for _ in 0..10 {
            call(&mut breaker, now, BreakerSignal::Failure);
        }
        assert_eq!(breaker.state(), BreakerState::Open);
        breaker
    }

    #[test]
    fn after_the_open_period_probes_close_the_breaker() {
        let start = Instant::now();
        let mut breaker = opened(start);
        assert!(breaker.try_pass(start + Duration::from_secs(29)).is_none());
        let later = start + Duration::from_secs(30);
        let probes: Vec<_> = (0..3).map(|_| breaker.try_pass(later)).collect();
        assert!(probes.iter().all(|pass| *pass == Some(BreakerPass::Probe)));
        assert_eq!(breaker.state(), BreakerState::HalfOpen);
        assert!(
            breaker.try_pass(later).is_none(),
            "only three probes at a time"
        );
        breaker.finish(later, BreakerPass::Probe, BreakerSignal::Success);
        breaker.finish(later, BreakerPass::Probe, BreakerSignal::Success);
        assert_eq!(breaker.state(), BreakerState::HalfOpen);
        breaker.finish(later, BreakerPass::Probe, BreakerSignal::Success);
        assert_eq!(breaker.state(), BreakerState::Closed);
        assert_eq!(breaker.try_pass(later), Some(BreakerPass::Normal));
    }

    #[test]
    fn a_failed_probe_opens_the_breaker_again() {
        let start = Instant::now();
        let mut breaker = opened(start);
        let later = start + Duration::from_secs(30);
        let pass = breaker.try_pass(later).unwrap();
        breaker.finish(later, pass, BreakerSignal::Failure);
        assert_eq!(breaker.state(), BreakerState::Open);
        assert!(breaker.try_pass(later + Duration::from_secs(29)).is_none());
        assert!(breaker.try_pass(later + Duration::from_secs(30)).is_some());
    }

    #[test]
    fn a_neutral_probe_frees_its_slot_and_stale_calls_do_not_move_the_phase() {
        let start = Instant::now();
        let mut breaker = opened(start);
        let later = start + Duration::from_secs(30);
        for _ in 0..3 {
            breaker.try_pass(later).unwrap();
        }
        assert!(breaker.try_pass(later).is_none());
        breaker.finish(later, BreakerPass::Probe, BreakerSignal::Neutral);
        assert!(breaker.try_pass(later).is_some(), "the slot is free again");
        // A normal call that started before the breaker opened ends now: no effect.
        breaker.finish(later, BreakerPass::Normal, BreakerSignal::Failure);
        assert_eq!(breaker.state(), BreakerState::HalfOpen);
    }

    #[test]
    fn the_bucket_spends_its_burst_then_smooths_and_refuses_beyond_the_wait() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(10, 3, now);
        let max_wait = Duration::from_millis(150);
        for _ in 0..3 {
            assert_eq!(bucket.reserve(now, max_wait), Ok(Duration::ZERO));
        }
        // 10 a second: the next tokens are 100 ms and 200 ms away.
        let wait = bucket.reserve(now, max_wait).expect("100 ms away");
        assert!((95..=105).contains(&wait.as_millis()), "{wait:?}");
        let refused = bucket.reserve(now, max_wait).expect_err("200 ms away");
        assert!((195..=205).contains(&refused.as_millis()), "{refused:?}");
        // A refusal takes nothing: half a second later the bucket has refilled past it.
        let later = now + Duration::from_millis(500);
        assert_eq!(bucket.reserve(later, max_wait), Ok(Duration::ZERO));
    }

    #[test]
    fn the_bucket_never_saves_more_than_its_burst() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(1_000, 2, now);
        let later = now + Duration::from_secs(60);
        assert_eq!(bucket.reserve(later, Duration::ZERO), Ok(Duration::ZERO));
        assert_eq!(bucket.reserve(later, Duration::ZERO), Ok(Duration::ZERO));
        assert!(bucket.reserve(later, Duration::ZERO).is_err());
    }

    #[test]
    fn defaults_follow_the_interface() {
        let apns = VendorLimits::defaults(Vendor::Apns);
        assert_eq!(
            (apns.max_concurrency, apns.rate_per_sec, apns.burst),
            (256, 2_000, 2_000)
        );
        let fcm = VendorLimits::defaults(Vendor::Fcm);
        assert_eq!(
            (fcm.max_concurrency, fcm.rate_per_sec, fcm.burst),
            (256, 1_000, 1_000)
        );
        let getui = VendorLimits::defaults(Vendor::Getui);
        assert_eq!(
            (getui.max_concurrency, getui.rate_per_sec, getui.burst),
            (64, 300, 300)
        );
        assert_eq!(apns.breaker, config());
        let odd = BreakerConfig {
            failure_ratio: f64::NAN,
            window: 0,
            min_calls: 50,
            open_for: Duration::ZERO,
            half_open_probes: 0,
        }
        .sanitized();
        assert_eq!(odd.failure_ratio, 0.5);
        assert_eq!((odd.window, odd.min_calls, odd.half_open_probes), (1, 1, 1));
    }
}
