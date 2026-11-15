//! Per-vendor governance of the worker: a concurrency limit and a circuit breaker per vendor,
//! a token bucket and a "throttled until" mark per vendor and application credential, and a
//! "throttled until" mark per binding for vendors that throttle one device token.
//!
//! The breaker is per vendor because it judges the vendor's health; it only hears answers about
//! that (server errors, no answer). Rate and throttling are per application: the vendor's quota
//! is per credential, and one application exhausting its quota says nothing about another's.
//! APNs throttles per device token instead (429: too many requests to the same device token):
//! such a mark holds back that one binding and leaves the application's other devices alone.
//!
//! The state lives as long as the worker, across credential reloads: a lease rotation rebuilds
//! the vendor adapters, not their governance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::domain::vendor_delivery::Vendor;
use crate::domain::vendor_policy::{
    BreakerPass, BreakerSignal, BreakerState, CircuitBreaker, TokenBucket, VendorLimits,
};
use crate::infrastructure::vendor_metrics;

/// How long a caller may wait for a token of the local rate limit before the application
/// counts as throttled: absorbs jitter without holding a consumer slot.
const MAX_TOKEN_WAIT: Duration = Duration::from_millis(50);
/// Applications tracked per vendor before idle ones are forgotten. The credentials document
/// holds at most 1024 applications; this only bounds a pathological churn.
const MAX_TRACKED_APPLICATIONS: usize = 4_096;
/// An application's quota state unused for this long can be forgotten.
const IDLE_APPLICATION: Duration = Duration::from_secs(600);
/// Throttled bindings remembered per vendor. A mark lasts at most the longest honoured
/// Retry-After (minutes); when the map is full of live marks a new one is not kept, and that
/// device's next push simply meets the vendor's own answer again.
const MAX_THROTTLED_BINDINGS: usize = 65_536;

/// One binding of one application, for per-binding throttle marks: a digest of the application
/// and the binding's vendor token, so the governance keeps no device token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BindingKey([u8; 16]);

impl BindingKey {
    pub fn of(application: &str, token: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update((application.len() as u64).to_be_bytes());
        hash.update(application.as_bytes());
        hash.update(token.as_bytes());
        let digest = hash.finalize();
        let mut key = [0u8; 16];
        key.copy_from_slice(&digest[..16]);
        Self(key)
    }
}

/// Rate state of one application on one vendor.
struct ApplicationQuota {
    bucket: TokenBucket,
    throttled_until: Option<Instant>,
    used_at: Instant,
}

struct VendorGate {
    vendor: Vendor,
    limits: VendorLimits,
    permits: Arc<Semaphore>,
    breaker: Mutex<CircuitBreaker>,
    applications: Mutex<HashMap<String, ApplicationQuota>>,
    /// Bindings the vendor throttled on their own, and until when.
    bindings: Mutex<HashMap<BindingKey, Instant>>,
}

impl VendorGate {
    fn new(vendor: Vendor, limits: VendorLimits) -> Self {
        let limits = limits.sanitized();
        vendor_metrics::set_breaker_state(vendor, BreakerState::Closed);
        Self {
            vendor,
            permits: Arc::new(Semaphore::new(limits.max_concurrency)),
            breaker: Mutex::new(CircuitBreaker::new(limits.breaker)),
            applications: Mutex::new(HashMap::new()),
            bindings: Mutex::new(HashMap::new()),
            limits,
        }
    }

    fn with_bindings<T>(&self, apply: impl FnOnce(&mut HashMap<BindingKey, Instant>) -> T) -> T {
        let mut bindings = self
            .bindings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        apply(&mut bindings)
    }

    fn with_breaker<T>(&self, apply: impl FnOnce(&mut CircuitBreaker) -> T) -> T {
        let mut breaker = self
            .breaker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = breaker.state();
        let result = apply(&mut breaker);
        let after = breaker.state();
        if before != after {
            vendor_metrics::set_breaker_state(self.vendor, after);
            tracing::info!(
                vendor = self.vendor.as_str(),
                from = ?before,
                to = ?after,
                "push vendor breaker changed state"
            );
        }
        result
    }

    fn with_application<T>(
        &self,
        application: &str,
        now: Instant,
        apply: impl FnOnce(&mut ApplicationQuota) -> T,
    ) -> T {
        let mut applications = self
            .applications
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if applications.len() >= MAX_TRACKED_APPLICATIONS && !applications.contains_key(application)
        {
            applications.retain(|_, quota| {
                now.saturating_duration_since(quota.used_at) < IDLE_APPLICATION
                    || quota.throttled_until.is_some_and(|until| until > now)
            });
        }
        let quota = applications
            .entry(application.to_string())
            .or_insert_with(|| ApplicationQuota {
                bucket: TokenBucket::new(self.limits.rate_per_sec, self.limits.burst, now),
                throttled_until: None,
                used_at: now,
            });
        quota.used_at = now;
        apply(quota)
    }

    fn finish(&self, pass: BreakerPass, signal: BreakerSignal) {
        let now = Instant::now();
        self.with_breaker(|breaker| breaker.finish(now, pass, signal));
    }
}

/// What the governance says about one attempt.
pub enum Admission {
    /// Go ahead: wait for a concurrency slot with [`GovernanceTicket::acquire_slot`], call the
    /// vendor, then report with [`GovernanceTicket::finish`].
    Granted(GovernanceTicket),
    /// The vendor's breaker is open.
    BreakerOpen,
    /// The application is throttled on this vendor until `until` (the vendor said so, or the
    /// local rate limit is empty).
    Throttled { until: Instant },
}

/// A granted attempt. Dropping it without [`Self::finish`] (the attempt was cancelled) tells
/// the breaker nothing but frees a half-open probe slot.
pub struct GovernanceTicket {
    gate: Arc<VendorGate>,
    pass: Option<BreakerPass>,
    token_wait: Duration,
}

impl GovernanceTicket {
    /// Wait out the rate-limit reservation, then for a concurrency slot of the vendor for at
    /// most `budget`. `None`: the vendor is saturated in this worker.
    pub async fn acquire_slot(&self, budget: Duration) -> Option<OwnedSemaphorePermit> {
        if !self.token_wait.is_zero() {
            tokio::time::sleep(self.token_wait).await;
        }
        tokio::time::timeout(budget, self.gate.permits.clone().acquire_owned())
            .await
            .ok()?
            .ok()
    }

    pub fn finish(mut self, signal: BreakerSignal) {
        if let Some(pass) = self.pass.take() {
            self.gate.finish(pass, signal);
        }
    }
}

impl Drop for GovernanceTicket {
    fn drop(&mut self) {
        if let Some(pass) = self.pass.take() {
            self.gate.finish(pass, BreakerSignal::Neutral);
        }
    }
}

/// The governance of all vendors.
pub struct VendorGovernance {
    gates: HashMap<Vendor, Arc<VendorGate>>,
}

impl VendorGovernance {
    pub fn new(limits: impl Fn(Vendor) -> VendorLimits) -> Self {
        Self {
            gates: Vendor::ALL
                .into_iter()
                .map(|vendor| (vendor, Arc::new(VendorGate::new(vendor, limits(vendor)))))
                .collect(),
        }
    }

    /// Every vendor with the default limits.
    pub fn with_defaults() -> Self {
        Self::new(VendorLimits::defaults)
    }

    fn gate(&self, vendor: Vendor) -> &Arc<VendorGate> {
        // Every vendor has a gate: `new` builds one for each.
        &self.gates[&vendor]
    }

    /// Ask to send one push for `application` through `vendor` at `now`. Checks, in order: the
    /// application's throttle mark (nothing is taken), the breaker (may take a probe slot), the
    /// rate limit (an empty bucket marks the application throttled until its next token).
    pub fn admit(&self, vendor: Vendor, application: &str, now: Instant) -> Admission {
        let gate = self.gate(vendor);
        let throttled = gate.with_application(application, now, |quota| {
            quota.throttled_until.filter(|until| *until > now)
        });
        if let Some(until) = throttled {
            return Admission::Throttled { until };
        }
        let Some(pass) = gate.with_breaker(|breaker| breaker.try_pass(now)) else {
            return Admission::BreakerOpen;
        };
        let reserved = gate.with_application(application, now, |quota| {
            match quota.bucket.reserve(now, MAX_TOKEN_WAIT) {
                Ok(wait) => Ok(wait),
                Err(wait) => {
                    let until = now + wait;
                    quota.throttled_until = Some(until);
                    Err(until)
                }
            }
        });
        match reserved {
            Ok(token_wait) => Admission::Granted(GovernanceTicket {
                gate: gate.clone(),
                pass: Some(pass),
                token_wait,
            }),
            Err(until) => {
                gate.finish(pass, BreakerSignal::Neutral);
                Admission::Throttled { until }
            }
        }
    }

    /// The vendor throttled `application`: no attempt for it before `until`. A later mark only
    /// ever extends the current one.
    pub fn mark_throttled(&self, vendor: Vendor, application: &str, until: Instant) {
        let now = Instant::now();
        self.gate(vendor)
            .with_application(application, now, |quota| {
                quota.throttled_until =
                    Some(quota.throttled_until.map_or(until, |at| at.max(until)));
            });
    }

    /// When `application` may use `vendor` again; `None` when it is not throttled now.
    pub fn throttled_until(
        &self,
        vendor: Vendor,
        application: &str,
        now: Instant,
    ) -> Option<Instant> {
        self.gate(vendor)
            .with_application(application, now, |quota| {
                quota.throttled_until.filter(|until| *until > now)
            })
    }

    /// The vendor throttled this one binding (APNs: too many requests to the same device
    /// token): no attempt through it before `until`. The application is not throttled. A later
    /// mark only ever extends the current one.
    pub fn mark_binding_throttled(&self, vendor: Vendor, binding: BindingKey, until: Instant) {
        let now = Instant::now();
        self.gate(vendor).with_bindings(|bindings| {
            if bindings.len() >= MAX_THROTTLED_BINDINGS && !bindings.contains_key(&binding) {
                bindings.retain(|_, at| *at > now);
                if bindings.len() >= MAX_THROTTLED_BINDINGS {
                    return;
                }
            }
            let at = bindings.entry(binding).or_insert(until);
            *at = (*at).max(until);
        });
    }

    /// When this binding may use `vendor` again; `None` when it is not throttled on its own now.
    pub fn binding_throttled_until(
        &self,
        vendor: Vendor,
        binding: BindingKey,
        now: Instant,
    ) -> Option<Instant> {
        self.gate(vendor).with_bindings(|bindings| {
            let until = bindings.get(&binding).copied()?;
            if until > now {
                Some(until)
            } else {
                bindings.remove(&binding);
                None
            }
        })
    }

    pub fn breaker_state(&self, vendor: Vendor) -> BreakerState {
        self.gate(vendor).with_breaker(|breaker| breaker.state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::vendor_policy::BreakerConfig;

    fn tight(_: Vendor) -> VendorLimits {
        VendorLimits {
            max_concurrency: 1,
            rate_per_sec: 2,
            burst: 2,
            breaker: BreakerConfig {
                window: 2,
                min_calls: 2,
                open_for: Duration::from_secs(30),
                half_open_probes: 1,
                ..BreakerConfig::default()
            },
        }
    }

    fn granted(admission: Admission) -> GovernanceTicket {
        match admission {
            Admission::Granted(ticket) => ticket,
            Admission::BreakerOpen => panic!("breaker open"),
            Admission::Throttled { .. } => panic!("throttled"),
        }
    }

    #[test]
    fn an_empty_bucket_throttles_that_application_only() {
        let governance = VendorGovernance::new(tight);
        let now = Instant::now();
        granted(governance.admit(Vendor::Fcm, "app-a", now)).finish(BreakerSignal::Success);
        granted(governance.admit(Vendor::Fcm, "app-a", now)).finish(BreakerSignal::Success);
        let Admission::Throttled { until } = governance.admit(Vendor::Fcm, "app-a", now) else {
            panic!("the third push in the same instant is over two a second");
        };
        assert!(until > now);
        assert_eq!(
            governance.throttled_until(Vendor::Fcm, "app-a", now),
            Some(until)
        );
        // Another application has its own quota, another vendor its own limits.
        granted(governance.admit(Vendor::Fcm, "app-b", now)).finish(BreakerSignal::Success);
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Success);
        // After the mark the application gets tokens again.
        granted(governance.admit(Vendor::Fcm, "app-a", until + Duration::from_millis(1)));
    }

    #[test]
    fn a_vendor_throttle_mark_holds_until_its_time_and_only_extends() {
        let governance = VendorGovernance::with_defaults();
        let now = Instant::now();
        let until = now + Duration::from_secs(10);
        governance.mark_throttled(Vendor::Getui, "app-a", until);
        governance.mark_throttled(Vendor::Getui, "app-a", now + Duration::from_secs(1));
        assert!(matches!(
            governance.admit(Vendor::Getui, "app-a", now),
            Admission::Throttled { until: at } if at == until
        ));
        granted(governance.admit(Vendor::Getui, "app-a", until + Duration::from_millis(1)));
    }

    #[test]
    fn failures_open_the_vendor_breaker_for_every_application() {
        let governance = VendorGovernance::new(tight);
        let now = Instant::now();
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Failure);
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Failure);
        assert_eq!(governance.breaker_state(Vendor::Apns), BreakerState::Open);
        assert!(matches!(
            governance.admit(Vendor::Apns, "app-b", now),
            Admission::BreakerOpen
        ));
        assert_eq!(governance.breaker_state(Vendor::Fcm), BreakerState::Closed);
    }

    #[test]
    fn a_dropped_probe_frees_its_slot() {
        let governance = VendorGovernance::new(tight);
        let now = Instant::now();
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Failure);
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Failure);
        // 结束一次调用时熔断器按真实时钟记打开时刻，半开要从那之后算满 30 s。
        let later = Instant::now() + Duration::from_secs(30);
        let probe = granted(governance.admit(Vendor::Apns, "app-b", later));
        assert!(matches!(
            governance.admit(Vendor::Apns, "app-c", later),
            Admission::BreakerOpen
        ));
        drop(probe);
        granted(governance.admit(Vendor::Apns, "app-c", later)).finish(BreakerSignal::Success);
        assert_eq!(governance.breaker_state(Vendor::Apns), BreakerState::Closed);
    }

    #[test]
    fn a_binding_throttle_mark_holds_that_binding_only() {
        let governance = VendorGovernance::with_defaults();
        let now = Instant::now();
        let until = now + Duration::from_secs(10);
        let throttled = BindingKey::of("app-a", "token-1");
        let other_device = BindingKey::of("app-a", "token-2");
        assert_ne!(throttled, other_device);
        assert_ne!(throttled, BindingKey::of("app-b", "token-1"));
        governance.mark_binding_throttled(Vendor::Apns, throttled, until);
        governance.mark_binding_throttled(Vendor::Apns, throttled, now + Duration::from_secs(1));
        assert_eq!(
            governance.binding_throttled_until(Vendor::Apns, throttled, now),
            Some(until),
            "a later, shorter mark does not shorten it"
        );
        // The application and its other devices are not throttled; nor is the binding on
        // another vendor.
        assert_eq!(governance.throttled_until(Vendor::Apns, "app-a", now), None);
        granted(governance.admit(Vendor::Apns, "app-a", now)).finish(BreakerSignal::Success);
        assert_eq!(
            governance.binding_throttled_until(Vendor::Apns, other_device, now),
            None
        );
        assert_eq!(
            governance.binding_throttled_until(Vendor::Fcm, throttled, now),
            None
        );
        // The mark ends at its time and is forgotten.
        assert_eq!(
            governance.binding_throttled_until(
                Vendor::Apns,
                throttled,
                until + Duration::from_millis(1)
            ),
            None
        );
        assert_eq!(
            governance.binding_throttled_until(Vendor::Apns, throttled, now),
            None
        );
    }

    #[tokio::test]
    async fn the_concurrency_limit_is_per_vendor() {
        let governance = VendorGovernance::new(tight);
        let now = Instant::now();
        let first = granted(governance.admit(Vendor::Getui, "app-a", now));
        let held = first
            .acquire_slot(Duration::from_millis(10))
            .await
            .expect("one slot");
        let second = granted(governance.admit(Vendor::Getui, "app-b", now));
        assert!(
            second
                .acquire_slot(Duration::from_millis(10))
                .await
                .is_none()
        );
        drop(held);
        assert!(
            second
                .acquire_slot(Duration::from_millis(10))
                .await
                .is_some()
        );
    }
}
