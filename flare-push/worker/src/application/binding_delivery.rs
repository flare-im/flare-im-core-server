//! One push per task per device, governed.
//!
//! The device's bindings are listed once by the caller. They are tried in the configured
//! preference order: the first binding whose vendor lets the attempt through takes the task.
//! The next binding is tried when the current one cannot take it: its vendor's breaker is
//! open, the vendor (or the local rate limit) throttles the application, or throttles this one
//! binding (APNs answers 429 per device token: only that binding waits, the application's other
//! devices keep using APNs), the attempt failed in a way a retry may fix, the token turned out
//! dead (it is removed), or the vendor has nothing to carry for this task on this device. A
//! terminal failure ends the task (dead letter).
//!
//! When no binding took the task:
//! - some binding was throttled: the task is held by the digest coalescer (acknowledged, sent
//!   merged once the vendor is available again; a full hold lets the broker redeliver it);
//! - otherwise a retryable failure is returned (the broker redelivers with backoff);
//! - a binding of a provider the application has no credentials for (and did not disable)
//!   is a terminal failure when nothing else is pending: the device is not silently "handled".

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_im_usage::{PushOutcome, UsageRecorder};
use flare_proto::PushTaskEnvelope;
use flare_server_core::{ErrorCode, FlareError};

use crate::application::digest_coalescer::{DigestCoalescer, is_coalesced_task};
use crate::application::vendor_governance::{Admission, BindingKey, VendorGovernance};
use crate::domain::vendor_delivery::{
    BindingDelivery, BindingOutcome, FailureReason, ThrottleScope, Vendor, order_bindings,
    receipt_capable, throttled_error,
};
use crate::domain::vendor_policy::BreakerSignal;
use crate::infrastructure::vendor_http;
use crate::infrastructure::vendor_metrics;
use crate::infrastructure::vendor_trace::{VendorEvent, VendorOutcome, VendorTrace};

/// One attempt through one binding, the vendor's answer included. A slow vendor costs at most
/// this before the next binding is tried; the consumer's 30 s deadline still bounds the task.
const ATTEMPT_DEADLINE: Duration = Duration::from_secs(12);
/// Wait for a concurrency slot of the vendor before treating it as saturated.
const SLOT_WAIT: Duration = Duration::from_secs(2);
/// A throttling answer without Retry-After keeps the application (or the binding) off the
/// vendor this long.
const DEFAULT_THROTTLE: Duration = Duration::from_secs(2);
/// Bounds of an honoured Retry-After.
const MIN_THROTTLE: Duration = Duration::from_secs(1);
const MAX_THROTTLE: Duration = Duration::from_secs(300);

/// What lives as long as the worker, shared by every application's delivery: the binding
/// order, the vendor governance, the digest coalescer, the trace and the usage meter.
pub struct DeliveryRuntime {
    pub binding_order: Vec<Vendor>,
    pub governance: Arc<VendorGovernance>,
    pub coalescer: Arc<DigestCoalescer>,
    pub trace: VendorTrace,
    pub usage: UsageRecorder,
}

#[cfg(test)]
impl DeliveryRuntime {
    /// Default order and limits, no trace, no usage reporting.
    pub(crate) fn for_tests() -> Arc<Self> {
        let governance = Arc::new(VendorGovernance::with_defaults());
        Arc::new(Self {
            binding_order: crate::domain::vendor_delivery::DEFAULT_BINDING_ORDER.to_vec(),
            coalescer: Arc::new(DigestCoalescer::new(
                crate::application::digest_coalescer::CoalescerLimits::default(),
                governance.clone(),
                VendorTrace::disabled(),
            )),
            governance,
            trace: VendorTrace::disabled(),
            usage: UsageRecorder::disabled(),
        })
    }
}

/// Delivery of one task to its device, given the device's bindings (listed once by the caller).
#[async_trait]
pub trait DeviceBindingsDelivery: Send + Sync {
    async fn deliver_bindings(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        bindings: Vec<DevicePushToken>,
    ) -> Result<(), FlareError>;
}

/// The vendor adapters of one application (its credentials) behind the binding selection.
pub struct DevicePush {
    application: String,
    adapters: Vec<(Vendor, Arc<dyn BindingDelivery>)>,
    disabled: Vec<Vendor>,
    runtime: Arc<DeliveryRuntime>,
}

impl DevicePush {
    pub fn new(
        application: String,
        adapters: Vec<(Vendor, Arc<dyn BindingDelivery>)>,
        disabled: Vec<Vendor>,
        runtime: Arc<DeliveryRuntime>,
    ) -> Self {
        Self {
            application,
            adapters,
            disabled,
            runtime,
        }
    }

    fn adapter(&self, vendor: Vendor) -> Option<&Arc<dyn BindingDelivery>> {
        self.adapters
            .iter()
            .find(|(candidate, _)| *candidate == vendor)
            .map(|(_, adapter)| adapter)
    }

    fn trace(&self, task: &PushTaskEnvelope, event: VendorEvent<'_>) {
        self.runtime.trace.record(task, event);
    }

    async fn attempt(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        vendor: Vendor,
        binding: &DevicePushToken,
        adapter: &dyn BindingDelivery,
    ) -> Step {
        let runtime = &self.runtime;
        let app_version = binding.app_version.as_str();
        let binding_key = BindingKey::of(&self.application, &binding.token);
        // A binding the vendor throttled on its own waits without taking a token or a slot.
        if let Some(until) =
            runtime
                .governance
                .binding_throttled_until(vendor, binding_key, Instant::now())
        {
            vendor_metrics::record_throttled(vendor);
            self.trace(
                task,
                VendorEvent::new(Some(vendor), VendorOutcome::Throttled)
                    .app_version(app_version)
                    .code("local"),
            );
            return Step::Switch(Cause::Throttled {
                until,
                binding: Some(binding_key),
            });
        }
        let ticket = match runtime
            .governance
            .admit(vendor, &self.application, Instant::now())
        {
            Admission::Granted(ticket) => ticket,
            Admission::BreakerOpen => {
                self.trace(
                    task,
                    VendorEvent::new(Some(vendor), VendorOutcome::BreakerOpen)
                        .app_version(app_version),
                );
                return Step::Switch(Cause::Unavailable(FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "push vendor circuit open",
                )));
            }
            Admission::Throttled { until } => {
                vendor_metrics::record_throttled(vendor);
                self.trace(
                    task,
                    VendorEvent::new(Some(vendor), VendorOutcome::Throttled)
                        .app_version(app_version)
                        .code("local"),
                );
                return Step::Switch(Cause::Throttled {
                    until,
                    binding: None,
                });
            }
        };
        let Some(slot) = ticket.acquire_slot(SLOT_WAIT).await else {
            vendor_metrics::record_failure(vendor, FailureReason::Saturated);
            self.trace(
                task,
                VendorEvent::new(Some(vendor), VendorOutcome::Retryable)
                    .app_version(app_version)
                    .code("saturated"),
            );
            return Step::Switch(Cause::Unavailable(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "push vendor saturated",
            )));
        };
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            ATTEMPT_DEADLINE,
            adapter.deliver_binding(ctx, task, binding),
        )
        .await
        .unwrap_or_else(|_| {
            BindingOutcome::retryable(
                vendor_http::failure("push vendor deadline; acceptance unknown"),
                FailureReason::Transport,
                "deadline",
            )
        });
        let latency = started.elapsed();
        drop(slot);
        ticket.finish(breaker_signal(&outcome));
        runtime
            .usage
            .push_attempt(&task.tenant_id, vendor.as_str(), usage_outcome(&outcome));
        if reached_vendor(&outcome) {
            vendor_metrics::record_latency(vendor, latency);
        }
        let event = |outcome| {
            VendorEvent::new(Some(vendor), outcome)
                .latency(latency)
                .app_version(app_version)
        };
        match outcome {
            BindingOutcome::Accepted => {
                vendor_metrics::record_sent(
                    vendor,
                    app_version,
                    receipt_capable(vendor, binding, task),
                );
                self.trace(task, event(VendorOutcome::Accepted));
                Step::Delivered
            }
            BindingOutcome::AlreadyAccepted => Step::Delivered,
            BindingOutcome::Skipped => Step::Next,
            BindingOutcome::InvalidToken { code } => {
                vendor_metrics::record_invalid_token(vendor);
                self.trace(task, event(VendorOutcome::InvalidToken).code(code));
                Step::Next
            }
            BindingOutcome::Throttled {
                retry_after,
                code,
                scope,
            } => {
                let until = Instant::now() + throttle_window(retry_after);
                let binding = match scope {
                    ThrottleScope::Application => {
                        runtime
                            .governance
                            .mark_throttled(vendor, &self.application, until);
                        None
                    }
                    ThrottleScope::Binding => {
                        runtime
                            .governance
                            .mark_binding_throttled(vendor, binding_key, until);
                        Some(binding_key)
                    }
                };
                vendor_metrics::record_throttled(vendor);
                self.trace(task, event(VendorOutcome::Throttled).code(code));
                Step::Switch(Cause::Throttled { until, binding })
            }
            BindingOutcome::Retryable {
                error,
                reason: FailureReason::Registry,
                code,
            } => {
                // The binding registry is down: no other vendor helps, the broker retries.
                vendor_metrics::record_failure(vendor, FailureReason::Registry);
                self.trace(task, event(VendorOutcome::Retryable).code(code));
                Step::Stop(error)
            }
            BindingOutcome::Retryable {
                error,
                reason,
                code,
            } => {
                vendor_metrics::record_failure(vendor, reason);
                self.trace(task, event(VendorOutcome::Retryable).code(code));
                Step::Switch(Cause::Unavailable(error))
            }
            BindingOutcome::Terminal {
                error,
                reason,
                code,
            } => {
                vendor_metrics::record_failure(vendor, reason);
                self.trace(task, event(VendorOutcome::Terminal).code(code));
                Step::Stop(error)
            }
        }
    }
}

/// Where the selection goes after one binding.
enum Step {
    /// The task is done (accepted now or earlier).
    Delivered,
    /// This binding had nothing to do (or its token was dead): next binding, no switch.
    Next,
    /// This binding could not take the task now: next binding.
    Switch(Cause),
    /// The task ends here with this error.
    Stop(FlareError),
}

enum Cause {
    /// Throttled until `until`: the application on the vendor, or only `binding` when the
    /// vendor throttled that one binding.
    Throttled {
        until: Instant,
        binding: Option<BindingKey>,
    },
    Unavailable(FlareError),
}

#[async_trait]
impl DeviceBindingsDelivery for DevicePush {
    async fn deliver_bindings(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        bindings: Vec<DevicePushToken>,
    ) -> Result<(), FlareError> {
        let mut missing_credentials = None;
        let mut unavailable: Option<FlareError> = None;
        let mut throttled: Option<(Vendor, Instant, Option<BindingKey>)> = None;
        let mut leaving: Option<Vendor> = None;
        for (vendor, binding) in order_bindings(bindings, &self.runtime.binding_order) {
            if self.disabled.contains(&vendor) {
                continue;
            }
            let Some(adapter) = self.adapter(vendor) else {
                missing_credentials.get_or_insert_with(|| {
                    FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "push application provider credentials not configured",
                    )
                });
                continue;
            };
            if let Some(from) = leaving.take() {
                vendor_metrics::record_switch(from, vendor);
                self.trace(
                    task,
                    VendorEvent::new(Some(vendor), VendorOutcome::Switched).code(from.as_str()),
                );
            }
            match self
                .attempt(ctx, task, vendor, &binding, adapter.as_ref())
                .await
            {
                Step::Delivered => return Ok(()),
                Step::Next => {}
                Step::Switch(cause) => {
                    match cause {
                        Cause::Throttled { until, binding } => {
                            if throttled.is_none_or(|(_, earliest, _)| until < earliest) {
                                throttled = Some((vendor, until, binding));
                            }
                        }
                        Cause::Unavailable(error) => unavailable = Some(error),
                    }
                    leaving = Some(vendor);
                }
                Step::Stop(error) => return Err(error),
            }
        }
        if let Some((vendor, until, binding)) = throttled {
            if is_coalesced_task(task) {
                // A held task the coalescer handed back: it keeps holding it.
                return Err(throttled_error());
            }
            return match self
                .runtime
                .coalescer
                .absorb(ctx, task, vendor, until, binding)
            {
                Ok(()) => Ok(()),
                Err(_) => {
                    tracing::warn!(
                        tenant_id = %task.tenant_id,
                        device_id = %task.device_id,
                        vendor = vendor.as_str(),
                        "throttled push not held: the hold is full; redelivering"
                    );
                    Err(throttled_error())
                }
            };
        }
        if let Some(error) = unavailable {
            return Err(error);
        }
        missing_credentials.map_or(Ok(()), Err)
    }
}

/// What the breaker hears from an attempt: only the vendor's own health.
fn breaker_signal(outcome: &BindingOutcome) -> BreakerSignal {
    match outcome {
        BindingOutcome::Accepted
        | BindingOutcome::InvalidToken { .. }
        | BindingOutcome::Terminal {
            reason: FailureReason::Rejected,
            ..
        } => BreakerSignal::Success,
        BindingOutcome::Retryable { reason, .. } | BindingOutcome::Terminal { reason, .. }
            if reason.counts_against_vendor() =>
        {
            BreakerSignal::Failure
        }
        _ => BreakerSignal::Neutral,
    }
}

/// `push.attempts` per attempt handed to a vendor adapter, by its vendor.
fn usage_outcome(outcome: &BindingOutcome) -> PushOutcome {
    match outcome {
        BindingOutcome::Accepted
        | BindingOutcome::AlreadyAccepted
        | BindingOutcome::Skipped
        | BindingOutcome::InvalidToken { .. } => PushOutcome::Completed,
        BindingOutcome::Throttled { .. } | BindingOutcome::Retryable { .. } => {
            PushOutcome::Retryable
        }
        BindingOutcome::Terminal { .. } => PushOutcome::Failed,
    }
}

/// Whether the attempt got as far as the vendor (its latency is the vendor's).
fn reached_vendor(outcome: &BindingOutcome) -> bool {
    !matches!(
        outcome,
        BindingOutcome::AlreadyAccepted
            | BindingOutcome::Skipped
            | BindingOutcome::Retryable {
                reason: FailureReason::Registry | FailureReason::Config,
                ..
            }
            | BindingOutcome::Terminal {
                reason: FailureReason::Registry | FailureReason::Config,
                ..
            }
    )
}

fn throttle_window(retry_after: Option<Duration>) -> Duration {
    retry_after
        .unwrap_or(DEFAULT_THROTTLE)
        .clamp(MIN_THROTTLE, MAX_THROTTLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::digest_coalescer::CoalescerLimits;
    use crate::domain::vendor_delivery::CAPABILITY_SERVICE_EXTENSION;
    use crate::domain::vendor_policy::{BreakerConfig, VendorLimits};
    use flare_proto::common::PushDisposition;
    use std::sync::Mutex;

    /// A vendor adapter that answers from a script and records which bindings it was given.
    struct Scripted {
        answers: Mutex<Vec<BindingOutcome>>,
        calls: Mutex<Vec<String>>,
    }

    impl Scripted {
        fn new(answers: Vec<BindingOutcome>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(answers),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl BindingDelivery for Scripted {
        async fn deliver_binding(
            &self,
            _: &Ctx,
            _: &PushTaskEnvelope,
            binding: &DevicePushToken,
        ) -> BindingOutcome {
            self.calls.lock().unwrap().push(binding.provider.clone());
            let mut answers = self.answers.lock().unwrap();
            if answers.is_empty() {
                BindingOutcome::Accepted
            } else {
                answers.remove(0)
            }
        }
    }

    fn binding(provider: &str) -> DevicePushToken {
        DevicePushToken {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
            platform: if provider == "apns" { "ios" } else { "android" }.into(),
            provider: provider.into(),
            token: format!("{provider}-token"),
            registration_id: format!("{provider}-generation"),
            registered_at_ms: 1,
            app_version: "2.0.1".into(),
            ..Default::default()
        }
    }

    fn task() -> PushTaskEnvelope {
        PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "d1".into(),
            conversation_id: "conv-1".into(),
            message_id: "m-1".into(),
            collapse_key: "c:0123456789abcdef0123456789abcdef".into(),
            disposition: PushDisposition::Loud as i32,
            expire_at: Some(i64::MAX),
            ..Default::default()
        }
    }

    fn runtime(limits: impl Fn(Vendor) -> VendorLimits) -> Arc<DeliveryRuntime> {
        let governance = Arc::new(VendorGovernance::new(limits));
        Arc::new(DeliveryRuntime {
            binding_order: crate::domain::vendor_delivery::DEFAULT_BINDING_ORDER.to_vec(),
            coalescer: Arc::new(DigestCoalescer::new(
                CoalescerLimits::default(),
                governance.clone(),
                VendorTrace::disabled(),
            )),
            governance,
            trace: VendorTrace::disabled(),
            usage: UsageRecorder::disabled(),
        })
    }

    fn device_push(
        runtime: Arc<DeliveryRuntime>,
        adapters: Vec<(Vendor, Arc<Scripted>)>,
    ) -> DevicePush {
        DevicePush::new(
            "t1".into(),
            adapters
                .into_iter()
                .map(|(vendor, adapter)| (vendor, adapter as Arc<dyn BindingDelivery>))
                .collect(),
            Vec::new(),
            runtime,
        )
    }

    fn retryable() -> BindingOutcome {
        BindingOutcome::retryable(
            FlareError::localized(ErrorCode::ServiceUnavailable, "controlled outage"),
            FailureReason::Server,
            "503",
        )
    }

    #[tokio::test]
    async fn only_the_first_binding_in_order_is_used_when_it_accepts() {
        let getui = Scripted::new(vec![]);
        let fcm = Scripted::new(vec![]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Getui, getui.clone()), (Vendor::Fcm, fcm.clone())],
        );
        let before = vendor_metrics::sent_count(Vendor::Getui, "2.0.1", false);
        push.deliver_bindings(
            &Ctx::default(),
            &task(),
            vec![binding("fcm"), binding("getui")],
        )
        .await
        .unwrap();
        assert_eq!((getui.calls(), fcm.calls()), (1, 0), "one push per device");
        assert!(vendor_metrics::sent_count(Vendor::Getui, "2.0.1", false) > before);
    }

    #[tokio::test]
    async fn a_retryable_failure_switches_to_the_next_binding() {
        let getui = Scripted::new(vec![retryable()]);
        let fcm = Scripted::new(vec![]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Getui, getui.clone()), (Vendor::Fcm, fcm.clone())],
        );
        let before = vendor_metrics::switch_count(Vendor::Getui, Vendor::Fcm);
        push.deliver_bindings(
            &Ctx::default(),
            &task(),
            vec![binding("getui"), binding("fcm")],
        )
        .await
        .unwrap();
        assert_eq!((getui.calls(), fcm.calls()), (1, 1));
        assert!(vendor_metrics::switch_count(Vendor::Getui, Vendor::Fcm) > before);

        // With no binding left the failure goes back to the broker.
        let getui = Scripted::new(vec![retryable()]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Getui, getui.clone())],
        );
        let error = push
            .deliver_bindings(&Ctx::default(), &task(), vec![binding("getui")])
            .await
            .unwrap_err();
        assert!(error.is_retryable());
    }

    #[tokio::test]
    async fn an_open_breaker_is_skipped_without_calling_the_vendor() {
        let limits = |vendor| VendorLimits {
            breaker: BreakerConfig {
                window: 1,
                min_calls: 1,
                ..BreakerConfig::default()
            },
            ..VendorLimits::defaults(vendor)
        };
        let shared = runtime(limits);
        let apns = Scripted::new(vec![retryable()]);
        let fcm = Scripted::new(vec![]);
        let push = device_push(
            shared.clone(),
            vec![(Vendor::Apns, apns.clone()), (Vendor::Fcm, fcm.clone())],
        );
        let mut ios_fcm = binding("fcm");
        ios_fcm.platform = "ios".into();
        // The first failure opens the APNs breaker (window of one call).
        push.deliver_bindings(
            &Ctx::default(),
            &task(),
            vec![binding("apns"), ios_fcm.clone()],
        )
        .await
        .unwrap();
        assert_eq!((apns.calls(), fcm.calls()), (1, 1));
        push.deliver_bindings(&Ctx::default(), &task(), vec![binding("apns"), ios_fcm])
            .await
            .unwrap();
        assert_eq!(
            (apns.calls(), fcm.calls()),
            (1, 2),
            "APNs is not called while open"
        );
    }

    #[tokio::test]
    async fn an_invalid_token_is_dropped_and_the_next_binding_tried() {
        let getui = Scripted::new(vec![BindingOutcome::InvalidToken {
            code: "20001".into(),
        }]);
        let fcm = Scripted::new(vec![]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Getui, getui.clone()), (Vendor::Fcm, fcm.clone())],
        );
        push.deliver_bindings(
            &Ctx::default(),
            &task(),
            vec![binding("getui"), binding("fcm")],
        )
        .await
        .unwrap();
        assert_eq!((getui.calls(), fcm.calls()), (1, 1));
    }

    #[tokio::test]
    async fn a_terminal_failure_ends_the_task() {
        let getui = Scripted::new(vec![BindingOutcome::terminal(
            FlareError::localized(ErrorCode::MessageDeliveryFailed, "rejected"),
            FailureReason::Config,
            "no_app_bundle",
        )]);
        let fcm = Scripted::new(vec![]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Getui, getui.clone()), (Vendor::Fcm, fcm.clone())],
        );
        let error = push
            .deliver_bindings(
                &Ctx::default(),
                &task(),
                vec![binding("getui"), binding("fcm")],
            )
            .await
            .unwrap_err();
        assert!(!error.is_retryable());
        assert_eq!(fcm.calls(), 0);
    }

    #[tokio::test]
    async fn throttling_everywhere_holds_the_task_for_a_digest() {
        let shared = runtime(VendorLimits::defaults);
        let getui = Scripted::new(vec![BindingOutcome::Throttled {
            retry_after: Some(Duration::from_secs(30)),
            code: "10005".into(),
            scope: ThrottleScope::Application,
        }]);
        let push = device_push(shared.clone(), vec![(Vendor::Getui, getui.clone())]);
        push.deliver_bindings(&Ctx::default(), &task(), vec![binding("getui")])
            .await
            .expect("held and acknowledged");
        assert_eq!(shared.coalescer.held_devices(), 1);
        assert!(
            shared
                .governance
                .throttled_until(Vendor::Getui, "t1", Instant::now())
                .is_some()
        );
        // While throttled the vendor is not called again: the next task is held as well.
        push.deliver_bindings(&Ctx::default(), &task(), vec![binding("getui")])
            .await
            .unwrap();
        assert_eq!(getui.calls(), 1);

        // The coalescer's own task is not held again: it is told the vendor is throttled.
        let mut held = task();
        held.headers.insert(
            crate::application::digest_coalescer::COALESCED_TASK_HEADER.into(),
            "1".into(),
        );
        let error = push
            .deliver_bindings(&Ctx::default(), &held, vec![binding("getui")])
            .await
            .unwrap_err();
        assert!(crate::domain::vendor_delivery::is_throttled_error(&error));
    }

    /// APNs answers 429 about one device token: that device waits (its task is held for a
    /// digest, its next task is held without calling APNs), while another device of the same
    /// application still goes through APNs.
    #[tokio::test]
    async fn a_binding_throttled_by_the_vendor_holds_only_that_device() {
        let shared = runtime(VendorLimits::defaults);
        let apns = Scripted::new(vec![BindingOutcome::Throttled {
            retry_after: Some(Duration::from_secs(30)),
            code: "TooManyRequests".into(),
            scope: ThrottleScope::Binding,
        }]);
        let push = device_push(shared.clone(), vec![(Vendor::Apns, apns.clone())]);
        let first = binding("apns");
        push.deliver_bindings(&Ctx::default(), &task(), vec![first.clone()])
            .await
            .expect("held and acknowledged");
        assert_eq!(apns.calls(), 1);
        assert_eq!(shared.coalescer.held_devices(), 1);
        let now = Instant::now();
        assert!(
            shared
                .governance
                .throttled_until(Vendor::Apns, "t1", now)
                .is_none(),
            "the application is not throttled"
        );
        assert!(
            shared
                .governance
                .binding_throttled_until(Vendor::Apns, BindingKey::of("t1", &first.token), now)
                .is_some()
        );
        // The same device: held again without calling APNs.
        push.deliver_bindings(&Ctx::default(), &task(), vec![first])
            .await
            .unwrap();
        assert_eq!(apns.calls(), 1);
        // Another device of the application: APNs takes it.
        let mut other = binding("apns");
        other.device_id = "d2".into();
        other.token = "apns-token-2".into();
        let mut other_task = task();
        other_task.device_id = "d2".into();
        push.deliver_bindings(&Ctx::default(), &other_task, vec![other])
            .await
            .unwrap();
        assert_eq!(apns.calls(), 2);
        assert_eq!(shared.coalescer.held_devices(), 1);
    }

    #[tokio::test]
    async fn a_registered_provider_without_credentials_is_not_a_silent_success() {
        let apns = Scripted::new(vec![]);
        let push = device_push(runtime(VendorLimits::defaults), vec![(Vendor::Apns, apns)]);
        let error = push
            .deliver_bindings(&Ctx::default(), &task(), vec![binding("fcm")])
            .await
            .unwrap_err();
        assert!(!error.is_retryable());
        // A disabled provider is an intentional no-send.
        let disabled = DevicePush::new(
            "t1".into(),
            Vec::new(),
            vec![Vendor::Fcm],
            runtime(VendorLimits::defaults),
        );
        disabled
            .deliver_bindings(&Ctx::default(), &task(), vec![binding("fcm")])
            .await
            .unwrap();
        // Another binding taking the task is enough.
        let apns = Scripted::new(vec![]);
        let push = device_push(
            runtime(VendorLimits::defaults),
            vec![(Vendor::Apns, apns.clone())],
        );
        push.deliver_bindings(
            &Ctx::default(),
            &task(),
            vec![binding("fcm"), binding("apns")],
        )
        .await
        .unwrap();
        assert_eq!(apns.calls(), 1);
    }

    #[test]
    fn the_sent_metric_knows_which_pushes_report_arrival() {
        let mut ios = binding("apns");
        ios.capabilities = vec![CAPABILITY_SERVICE_EXTENSION.into()];
        assert!(receipt_capable(Vendor::Apns, &ios, &task()));
        assert_eq!(throttle_window(None), DEFAULT_THROTTLE);
        assert_eq!(throttle_window(Some(Duration::ZERO)), MIN_THROTTLE);
        assert_eq!(
            throttle_window(Some(Duration::from_secs(86_400))),
            MAX_THROTTLE
        );
    }
}
