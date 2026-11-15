//! One push per task per device: which vendor binding a task goes through, and what one
//! attempt through one binding came to.
//!
//! A device can be bound through more than one vendor (an Android phone through Getui and FCM,
//! an iPhone through APNs and, rarely, FCM). A task goes out through ONE of them: the first
//! binding in the configured preference order whose vendor is available. Sending the same task
//! through two vendors would ring the device twice; the next binding is only tried when the
//! current one could not take the task (breaker open, throttled, a failure a retry may fix, a
//! dead token, or nothing this vendor can carry for the task).
//!
//! Everything here is free of IO; the selection with its governance lives in
//! `application::binding_delivery`.

use std::time::Duration;

use async_trait::async_trait;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_proto::common::PushDisposition;
use flare_server_core::{ErrorCode, FlareError};

use crate::domain::offline_delivery::task_disposition;

/// Binding capability: the iOS app ships a notification service extension.
pub const CAPABILITY_SERVICE_EXTENSION: &str = "serviceExtension";
/// Binding capability: the Android app receives FCM data messages with its own code.
pub const CAPABILITY_DATA_MESSAGES: &str = "dataMessages";

/// A vendor push channel a device can be bound through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Vendor {
    Apns,
    Fcm,
    Getui,
}

impl Vendor {
    pub const ALL: [Vendor; 3] = [Vendor::Apns, Vendor::Fcm, Vendor::Getui];

    /// The provider name bindings, credentials, metrics and traces use.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Apns => "apns",
            Self::Fcm => "fcm",
            Self::Getui => "getui",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        let name = name.trim();
        Self::ALL
            .into_iter()
            .find(|vendor| vendor.as_str().eq_ignore_ascii_case(name))
    }
}

/// Binding preference when none is configured: APNs reaches an iPhone directly, Getui reaches
/// Android phones in China through the phone maker's channel, FCM covers the rest.
pub const DEFAULT_BINDING_ORDER: [Vendor; 3] = [Vendor::Apns, Vendor::Getui, Vendor::Fcm];

/// The configured binding order (`[services.push_worker] binding_order`). Names must be known
/// vendors, each at most once. Vendors left out follow the listed ones in their default order:
/// an incomplete list never silently stops a channel (an application turns a vendor off with
/// `disabled_providers` in its credentials).
pub fn resolve_binding_order(names: &[String]) -> Result<Vec<Vendor>, FlareError> {
    let mut order = Vec::with_capacity(Vendor::ALL.len());
    for name in names {
        let vendor = Vendor::parse(name).ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                "push_worker.binding_order lists an unknown vendor; use apns, fcm or getui",
            )
        })?;
        if order.contains(&vendor) {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "push_worker.binding_order lists a vendor twice",
            ));
        }
        order.push(vendor);
    }
    for vendor in DEFAULT_BINDING_ORDER {
        if !order.contains(&vendor) {
            order.push(vendor);
        }
    }
    Ok(order)
}

/// The device's usable bindings in preference order, at most one per vendor. Bindings of a
/// provider this build does not know, or unusable ones (no token, no registration), are left
/// out.
pub fn order_bindings(
    bindings: Vec<DevicePushToken>,
    order: &[Vendor],
) -> Vec<(Vendor, DevicePushToken)> {
    let mut ranked: Vec<(usize, Vendor, DevicePushToken)> = bindings
        .into_iter()
        .filter_map(|binding| {
            let vendor = Vendor::parse(&binding.provider)?;
            if !binding.usable_for_provider(vendor.as_str()) {
                return None;
            }
            let rank = order
                .iter()
                .position(|candidate| *candidate == vendor)
                .unwrap_or(order.len() + vendor as usize);
            Some((rank, vendor, binding))
        })
        .collect();
    // Stable: of two rows for one vendor (the registry keeps one) the first listed wins.
    ranked.sort_by_key(|(rank, ..)| *rank);
    ranked.dedup_by_key(|(_, vendor, _)| *vendor);
    ranked
        .into_iter()
        .map(|(_, vendor, binding)| (vendor, binding))
        .collect()
}

/// Why an attempt through a binding failed. The label of
/// `flare_push_vendor_failures_total{reason}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// The vendor answered with a server error, or with an answer it should not give.
    Server,
    /// No answer: connection, TLS, deadline. Whether the vendor accepted is unknown.
    Transport,
    /// The application's vendor credentials were refused or could not be refreshed.
    Auth,
    /// The vendor refused this request (a client error other than throttling or a dead token).
    Rejected,
    /// The binding or the credentials cannot address this device (no app bundle, a topic the
    /// credentials do not allow, a malformed token, an undeliverable task).
    Config,
    /// The binding registry (receipts, current-binding check, removal) is unavailable.
    Registry,
    /// No concurrency slot for the vendor within the attempt budget.
    Saturated,
}

impl FailureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Transport => "transport",
            Self::Auth => "auth",
            Self::Rejected => "rejected",
            Self::Config => "config",
            Self::Registry => "registry",
            Self::Saturated => "saturated",
        }
    }

    /// Only answers about the vendor's own health move its breaker. A refused credential or a
    /// rejected payload belongs to one application or one task: counting it would let one
    /// tenant's broken credential cut every tenant off the vendor.
    pub const fn counts_against_vendor(self) -> bool {
        matches!(self, Self::Server | Self::Transport)
    }
}

/// What one delivery attempt through one binding came to.
#[derive(Debug)]
pub enum BindingOutcome {
    /// The vendor accepted the push; the acceptance receipt is recorded.
    Accepted,
    /// An earlier delivery of the same task through this binding was already accepted
    /// (a broker redelivery); nothing was sent.
    AlreadyAccepted,
    /// Nothing to send through this binding: the vendor cannot carry what the task asks for on
    /// this device, or the binding was replaced meanwhile. The next binding may carry it.
    Skipped,
    /// The vendor confirmed the token is dead; the binding is removed.
    InvalidToken { code: String },
    /// The vendor asked to slow down (HTTP 429, quota exhausted), with its Retry-After when it
    /// gave one. `scope` says whom the answer is about.
    Throttled {
        retry_after: Option<Duration>,
        code: String,
        scope: ThrottleScope,
    },
    /// Failed in a way a later attempt may fix.
    Retryable {
        error: FlareError,
        reason: FailureReason,
        code: String,
    },
    /// Failed in a way no retry fixes.
    Terminal {
        error: FlareError,
        reason: FailureReason,
        code: String,
    },
}

/// Whom a vendor's throttling answer is about, and so who waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleScope {
    /// The application's credential (FCM `QUOTA_EXCEEDED`, Getui call frequency): every device
    /// of the application waits for the vendor.
    Application,
    /// One device token: APNs answers 429 when too many requests went to the same device token
    /// (Apple, "Handling notification responses from APNs"). Only that binding waits; the
    /// application's other devices keep using the vendor.
    Binding,
}

impl BindingOutcome {
    pub fn retryable(error: FlareError, reason: FailureReason, code: impl Into<String>) -> Self {
        Self::Retryable {
            error,
            reason,
            code: code.into(),
        }
    }

    pub fn terminal(error: FlareError, reason: FailureReason, code: impl Into<String>) -> Self {
        Self::Terminal {
            error,
            reason,
            code: code.into(),
        }
    }

    /// A failure of the binding registry, not of the vendor.
    pub fn registry(error: FlareError) -> Self {
        Self::retryable(error, FailureReason::Registry, "registry")
    }

    /// An error from a step that is not the vendor call (planning, signing, the registry),
    /// classified by its retryability.
    pub fn from_error(error: FlareError, reason: FailureReason, code: &str) -> Self {
        if error.is_retryable() {
            Self::retryable(error, reason, code)
        } else {
            Self::terminal(error, reason, code)
        }
    }
}

/// A vendor adapter's delivery through one binding.
#[async_trait]
pub trait BindingDelivery: Send + Sync {
    /// Deliver `task` through `binding`, one of the task's device's bindings for this vendor.
    /// Checks the binding's receipt first and records it on acceptance. Never sends through
    /// another binding and never retries by itself: switching and retrying are the caller's.
    async fn deliver_binding(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        binding: &DevicePushToken,
    ) -> BindingOutcome;
}

/// The vendor (or the local rate limit) asked to slow down. Retryable, and distinct from other
/// retryable failures: the digest coalescer waits for the vendor instead of counting it as a
/// failed flush.
pub fn throttled_error() -> FlareError {
    FlareError::localized(ErrorCode::ResourceExhausted, "push vendor throttled")
}

/// Whether an error is [`throttled_error`].
pub fn is_throttled_error(error: &FlareError) -> bool {
    error.code() == Some(ErrorCode::ResourceExhausted)
}

/// Apple devices: their notifications are shown by the system, or by the notification service
/// extension when the app ships one.
pub fn is_apple_platform(platform: &str) -> bool {
    matches!(
        platform.trim().to_ascii_lowercase().as_str(),
        "ios" | "ipados" | "iphone" | "ipad" | "macos"
    )
}

pub fn binding_has_capability(binding: &DevicePushToken, capability: &str) -> bool {
    binding
        .capabilities
        .iter()
        .any(|declared| declared == capability)
}

/// Dispositions that show (or replace) a notification the user sees: the iOS notification
/// service extension runs for these.
pub fn is_alert_disposition(task: &PushTaskEnvelope) -> bool {
    matches!(
        task_disposition(task),
        Some(PushDisposition::Loud | PushDisposition::Digest | PushDisposition::RecallReplace)
    )
}

/// Whether the device reports arrival for this push: an iOS binding with a notification service
/// extension and an alert disposition, or an Android FCM binding that receives data messages.
/// The denominator of the arrival rate (`receipt_capable` on the sent metric).
pub fn receipt_capable(vendor: Vendor, binding: &DevicePushToken, task: &PushTaskEnvelope) -> bool {
    let apple = is_apple_platform(&binding.platform);
    (apple
        && binding_has_capability(binding, CAPABILITY_SERVICE_EXTENSION)
        && is_alert_disposition(task))
        || (vendor == Vendor::Fcm
            && !apple
            && binding_has_capability(binding, CAPABILITY_DATA_MESSAGES))
}

/// The app version as a trace field: at most 32 characters of `[0-9A-Za-z._+-]`, anything else
/// (empty included) is `other`. Arrival receipts use the same rule (one helper in the service
/// kit). The sent metric bounds the label further: see `vendor_metrics::record_sent`.
pub fn app_version_label(raw: &str) -> &str {
    flare_im_service_kit::metrics::sanitized_app_version(raw)
}

/// A vendor code for traces: short, printable, no vendor body. Anything else is dropped.
pub fn trace_code(raw: &str) -> Option<String> {
    let raw = raw.trim();
    (!raw.is_empty()
        && raw.len() <= 40
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-')))
    .then(|| raw.to_string())
}

/// `Retry-After` in seconds, as APNs and FCM send it. HTTP dates are not used by either.
pub fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    value?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|seconds| Duration::from_secs(seconds.min(86_400)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(provider: &str, device: &str) -> DevicePushToken {
        DevicePushToken {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: device.into(),
            platform: "android".into(),
            provider: provider.into(),
            token: format!("{provider}-token"),
            registration_id: format!("{provider}-generation"),
            registered_at_ms: 1,
            ..Default::default()
        }
    }

    #[test]
    fn the_order_defaults_to_apns_getui_fcm_and_completes_a_partial_list() {
        assert_eq!(
            resolve_binding_order(&[]).unwrap(),
            vec![Vendor::Apns, Vendor::Getui, Vendor::Fcm]
        );
        assert_eq!(
            resolve_binding_order(&["FCM".to_string()]).unwrap(),
            vec![Vendor::Fcm, Vendor::Apns, Vendor::Getui]
        );
        assert!(resolve_binding_order(&["hms".to_string()]).is_err());
        assert!(resolve_binding_order(&["fcm".to_string(), "fcm".to_string()]).is_err());
    }

    #[test]
    fn bindings_are_ordered_by_preference_and_unusable_ones_are_left_out() {
        let mut unusable = binding("apns", "d1");
        unusable.token.clear();
        let ordered = order_bindings(
            vec![
                binding("fcm", "d1"),
                binding("getui", "d1"),
                unusable,
                binding("hms", "d1"),
            ],
            &DEFAULT_BINDING_ORDER,
        );
        let vendors: Vec<_> = ordered.iter().map(|(vendor, _)| *vendor).collect();
        assert_eq!(vendors, vec![Vendor::Getui, Vendor::Fcm]);
        let ordered = order_bindings(
            vec![binding("getui", "d1"), binding("fcm", "d1")],
            &[Vendor::Fcm, Vendor::Getui, Vendor::Apns],
        );
        assert_eq!(ordered[0].0, Vendor::Fcm);
    }

    #[test]
    fn receipt_capability_follows_platform_capability_and_disposition() {
        let mut ios = binding("apns", "d1");
        ios.platform = "ios".into();
        let mut task = PushTaskEnvelope {
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        };
        assert!(!receipt_capable(Vendor::Apns, &ios, &task));
        ios.capabilities = vec![CAPABILITY_SERVICE_EXTENSION.into()];
        assert!(receipt_capable(Vendor::Apns, &ios, &task));
        task.disposition = PushDisposition::Quiet as i32;
        assert!(!receipt_capable(Vendor::Apns, &ios, &task));

        let mut android = binding("fcm", "d2");
        assert!(!receipt_capable(Vendor::Fcm, &android, &task));
        android.capabilities = vec![CAPABILITY_DATA_MESSAGES.into()];
        assert!(receipt_capable(Vendor::Fcm, &android, &task));
        let mut getui = binding("getui", "d2");
        getui.capabilities = vec![CAPABILITY_DATA_MESSAGES.into()];
        assert!(!receipt_capable(Vendor::Getui, &getui, &task));
    }

    #[test]
    fn app_versions_and_codes_are_bounded_labels() {
        assert_eq!(app_version_label("1.4.0+build.7"), "1.4.0+build.7");
        assert_eq!(app_version_label(" 1.4.0 "), "1.4.0");
        assert_eq!(app_version_label(""), "other");
        assert_eq!(app_version_label("1.4 beta"), "other");
        assert_eq!(app_version_label(&"9".repeat(33)), "other");
        assert_eq!(
            trace_code("BadDeviceToken").as_deref(),
            Some("BadDeviceToken")
        );
        assert_eq!(trace_code("503").as_deref(), Some("503"));
        assert_eq!(trace_code("not a code"), None);
        assert_eq!(
            parse_retry_after(Some(" 30 ")),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after(Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            None
        );
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn only_vendor_health_moves_the_breaker() {
        assert!(FailureReason::Server.counts_against_vendor());
        assert!(FailureReason::Transport.counts_against_vendor());
        for reason in [
            FailureReason::Auth,
            FailureReason::Rejected,
            FailureReason::Config,
            FailureReason::Registry,
            FailureReason::Saturated,
        ] {
            assert!(!reason.counts_against_vendor(), "{reason:?}");
        }
        assert!(is_throttled_error(&throttled_error()));
        assert!(throttled_error().is_retryable());
    }
}
