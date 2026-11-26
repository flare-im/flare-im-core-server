//! Runtime convergence: what one pass does with a binding, as pure rules. The worker
//! ([`crate::application::handler::plugin_reconcile`]) does the I/O.
//!
//! A deny is effective the moment the projection arrives: resolving a snapshot never waits for the
//! plugin. These rules only bring the plugin process in line, and keep its lease alive.
use flare_im_core_capability_core::plugin::PluginManifest;
use serde_json::Value;

use super::{AppPluginState, RuntimePatch, StateLifecycle};

/// Attempts for one desired revision before the reconciler stops retrying it.
pub const MAX_RUNTIME_ATTEMPTS: i32 = 10;
/// Clock-skew headroom between the database clock and plugin hosts.
pub const LEASE_SKEW_MARGIN_MS: i64 = 30_000;
/// How long an acknowledged state stays healthy.
pub const HEALTH_LEASE_MS: i64 = 240_000;
/// A lease with less than this left is renewed.
pub const RENEW_BEFORE_MS: i64 = 120_000;
/// Bound of a stored refusal reason.
pub const MAX_REASON_BYTES: usize = 512;

/// Delay before attempt `attempts + 1`: 10 s doubling per failure, at most 30 minutes.
pub fn runtime_backoff_ms(attempts: i32) -> i64 {
    10_000_i64
        .saturating_mul(1_i64 << attempts.clamp(0, 20))
        .min(30 * 60 * 1000)
}

/// A refusal text as stored: no control characters, at most [`MAX_REASON_BYTES`] bytes on a
/// character boundary.
pub fn bounded_reason(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars().map(|c| if c.is_control() { ' ' } else { c }) {
        if out.len() + ch.len_utf8() > MAX_REASON_BYTES {
            break;
        }
        out.push(ch);
    }
    out.trim().to_owned()
}

/// A binding as a pass sees it. `now_ms` is the database clock of the read.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconcileRow {
    pub state: AppPluginState,
    pub version_revoked: bool,
    pub version_suspended: bool,
    pub now_ms: i64,
}

/// What a pass sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// The desired revision (a new configuration, enable state, deny or authorization).
    Desired,
    /// The last acknowledged revision again: a lease renewal while the desired change waits
    /// (backoff, exhausted attempts) or after it converged.
    Acknowledged,
}

impl Target {
    pub fn name(self) -> &'static str {
        match self {
            Self::Desired => "desired",
            Self::Acknowledged => "acknowledged",
        }
    }
}

/// How a delivery ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Acknowledged,
    /// The host or plugin refused; retrying the same state cannot succeed.
    Rejected(String),
    /// Outcome unknown (transport, timeout, mismatched answer).
    Unavailable(&'static str),
}

impl Outcome {
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Acknowledged => None,
            Self::Rejected(_) => Some("runtime_rejected"),
            Self::Unavailable(code) => Some(*code),
        }
    }

    fn detail(&self) -> Option<String> {
        match self {
            Self::Rejected(reason) => Some(reason.clone()),
            _ => None,
        }
    }
}

/// The state one delivery carries, before it is signed. `configuration` is the effective
/// configuration in the clear.
#[derive(Debug, Clone, PartialEq)]
pub struct TenantDelivery {
    pub organization_id: String,
    pub deployment_id: String,
    pub app_id: String,
    pub grant_id: String,
    pub revision: u64,
    pub manifest_digest: String,
    pub configuration: Value,
    pub configuration_digest: String,
    pub enabled: bool,
    pub valid_until_ms: i64,
    pub validate_configuration: bool,
    pub approved_operations: Vec<String>,
    pub permissions: Vec<String>,
    pub data_scopes: Vec<String>,
}

impl ReconcileRow {
    fn suspended(&self) -> bool {
        self.state.suspended || self.version_suspended
    }

    /// Even a revoked version or a changed application owner can receive a deny.
    pub fn admitted(&self) -> bool {
        self.state.owned && !self.version_revoked
    }

    pub fn terminal(&self) -> bool {
        matches!(
            self.state.lifecycle,
            StateLifecycle::Revoked | StateLifecycle::Uninstalled
        )
    }

    /// What the plugin may do at the desired revision.
    pub fn enabled(&self) -> bool {
        self.state.enabled
            && !self.suspended()
            && self.admitted()
            && self.state.lifecycle == StateLifecycle::Installed
    }

    /// The desired revision differs from what the runtime holds.
    pub fn pending(&self) -> bool {
        self.state.runtime_revision != self.state.revision
    }

    /// The projection of the desired revision arrived, so its fields describe one state.
    pub fn deliverable(&self) -> bool {
        self.pending() && self.state.projected_revision == self.state.revision
    }

    /// Whether a pass delivers to this binding. A disabled target of an offline upgrade cannot
    /// validate its configuration against the old schema: its attempts stay untouched until the
    /// migration is confirmed. A pending deny still reconciles.
    pub fn is_due(&self) -> bool {
        use StateLifecycle::{Failed, Installed, Pending, Uninstalled};
        let state = &self.state;
        if !state.configured || state.lifecycle == Failed || state.projected_revision == 0 {
            return false;
        }
        if state.offline_hold
            && !state.enabled
            && !state.runtime_enabled
            && matches!(state.lifecycle, Pending | Installed)
        {
            return false;
        }
        let delivery = self.deliverable()
            && state.runtime_next_attempt_ms <= self.now_ms
            && (state.runtime_attempts < MAX_RUNTIME_ATTEMPTS
                || !state.enabled
                || self.suspended()
                || state.lifecycle != Installed);
        let renewal = state.runtime_revision > 0
            && state.runtime_healthy_until_ms < self.now_ms + RENEW_BEFORE_MS
            && (!self.pending() || (state.enabled && state.lifecycle == Installed))
            && (matches!(state.lifecycle, Pending | Installed)
                || (state.lifecycle == Uninstalled
                    && state
                        .export_until_ms
                        .is_some_and(|until| until > self.now_ms)));
        delivery || renewal
    }

    /// A change that keeps serving parks after its attempts; denies are always delivered. A state
    /// on its way to a deny is never renewed: the deny is sent instead.
    pub fn target(&self) -> Target {
        let enabled = self.enabled();
        let parked = enabled && self.state.runtime_attempts >= MAX_RUNTIME_ATTEMPTS;
        let due = !parked && self.state.runtime_next_attempt_ms <= self.now_ms;
        if self.deliverable() && (due || !enabled) {
            Target::Desired
        } else {
            Target::Acknowledged
        }
    }

    /// The delivery of `target` to `deployment_id`. `None`: nothing was acknowledged to renew.
    /// `effective` is the configuration of a [`Target::Desired`]. Digest errors are the caller's:
    /// it computes `configuration_digest` of what this names.
    pub fn delivery(
        &self,
        target: Target,
        deployment_id: &str,
        effective: &Value,
        digest_of: impl Fn(&Value) -> Option<String>,
    ) -> Option<TenantDelivery> {
        let state = &self.state;
        let enabled = self.enabled();
        let (revision, configuration, enabled) = match target {
            Target::Desired => (state.revision, effective, enabled),
            Target::Acknowledged => (
                state.runtime_revision,
                state
                    .runtime_configuration
                    .as_ref()
                    .or((!self.pending()).then_some(effective))?,
                state.runtime_enabled && enabled,
            ),
        };
        if revision <= 0 {
            return None;
        }
        Some(TenantDelivery {
            organization_id: state.organization_id.clone(),
            deployment_id: deployment_id.to_owned(),
            app_id: state.app_id.clone(),
            grant_id: state.binding_id.clone(),
            revision: revision as u64,
            manifest_digest: state.manifest_digest.clone(),
            configuration: configuration.clone(),
            configuration_digest: digest_of(configuration)?,
            enabled,
            // Hosts reject a lease beyond their own now + the maximum, and `now_ms` is the
            // database clock.
            valid_until_ms: self.now_ms + LEASE_MS,
            validate_configuration: self.admitted() && !self.terminal(),
            approved_operations: state.approved.operations.iter().cloned().collect(),
            permissions: state.approved.permissions.iter().cloned().collect(),
            data_scopes: state.approved.data_scopes.iter().cloned().collect(),
        })
    }

    /// The facts a finished delivery leaves. A tenant's configuration failure must not poison
    /// others on the deployment: only a successful observation renews a lease, and a failed row
    /// denies its own projection at once.
    pub fn after(
        &self,
        target: Target,
        delivery: &TenantDelivery,
        outcome: &Outcome,
    ) -> RuntimePatch {
        let now = self.now_ms;
        let admitted = self.admitted();
        let healthy = match outcome {
            Outcome::Acknowledged if admitted => now + HEALTH_LEASE_MS,
            _ => 0,
        };
        let code = outcome.code().map(str::to_owned);
        let detail = outcome.detail();
        let pending = self.pending();
        match (target, outcome) {
            (Target::Desired, Outcome::Acknowledged) => {
                let refused = !(admitted || self.terminal());
                RuntimePatch {
                    deployment_id: Some(Some(delivery.deployment_id.clone())),
                    runtime_revision: Some(self.state.revision),
                    runtime_configuration_digest: Some(Some(delivery.configuration_digest.clone())),
                    runtime_configuration: Some(Some(delivery.configuration.clone())),
                    runtime_enabled: Some(delivery.enabled),
                    runtime_healthy_until_ms: Some(healthy),
                    runtime_error: Some(refused.then(|| "authorization_unavailable".to_owned())),
                    runtime_error_detail: Some(None),
                    runtime_attempts: Some(0),
                    runtime_next_attempt_ms: Some(0),
                    runtime_enabled_digest: delivery
                        .enabled
                        .then(|| Some(delivery.configuration_digest.clone())),
                    ..RuntimePatch::default()
                }
            }
            (Target::Desired, failure) => {
                let attempts = self.attempts_after(failure);
                RuntimePatch {
                    runtime_error: Some(code),
                    runtime_error_detail: Some(detail),
                    runtime_attempts: Some(attempts),
                    runtime_next_attempt_ms: Some(now + runtime_backoff_ms(attempts)),
                    runtime_healthy_until_ms: (!delivery.enabled).then_some(0),
                    // Never acknowledged: the installation fails. An installed one keeps its
                    // acknowledged revision serving; denies keep being delivered.
                    lifecycle: (self.exhausted_by(failure)
                        && self.state.lifecycle == StateLifecycle::Pending)
                        .then_some(StateLifecycle::Failed),
                    ..RuntimePatch::default()
                }
            }
            (Target::Acknowledged, Outcome::Acknowledged) => RuntimePatch {
                runtime_healthy_until_ms: Some(healthy),
                runtime_error: (!pending).then(|| {
                    (!admitted && !self.terminal()).then(|| "authorization_unavailable".to_owned())
                }),
                runtime_error_detail: (!pending).then_some(None),
                ..RuntimePatch::default()
            },
            // A pending change keeps its own error; the zero lease is the signal.
            (Target::Acknowledged, _) => RuntimePatch {
                runtime_healthy_until_ms: Some(0),
                runtime_error: (!pending).then_some(code),
                runtime_error_detail: (!pending).then_some(detail),
                ..RuntimePatch::default()
            },
        }
    }

    /// A refusal cannot succeed by repetition: it spends the whole budget.
    fn attempts_after(&self, failure: &Outcome) -> i32 {
        if matches!(failure, Outcome::Rejected(_)) {
            MAX_RUNTIME_ATTEMPTS
        } else {
            (self.state.runtime_attempts + 1).min(MAX_RUNTIME_ATTEMPTS)
        }
    }

    /// This failure spent the last attempt of the revision.
    pub fn exhausted_by(&self, failure: &Outcome) -> bool {
        self.attempts_after(failure) >= MAX_RUNTIME_ATTEMPTS
            && self.state.runtime_attempts < MAX_RUNTIME_ATTEMPTS
    }
}

/// Lease of a delivered state: the host's maximum less the skew margin.
const LEASE_MS: i64 =
    flare_im_core_capability_core::plugin::runtime::MAX_PROJECTION_LEASE_MS - LEASE_SKEW_MARGIN_MS;

/// Whether the deployment may serve an enabled binding of `manifest`: a version that promises data
/// export (`admin.lifecycle.uninstall.export`) needs a deployment reporting `flare.plugin.data.v1`.
/// `Err` is the refusal recorded on the binding.
pub fn check_protocols(manifest: &PluginManifest, reported: &[String]) -> Result<(), String> {
    let exports = manifest
        .admin
        .as_ref()
        .and_then(|admin| admin.lifecycle.as_ref())
        .and_then(|lifecycle| lifecycle.uninstall.as_ref())
        .is_some_and(|uninstall| uninstall.export);
    let data = flare_im_core_capability_core::plugin::data::OPERATION;
    if exports && !reported.iter().any(|protocol| protocol == data) {
        return Err(format!(
            "the deployment does not serve {data}, which the version's data export needs"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{ApprovedGrant, DesiredAppState};
    use super::*;
    use serde_json::json;

    const NOW: i64 = 1_000_000;

    fn row(change: impl FnOnce(&mut AppPluginState)) -> ReconcileRow {
        let mut state = DesiredAppState {
            app_id: "201".into(),
            binding_id: "b1".into(),
            plugin_id: "example.poll".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: "sha256:x".into(),
            revision: 3,
            enabled: true,
            configured: true,
            configuration: json!({"limit": 5}),
            secrets_from: None,
        }
        .applied_to(None)
        .unwrap();
        state.organization_id = "101".into();
        state.lifecycle = StateLifecycle::Installed;
        state.owned = true;
        state.approved = ApprovedGrant::default();
        state.projected_revision = 3;
        state.runtime_revision = 3;
        state.runtime_enabled = true;
        state.runtime_healthy_until_ms = NOW + 200_000;
        state.runtime_configuration = Some(json!({"limit": 4}));
        change(&mut state);
        ReconcileRow {
            state,
            version_revoked: false,
            version_suspended: false,
            now_ms: NOW,
        }
    }

    fn digest(_: &Value) -> Option<String> {
        Some("sha256:d".into())
    }

    fn delivery(row: &ReconcileRow, target: Target) -> TenantDelivery {
        row.delivery(target, "d1", &json!({"limit": 5}), digest)
            .unwrap()
    }

    #[test]
    fn a_converged_healthy_binding_is_left_alone() {
        assert!(!row(|_| {}).is_due());
    }

    #[test]
    fn a_pending_revision_waits_for_its_backoff_and_parks_after_its_attempts() {
        assert!(row(|s| s.runtime_revision = 2).is_due());
        assert!(
            !row(|s| {
                s.runtime_revision = 2;
                s.runtime_next_attempt_ms = NOW + 1;
            })
            .is_due()
        );
        let spent = |s: &mut AppPluginState| {
            s.runtime_revision = 2;
            s.runtime_attempts = MAX_RUNTIME_ATTEMPTS;
        };
        assert!(!row(spent).is_due());
        // A deny is always delivered.
        assert!(
            row(|s| {
                spent(s);
                s.enabled = false;
            })
            .is_due()
        );
    }

    #[test]
    fn a_revision_waits_for_its_projection() {
        let ahead = row(|s| {
            s.revision = 4;
            s.projected_revision = 3;
        });
        assert!(!ahead.is_due());
        assert_eq!(ahead.target(), Target::Acknowledged);
    }

    #[test]
    fn nothing_is_delivered_before_the_first_projection() {
        assert!(
            !row(|s| {
                s.runtime_revision = 0;
                s.projected_revision = 0;
            })
            .is_due()
        );
    }

    #[test]
    fn a_lease_near_expiry_is_renewed_only_where_something_may_still_serve() {
        let near = |s: &mut AppPluginState| s.runtime_healthy_until_ms = NOW + 1000;
        assert!(row(near).is_due());
        let gone = |s: &mut AppPluginState| {
            near(s);
            s.lifecycle = StateLifecycle::Uninstalled;
            s.enabled = false;
        };
        assert!(
            !row(|s| {
                gone(s);
                s.export_until_ms = Some(NOW - 1);
            })
            .is_due()
        );
        assert!(
            row(|s| {
                gone(s);
                s.export_until_ms = Some(NOW + 1);
            })
            .is_due()
        );
    }

    #[test]
    fn an_offline_target_waits_for_its_migration() {
        let target = |s: &mut AppPluginState| {
            s.runtime_revision = 0;
            s.enabled = false;
            s.runtime_enabled = false;
            s.lifecycle = StateLifecycle::Pending;
        };
        assert!(row(target).is_due());
        assert!(
            !row(|s| {
                target(s);
                s.offline_hold = true;
            })
            .is_due()
        );
    }

    #[test]
    fn suspension_or_a_lost_owner_turns_a_delivery_into_a_deny() {
        let suspended = row(|s| {
            s.revision = 4;
            s.projected_revision = 4;
            s.suspended = true;
        });
        assert!(!delivery(&suspended, Target::Desired).enabled);
        let orphan = row(|s| s.owned = false);
        assert!(!delivery(&orphan, Target::Acknowledged).enabled);
        let mut revoked = row(|_| {});
        revoked.version_revoked = true;
        assert!(!revoked.enabled());
    }

    #[test]
    fn a_renewal_resends_what_was_acknowledged_not_the_desired_change() {
        let waiting = row(|s| {
            s.revision = 4;
            s.projected_revision = 4;
            s.runtime_next_attempt_ms = NOW + 1;
        });
        assert_eq!(waiting.target(), Target::Acknowledged);
        let sent = delivery(&waiting, Target::Acknowledged);
        assert_eq!(sent.revision, 3);
        assert_eq!(sent.configuration, json!({"limit": 4}));
    }

    #[test]
    fn a_deny_is_sent_even_while_backing_off() {
        let deny = row(|s| {
            s.revision = 4;
            s.projected_revision = 4;
            s.enabled = false;
            s.runtime_next_attempt_ms = NOW + 1;
        });
        assert_eq!(deny.target(), Target::Desired);
    }

    #[test]
    fn an_acknowledgement_records_the_effective_configuration_and_a_fresh_lease() {
        let change = row(|s| {
            s.revision = 4;
            s.projected_revision = 4;
        });
        let mut state = change.state.clone();
        let sent = delivery(&change, Target::Desired);
        change
            .after(Target::Desired, &sent, &Outcome::Acknowledged)
            .apply(&mut state);
        assert_eq!(state.runtime_revision, 4);
        assert_eq!(state.runtime_configuration, Some(json!({"limit": 5})));
        assert_eq!(state.runtime_healthy_until_ms, NOW + HEALTH_LEASE_MS);
        assert_eq!(state.runtime_enabled_digest.as_deref(), Some("sha256:d"));
    }

    #[test]
    fn a_refusal_spends_the_budget_and_fails_a_binding_that_never_installed() {
        let first = row(|s| {
            s.runtime_revision = 0;
            s.runtime_configuration = None;
            s.enabled = false;
            s.lifecycle = StateLifecycle::Pending;
        });
        let sent = delivery(&first, Target::Desired);
        let refused = Outcome::Rejected("bad configuration".into());
        let mut state = first.state.clone();
        first
            .after(Target::Desired, &sent, &refused)
            .apply(&mut state);
        assert_eq!(state.runtime_attempts, MAX_RUNTIME_ATTEMPTS);
        assert_eq!(state.lifecycle, StateLifecycle::Failed);
        assert_eq!(state.runtime_error.as_deref(), Some("runtime_rejected"));
        assert_eq!(
            state.runtime_error_detail.as_deref(),
            Some("bad configuration")
        );
    }

    #[test]
    fn a_failed_renewal_keeps_the_error_of_a_pending_change() {
        let waiting = row(|s| {
            s.revision = 4;
            s.projected_revision = 4;
            s.runtime_error = Some("runtime_rejected".into());
        });
        let sent = delivery(&waiting, Target::Acknowledged);
        let mut state = waiting.state.clone();
        waiting
            .after(
                Target::Acknowledged,
                &sent,
                &Outcome::Unavailable("runtime_unavailable"),
            )
            .apply(&mut state);
        assert_eq!(state.runtime_healthy_until_ms, 0);
        assert_eq!(state.runtime_error.as_deref(), Some("runtime_rejected"));
    }

    #[test]
    fn the_backoff_doubles_up_to_half_an_hour() {
        assert_eq!(runtime_backoff_ms(0), 10_000);
        assert_eq!(runtime_backoff_ms(3), 80_000);
        assert_eq!(runtime_backoff_ms(MAX_RUNTIME_ATTEMPTS), 30 * 60 * 1000);
    }

    #[test]
    fn a_reason_is_stored_bounded_and_without_control_characters() {
        assert_eq!(bounded_reason("a\nb"), "a b");
        assert_eq!(bounded_reason(&"x".repeat(600)).len(), MAX_REASON_BYTES);
    }
}
