//! `AppPluginState`: what an application wants of one binding, what the runtime last confirmed,
//! and its configuration. The configuration has one home, here; secret values are sealed at rest by
//! the store, so this type always holds them in the clear.
//!
//! Two writers, one per kind of field. `plugin_config.put` carries the configuration; the platform's
//! projection carries everything else ([`AppPluginState::apply_projected`]), so a deny needs no call
//! to Core. A revision is delivered only once the projection of that revision arrived: the fields
//! of both writers then describe one desired state.
use std::collections::{BTreeMap, BTreeSet};

use flare_im_core_capability_core::plugin::runtime::{
    BindingLifecycle, ProjectedBinding, configuration_digest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{PluginRegistryError, RegistryResult};

/// [`BindingLifecycle`], and `failed`: a binding that never installed and ran out of attempts. Only
/// a new revision takes it out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateLifecycle {
    Pending,
    Installed,
    Uninstalled,
    Revoked,
    Failed,
}

impl StateLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Installed => "installed",
            Self::Uninstalled => "uninstalled",
            Self::Revoked => "revoked",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "installed" => Some(Self::Installed),
            "uninstalled" => Some(Self::Uninstalled),
            "revoked" => Some(Self::Revoked),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

impl From<BindingLifecycle> for StateLifecycle {
    fn from(value: BindingLifecycle) -> Self {
        match value {
            BindingLifecycle::Pending => Self::Pending,
            BindingLifecycle::Installed => Self::Installed,
            BindingLifecycle::Uninstalled => Self::Uninstalled,
            BindingLifecycle::Revoked => Self::Revoked,
        }
    }
}

/// What a binding is approved to do, as the plugin process receives it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ApprovedGrant {
    pub operations: BTreeSet<String>,
    pub permissions: BTreeSet<String>,
    pub data_scopes: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppPluginState {
    pub app_id: String,
    pub binding_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub revision: i64,
    pub enabled: bool,
    pub configured: bool,
    pub configuration: Value,
    pub deployment_id: Option<String>,
    pub runtime_revision: i64,
    pub runtime_configuration_digest: Option<String>,
    pub runtime_enabled: bool,
    pub runtime_healthy_until_ms: i64,
    pub runtime_error: Option<String>,
    pub runtime_error_detail: Option<String>,
    pub runtime_configuration: Option<Value>,
    pub runtime_attempts: i32,
    pub runtime_next_attempt_ms: i64,
    pub runtime_enabled_digest: Option<String>,
    pub organization_id: String,
    pub lifecycle: StateLifecycle,
    pub suspended: bool,
    pub owned: bool,
    pub offline_hold: bool,
    pub export_until_ms: Option<i64>,
    pub approved: ApprovedGrant,
    pub features: BTreeMap<String, bool>,
    /// The revision of the last projection that named this binding; 0 before the first.
    pub projected_revision: i64,
    pub updated_at: i64,
}

impl AppPluginState {
    /// Takes what the projection says of this binding; whether anything changed. An older
    /// projection, or one of another release, changes nothing: `plugin_config.put` may have run
    /// ahead of the outbox, and then its revision stays until the projection catches up.
    pub fn apply_projected(&mut self, binding: &ProjectedBinding) -> bool {
        let revision = binding.revision as i64;
        if revision < self.projected_revision
            || self.plugin_id != binding.plugin_id
            || self.plugin_version != binding.plugin_version
            || self.manifest_digest != binding.manifest_digest
        {
            return false;
        }
        let before = self.clone();
        let advanced = revision > self.projected_revision;
        if revision >= self.revision {
            self.revision = revision;
            self.enabled = binding.enabled;
            self.configured = binding.configured;
        }
        // A new desired revision gets a fresh retry budget.
        if advanced {
            self.runtime_attempts = 0;
            self.runtime_next_attempt_ms = 0;
            self.runtime_error = None;
            self.runtime_error_detail = None;
        }
        // The migration ran: deliver now, not at the backoff of an attempt made before it.
        if self.offline_hold && !binding.hold {
            self.runtime_next_attempt_ms = 0;
        }
        // The same revision again is a lease renewal: it does not restart a failed installation.
        if advanced || self.lifecycle != StateLifecycle::Failed {
            self.lifecycle = binding.lifecycle.into();
        }
        self.organization_id = binding.organization_id.clone();
        self.suspended = binding.suspended;
        self.owned = binding.owned;
        self.offline_hold = binding.hold;
        self.export_until_ms = binding.export_until_ms;
        self.approved = ApprovedGrant {
            operations: binding.approved_operations.clone(),
            permissions: binding.approved_permissions.clone(),
            data_scopes: binding.approved_data_scopes.clone(),
        };
        self.features = binding.features.clone();
        self.projected_revision = revision;
        *self != before
    }
}

/// `plugin_config.put`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DesiredAppState {
    pub app_id: String,
    pub binding_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub revision: i64,
    pub enabled: bool,
    pub configured: bool,
    pub configuration: Value,
    /// The binding whose stored secrets the configuration's masked values refer to, when it is not
    /// this one: an upgrade carries the configuration to a new binding of the same plugin.
    #[serde(default)]
    pub secrets_from: Option<String>,
}

impl DesiredAppState {
    pub fn validate(&self) -> RegistryResult<()> {
        if self.revision <= 0 || self.binding_id.is_empty() || self.binding_id.len() > 128 {
            return Err(PluginRegistryError::Invalid(
                "invalid binding or revision".into(),
            ));
        }
        // An object within the configuration bound.
        configuration_digest(&self.configuration)
            .map(|_| ())
            .map_err(|_| PluginRegistryError::Invalid("invalid configuration".into()))
    }

    /// A new row, or `current` with this desired state. A lower revision is refused; the same
    /// revision is the retry of a write whose answer was lost.
    pub fn applied_to(&self, current: Option<AppPluginState>) -> RegistryResult<AppPluginState> {
        let Some(mut state) = current else {
            return Ok(AppPluginState {
                app_id: self.app_id.clone(),
                binding_id: self.binding_id.clone(),
                plugin_id: self.plugin_id.clone(),
                plugin_version: self.plugin_version.clone(),
                manifest_digest: self.manifest_digest.clone(),
                revision: self.revision,
                enabled: self.enabled,
                configured: self.configured,
                configuration: self.configuration.clone(),
                deployment_id: None,
                runtime_revision: 0,
                runtime_configuration_digest: None,
                runtime_enabled: false,
                runtime_healthy_until_ms: 0,
                runtime_error: None,
                runtime_error_detail: None,
                runtime_configuration: None,
                runtime_attempts: 0,
                runtime_next_attempt_ms: 0,
                runtime_enabled_digest: None,
                organization_id: String::new(),
                lifecycle: StateLifecycle::Pending,
                suspended: false,
                owned: false,
                offline_hold: false,
                export_until_ms: None,
                approved: ApprovedGrant::default(),
                features: BTreeMap::new(),
                projected_revision: 0,
                updated_at: 0,
            });
        };
        if state.plugin_id != self.plugin_id
            || state.plugin_version != self.plugin_version
            || state.manifest_digest != self.manifest_digest
        {
            return Err(PluginRegistryError::Conflict(
                "a binding keeps its release".into(),
            ));
        }
        if self.revision < state.revision {
            return Err(PluginRegistryError::Conflict(
                "binding revision is older than the stored one".into(),
            ));
        }
        state.revision = self.revision;
        state.enabled = self.enabled;
        state.configured = self.configured;
        state.configuration = self.configuration.clone();
        Ok(state)
    }
}

/// What a delivery learned about a binding's facts. `None` leaves a field; `Some(None)` clears it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RuntimePatch {
    pub deployment_id: Option<Option<String>>,
    pub runtime_revision: Option<i64>,
    pub runtime_configuration_digest: Option<Option<String>>,
    pub runtime_enabled: Option<bool>,
    pub runtime_healthy_until_ms: Option<i64>,
    pub runtime_error: Option<Option<String>>,
    pub runtime_error_detail: Option<Option<String>>,
    pub runtime_configuration: Option<Option<Value>>,
    pub runtime_attempts: Option<i32>,
    pub runtime_next_attempt_ms: Option<i64>,
    pub runtime_enabled_digest: Option<Option<String>>,
    pub lifecycle: Option<StateLifecycle>,
}

impl RuntimePatch {
    /// A tenant moved to `deployment_id`: nothing the old one acknowledged holds there.
    pub fn rebound(deployment_id: String) -> Self {
        Self {
            deployment_id: Some(Some(deployment_id)),
            runtime_revision: Some(0),
            runtime_enabled: Some(false),
            runtime_healthy_until_ms: Some(0),
            runtime_error: Some(None),
            ..Self::default()
        }
    }

    pub fn apply(self, state: &mut AppPluginState) {
        fn set<T>(field: &mut T, patch: Option<T>) {
            if let Some(value) = patch {
                *field = value;
            }
        }
        set(&mut state.deployment_id, self.deployment_id);
        set(&mut state.runtime_revision, self.runtime_revision);
        set(
            &mut state.runtime_configuration_digest,
            self.runtime_configuration_digest,
        );
        set(&mut state.runtime_enabled, self.runtime_enabled);
        set(
            &mut state.runtime_healthy_until_ms,
            self.runtime_healthy_until_ms,
        );
        set(&mut state.runtime_error, self.runtime_error);
        set(&mut state.runtime_error_detail, self.runtime_error_detail);
        set(&mut state.runtime_configuration, self.runtime_configuration);
        set(&mut state.runtime_attempts, self.runtime_attempts);
        set(
            &mut state.runtime_next_attempt_ms,
            self.runtime_next_attempt_ms,
        );
        set(
            &mut state.runtime_enabled_digest,
            self.runtime_enabled_digest,
        );
        set(&mut state.lifecycle, self.lifecycle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn desired(revision: i64) -> DesiredAppState {
        DesiredAppState {
            app_id: "201".into(),
            binding_id: "b1".into(),
            plugin_id: "p".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: "sha256:x".into(),
            revision,
            enabled: true,
            configured: true,
            configuration: json!({"a": 1}),
            secrets_from: None,
        }
    }

    #[test]
    fn a_put_keeps_the_facts_and_refuses_an_older_revision() {
        let mut state = desired(2).applied_to(None).unwrap();
        state.runtime_revision = 2;
        let retried = desired(2).applied_to(Some(state.clone())).unwrap();
        assert_eq!(retried.runtime_revision, 2);
        assert!(matches!(
            desired(1).applied_to(Some(state)),
            Err(PluginRegistryError::Conflict(_))
        ));
    }

    #[test]
    fn a_patch_changes_only_its_fields_and_clears_on_request() {
        let mut state = desired(1).applied_to(None).unwrap();
        state.runtime_error = Some("x".into());
        state.runtime_attempts = 3;
        RuntimePatch {
            runtime_error: Some(None),
            runtime_revision: Some(1),
            ..RuntimePatch::default()
        }
        .apply(&mut state);
        assert_eq!(state.runtime_error, None);
        assert_eq!(state.runtime_revision, 1);
        assert_eq!(state.runtime_attempts, 3);
    }

    fn binding(revision: u64) -> ProjectedBinding {
        ProjectedBinding {
            binding_id: "b1".into(),
            plugin_id: "p".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: "sha256:x".into(),
            organization_id: "101".into(),
            revision,
            lifecycle: BindingLifecycle::Installed,
            enabled: false,
            suspended: false,
            configured: true,
            owned: true,
            hold: false,
            export_until_ms: None,
            approved_operations: BTreeSet::new(),
            approved_permissions: BTreeSet::new(),
            approved_data_scopes: BTreeSet::new(),
            features: BTreeMap::new(),
        }
    }

    #[test]
    fn a_deny_needs_only_the_projection_and_starts_a_fresh_budget() {
        let mut state = desired(2).applied_to(None).unwrap();
        state.runtime_attempts = 4;
        state.runtime_error = Some("runtime_unavailable".into());
        assert!(state.apply_projected(&binding(3)));
        assert_eq!(state.revision, 3);
        assert!(!state.enabled);
        assert_eq!(state.runtime_attempts, 0);
        assert_eq!(state.runtime_error, None);
        assert_eq!(state.configuration, json!({"a": 1}));
    }

    #[test]
    fn a_put_ahead_of_the_outbox_keeps_its_revision_until_the_projection_catches_up() {
        let mut state = desired(5).applied_to(None).unwrap();
        assert!(state.apply_projected(&binding(4)));
        assert_eq!(state.revision, 5);
        assert!(state.enabled);
        assert_eq!(state.projected_revision, 4);
        assert!(state.apply_projected(&binding(5)));
        assert_eq!(state.projected_revision, 5);
        assert!(!state.enabled);
    }

    #[test]
    fn a_renewal_does_not_restart_a_failed_installation_but_a_new_revision_does() {
        let mut state = desired(2).applied_to(None).unwrap();
        let mut pending = binding(2);
        pending.lifecycle = BindingLifecycle::Pending;
        state.apply_projected(&pending);
        state.lifecycle = StateLifecycle::Failed;
        state.runtime_attempts = 10;
        assert!(!state.apply_projected(&pending));
        assert_eq!(state.lifecycle, StateLifecycle::Failed);
        pending.revision = 3;
        assert!(state.apply_projected(&pending));
        assert_eq!(state.lifecycle, StateLifecycle::Pending);
        assert_eq!(state.runtime_attempts, 0);
    }

    #[test]
    fn an_older_projection_or_another_release_changes_nothing() {
        let mut state = desired(2).applied_to(None).unwrap();
        state.apply_projected(&binding(3));
        assert!(!state.apply_projected(&binding(2)));
        let mut other = binding(4);
        other.plugin_version = "2.0.0".into();
        assert!(!state.apply_projected(&other));
    }

    #[test]
    fn the_end_of_an_offline_hold_delivers_without_waiting_for_the_backoff() {
        let mut state = desired(2).applied_to(None).unwrap();
        let mut held = binding(2);
        held.hold = true;
        state.apply_projected(&held);
        state.runtime_next_attempt_ms = 99;
        held.hold = false;
        assert!(state.apply_projected(&held));
        assert_eq!(state.runtime_next_attempt_ms, 0);
    }
}
