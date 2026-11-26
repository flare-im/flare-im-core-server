//! Runtime convergence (`docs/design/plugin-lifecycle-and-feature-publishing.md` 6.5): compares the
//! desired and the acknowledged state of every binding and delivers the difference to its plugin
//! process, under Core's own ticket.
//!
//! - A deny is not waiting for this: the projection took effect when it arrived. This only tells
//!   the plugin, and keeps trying.
//! - Same-version fast re-enable: the platform lets the enable through, the accepted projection
//!   wakes a pass, and the delivery does not wait for the tick.
//! - The plugin runs with the stored configuration with configuration-gated features overlaid. The
//!   acknowledged facts record that effective configuration.
//! - Replicas take turns: a pass runs under a lease, so a binding is delivered by one worker.
//!
//! The business system reads the outcome with `plugin_app_status.get`; it holds no connection to plugins.
use std::sync::Arc;
use std::time::Duration;

use flare_im_core_capability_core::plugin::data::DataRequest;
use flare_im_core_capability_core::plugin::invocation::{
    PluginCallSigner, PluginDataClaims, PluginTenantStateClaims,
};
use flare_im_core_capability_core::plugin::runtime::configuration_digest;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::domain::plugin::reconcile::{HEALTH_LEASE_MS, bounded_reason, check_protocols};
use crate::domain::plugin::{
    AppPluginState, DeploymentFilter, Outcome, PluginDeploymentRecord, PluginRegistryError,
    ReconcileRow, RegistryResult, RuntimePatch, Target, TenantDelivery,
};
use crate::domain::repository::plugin_registry::{PluginRegistryRepository, ReconcileCursor};
use crate::domain::repository::plugin_runtime::{
    PluginRuntimeGateway, RuntimeAck, RuntimeFailure, RuntimeInspection,
};

/// Bindings one pass delivers to.
const DUE_PER_PASS: usize = 100;
/// Bindings read per page while looking for due ones.
const SCAN_PAGE: i64 = 500;
/// Time between passes when nothing wakes the worker.
const TICK: Duration = Duration::from_secs(10);
const PASS_BUDGET: Duration = Duration::from_secs(20);
/// Outlives a pass; a replica that dies gives its turn up by itself.
const LEASE_MS: i64 = 30_000;
const TICKET_TTL_MS: i64 = 30_000;
/// A data ticket covers one call, which the gateway gives 20 seconds.
const DATA_TICKET_TTL_MS: i64 = 30_000;
/// Lease of a state a migration delivers: long enough for its step, disabled anyway.
const MIGRATION_LEASE_MS: i64 = 300_000;

/// Wakes a pass, from anywhere that learned of a new desired state.
#[derive(Debug, Default)]
pub struct ReconcileSignal(Notify);

impl ReconcileSignal {
    pub fn wake(&self) {
        self.0.notify_one();
    }
}

pub struct PluginReconciler {
    repository: Arc<dyn PluginRegistryRepository>,
    runtime: Arc<dyn PluginRuntimeGateway>,
    signer: Arc<PluginCallSigner>,
    signal: Arc<ReconcileSignal>,
    /// This replica, for the lease.
    holder: String,
    development: bool,
}

/// A deployment that is what it was registered as, and ready.
fn admitted(
    inspection: Result<RuntimeInspection, RuntimeFailure>,
    deployment: &PluginDeploymentRecord,
) -> Result<RuntimeInspection, Outcome> {
    let inspection = inspection.map_err(failed)?;
    if !inspection.identifies(deployment) {
        return Err(Outcome::Rejected(
            "deployment identity mismatch: the address serves another deployment, version or artifact"
                .to_owned(),
        ));
    }
    if !inspection.ready || inspection.draining {
        return Err(Outcome::Unavailable("runtime_unavailable"));
    }
    Ok(inspection)
}

fn failed(failure: RuntimeFailure) -> Outcome {
    match failure {
        RuntimeFailure::Refused(reason) => Outcome::Rejected(bounded_reason(&reason)),
        RuntimeFailure::Unavailable => Outcome::Unavailable("runtime_unavailable"),
    }
}

/// An answer for another state than the one sent: the outcome is unknown.
fn matches_delivery(ack: &RuntimeAck, delivery: &TenantDelivery) -> bool {
    ack.revision == delivery.revision
        && ack.configuration_digest == delivery.configuration_digest
        && ack.enabled == delivery.enabled
}

impl PluginReconciler {
    pub fn new(
        repository: Arc<dyn PluginRegistryRepository>,
        runtime: Arc<dyn PluginRuntimeGateway>,
        signer: Arc<PluginCallSigner>,
        signal: Arc<ReconcileSignal>,
        development: bool,
    ) -> Self {
        Self {
            repository,
            runtime,
            signer,
            signal,
            holder: uuid::Uuid::new_v4().to_string(),
            development,
        }
    }

    /// Runs until `shutdown` resolves. A pass in progress finishes first.
    pub async fn run(self: Arc<Self>, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            if let Err(error) = self.pass().await {
                tracing::warn!(%error, "plugin runtime reconcile pass failed; next tick retries");
            }
            tokio::select! {
                _ = &mut shutdown => return,
                _ = self.signal.0.notified() => {}
                _ = tokio::time::sleep(TICK) => {}
            }
        }
    }

    pub fn signal(&self) -> Arc<ReconcileSignal> {
        Arc::clone(&self.signal)
    }

    /// One pass; how many bindings it delivered to.
    pub async fn pass(&self) -> RegistryResult<usize> {
        if !self
            .repository
            .reconcile_lease(&self.holder, LEASE_MS)
            .await?
        {
            return Ok(0);
        }
        let deadline = Instant::now() + PASS_BUDGET;
        let mut delivered = 0;
        for row in self.due().await? {
            if Instant::now() >= deadline {
                break;
            }
            match self.reconcile(&row, deadline).await {
                Ok(true) => delivered += 1,
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    app = %row.state.app_id,
                    binding = %row.state.binding_id,
                    %error,
                    "plugin runtime reconcile of a binding failed"
                ),
            }
        }
        Ok(delivered)
    }

    /// Due bindings, least recently touched first: a delivery of any outcome advances `updated_at`.
    async fn due(&self) -> RegistryResult<Vec<ReconcileRow>> {
        let mut due = Vec::new();
        let mut after: Option<ReconcileCursor> = None;
        loop {
            let page = self
                .repository
                .reconcile_page(after.clone(), SCAN_PAGE, self.development)
                .await?;
            let Some(last) = page.last() else {
                return Ok(due);
            };
            after = Some((
                last.state.updated_at,
                last.state.app_id.clone(),
                last.state.binding_id.clone(),
            ));
            let scanned = page.len() as i64;
            for row in page {
                if row.is_due() {
                    due.push(row);
                    if due.len() == DUE_PER_PASS {
                        return Ok(due);
                    }
                }
            }
            if scanned < SCAN_PAGE {
                return Ok(due);
            }
        }
    }

    /// The bound deployment, or the oldest of the release: bindings without one take it, so the
    /// choice must be stable.
    async fn deployment_of(
        &self,
        state: &AppPluginState,
    ) -> RegistryResult<Option<PluginDeploymentRecord>> {
        let filter = DeploymentFilter {
            deployment_id: state.deployment_id.clone(),
            plugin_id: Some(state.plugin_id.clone()),
            plugin_version: Some(state.plugin_version.clone()),
            manifest_digest: Some(state.manifest_digest.clone()),
            limit: None,
        };
        let mut rows = self.repository.runtime_deployments(&filter).await?;
        rows.sort_by(|a, b| {
            (a.created_at, &a.deployment_id).cmp(&(b.created_at, &b.deployment_id))
        });
        Ok(rows.into_iter().next())
    }

    fn ticket(&self, delivery: &TenantDelivery) -> Result<String, RuntimeFailure> {
        let now = chrono::Utc::now().timestamp_millis();
        self.signer
            .sign_tenant_state(
                &PluginTenantStateClaims {
                    tenant_id: delivery.app_id.clone(),
                    organization_id: delivery.organization_id.clone(),
                    deployment_id: delivery.deployment_id.clone(),
                    manifest_digest: delivery.manifest_digest.clone(),
                    grant_id: delivery.grant_id.clone(),
                    revision: delivery.revision,
                    configuration_digest: delivery.configuration_digest.clone(),
                    enabled: delivery.enabled,
                    issued_at_ms: now,
                    expires_at_ms: now.saturating_add(TICKET_TTL_MS),
                },
                now,
            )
            .map_err(|_| RuntimeFailure::Unavailable)
    }

    /// Enabling needs the deployment to serve what the version promises; a deny is always
    /// delivered.
    async fn deliver(
        &self,
        deployment: &PluginDeploymentRecord,
        delivery: &TenantDelivery,
        manifest_digest: &str,
    ) -> Outcome {
        let inspection = match admitted(self.runtime.inspect(deployment).await, deployment) {
            Ok(inspection) => inspection,
            Err(outcome) => return outcome,
        };
        if delivery.enabled {
            let Ok(manifest) = self.repository.manifest(manifest_digest).await else {
                return Outcome::Unavailable("runtime_unavailable");
            };
            if let Err(reason) = check_protocols(&manifest, &inspection.protocols) {
                return Outcome::Rejected(bounded_reason(&reason));
            }
        }
        let ticket = match self.ticket(delivery) {
            Ok(ticket) => ticket,
            Err(failure) => return failed(failure),
        };
        match self.runtime.apply(deployment, delivery, &ticket).await {
            Ok(ack) if matches_delivery(&ack, delivery) => Outcome::Acknowledged,
            Ok(_) => Outcome::Unavailable("runtime_unavailable"),
            Err(failure) => failed(failure),
        }
    }

    /// One binding; whether something was delivered.
    async fn reconcile(&self, row: &ReconcileRow, deadline: Instant) -> RegistryResult<bool> {
        let state = &row.state;
        let manifest = self.repository.manifest(&state.manifest_digest).await?;
        let effective = manifest.effective_configuration(&state.configuration, &state.features);
        let deployment = self.deployment_of(state).await?;
        let target = row.target();
        let bound = deployment.as_ref().map_or("", |d| d.deployment_id.as_str());
        let Some(delivery) = row.delivery(target, bound, &effective, |configuration| {
            configuration_digest(configuration).ok()
        }) else {
            // Nothing acknowledged to renew; move it behind the others.
            self.repository
                .record_delivery(state, RuntimePatch::default())
                .await?;
            return Ok(false);
        };
        let outcome = match &deployment {
            None => Outcome::Unavailable("deployment_unavailable"),
            Some(deployment) => tokio::time::timeout_at(
                deadline,
                self.deliver(deployment, &delivery, &state.manifest_digest),
            )
            .await
            .unwrap_or(Outcome::Unavailable("runtime_unavailable")),
        };
        self.record(row, target, &delivery, &outcome).await?;
        Ok(outcome == Outcome::Acknowledged)
    }

    async fn record(
        &self,
        row: &ReconcileRow,
        target: Target,
        delivery: &TenantDelivery,
        outcome: &Outcome,
    ) -> RegistryResult<()> {
        // Only a successful observation renews a lease, and only for a binding that may serve.
        if *outcome == Outcome::Acknowledged && row.admitted() {
            self.repository
                .report_deployment_health(
                    &delivery.deployment_id,
                    row.now_ms + HEALTH_LEASE_MS,
                    None,
                )
                .await?;
        }
        if target == Target::Desired
            && *outcome != Outcome::Acknowledged
            && row.exhausted_by(outcome)
        {
            tracing::error!(
                alert = "plugin_runtime_retry_exhausted",
                app = %row.state.app_id,
                plugin = %row.state.plugin_id,
                version = %row.state.plugin_version,
                binding = %row.state.binding_id,
                error = outcome.code().unwrap_or_default(),
                "plugin runtime stopped retrying the desired revision"
            );
        }
        self.repository
            .record_delivery(&row.state, row.after(target, delivery, outcome))
            .await?;
        Ok(())
    }

    /// A tenant migration's disabled state for one deployment, at the revision the migration holds
    /// there. The acknowledgement is the migration's business: no fact changes here.
    pub async fn deliver_disabled(
        &self,
        app_id: &str,
        binding_id: &str,
        deployment_id: &str,
        revision: i64,
    ) -> RegistryResult<()> {
        let binding = [binding_id.to_owned()];
        let state = self
            .repository
            .app_states(&[app_id.to_owned()], Some(&binding))
            .await?
            .pop()
            .ok_or(PluginRegistryError::NotFound)?;
        let filter = DeploymentFilter {
            deployment_id: Some(deployment_id.to_owned()),
            limit: Some(1),
            ..DeploymentFilter::default()
        };
        let deployment = self
            .repository
            .runtime_deployments(&filter)
            .await?
            .pop()
            .ok_or(PluginRegistryError::NotFound)?;
        if deployment.plugin_id != state.plugin_id || revision <= 0 {
            return Err(PluginRegistryError::Invalid(
                "the deployment does not serve this binding".into(),
            ));
        }
        let manifest = self.repository.manifest(&state.manifest_digest).await?;
        let configuration = manifest.effective_configuration(&state.configuration, &state.features);
        let digest = configuration_digest(&configuration)
            .map_err(|_| PluginRegistryError::Invalid("invalid configuration".into()))?;
        let delivery = disabled_delivery(&state, &deployment, revision, configuration, digest);
        match self
            .deliver(&deployment, &delivery, &state.manifest_digest)
            .await
        {
            Outcome::Acknowledged => Ok(()),
            Outcome::Rejected(reason) => Err(PluginRegistryError::Conflict(reason)),
            Outcome::Unavailable(_) => Err(PluginRegistryError::Unavailable),
        }
    }
}

impl PluginReconciler {
    /// One step of a platform data job, signed by Core and sent to the deployment it names. The
    /// platform owns the job and its checks; Core only requires that the request names a binding
    /// it knows, served by that deployment, and answers what the plugin answered.
    pub async fn forward_data(&self, request: &DataRequest) -> RegistryResult<Value> {
        let digest = request
            .digest()
            .map_err(|_| PluginRegistryError::Invalid("invalid data request".into()))?;
        let binding = [request.grant_id.clone()];
        let state = self
            .repository
            .app_states(std::slice::from_ref(&request.app_id), Some(&binding))
            .await?
            .pop()
            .ok_or(PluginRegistryError::NotFound)?;
        let filter = DeploymentFilter {
            deployment_id: Some(request.deployment_id.clone()),
            limit: Some(1),
            ..DeploymentFilter::default()
        };
        let deployment = self
            .repository
            .runtime_deployments(&filter)
            .await?
            .pop()
            .ok_or(PluginRegistryError::NotFound)?;
        if deployment.plugin_id != state.plugin_id
            || deployment.manifest_digest != request.manifest_digest
        {
            return Err(PluginRegistryError::Invalid(
                "the deployment does not serve this binding".into(),
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let ticket = self
            .signer
            .sign_data(
                &PluginDataClaims {
                    tenant_id: request.app_id.clone(),
                    deployment_id: request.deployment_id.clone(),
                    manifest_digest: request.manifest_digest.clone(),
                    grant_id: request.grant_id.clone(),
                    grant_revision: request.grant_revision,
                    request_digest: digest,
                    issued_at_ms: now,
                    expires_at_ms: now + DATA_TICKET_TTL_MS,
                },
                now,
            )
            .map_err(|_| PluginRegistryError::Unavailable)?;
        self.runtime
            .data(&deployment, request, &ticket)
            .await
            .map_err(|failure| match failure {
                RuntimeFailure::Refused(reason) => PluginRegistryError::Conflict(reason),
                RuntimeFailure::Unavailable => PluginRegistryError::Unavailable,
            })
    }
}

fn disabled_delivery(
    state: &AppPluginState,
    deployment: &PluginDeploymentRecord,
    revision: i64,
    configuration: Value,
    configuration_digest: String,
) -> TenantDelivery {
    TenantDelivery {
        organization_id: state.organization_id.clone(),
        deployment_id: deployment.deployment_id.clone(),
        app_id: state.app_id.clone(),
        grant_id: state.binding_id.clone(),
        revision: revision as u64,
        manifest_digest: deployment.manifest_digest.clone(),
        configuration,
        configuration_digest,
        enabled: false,
        valid_until_ms: chrono::Utc::now().timestamp_millis() + MIGRATION_LEASE_MS,
        validate_configuration: true,
        // The approved set of this revision, as the convergence task delivers it: a host keeps
        // one state per revision and refuses the same revision with different contents.
        approved_operations: state.approved.operations.iter().cloned().collect(),
        permissions: state.approved.permissions.iter().cloned().collect(),
        data_scopes: state.approved.data_scopes.iter().cloned().collect(),
    }
}

#[cfg(test)]
#[path = "plugin_reconcile_tests.rs"]
mod tests;
