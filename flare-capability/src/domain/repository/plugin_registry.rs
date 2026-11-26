//! The registry's persistence port. Every write that changes what a runtime may do raises the
//! matching revision in the same transaction, so a resolved snapshot cannot outlive it.
use crate::domain::plugin::{
    AppPluginState, DeploymentFilter, DeploymentRegistration, DesiredAppState, DevRegistered,
    DevRelease, NewPluginVersion, PluginAssetContent, PluginDeploymentRecord, PluginVersion,
    PluginVersionDetail, ReconcileRow, RegistryResult, RuntimePatch, VersionFilter, VersionKey,
    VersionStateChange, WebBundleApproval, WebBundleAttachment, WebBundleFilter, WebBundleRef,
};
use flare_im_core_capability_core::plugin::PluginManifest;
use std::sync::Arc;

/// Where a page of [`PluginRegistryRepository::reconcile_page`] resumes: `updated_at`, application,
/// binding of the last row.
pub type ReconcileCursor = (i64, String, String);

#[async_trait::async_trait]
pub trait PluginRegistryRepository: Send + Sync {
    /// Stores a verified package; the same digest again answers `created = false`. Refuses another
    /// publisher's plugin id, another digest for a version, and a namespace another plugin holds.
    async fn register_version(
        &self,
        version: &NewPluginVersion,
    ) -> RegistryResult<PluginVersionDetail>;

    async fn set_version_state(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        change: &VersionStateChange,
    ) -> RegistryResult<PluginVersion>;

    async fn list_versions(&self, filter: &VersionFilter) -> RegistryResult<Vec<PluginVersion>>;

    async fn version(&self, key: &VersionKey) -> RegistryResult<PluginVersionDetail>;

    async fn asset(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        path: &str,
    ) -> RegistryResult<PluginAssetContent>;

    /// Checks the binding against the signed manifest; an existing id must be identical.
    async fn register_deployment(
        &self,
        registration: &DeploymentRegistration,
    ) -> RegistryResult<PluginDeploymentRecord>;

    async fn list_deployments(
        &self,
        filter: &DeploymentFilter,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>>;

    /// Keeps the later lease. `last_error` is what the deployment reports now, `None` to clear.
    async fn report_deployment_health(
        &self,
        deployment_id: &str,
        healthy_until_ms: i64,
        last_error: Option<&str>,
    ) -> RegistryResult<()>;

    async fn put_config(
        &self,
        desired: &DesiredAppState,
        runtime: Option<RuntimePatch>,
    ) -> RegistryResult<AppPluginState>;

    async fn app_states(
        &self,
        apps: &[String],
        bindings: Option<&[String]>,
    ) -> RegistryResult<Vec<AppPluginState>>;

    /// Bindings a pass looks at, least recently touched first. Rows of a development version only
    /// with `development`.
    async fn reconcile_page(
        &self,
        after: Option<ReconcileCursor>,
        limit: i64,
        development: bool,
    ) -> RegistryResult<Vec<ReconcileRow>>;

    /// The deployments a binding may run on, development ones included: the same filter as
    /// [`Self::list_deployments`], for convergence rather than for a reader.
    async fn runtime_deployments(
        &self,
        filter: &DeploymentFilter,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>>;

    /// Whether this holder may run a pass now; the lease ends after `ttl_ms` by itself.
    async fn reconcile_lease(&self, holder: &str, ttl_ms: i64) -> RegistryResult<bool>;

    /// Records what a delivery learned unless the binding moved on since `seen`; whether it was
    /// recorded. Advances `updated_at` either way the row is written, for fairness in the pass.
    async fn record_delivery(
        &self,
        seen: &AppPluginState,
        patch: RuntimePatch,
    ) -> RegistryResult<bool>;

    /// Records the platform's approval of a web bundle of a (non-development) version. The
    /// manifest must declare it with the same digest and size. The same approval again answers
    /// the stored row; another digest for the bundle, or a withdrawn bundle, is a conflict.
    /// Raises the global revision, so the next catalog read of every application may serve it.
    async fn attach_web_bundle(
        &self,
        approval: &WebBundleApproval,
    ) -> RegistryResult<WebBundleAttachment>;

    /// Withdraws an attached web bundle for good. Raises the global revision: from the next
    /// catalog read no application serves it, whatever its projection still lists. Withdrawing
    /// again answers the stored row; a bundle never attached (withdrawn before approval) answers
    /// `None`: there is nothing to stop serving.
    async fn withdraw_web_bundle(
        &self,
        bundle: &WebBundleRef,
    ) -> RegistryResult<Option<WebBundleAttachment>>;

    async fn web_bundles(
        &self,
        filter: &WebBundleFilter,
    ) -> RegistryResult<Vec<WebBundleAttachment>>;

    /// The parsed manifest of a digest; immutable, so it is cached.
    async fn manifest(&self, digest: &str) -> RegistryResult<Arc<PluginManifest>>;

    /// Refuses an application not flagged for development. Replaces the binding's earlier release
    /// and clears its runtime facts, so convergence applies the new one; the same release, process
    /// and configuration change nothing.
    async fn register_development(&self, release: &DevRelease) -> RegistryResult<DevRegistered>;

    /// Clearing the flag removes the application's development bindings.
    async fn set_development_app(&self, app_id: &str, development: bool) -> RegistryResult<()>;
}
