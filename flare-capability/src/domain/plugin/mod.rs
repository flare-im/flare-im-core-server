//! Plugin registry: the three aggregates Core owns whatever sits above it. A deployment without a
//! marketplace still needs them.
//!
//! - [`version`]: `PluginVersion`, an immutable signed package.
//! - [`deployment`]: `PluginDeployment`, the operator's service for a version, and its health.
//! - [`app_state`]: `AppPluginState`, an application's desired and observed state and its
//!   configuration.
//! - [`resolve`]: what the runtime may do, from a projection and these three.
//! - [`reconcile`]: what a convergence pass does with a binding.
//! - [`development`]: a local process in an application flagged for development.
//!
//! Hooks are not here: they are a separate control plane.

pub mod app_state;
pub mod deployment;
pub mod development;
pub mod reconcile;
pub mod resolve;
pub mod version;

pub use app_state::{AppPluginState, ApprovedGrant, DesiredAppState, RuntimePatch, StateLifecycle};
pub use deployment::{
    DeploymentFilter, DeploymentRegistration, PluginDeploymentRecord, valid_deployment_id,
};
pub use development::{
    DevAppFlag, DevBinding, DevRegistered, DevRegistration, DevRelease, with_development,
};
pub use reconcile::{Outcome, ReconcileRow, Target, TenantDelivery};
pub use resolve::{ApprovedWebBundles, Resolution, VersionFlags, resolve};
pub use version::{
    NewPluginAsset, NewPluginVersion, PluginAssetContent, PluginAssetRef, PluginVersion,
    PluginVersionDetail, VersionFilter, VersionKey, VersionStateChange, WebBundleApproval,
    WebBundleAttachment, WebBundleFilter, WebBundleRef, namespace_conflict, served_web_bundles,
};

/// Why a registry operation failed. Hand-written `Display`: IM production code keeps one error
/// base, and `error_boundary` refuses a second error macro here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginRegistryError {
    Invalid(String),
    NotFound,
    Conflict(String),
    /// The operation exists and this application or process may not use it.
    Denied(String),
    Unavailable,
}

impl std::fmt::Display for PluginRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) | Self::Conflict(reason) | Self::Denied(reason) => {
                f.write_str(reason)
            }
            Self::NotFound => f.write_str("plugin registry record not found"),
            Self::Unavailable => f.write_str("plugin registry unavailable"),
        }
    }
}

impl std::error::Error for PluginRegistryError {}

pub type RegistryResult<T> = std::result::Result<T, PluginRegistryError>;
