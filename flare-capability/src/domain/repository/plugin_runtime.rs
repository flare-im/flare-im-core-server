//! The plugin processes, as the reconciler reaches them.
use flare_im_core_capability_core::plugin::data::DataRequest;
use serde_json::Value;

use crate::domain::plugin::{PluginDeploymentRecord, TenantDelivery};

/// What a deployment says about itself. An identity check, not attestation: only the operator's
/// registration binds an artifact to an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeInspection {
    pub deployment_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub artifact_digest: String,
    pub ready: bool,
    pub draining: bool,
    pub protocols: Vec<String>,
}

impl RuntimeInspection {
    pub fn identifies(&self, deployment: &PluginDeploymentRecord) -> bool {
        self.deployment_id == deployment.deployment_id
            && self.plugin_id == deployment.plugin_id
            && self.plugin_version == deployment.plugin_version
            && self.manifest_digest == deployment.manifest_digest
            && self.artifact_digest == deployment.artifact_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAck {
    pub revision: u64,
    pub configuration_digest: String,
    pub enabled: bool,
}

/// `Refused`: a definite answer (configuration rejected, identity or revision mismatch), with its
/// reason. `Unavailable`: the outcome is unknown (transport, timeout, draining).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeFailure {
    Refused(String),
    Unavailable,
}

#[async_trait::async_trait]
pub trait PluginRuntimeGateway: Send + Sync {
    async fn inspect(
        &self,
        deployment: &PluginDeploymentRecord,
    ) -> Result<RuntimeInspection, RuntimeFailure>;

    /// Delivers one tenant state under Core's ticket for it.
    async fn apply(
        &self,
        deployment: &PluginDeploymentRecord,
        delivery: &TenantDelivery,
        ticket: &str,
    ) -> Result<RuntimeAck, RuntimeFailure>;

    /// One data job request under Core's ticket for it; the plugin's answer as it came.
    async fn data(
        &self,
        deployment: &PluginDeploymentRecord,
        request: &DataRequest,
        ticket: &str,
    ) -> Result<Value, RuntimeFailure>;
}
