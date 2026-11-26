//! Registry write path: open and verify packages, then the aggregates' rules through the port.
use std::sync::Arc;

use flare_im_core_capability_core::plugin::ProtocolError;
use flare_im_core_capability_core::plugin::package::PluginPackage;
use flare_im_core_capability_core::plugin::signature::PublisherTrustStore;

use crate::domain::plugin::{
    AppPluginState, DeploymentRegistration, DesiredAppState, DevAppFlag, DevRegistered,
    DevRegistration, NewPluginVersion, PluginDeploymentRecord, PluginRegistryError, PluginVersion,
    PluginVersionDetail, RegistryResult, RuntimePatch, VersionStateChange, WebBundleApproval,
    WebBundleAttachment, WebBundleRef,
};
use crate::domain::repository::plugin_registry::PluginRegistryRepository;

pub struct PluginRegistryCommands {
    repository: Arc<dyn PluginRegistryRepository>,
    trust: Arc<PublisherTrustStore>,
}

impl PluginRegistryCommands {
    pub fn new(repository: Arc<dyn PluginRegistryRepository>, trust: PublisherTrustStore) -> Self {
        Self {
            repository,
            trust: Arc::new(trust),
        }
    }

    /// Verifying hashes up to the whole package, so it stays off the async workers.
    async fn open(&self, package: Vec<u8>) -> RegistryResult<NewPluginVersion> {
        let trust = Arc::clone(&self.trust);
        tokio::task::spawn_blocking(move || {
            PluginPackage::open(&package, &trust)
                .map_err(|error| match error {
                    ProtocolError::InvalidPackage(_) => {
                        PluginRegistryError::Invalid(error.to_string())
                    }
                    _ => PluginRegistryError::Invalid(
                        "plugin publisher signature or manifest is invalid".into(),
                    ),
                })
                .and_then(|package| NewPluginVersion::from_package(&package))
        })
        .await
        .map_err(|_| PluginRegistryError::Unavailable)?
    }

    /// What a package holds, nothing stored: the caller's own checks run on this.
    pub async fn verify(&self, package: Vec<u8>) -> RegistryResult<PluginVersionDetail> {
        Ok(self.open(package).await?.preview())
    }

    pub async fn register(&self, package: Vec<u8>) -> RegistryResult<PluginVersionDetail> {
        let version = self.open(package).await?;
        self.repository.register_version(&version).await
    }

    pub async fn set_state(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        change: &VersionStateChange,
    ) -> RegistryResult<PluginVersion> {
        self.repository
            .set_version_state(plugin_id, plugin_version, change)
            .await
    }

    /// The platform approved a web bundle of a version (its review passed); see
    /// [`PluginRegistryRepository::attach_web_bundle`].
    pub async fn attach_web_bundle(
        &self,
        approval: &WebBundleApproval,
    ) -> RegistryResult<WebBundleAttachment> {
        self.repository.attach_web_bundle(approval).await
    }

    /// The platform withdrew a web bundle (a security problem): no application serves it again.
    pub async fn withdraw_web_bundle(
        &self,
        bundle: &WebBundleRef,
    ) -> RegistryResult<Option<WebBundleAttachment>> {
        self.repository.withdraw_web_bundle(bundle).await
    }

    pub async fn register_deployment(
        &self,
        registration: &DeploymentRegistration,
    ) -> RegistryResult<PluginDeploymentRecord> {
        self.repository.register_deployment(registration).await
    }

    pub async fn put_config(
        &self,
        desired: &DesiredAppState,
        runtime: Option<RuntimePatch>,
    ) -> RegistryResult<AppPluginState> {
        self.repository.put_config(desired, runtime).await
    }

    /// Unsigned on purpose: the application flag and the development switch stand in for the
    /// publisher signature, so this never reaches the package path.
    pub async fn register_development(
        &self,
        registration: DevRegistration,
    ) -> RegistryResult<DevRegistered> {
        let release = registration.release()?;
        self.repository.register_development(&release).await
    }

    pub async fn flag_development_app(&self, flag: &DevAppFlag) -> RegistryResult<()> {
        self.repository
            .set_development_app(&flag.app_id, flag.development)
            .await
    }
}
