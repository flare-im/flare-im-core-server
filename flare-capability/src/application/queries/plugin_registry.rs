//! Registry read path.
use std::collections::BTreeMap;
use std::sync::Arc;

use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::configuration::mask_secrets;
use flare_im_capability_core::plugin::runtime::configuration_digest;
use serde::Serialize;

use crate::domain::plugin::{
    AppPluginState, DeploymentFilter, PluginAssetContent, PluginDeploymentRecord,
    PluginRegistryError, PluginVersion, PluginVersionDetail, RegistryResult, VersionFilter,
    VersionKey,
};
use crate::domain::repository::plugin_registry::PluginRegistryRepository;

pub const MAX_VERSION_PAGE: i64 = 500;

/// A state as it leaves Core: secret values are masked, so the digest of what is delivered comes
/// along, for readers that compare it with the acknowledged one.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresentedAppState {
    #[serde(flatten)]
    pub state: AppPluginState,
    pub configuration_digest: Option<String>,
}

pub struct PluginRegistryQueries {
    repository: Arc<dyn PluginRegistryRepository>,
}

impl PluginRegistryQueries {
    pub fn new(repository: Arc<dyn PluginRegistryRepository>) -> Self {
        Self { repository }
    }

    pub async fn versions(&self, mut filter: VersionFilter) -> RegistryResult<Vec<PluginVersion>> {
        filter.limit = filter.limit.clamp(1, MAX_VERSION_PAGE);
        filter.offset = filter.offset.max(0);
        self.repository.list_versions(&filter).await
    }

    pub async fn version(&self, key: &VersionKey) -> RegistryResult<PluginVersionDetail> {
        self.repository.version(key).await
    }

    pub async fn asset(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        path: &str,
    ) -> RegistryResult<PluginAssetContent> {
        self.repository.asset(plugin_id, plugin_version, path).await
    }

    pub async fn deployments(
        &self,
        filter: &DeploymentFilter,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>> {
        self.repository.list_deployments(filter).await
    }

    pub async fn states(
        &self,
        apps: &[String],
        bindings: Option<&[String]>,
    ) -> RegistryResult<Vec<PresentedAppState>> {
        let states = self.repository.app_states(apps, bindings).await?;
        self.presented(states).await
    }

    pub async fn state(&self, app_id: &str, binding_id: &str) -> RegistryResult<PresentedAppState> {
        let binding = [binding_id.to_owned()];
        let states = self
            .repository
            .app_states(&[app_id.to_owned()], Some(&binding))
            .await?;
        self.presented(states)
            .await?
            .pop()
            .ok_or(PluginRegistryError::NotFound)
    }

    /// One state just written, as [`Self::state`] would answer it.
    pub async fn present(&self, state: AppPluginState) -> RegistryResult<PresentedAppState> {
        self.presented(vec![state])
            .await?
            .pop()
            .ok_or(PluginRegistryError::Unavailable)
    }

    async fn presented(
        &self,
        states: Vec<AppPluginState>,
    ) -> RegistryResult<Vec<PresentedAppState>> {
        let mut manifests: BTreeMap<String, Arc<PluginManifest>> = BTreeMap::new();
        let mut presented = Vec::with_capacity(states.len());
        for mut state in states {
            let manifest = match manifests.get(&state.manifest_digest) {
                Some(manifest) => Arc::clone(manifest),
                None => {
                    let manifest = self.repository.manifest(&state.manifest_digest).await?;
                    manifests.insert(state.manifest_digest.clone(), Arc::clone(&manifest));
                    manifest
                }
            };
            let delivered = manifest.effective_configuration(&state.configuration, &state.features);
            let configuration_digest = configuration_digest(&delivered).ok();
            let paths = manifest.secret_configuration_paths();
            mask_secrets(&mut state.configuration, &paths);
            if let Some(acknowledged) = &mut state.runtime_configuration {
                mask_secrets(acknowledged, &paths);
            }
            presented.push(PresentedAppState {
                state,
                configuration_digest,
            });
        }
        Ok(presented)
    }
}
