//! Development mode: `flare-plugin dev` runs a local process in an application flagged for it.
//!
//! The path has three locks, all required: Capability started with `FLARE_PLUGIN_DEV_MODE=1`, which
//! release builds and deployment templates never set; the application in `plugin_dev_apps`, which
//! only platform staff change; and `plugin_versions.development` on the release. A development
//! version is unsigned, so every production read leaves it out: the market mirror, `plugin_version`
//! and `plugin_deployment` reads, `plugin_config.put`, and the projection resolver. Without the
//! switch the rows are inert: nothing serves them.
//!
//! The rows are the ordinary version, deployment and application state, so runtime convergence
//! treats them like any other binding. The platform knows nothing of them: [`with_development`]
//! adds them to the application's projection at read time.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
use flare_im_capability_core::plugin::manifest::PluginKind;
use flare_im_capability_core::plugin::runtime::{
    PluginDeployment, PluginProjection, ProjectedLifecycle, ProjectedPlugin, configuration_digest,
    digest_bytes,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{
    AppPluginState, ApprovedGrant, NewPluginVersion, PluginRegistryError, RegistryResult,
    StateLifecycle, valid_deployment_id,
};

/// The switch. Its name keeps it out of `release/supervise-core.toml`'s inherited prefixes, so a
/// supervised release does not receive it even from a polluted environment.
pub const PLUGIN_DEV_MODE_ENV: &str = "FLARE_PLUGIN_DEV_MODE";
/// Stands in for the key id and the signature of a signed version.
const UNSIGNED: &str = "development-unsigned";
/// A development deployment is named as one, so it cannot take the id of a real one.
const DEPLOYMENT_PREFIX: &str = "dev-";
/// The lease of the projection made up for an application the platform has not projected. Reads
/// are not cached, so this only has to outlast one dispatch.
const STANDALONE_LEASE_MS: i64 = 60_000;
/// The owner a plugin process sees for a development binding.
const DEVELOPMENT_ORGANIZATION: &str = "development";

/// `plugin_dev.register`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DevRegistration {
    pub app_id: String,
    /// The bytes of `manifest.json`: the digest must equal what the local process computes.
    pub manifest_json: String,
    pub deployment_id: String,
    pub grpc_authority: String,
    #[serde(default = "empty_configuration")]
    pub configuration: Value,
}

fn empty_configuration() -> Value {
    Value::Object(Map::new())
}

/// A registration checked against its manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct DevRelease {
    pub app_id: String,
    pub binding_id: String,
    pub version: NewPluginVersion,
    pub deployment: PluginDeployment,
    pub configuration: Value,
}

impl DevRegistration {
    pub fn release(self) -> RegistryResult<DevRelease> {
        let invalid = |reason: &str| PluginRegistryError::Invalid(reason.to_owned());
        let raw = self.manifest_json.as_bytes();
        let manifest = PluginManifest::from_json(raw)
            .map_err(|error| PluginRegistryError::Invalid(format!("invalid manifest: {error}")))?;
        let digest = digest_bytes(raw);
        if !self.deployment_id.starts_with(DEPLOYMENT_PREFIX)
            || !valid_deployment_id(&self.deployment_id)
        {
            return Err(invalid("a development deployment id starts with `dev-`"));
        }
        let deployment = PluginDeployment {
            deployment_id: self.deployment_id,
            plugin_id: manifest.plugin_id.clone(),
            plugin_version: manifest.version.clone(),
            manifest_digest: digest.clone(),
            artifact_digest: manifest
                .artifact
                .as_ref()
                .map(|artifact| artifact.digest.clone())
                .ok_or_else(|| invalid("the manifest needs an `artifact`"))?,
            grpc_authority: self.grpc_authority,
        };
        deployment.validate(&manifest, &digest).map_err(|_| {
            invalid("the deployment id, the process address or the runtime kind is invalid")
        })?;
        configuration_digest(&self.configuration).map_err(|_| invalid("invalid configuration"))?;
        Ok(DevRelease {
            app_id: self.app_id,
            binding_id: binding_id(&manifest.plugin_id),
            version: NewPluginVersion {
                plugin_id: manifest.plugin_id.clone(),
                plugin_version: manifest.version.clone(),
                display_name: manifest.display_name.clone(),
                publisher_id: manifest.publisher.publisher_id.clone(),
                manifest_json: self.manifest_json,
                manifest_digest: digest,
                key_id: UNSIGNED.to_owned(),
                signature: UNSIGNED.to_owned(),
                kind: match manifest.kind() {
                    PluginKind::Im => "im",
                    PluginKind::Business => "business",
                },
                readme: None,
                changelog: None,
                namespaces: manifest.claimed_namespaces().into_iter().collect(),
                assets: Vec::new(),
            },
            deployment,
            configuration: self.configuration,
        })
    }
}

/// `plugin_dev.flag_app`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DevAppFlag {
    pub app_id: String,
    pub development: bool,
}

/// What `plugin_dev.register` answers. `changed` is false when the same manifest, process and
/// configuration were already registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DevRegistered {
    pub binding_id: String,
    pub deployment_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub revision: i64,
    pub changed: bool,
}

/// One binding per plugin and application, stable across edits. Hashed: a plugin id alone may
/// already fill the 128 bytes of a binding id.
pub fn binding_id(plugin_id: &str) -> String {
    let digest = digest_bytes(plugin_id.as_bytes());
    let hex = digest.trim_start_matches("sha256:");
    format!("dev-{}", &hex[..32])
}

/// A development binding as `plugin_app_states` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevBinding {
    pub binding_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub revision: i64,
}

/// A development binding has no projection, so its desired state is complete from the start and
/// convergence treats it like any other. It belongs to the developer's application, which has no
/// organization here.
pub fn ready_to_converge(state: &mut AppPluginState, manifest: &PluginManifest) {
    state.organization_id = DEVELOPMENT_ORGANIZATION.to_owned();
    state.lifecycle = StateLifecycle::Installed;
    state.owned = true;
    state.approved = ApprovedGrant {
        operations: manifest.operation_ids(),
        permissions: manifest.permissions.iter().map(|p| p.id.clone()).collect(),
        data_scopes: manifest
            .permissions
            .iter()
            .flat_map(|p| p.data_scopes.iter().cloned())
            .collect(),
    };
    state.features = default_features(manifest);
    state.projected_revision = state.revision;
}

/// `base` with `bindings` added, for an application in `plugin_dev_apps`. A binding takes the place
/// of the platform's installation of the same plugin. Everything the manifest declares is granted
/// and every feature follows its default: the application is the developer's own. The version moves
/// with each revision, so a client sees a changed catalog.
pub fn with_development(
    tenant: &str,
    base: Option<PluginProjection>,
    bindings: &[DevBinding],
    manifests: &HashMap<String, Arc<PluginManifest>>,
    now_ms: i64,
) -> PluginProjection {
    let mut projection = base.unwrap_or_else(|| PluginProjection {
        tenant_id: tenant.to_owned(),
        organization_id: None,
        version: 0,
        issued_at_ms: now_ms,
        valid_until_ms: now_ms.saturating_add(STANDALONE_LEASE_MS),
        plugins: Vec::new(),
        bindings: Vec::new(),
    });
    for binding in bindings {
        let Some(manifest) = manifests.get(&binding.manifest_digest) else {
            continue;
        };
        projection
            .plugins
            .retain(|plugin| plugin.grant.plugin_id != binding.plugin_id);
        projection.version = projection.version.saturating_add(binding.revision as u64);
        projection.plugins.push(ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: binding.binding_id.clone(),
                revision: binding.revision as u64,
                tenant_id: tenant.to_owned(),
                plugin_id: binding.plugin_id.clone(),
                plugin_version: binding.plugin_version.clone(),
                manifest_digest: binding.manifest_digest.clone(),
                approved_operations: manifest.operation_ids(),
                approved_permissions: manifest.permissions.iter().map(|p| p.id.clone()).collect(),
                approved_data_scopes: manifest
                    .permissions
                    .iter()
                    .flat_map(|p| p.data_scopes.iter().cloned())
                    .collect(),
                enabled: true,
                revoked: false,
                suspended: false,
                valid_from_ms: projection.issued_at_ms,
                valid_until_ms: projection.valid_until_ms,
            },
            lifecycle: ProjectedLifecycle::Installed,
            export_until_ms: None,
            features: default_features(manifest),
        });
    }
    projection
}

fn default_features(manifest: &PluginManifest) -> BTreeMap<String, bool> {
    manifest
        .features
        .iter()
        .map(|feature| (feature.id.clone(), feature.default_enabled))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration(deployment_id: &str) -> DevRegistration {
        DevRegistration {
            app_id: "201".into(),
            manifest_json: "{}".into(),
            deployment_id: deployment_id.into(),
            grpc_authority: "http://127.0.0.1:7900".into(),
            configuration: empty_configuration(),
        }
    }

    #[test]
    fn the_release_supervisor_neither_sets_nor_inherits_the_switch() {
        let template = include_str!("../../../../release/supervise-core.toml");
        assert!(!template.contains(PLUGIN_DEV_MODE_ENV));
        assert!(!PLUGIN_DEV_MODE_ENV.starts_with("CAPABILITY_"));
        assert!(!PLUGIN_DEV_MODE_ENV.starts_with("FLARE_CAPABILITY_"));
        assert!(!PLUGIN_DEV_MODE_ENV.starts_with("PLUGIN_"));
    }

    #[test]
    fn a_binding_id_is_stable_and_fits_whatever_the_plugin_id() {
        let long = "a".repeat(128);
        assert_eq!(binding_id(&long), binding_id(&long));
        assert_ne!(binding_id("acme.poll"), binding_id("acme.vote"));
        assert_eq!(binding_id(&long).len(), 36);
    }

    #[test]
    fn a_registration_needs_a_valid_manifest() {
        assert!(matches!(
            registration("dev-acme").release(),
            Err(PluginRegistryError::Invalid(_))
        ));
    }

    #[test]
    fn an_empty_application_gets_a_projection_of_its_own() {
        let projection = with_development("201", None, &[], &HashMap::new(), 1_000);
        assert_eq!(projection.tenant_id, "201");
        assert_eq!(projection.valid_until_ms, 61_000);
        assert!(projection.plugins.is_empty());
    }

    #[test]
    fn the_request_defaults_to_an_empty_configuration() {
        let input: DevRegistration = serde_json::from_str(
            r#"{"appId":"201","manifestJson":"{}","deploymentId":"dev-a","grpcAuthority":"http://127.0.0.1:1"}"#,
        )
        .unwrap();
        assert_eq!(input.configuration, empty_configuration());
    }
}
