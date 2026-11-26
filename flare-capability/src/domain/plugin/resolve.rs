//! A projection against the registry. The projection says what the platform wants; this works out
//! what the runtime may do now. A plugin is served at the revision and configuration its runtime
//! last acknowledged, and every missing fact removes it, so a deny needs no acknowledgement.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use flare_im_core_capability_core::plugin::PluginManifest;
use flare_im_core_capability_core::plugin::resource::HostCapability;
use flare_im_core_capability_core::plugin::runtime::{
    PluginDeployment, PluginProjection, PluginRuntimeSnapshot, ProjectedLifecycle, ProjectedPlugin,
    ResolvedPlugin, configuration_digest,
};

use super::{AppPluginState, PluginDeploymentRecord};

/// The mutable part of a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionFlags {
    pub revoked: bool,
    pub suspended: bool,
}

/// The web bundles the registry holds approved and not withdrawn, by manifest digest, then by
/// bundle id: their digest.
pub type ApprovedWebBundles = HashMap<String, BTreeMap<String, String>>;

#[derive(Debug, Clone)]
pub struct Resolution {
    pub snapshot: PluginRuntimeSnapshot,
    /// When a health lease of a served plugin ends: the result is not reused past it.
    pub fresh_until_ms: i64,
    /// Digests the registry does not hold: those plugins are refused, the others are not touched.
    pub unknown_digests: Vec<String>,
}

fn has_recovery_surface(manifest: &PluginManifest) -> bool {
    !manifest.service_operations.is_empty()
        || manifest.capabilities.iter().any(|capability| {
            capability.operations.iter().any(|operation| {
                operation
                    .host_capabilities
                    .contains(&HostCapability::ServiceDispatch)
            })
        })
}

pub fn resolve(
    projection: &PluginProjection,
    manifests: &HashMap<String, Arc<PluginManifest>>,
    versions: &HashMap<String, VersionFlags>,
    states: &HashMap<String, AppPluginState>,
    deployments: &HashMap<String, PluginDeploymentRecord>,
    web_bundles: &ApprovedWebBundles,
    now_ms: i64,
) -> Resolution {
    let mut plugins = Vec::with_capacity(projection.plugins.len());
    let mut unknown_digests = Vec::new();
    let mut fresh_until_ms = projection.valid_until_ms;
    for projected in &projection.plugins {
        let digest = projected.manifest_digest();
        let (Some(manifest), Some(flags)) = (manifests.get(digest), versions.get(digest)) else {
            unknown_digests.push(digest.to_owned());
            continue;
        };
        let Some((plugin, until)) = resolve_one(
            projected,
            manifest,
            *flags,
            states.get(&projected.grant.grant_id),
            deployments,
            web_bundles.get(digest),
            now_ms,
        ) else {
            continue;
        };
        fresh_until_ms = fresh_until_ms.min(until);
        plugins.push(plugin);
    }
    retain_satisfied(&mut plugins);
    Resolution {
        snapshot: PluginRuntimeSnapshot {
            tenant_id: projection.tenant_id.clone(),
            organization_id: projection.organization_id.clone(),
            version: projection.version,
            issued_at_ms: projection.issued_at_ms,
            valid_until_ms: projection.valid_until_ms,
            plugins,
        },
        fresh_until_ms,
        unknown_digests,
    }
}

fn resolve_one(
    projected: &ProjectedPlugin,
    manifest: &Arc<PluginManifest>,
    flags: VersionFlags,
    state: Option<&AppPluginState>,
    deployments: &HashMap<String, PluginDeploymentRecord>,
    web_bundles: Option<&BTreeMap<String, String>>,
    now_ms: i64,
) -> Option<(ResolvedPlugin, i64)> {
    let grant = &projected.grant;
    let in_window = match projected.lifecycle {
        ProjectedLifecycle::Installed => true,
        ProjectedLifecycle::Uninstalled => projected
            .export_until_ms
            .is_some_and(|until| until > now_ms),
        ProjectedLifecycle::Revoked => false,
    };
    if !in_window || grant.revoked || flags.revoked {
        return None;
    }
    let state = state?;
    let acknowledged = state.runtime_revision > 0
        && state.runtime_revision as u64 <= grant.revision
        && state.runtime_healthy_until_ms > now_ms
        && state.manifest_digest == grant.manifest_digest;
    if !acknowledged {
        return None;
    }
    let configuration = state.runtime_configuration.as_ref()?;
    let deployment = deployments.get(state.deployment_id.as_deref()?)?;
    if deployment.plugin_id != grant.plugin_id
        || deployment.plugin_version != grant.plugin_version
        || deployment.manifest_digest != grant.manifest_digest
        || deployment.healthy_until_ms <= now_ms
    {
        return None;
    }
    let installed = projected.lifecycle == ProjectedLifecycle::Installed;
    let suspended = installed && (grant.suspended || flags.suspended);
    let enabled = installed && grant.enabled && state.runtime_enabled && !suspended;
    // A disabled trusted installation keeps only its recovery surface; user dispatch still calls
    // the grant, which rejects `enabled = false`. A suspended plugin stays so Core and clients can
    // say why it is closed.
    if !enabled && !suspended && !has_recovery_surface(manifest) {
        return None;
    }
    let digest = configuration_digest(configuration).ok()?;
    if state.runtime_configuration_digest.as_deref() != Some(digest.as_str()) {
        return None;
    }
    let client_configuration = match manifest.client_visible_configuration(configuration) {
        Ok(visible) => visible,
        Err(_) => {
            tracing::warn!(
                app = %grant.tenant_id,
                plugin = %grant.plugin_id,
                "plugin client configuration is not projectable"
            );
            return None;
        }
    };
    let mut served = grant.clone();
    served.revision = state.runtime_revision as u64;
    served.enabled = enabled;
    served.suspended = suspended;
    let resolved = ResolvedPlugin::new(
        served,
        Arc::clone(manifest),
        PluginDeployment {
            deployment_id: deployment.deployment_id.clone(),
            plugin_id: deployment.plugin_id.clone(),
            plugin_version: deployment.plugin_version.clone(),
            manifest_digest: deployment.manifest_digest.clone(),
            artifact_digest: deployment.artifact_digest.clone(),
            grpc_authority: deployment.grpc_authority.clone(),
        },
        digest,
        client_configuration,
        projected.features.clone(),
    )
    .and_then(|plugin| {
        let served = super::served_web_bundles(manifest, &projected.web_bundles, web_bundles);
        if served.len() < projected.web_bundles.len() {
            tracing::warn!(
                app = %grant.tenant_id,
                plugin = %grant.plugin_id,
                projected = projected.web_bundles.len(),
                served = served.len(),
                "projected web bundles not approved in the registry or not declared; left out"
            );
        }
        plugin.with_web_bundles(served)
    });
    match resolved {
        Ok(plugin) => Some((
            plugin,
            state
                .runtime_healthy_until_ms
                .min(deployment.healthy_until_ms),
        )),
        Err(error) => {
            tracing::warn!(
                app = %grant.tenant_id,
                plugin = %grant.plugin_id,
                %error,
                "projected plugin does not match the registry"
            );
            None
        }
    }
}

/// Dependencies stay enabled at compatible versions, including when a parent lost its runtime
/// health here and the platform does not know yet.
fn retain_satisfied(plugins: &mut Vec<ResolvedPlugin>) {
    loop {
        let versions: BTreeMap<String, String> = plugins
            .iter()
            .filter(|p| p.grant.enabled && !p.grant.revoked)
            .map(|p| (p.grant.plugin_id.clone(), p.grant.plugin_version.clone()))
            .collect();
        let before = plugins.len();
        plugins.retain(|plugin| {
            !plugin.grant.enabled
                || plugin.manifest().dependencies.iter().all(|dependency| {
                    match versions.get(&dependency.plugin_id) {
                        None => dependency.optional,
                        Some(version) => semver::VersionReq::parse(&dependency.version_range)
                            .ok()
                            .zip(semver::Version::parse(version).ok())
                            .is_some_and(|(range, version)| range.matches(&version)),
                    }
                })
        });
        if before == plugins.len() {
            return;
        }
    }
}
