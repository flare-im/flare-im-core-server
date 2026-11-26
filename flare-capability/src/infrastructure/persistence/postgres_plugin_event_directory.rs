//! Desired subscriptions outlive expiring routing/health leases. Only an explicit
//! installation, approval or feature deny removes them from delivery.
use super::*;
use crate::domain::{plugin::ApprovedGrant, repository::PluginEventDirectory};
use flare_im_core_capability_core::plugin::{
    event::{MESSAGE_BODY_SCOPE, MESSAGE_CREATED, MESSAGE_EVENTS_SCOPE},
    runtime::ProjectedLifecycle,
};
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};

fn add_subscriptions(
    output: &mut BTreeSet<(String, String)>,
    grant: &str,
    manifest: &PluginManifest,
    approved: &ApprovedGrant,
    features: &BTreeMap<String, bool>,
) {
    let closed = manifest.closed_operations(features);
    for subscription in &manifest.event_subscriptions {
        if subscription.event_type != MESSAGE_CREATED
            || closed.contains(&subscription.operation)
            || !approved.operations.contains(&subscription.operation)
            || !subscription
                .permission_ids
                .iter()
                .all(|id| approved.permissions.contains(id))
            || !approved.data_scopes.contains(MESSAGE_EVENTS_SCOPE)
            || subscription.include_body && !approved.data_scopes.contains(MESSAGE_BODY_SCOPE)
        {
            continue;
        }
        if manifest
            .operation(&subscription.operation)
            .is_some_and(|op| {
                op.permission_ids.iter().all(|id| {
                    approved.permissions.contains(id)
                        && manifest
                            .permissions
                            .iter()
                            .find(|p| &p.id == id)
                            .is_some_and(|p| {
                                p.data_scopes
                                    .iter()
                                    .all(|scope| approved.data_scopes.contains(scope))
                            })
                })
            })
        {
            output.insert((grant.into(), subscription.operation.clone()));
        }
    }
}
impl PostgresPluginAuthorization {
    pub(super) async fn read_event_directory(&self, tenant: &str) -> Result<PluginEventDirectory> {
        // Read the stored desired control projection even when its routing lease expired.
        // Its old authority cannot dispatch anything, but still proves a subscriber exists
        // and must not be silently forgotten while the next projection is in transit.
        let stored: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT snapshot FROM plugin_authorization_snapshots WHERE app_id=$1",
        )
        .bind(tenant)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let mut subscriptions = BTreeSet::new();
        let mut unavailable_grants = BTreeSet::new();
        if let Some(stored) = stored {
            let projection: PluginProjection =
                serde_json::from_value(stored).map_err(unavailable)?;
            if projection.tenant_id != tenant {
                return Err(unavailable("event directory tenant mismatch"));
            }
            let digests = projection
                .plugins
                .iter()
                .map(|p| p.manifest_digest().to_owned())
                .collect::<Vec<_>>();
            let (manifests, versions) = self.releases(&digests, false).await?;
            for projected in &projection.plugins {
                let grant = &projected.grant;
                if projected.lifecycle != ProjectedLifecycle::Installed
                    || !grant.enabled
                    || grant.revoked
                    || grant.suspended
                    || !grant.approved_data_scopes.contains(MESSAGE_EVENTS_SCOPE)
                {
                    continue;
                }
                let Some(flags) = versions.get(projected.manifest_digest()) else {
                    unavailable_grants.insert(grant.grant_id.clone());
                    continue;
                };
                if flags.revoked || flags.suspended {
                    continue;
                }
                let Some(manifest) = manifests.get(projected.manifest_digest()) else {
                    unavailable_grants.insert(grant.grant_id.clone());
                    continue;
                };
                add_subscriptions(
                    &mut subscriptions,
                    &grant.grant_id,
                    manifest,
                    &ApprovedGrant {
                        operations: grant.approved_operations.clone(),
                        permissions: grant.approved_permissions.clone(),
                        data_scopes: grant.approved_data_scopes.clone(),
                    },
                    &projected.features,
                );
            }
        }
        if self.development {
            // Development bindings have their own authenticated local approval, and never
            // enter the platform projection. Require the application opt-in as runtime does.
            let rows = sqlx::query(
                "SELECT a.binding_id,a.manifest_digest,a.approved,a.features,v.manifest_json \
                 FROM plugin_app_states a \
                 LEFT JOIN plugin_versions v ON a.manifest_digest=v.manifest_digest \
                 JOIN plugin_dev_apps d ON d.app_id=a.app_id \
                 WHERE a.app_id=$1 \
                 AND (v.manifest_digest IS NULL OR (v.development AND NOT v.revoked AND NOT v.suspended)) \
                 AND a.enabled AND NOT a.suspended AND a.lifecycle NOT IN ('uninstalled','revoked')",
            )
            .bind(tenant)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(unavailable)?;
            for row in rows {
                let grant: String = row.try_get("binding_id").map_err(unavailable)?;
                let digest: String = row.try_get("manifest_digest").map_err(unavailable)?;
                let approved: ApprovedGrant =
                    match serde_json::from_value(row.try_get("approved").map_err(unavailable)?) {
                        Ok(approved) => approved,
                        Err(_) => {
                            unavailable_grants.insert(grant);
                            continue;
                        }
                    };
                if !approved.data_scopes.contains(MESSAGE_EVENTS_SCOPE) {
                    continue;
                }
                let raw: Option<String> = row.try_get("manifest_json").map_err(unavailable)?;
                let Some(raw) = raw else {
                    unavailable_grants.insert(grant);
                    continue;
                };
                let manifest = match self.manifests.get(&digest) {
                    Some(manifest) => manifest,
                    None => match self.manifests.insert(&digest, raw.as_bytes()) {
                        Ok(manifest) => manifest,
                        Err(error) => {
                            tracing::error!(%grant, %digest, %error, "stored development event manifest does not parse");
                            unavailable_grants.insert(grant);
                            continue;
                        }
                    },
                };
                let features =
                    match serde_json::from_value(row.try_get("features").map_err(unavailable)?) {
                        Ok(features) => features,
                        Err(_) => {
                            unavailable_grants.insert(grant);
                            continue;
                        }
                    };
                add_subscriptions(&mut subscriptions, &grant, &manifest, &approved, &features);
            }
        }
        let routes = if subscriptions.is_empty() {
            None
        } else {
            self.plugin_snapshot(tenant).await?
        };
        Ok(PluginEventDirectory {
            subscriptions,
            unavailable_grants,
            routes,
        })
    }
}
