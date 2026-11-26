//! [`PluginCardBindingSource`] on the Core database: `plugin_entity_bindings` (migration 0016, the
//! index on `(tenant_id, server_msg_id)`), `plugin_authorization_snapshots` (0017) and the
//! registry's `plugin_versions`, read only. A projection names its manifests by digest: each digest
//! is read and parsed once per process, and the rules are cached per (tenant, projection version),
//! like Storage Writer does for binding.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use flare_im_core_capability_core::plugin::PluginManifest;
use flare_im_core_capability_core::plugin::runtime::{ManifestCache, PluginProjection};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use sqlx::{Pool, Postgres};

use crate::domain::model::plugin_card::{EntityRules, PluginCardBindingSource};

/// Tenants whose rules are kept; the cache starts over when it is full.
const MAX_CACHED_TENANTS: usize = 1024;

pub struct PostgresPluginCardBindings {
    pool: Pool<Postgres>,
    rules: Mutex<HashMap<String, Arc<EntityRules>>>,
    manifests: ManifestCache,
}

fn db_error(operation: &str, err: sqlx::Error) -> flare_server_core::error::FlareError {
    map_infra_error(err, ErrorCode::DatabaseError, operation)
}

impl PostgresPluginCardBindings {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            rules: Mutex::new(HashMap::new()),
            manifests: ManifestCache::default(),
        }
    }

    /// The manifests of `projection` by digest. The registry is read only for digests this process
    /// has not parsed yet. A digest it does not hold, or whose text does not hash to it, stays out
    /// of the answer, so its plugin is skipped.
    async fn manifests(
        &self,
        projection: &PluginProjection,
    ) -> Result<HashMap<String, Arc<PluginManifest>>> {
        let mut found = HashMap::new();
        let mut missing = Vec::new();
        for projected in &projection.plugins {
            let digest = projected.manifest_digest();
            match self.manifests.get(digest) {
                Some(manifest) => {
                    found.insert(digest.to_owned(), manifest);
                }
                None => missing.push(digest.to_owned()),
            }
        }
        if missing.is_empty() {
            return Ok(found);
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT manifest_digest, manifest_json FROM plugin_versions \
             WHERE manifest_digest = ANY($1)",
        )
        .bind(&missing)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_error("read plugin manifests", err))?;
        for (digest, raw) in rows {
            match self.manifests.insert(&digest, raw.as_bytes()) {
                Ok(manifest) => {
                    found.insert(digest, manifest);
                }
                Err(error) => {
                    tracing::warn!(digest, %error, "registered plugin manifest is invalid; its plugin cards stay unmarked");
                }
            }
        }
        for digest in missing.iter().filter(|digest| !found.contains_key(*digest)) {
            tracing::warn!(
                digest,
                "plugin manifest is not in the registry; its plugin cards stay unmarked"
            );
        }
        Ok(found)
    }
}

#[async_trait::async_trait]
impl PluginCardBindingSource for PostgresPluginCardBindings {
    async fn bound_cards(
        &self,
        tenant_id: &str,
        server_ids: &[String],
    ) -> Result<HashSet<(String, String)>> {
        if server_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT server_msg_id, plugin_id FROM plugin_entity_bindings \
             WHERE tenant_id = $1 AND server_msg_id = ANY($2)",
        )
        .bind(tenant_id)
        .bind(server_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_error("read plugin card bindings", err))?;
        Ok(rows.into_iter().collect())
    }

    async fn entity_rules(&self, tenant_id: &str, now_ms: i64) -> Result<Option<Arc<EntityRules>>> {
        let version: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM plugin_authorization_snapshots WHERE app_id = $1 AND valid_until_ms > $2",
        )
        .bind(tenant_id)
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("read plugin projection version", err))?;
        let Some(version) = version else {
            return Ok(None);
        };
        if let Some(cached) = self
            .rules
            .lock()
            .ok()
            .and_then(|rules| rules.get(tenant_id).cloned())
            && cached.version == version as u64
            && cached.valid_until_ms > now_ms
        {
            return Ok(Some(cached));
        }
        let snapshot: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT snapshot FROM plugin_authorization_snapshots WHERE app_id = $1 AND version = $2",
        )
        .bind(tenant_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("read plugin projection", err))?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let projection = match serde_json::from_value::<PluginProjection>(snapshot) {
            Ok(projection) if projection.validate(now_ms).is_ok() => projection,
            _ => {
                tracing::warn!(
                    tenant_id,
                    version,
                    "stored plugin projection is invalid; plugin card copies stay unmarked"
                );
                return Ok(None);
            }
        };
        let manifests = self.manifests(&projection).await?;
        let rules = Arc::new(EntityRules::from_projection(
            &projection,
            &manifests,
            now_ms,
        ));
        if let Ok(mut cache) = self.rules.lock() {
            if cache.len() >= MAX_CACHED_TENANTS && !cache.contains_key(tenant_id) {
                cache.clear();
            }
            cache.insert(tenant_id.to_owned(), rules.clone());
        }
        Ok(Some(rules))
    }
}
