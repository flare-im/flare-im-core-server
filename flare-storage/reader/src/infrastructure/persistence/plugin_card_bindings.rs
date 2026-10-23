//! [`PluginCardBindingSource`] on the Core database: `plugin_entity_bindings` (migration 0016, the
//! index on `(tenant_id, server_msg_id)`) and `plugin_authorization_snapshots` (0017), read only.
//! Parsing a projection validates every manifest in it, so parsed rules are cached per (tenant,
//! projection version), like Storage Writer does for binding.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use sqlx::{Pool, Postgres};

use crate::domain::model::plugin_card::{EntityRules, PluginCardBindingSource};

/// Tenants whose parsed projection is kept; the cache starts over when it is full.
const MAX_CACHED_TENANTS: usize = 1024;

pub struct PostgresPluginCardBindings {
    pool: Pool<Postgres>,
    rules: Mutex<HashMap<String, Arc<EntityRules>>>,
}

fn db_error(operation: &str, err: sqlx::Error) -> flare_server_core::error::FlareError {
    map_infra_error(err, ErrorCode::DatabaseError, operation)
}

impl PostgresPluginCardBindings {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            rules: Mutex::new(HashMap::new()),
        }
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
        let snapshot = match serde_json::from_value::<PluginRuntimeSnapshot>(snapshot) {
            Ok(snapshot) if snapshot.validate(now_ms).is_ok() => snapshot,
            _ => {
                tracing::warn!(
                    tenant_id,
                    version,
                    "stored plugin projection is invalid; plugin card copies stay unmarked"
                );
                return Ok(None);
            }
        };
        let rules = Arc::new(EntityRules::from_snapshot(&snapshot, now_ms));
        if let Ok(mut cache) = self.rules.lock() {
            if cache.len() >= MAX_CACHED_TENANTS && !cache.contains_key(tenant_id) {
                cache.clear();
            }
            cache.insert(tenant_id.to_owned(), rules.clone());
        }
        Ok(Some(rules))
    }
}
