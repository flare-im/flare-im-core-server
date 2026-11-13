//! Plugin authorization: the platform's projection, durable, resolved against the registry.
//!
//! A dispatch reads a parsed snapshot. Manifests are cached by digest, forever: a digest names
//! exact bytes. Resolved snapshots are cached by application and checked with one light query for
//! the projection version and the registry revision, so a new projection, a revocation or a runtime
//! fact takes effect on the next read while an unchanged application parses nothing.
use crate::domain::plugin::{
    AppPluginState, ApprovedWebBundles, DevBinding, PluginDeploymentRecord, VersionFlags, resolve,
    with_development,
};
use crate::domain::{model::TenantProjectionOutcome, repository::PluginAuthorizationReader};
use dashmap::DashMap;
use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::runtime::{
    ManifestCache, PluginProjection, PluginRuntimeSnapshot,
};
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

use super::postgres_plugin_registry::{
    GLOBAL_SCOPE, apply_projected_bindings, deployment_row, state_row,
};
use crate::infrastructure::config::plugin_config_key::PluginConfigCipher;
#[path = "postgres_plugin_event_directory.rs"]
mod event_directory;

struct CachedSnapshot {
    version: i64,
    registry_revision: i64,
    fresh_until_ms: i64,
    snapshot: Arc<PluginRuntimeSnapshot>,
}

pub struct PostgresPluginAuthorization {
    pool: Arc<PgPool>,
    manifests: ManifestCache,
    resolved: DashMap<String, Arc<CachedSnapshot>>,
    /// `FLARE_PLUGIN_DEV_MODE`: without it no development row is ever served.
    development: bool,
    cipher: Arc<PluginConfigCipher>,
}
fn unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("plugin authorization storage unavailable")
}
impl PostgresPluginAuthorization {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            manifests: ManifestCache::default(),
            resolved: DashMap::new(),
            development: false,
            cipher: Arc::new(PluginConfigCipher::default()),
        }
    }
    pub fn with_cipher(mut self, cipher: Arc<PluginConfigCipher>) -> Self {
        self.cipher = cipher;
        self
    }
    pub fn with_development(mut self, enabled: bool) -> Self {
        self.development = enabled;
        self
    }
    pub async fn replace(&self, projection: &PluginProjection) -> Result<TenantProjectionOutcome> {
        let now: i64 =
            sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(self.pool.as_ref())
                .await
                .map_err(unavailable)?;
        projection.validate(now).map_err(unavailable)?;
        let snapshot = projection;
        let json = serde_json::to_value(snapshot).map_err(unavailable)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET LOCAL lock_timeout='2s'")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let version:Option<i64>=sqlx::query_scalar("INSERT INTO plugin_authorization_snapshots(app_id,version,snapshot,valid_until_ms) VALUES($1,$2,$3,$4) ON CONFLICT(app_id) DO UPDATE SET version=EXCLUDED.version,snapshot=EXCLUDED.snapshot,valid_until_ms=EXCLUDED.valid_until_ms WHERE plugin_authorization_snapshots.version<EXCLUDED.version RETURNING version")
            .bind(&snapshot.tenant_id).bind(snapshot.version as i64).bind(&json).bind(snapshot.valid_until_ms).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let accepted = version.is_some();
        let current = match version {
            Some(v) => v,
            None => sqlx::query_scalar(
                "SELECT version FROM plugin_authorization_snapshots WHERE app_id=$1",
            )
            .bind(&snapshot.tenant_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?,
        };
        if !accepted && current == snapshot.version as i64 {
            let same: bool = sqlx::query_scalar(
                "SELECT snapshot=$2 FROM plugin_authorization_snapshots WHERE app_id=$1",
            )
            .bind(&snapshot.tenant_id)
            .bind(json)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            if !same {
                return Err(FlareError::system(
                    "plugin projection version reused with different content",
                ));
            }
        }
        // The deny in the projection is effective from this commit, whatever the plugin says.
        if accepted {
            apply_projected_bindings(&mut tx, snapshot)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(TenantProjectionOutcome {
            accepted,
            current_version: current as u64,
        })
    }
    pub async fn versions(&self, apps: &[String]) -> Result<Vec<(String, u64)>> {
        let rows:Vec<(String,i64)>=sqlx::query_as("SELECT app_id,version FROM plugin_authorization_snapshots WHERE cardinality($1::text[])=0 OR app_id=ANY($1)").bind(apps).fetch_all(self.pool.as_ref()).await.map_err(unavailable)?;
        Ok(rows.into_iter().map(|(a, v)| (a, v as u64)).collect())
    }

    /// The manifests and version flags of `digests`, reading only what the cache lacks. A
    /// development version is unknown here unless `development`: a projection cannot name one.
    async fn releases(
        &self,
        digests: &[String],
        development: bool,
    ) -> Result<(
        HashMap<String, Arc<PluginManifest>>,
        HashMap<String, VersionFlags>,
    )> {
        let flags: Vec<(String, bool, bool)> = sqlx::query_as(
            "SELECT manifest_digest,revoked,suspended FROM plugin_versions \
             WHERE manifest_digest=ANY($1) AND ($2 OR NOT development)",
        )
        .bind(digests)
        .bind(development)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let mut manifests = HashMap::new();
        let mut missing = Vec::new();
        for (digest, _, _) in &flags {
            match self.manifests.get(digest) {
                Some(manifest) => {
                    manifests.insert(digest.clone(), manifest);
                }
                None => missing.push(digest.clone()),
            }
        }
        if !missing.is_empty() {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT manifest_digest,manifest_json FROM plugin_versions WHERE manifest_digest=ANY($1)",
            )
            .bind(&missing)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(unavailable)?;
            for (digest, raw) in rows {
                match self.manifests.insert(&digest, raw.as_bytes()) {
                    Ok(manifest) => {
                        manifests.insert(digest, manifest);
                    }
                    Err(error) => {
                        tracing::error!(%digest, %error, "stored plugin manifest does not parse");
                    }
                }
            }
        }
        let flags = flags
            .into_iter()
            .map(|(digest, revoked, suspended)| (digest, VersionFlags { revoked, suspended }))
            .collect();
        Ok((manifests, flags))
    }

    /// The approved, not withdrawn web bundles of the versions `digests` name. Attaching and
    /// withdrawing raise the global registry revision, so a cached resolution never outlives one.
    async fn approved_web_bundles(&self, digests: &[String]) -> Result<ApprovedWebBundles> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT v.manifest_digest,b.bundle_id,b.sha256 FROM plugin_web_bundles b \
             JOIN plugin_versions v ON v.plugin_id=b.plugin_id AND v.plugin_version=b.plugin_version \
             WHERE v.manifest_digest=ANY($1) AND b.withdrawn_at IS NULL",
        )
        .bind(digests)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let mut bundles = ApprovedWebBundles::new();
        for (digest, bundle, sha256) in rows {
            bundles.entry(digest).or_default().insert(bundle, sha256);
        }
        Ok(bundles)
    }

    async fn resolve(
        &self,
        tenant: &str,
        projection: &PluginProjection,
        now: i64,
        development: bool,
    ) -> Result<crate::domain::plugin::Resolution> {
        let digests: Vec<String> = projection
            .plugins
            .iter()
            .map(|plugin| plugin.manifest_digest().to_owned())
            .collect();
        let (manifests, versions) = self.releases(&digests, development).await?;
        let rows = sqlx::query("SELECT * FROM plugin_app_states WHERE app_id=$1")
            .bind(tenant)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(unavailable)?;
        let mut states: HashMap<String, AppPluginState> = HashMap::with_capacity(rows.len());
        for row in &rows {
            let mut state = state_row(row).map_err(unavailable)?;
            // Without its state this plugin is refused; the others are not touched.
            if self.cipher.open_state(&mut state).is_err() {
                tracing::error!(
                    tenant,
                    binding = %state.binding_id,
                    "plugin configuration does not open with the configured keys"
                );
                continue;
            }
            states.insert(state.binding_id.clone(), state);
        }
        let bound: Vec<String> = states
            .values()
            .filter_map(|state| state.deployment_id.clone())
            .collect();
        let rows = sqlx::query("SELECT * FROM plugin_deployments WHERE deployment_id=ANY($1)")
            .bind(&bound)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(unavailable)?;
        let deployments: HashMap<String, PluginDeploymentRecord> = rows
            .iter()
            .map(|row| deployment_row(row).map(|d| (d.deployment_id.clone(), d)))
            .collect::<std::result::Result<_, _>>()
            .map_err(unavailable)?;
        let web_bundles = self.approved_web_bundles(&digests).await?;
        let resolution = resolve(
            projection,
            &manifests,
            &versions,
            &states,
            &deployments,
            &web_bundles,
            now,
        );
        for digest in &resolution.unknown_digests {
            tracing::warn!(tenant, %digest, "projected plugin is not in the registry; refused");
        }
        Ok(resolution)
    }

    /// The application's projection with its development bindings, or `None` when it has none and
    /// the normal path applies. Not cached: a development read is one person's traffic, and the
    /// application may have no projection to key a cache on. A binding that breaks the snapshot,
    /// such as a namespace another plugin of the application holds, must not take the others down.
    async fn development_snapshot(
        &self,
        tenant: &str,
    ) -> Result<Option<Arc<PluginRuntimeSnapshot>>> {
        let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
            "SELECT a.binding_id,a.plugin_id,a.plugin_version,a.manifest_digest,a.revision \
             FROM plugin_app_states a \
             JOIN plugin_dev_apps d ON d.app_id=a.app_id \
             JOIN plugin_versions v ON v.manifest_digest=a.manifest_digest \
             WHERE a.app_id=$1 AND v.development ORDER BY a.binding_id",
        )
        .bind(tenant)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let bindings: Vec<DevBinding> = rows
            .into_iter()
            .map(
                |(binding_id, plugin_id, plugin_version, manifest_digest, revision)| DevBinding {
                    binding_id,
                    plugin_id,
                    plugin_version,
                    manifest_digest,
                    revision,
                },
            )
            .collect();
        let (now, stored): (i64, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint,\
             (SELECT snapshot FROM plugin_authorization_snapshots WHERE app_id=$1)",
        )
        .bind(tenant)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let base = stored
            .and_then(|value| serde_json::from_value::<PluginProjection>(value).ok())
            .filter(|projection| projection.validate(now).is_ok());
        let digests: Vec<String> = bindings
            .iter()
            .map(|binding| binding.manifest_digest.clone())
            .collect();
        let (manifests, _) = self.releases(&digests, true).await?;
        let projection = with_development(tenant, base, &bindings, &manifests, now);
        let resolution = self.resolve(tenant, &projection, now, true).await?;
        match resolution.snapshot.validate(now) {
            Ok(()) => Ok(Some(Arc::new(resolution.snapshot))),
            Err(error) => {
                tracing::warn!(tenant, %error, "development bindings do not fit this application");
                Ok(None)
            }
        }
    }
}
#[async_trait::async_trait]
impl PluginAuthorizationReader for PostgresPluginAuthorization {
    async fn event_directory(
        &self,
        tenant: &str,
    ) -> Result<crate::domain::repository::PluginEventDirectory> {
        self.read_event_directory(tenant).await
    }
    async fn event_active_since_ms(
        &self,
        tenant: &str,
        grant: &str,
        revision: u64,
        configuration_digest: &str,
    ) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT event_active_since_ms FROM plugin_app_states WHERE app_id=$1 AND binding_id=$2 AND runtime_revision=$3 AND runtime_configuration_digest=$4")
            .bind(tenant).bind(grant).bind(i64::try_from(revision).map_err(unavailable)?).bind(configuration_digest)
            .fetch_optional(self.pool.as_ref()).await.map_err(unavailable)
    }
    async fn plugin_snapshot(&self, tenant: &str) -> Result<Option<Arc<PluginRuntimeSnapshot>>> {
        if self.development
            && let Some(snapshot) = self.development_snapshot(tenant).await?
        {
            return Ok(Some(snapshot));
        }
        // Read before the content below, so a change racing this read is seen by the next one.
        let probe: Option<(i64, i64, i64)> = sqlx::query_as(
            "SELECT s.version,(extract(epoch FROM clock_timestamp())*1000)::bigint,\
             (SELECT COALESCE(sum(revision),0)::bigint FROM plugin_registry_revisions WHERE scope=$2 OR scope=$1) \
             FROM plugin_authorization_snapshots s \
             WHERE s.app_id=$1 AND s.valid_until_ms>(extract(epoch FROM clock_timestamp())*1000)::bigint",
        )
        .bind(tenant)
        .bind(GLOBAL_SCOPE)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let Some((version, now, registry_revision)) = probe else {
            self.resolved.remove(tenant);
            return Ok(None);
        };
        if let Some(cached) = self.resolved.get(tenant)
            && cached.version == version
            && cached.registry_revision == registry_revision
            && now < cached.fresh_until_ms
        {
            return Ok(Some(Arc::clone(&cached.snapshot)));
        }
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT snapshot FROM plugin_authorization_snapshots WHERE app_id=$1",
        )
        .bind(tenant)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let Some(value) = value else {
            return Ok(None);
        };
        let projection: PluginProjection = serde_json::from_value(value).map_err(unavailable)?;
        projection.validate(now).map_err(unavailable)?;
        let resolution = self.resolve(tenant, &projection, now, false).await?;
        resolution.snapshot.validate(now).map_err(unavailable)?;
        let snapshot = Arc::new(resolution.snapshot);
        self.resolved.insert(
            tenant.to_owned(),
            Arc::new(CachedSnapshot {
                // The stored row may be newer than the probe; the next probe then re-resolves.
                version,
                registry_revision,
                fresh_until_ms: resolution.fresh_until_ms,
                snapshot: Arc::clone(&snapshot),
            }),
        );
        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires an explicitly owned disposable plugin test PostgreSQL cluster"]
    async fn plugins_pg_persists_revocation_watermark_and_rejects_revision_reuse() {
        let url =
            std::env::var("FLARE_PLUGIN_TEST_DATABASE_URL").expect("owned fixture URL required");
        let pool = Arc::new(PgPool::connect(&url).await.unwrap());
        let owned:bool=sqlx::query_scalar("SELECT current_database() LIKE 'flare_plugin_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-plugin-owned/v1' AND to_regclass('public.plugin_authorization_snapshots') IS NULL").fetch_one(pool.as_ref()).await.unwrap();
        assert!(owned, "refuse unowned/nonempty Core fixture");
        let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../deploy/init.sql"));
        let start = source
            .find("CREATE TABLE IF NOT EXISTS plugin_authorization_snapshots (")
            .unwrap();
        let end = start + source[start..].find(");").unwrap() + 2;
        sqlx::raw_sql(&source[start..end])
            .execute(pool.as_ref())
            .await
            .unwrap();
        let start = source
            .find("CREATE TABLE IF NOT EXISTS plugin_versions (")
            .unwrap();
        let tail = "CREATE TABLE IF NOT EXISTS plugin_registry_revisions (";
        let end = source.find(tail).unwrap();
        let end = end + source[end..].find(");").unwrap() + 2;
        sqlx::raw_sql(&source[start..end])
            .execute(pool.as_ref())
            .await
            .unwrap();
        let repo = PostgresPluginAuthorization::new(pool.clone());
        let now = chrono::Utc::now().timestamp_millis();
        let snapshot = PluginProjection {
            organization_id: Some("org-1".into()),
            tenant_id: "201".into(),
            version: 20,
            issued_at_ms: now,
            valid_until_ms: now + 60000,
            plugins: vec![],
            bindings: Vec::new(),
        };
        assert!(repo.replace(&snapshot).await.unwrap().accepted);
        assert!(!repo.replace(&snapshot).await.unwrap().accepted);
        assert!(
            repo.replace(&PluginProjection {
                valid_until_ms: now + 59000,
                ..snapshot.clone()
            })
            .await
            .is_err()
        );
        assert_eq!(
            repo.replace(&PluginProjection {
                version: 19,
                ..snapshot.clone()
            })
            .await
            .unwrap()
            .current_version,
            20
        );
        let restart = PostgresPluginAuthorization::new(pool.clone());
        let served = restart.plugin_snapshot("201").await.unwrap().unwrap();
        assert_eq!(served.version, snapshot.version);
        assert!(served.plugins.is_empty());
        assert!(restart.plugin_snapshot("202").await.unwrap().is_none());
        sqlx::query("UPDATE plugin_authorization_snapshots SET valid_until_ms=1,snapshot=jsonb_set(snapshot,'{validUntilMs}','1') WHERE app_id='201'").execute(pool.as_ref()).await.unwrap();
        assert!(restart.plugin_snapshot("201").await.unwrap().is_none());
        assert_eq!(
            restart.versions(&[]).await.unwrap(),
            vec![("201".into(), 20)]
        );
        super::super::postgres_resource_budget::tests::run(pool.clone()).await;
        pool.close().await;
    }
}
