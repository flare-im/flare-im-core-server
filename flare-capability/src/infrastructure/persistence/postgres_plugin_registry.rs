//! The plugin registry on PostgreSQL (`plugin_versions`, `plugin_namespaces`,
//! `plugin_version_assets`, `plugin_deployments`, `plugin_app_states`,
//! `plugin_registry_revisions`).
use std::sync::Arc;

use flare_im_core_capability_core::plugin::PluginManifest;
use flare_im_core_capability_core::plugin::configuration::restore_secrets;
use flare_im_core_capability_core::plugin::runtime::{
    ManifestCache, PluginDeployment, PluginProjection,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::domain::plugin::{
    AppPluginState, DeploymentFilter, DeploymentRegistration, DesiredAppState, DevRegistered,
    DevRelease, NewPluginVersion, PluginAssetContent, PluginAssetRef, PluginDeploymentRecord,
    PluginRegistryError, PluginVersion, PluginVersionDetail, ReconcileRow, RegistryResult,
    RuntimePatch, StateLifecycle, VersionFilter, VersionKey, VersionStateChange, WebBundleApproval,
    WebBundleAttachment, WebBundleFilter, WebBundleRef, namespace_conflict,
};
use crate::domain::repository::plugin_registry::{PluginRegistryRepository, ReconcileCursor};
use crate::infrastructure::config::plugin_config_key::PluginConfigCipher;

const VERSION_COLUMNS: &str = "plugin_id,plugin_version,display_name,publisher_id,manifest_digest,kind,published,revoked,suspended,created_at,updated_at";
const DEPLOYMENT_COLUMNS: &str = "deployment_id,plugin_id,plugin_version,manifest_digest,artifact_digest,grpc_authority,healthy_until_ms,last_error,created_at";
const STATE_COLUMNS: &str = "app_id,binding_id,plugin_id,plugin_version,manifest_digest,revision,enabled,configured,configuration,deployment_id,runtime_revision,runtime_configuration_digest,runtime_enabled,runtime_healthy_until_ms,runtime_error,runtime_error_detail,runtime_configuration,runtime_attempts,runtime_next_attempt_ms,runtime_enabled_digest,organization_id,lifecycle,suspended,owned,offline_hold,export_until_ms,approved,features,projected_revision,updated_at";
const NOW_MS: &str = "(extract(epoch FROM clock_timestamp())*1000)::bigint";
const WEB_BUNDLE_COLUMNS: &str =
    "plugin_id,plugin_version,bundle_id,sha256,size,approved_at,withdrawn_at,updated_at";
/// The scope of a registry revision no single application owns.
pub const GLOBAL_SCOPE: &str = "";

#[derive(Clone, Copy)]
enum ConfigurationWrite {
    Replace,
    Preserve,
}

fn configuration_write(
    current: &Option<AppPluginState>,
    next: &serde_json::Value,
) -> ConfigurationWrite {
    if current
        .as_ref()
        .is_some_and(|state| &state.configuration == next)
    {
        ConfigurationWrite::Preserve
    } else {
        ConfigurationWrite::Replace
    }
}

fn unavailable(error: sqlx::Error) -> PluginRegistryError {
    tracing::warn!(%error, "plugin registry storage failed");
    PluginRegistryError::Unavailable
}

fn version_row(row: &PgRow) -> Result<PluginVersion, sqlx::Error> {
    Ok(PluginVersion {
        plugin_id: row.try_get("plugin_id")?,
        plugin_version: row.try_get("plugin_version")?,
        display_name: row.try_get("display_name")?,
        publisher_id: row.try_get("publisher_id")?,
        manifest_digest: row.try_get("manifest_digest")?,
        kind: row.try_get("kind")?,
        published: row.try_get("published")?,
        revoked: row.try_get("revoked")?,
        suspended: row.try_get("suspended")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn web_bundle_row(row: &PgRow) -> Result<WebBundleAttachment, sqlx::Error> {
    Ok(WebBundleAttachment {
        plugin_id: row.try_get("plugin_id")?,
        plugin_version: row.try_get("plugin_version")?,
        bundle_id: row.try_get("bundle_id")?,
        sha256: row.try_get("sha256")?,
        size: row.try_get("size")?,
        approved_at: row.try_get("approved_at")?,
        withdrawn_at: row.try_get("withdrawn_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn deployment_row(row: &PgRow) -> Result<PluginDeploymentRecord, sqlx::Error> {
    Ok(PluginDeploymentRecord {
        deployment_id: row.try_get("deployment_id")?,
        plugin_id: row.try_get("plugin_id")?,
        plugin_version: row.try_get("plugin_version")?,
        manifest_digest: row.try_get("manifest_digest")?,
        artifact_digest: row.try_get("artifact_digest")?,
        grpc_authority: row.try_get("grpc_authority")?,
        healthy_until_ms: row.try_get("healthy_until_ms")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) fn state_row(row: &PgRow) -> Result<AppPluginState, sqlx::Error> {
    Ok(AppPluginState {
        app_id: row.try_get("app_id")?,
        binding_id: row.try_get("binding_id")?,
        plugin_id: row.try_get("plugin_id")?,
        plugin_version: row.try_get("plugin_version")?,
        manifest_digest: row.try_get("manifest_digest")?,
        revision: row.try_get("revision")?,
        enabled: row.try_get("enabled")?,
        configured: row.try_get("configured")?,
        configuration: row.try_get("configuration")?,
        deployment_id: row.try_get("deployment_id")?,
        runtime_revision: row.try_get("runtime_revision")?,
        runtime_configuration_digest: row.try_get("runtime_configuration_digest")?,
        runtime_enabled: row.try_get("runtime_enabled")?,
        runtime_healthy_until_ms: row.try_get("runtime_healthy_until_ms")?,
        runtime_error: row.try_get("runtime_error")?,
        runtime_error_detail: row.try_get("runtime_error_detail")?,
        runtime_configuration: row.try_get("runtime_configuration")?,
        runtime_attempts: row.try_get("runtime_attempts")?,
        runtime_next_attempt_ms: row.try_get("runtime_next_attempt_ms")?,
        runtime_enabled_digest: row.try_get("runtime_enabled_digest")?,
        organization_id: row.try_get("organization_id")?,
        lifecycle: StateLifecycle::parse(row.try_get("lifecycle")?)
            .ok_or_else(|| sqlx::Error::Decode("unknown plugin binding lifecycle".into()))?,
        suspended: row.try_get("suspended")?,
        owned: row.try_get("owned")?,
        offline_hold: row.try_get("offline_hold")?,
        export_until_ms: row.try_get("export_until_ms")?,
        approved: serde_json::from_value(row.try_get("approved")?)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
        features: serde_json::from_value(row.try_get("features")?)
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
        projected_revision: row.try_get("projected_revision")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn raise_revision(tx: &mut Transaction<'_, Postgres>, scope: &str) -> RegistryResult<()> {
    sqlx::query(
        "INSERT INTO plugin_registry_revisions(scope,revision) VALUES($1,1) \
         ON CONFLICT(scope) DO UPDATE SET revision=plugin_registry_revisions.revision+1",
    )
    .bind(scope)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn registry_lock(tx: &mut Transaction<'_, Postgres>) -> RegistryResult<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('plugins:registry', 543292))")
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

/// Stores a state whose secrets are already sealed.
async fn store_state(
    tx: &mut Transaction<'_, Postgres>,
    state: &AppPluginState,
    configuration: ConfigurationWrite,
) -> RegistryResult<PgRow> {
    let json = |value: serde_json::Result<serde_json::Value>| {
        value.map_err(|_| PluginRegistryError::Unavailable)
    };
    let row = sqlx::query(&format!(
        "INSERT INTO plugin_app_states({columns}) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,\
         $21,$22,$23,$24,$25,$26,$27,$28,$29,{NOW_MS}) \
         ON CONFLICT(app_id,binding_id) DO UPDATE SET \
         revision=EXCLUDED.revision,enabled=EXCLUDED.enabled,configured=EXCLUDED.configured,\
         configuration=CASE WHEN $30 THEN plugin_app_states.configuration ELSE EXCLUDED.configuration END,deployment_id=EXCLUDED.deployment_id,\
         runtime_revision=EXCLUDED.runtime_revision,\
         runtime_configuration_digest=EXCLUDED.runtime_configuration_digest,\
         runtime_enabled=EXCLUDED.runtime_enabled,\
         runtime_healthy_until_ms=EXCLUDED.runtime_healthy_until_ms,\
         runtime_error=EXCLUDED.runtime_error,runtime_error_detail=EXCLUDED.runtime_error_detail,\
         runtime_configuration=EXCLUDED.runtime_configuration,\
         runtime_attempts=EXCLUDED.runtime_attempts,\
         runtime_next_attempt_ms=EXCLUDED.runtime_next_attempt_ms,\
         runtime_enabled_digest=EXCLUDED.runtime_enabled_digest,\
         organization_id=EXCLUDED.organization_id,lifecycle=EXCLUDED.lifecycle,\
         suspended=EXCLUDED.suspended,owned=EXCLUDED.owned,offline_hold=EXCLUDED.offline_hold,\
         export_until_ms=EXCLUDED.export_until_ms,approved=EXCLUDED.approved,\
         features=EXCLUDED.features,projected_revision=EXCLUDED.projected_revision,\
         updated_at=EXCLUDED.updated_at \
         RETURNING {STATE_COLUMNS}",
        columns = STATE_COLUMNS,
    ))
    .bind(&state.app_id)
    .bind(&state.binding_id)
    .bind(&state.plugin_id)
    .bind(&state.plugin_version)
    .bind(&state.manifest_digest)
    .bind(state.revision)
    .bind(state.enabled)
    .bind(state.configured)
    .bind(&state.configuration)
    .bind(&state.deployment_id)
    .bind(state.runtime_revision)
    .bind(&state.runtime_configuration_digest)
    .bind(state.runtime_enabled)
    .bind(state.runtime_healthy_until_ms)
    .bind(&state.runtime_error)
    .bind(&state.runtime_error_detail)
    .bind(&state.runtime_configuration)
    .bind(state.runtime_attempts)
    .bind(state.runtime_next_attempt_ms)
    .bind(&state.runtime_enabled_digest)
    .bind(&state.organization_id)
    .bind(state.lifecycle.as_str())
    .bind(state.suspended)
    .bind(state.owned)
    .bind(state.offline_hold)
    .bind(state.export_until_ms)
    .bind(json(serde_json::to_value(&state.approved))?)
    .bind(json(serde_json::to_value(&state.features))?)
    .bind(state.projected_revision)
    .bind(matches!(configuration,ConfigurationWrite::Preserve))
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| match &error {
        // The release or the deployment does not exist in the registry.
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => {
            PluginRegistryError::Invalid("binding names an unknown release or deployment".into())
        }
        _ => unavailable(error),
    })?;
    raise_revision(tx, &state.app_id).await?;
    Ok(row)
}

/// Takes the projection's desired state in the transaction that accepted it. Only the projected
/// columns change, so sealed configurations stay as they are. A binding Core holds no row for is
/// left alone: its `plugin_config.put` has not arrived, and the next projection carries it again.
pub(super) async fn apply_projected_bindings(
    tx: &mut Transaction<'_, Postgres>,
    projection: &PluginProjection,
) -> RegistryResult<()> {
    if projection.bindings.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query(&format!(
        "SELECT {STATE_COLUMNS} FROM plugin_app_states WHERE app_id=$1 FOR UPDATE"
    ))
    .bind(&projection.tenant_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    let mut states: std::collections::HashMap<String, AppPluginState> = rows
        .iter()
        .map(|row| state_row(row).map(|state| (state.binding_id.clone(), state)))
        .collect::<Result<_, _>>()
        .map_err(unavailable)?;
    let mut changed = false;
    for binding in &projection.bindings {
        let Some(state) = states.get_mut(&binding.binding_id) else {
            tracing::warn!(
                app = %projection.tenant_id,
                binding = %binding.binding_id,
                "projected binding has no plugin_app_states row"
            );
            continue;
        };
        if !state.apply_projected(binding) {
            continue;
        }
        changed = true;
        sqlx::query(&format!(
            "UPDATE plugin_app_states SET revision=$3,enabled=$4,configured=$5,runtime_error=$6,\
             runtime_error_detail=$7,runtime_attempts=$8,runtime_next_attempt_ms=$9,\
             organization_id=$10,lifecycle=$11,suspended=$12,owned=$13,offline_hold=$14,\
             export_until_ms=$15,approved=$16,features=$17,projected_revision=$18,updated_at={NOW_MS} \
             WHERE app_id=$1 AND binding_id=$2"
        ))
        .bind(&state.app_id)
        .bind(&state.binding_id)
        .bind(state.revision)
        .bind(state.enabled)
        .bind(state.configured)
        .bind(&state.runtime_error)
        .bind(&state.runtime_error_detail)
        .bind(state.runtime_attempts)
        .bind(state.runtime_next_attempt_ms)
        .bind(&state.organization_id)
        .bind(state.lifecycle.as_str())
        .bind(state.suspended)
        .bind(state.owned)
        .bind(state.offline_hold)
        .bind(state.export_until_ms)
        .bind(serde_json::to_value(&state.approved).map_err(|_| PluginRegistryError::Unavailable)?)
        .bind(serde_json::to_value(&state.features).map_err(|_| PluginRegistryError::Unavailable)?)
        .bind(state.projected_revision)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    if changed {
        raise_revision(tx, &projection.tenant_id).await?;
    }
    Ok(())
}

/// Removes the development deployments and versions of `plugin_id` no binding holds any more.
async fn prune_development(
    tx: &mut Transaction<'_, Postgres>,
    plugin_id: &str,
) -> RegistryResult<()> {
    sqlx::query(
        "DELETE FROM plugin_deployments d USING plugin_versions v \
         WHERE v.manifest_digest=d.manifest_digest AND v.development AND d.plugin_id=$1 \
         AND NOT EXISTS(SELECT 1 FROM plugin_app_states a WHERE a.deployment_id=d.deployment_id)",
    )
    .bind(plugin_id)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        "DELETE FROM plugin_versions v WHERE v.plugin_id=$1 AND v.development \
         AND NOT EXISTS(SELECT 1 FROM plugin_app_states a WHERE a.manifest_digest=v.manifest_digest) \
         AND NOT EXISTS(SELECT 1 FROM plugin_deployments d WHERE d.manifest_digest=v.manifest_digest)",
    )
    .bind(plugin_id)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

/// What a development release may not take: a production plugin's identity or namespace, a
/// production version, or what another binding is using.
async fn check_development_release(
    tx: &mut Transaction<'_, Postgres>,
    release: &DevRelease,
) -> RegistryResult<()> {
    let version = &release.version;
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT publisher_id FROM plugin_versions WHERE plugin_id=$1 AND NOT development LIMIT 1",
    )
    .bind(&version.plugin_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if owner.is_some_and(|owner| owner != version.publisher_id) {
        return Err(PluginRegistryError::Conflict(
            "plugin identity belongs to another publisher".into(),
        ));
    }
    let owners: Vec<(String, String)> =
        sqlx::query_as("SELECT plugin_id,namespace FROM plugin_namespaces")
            .fetch_all(&mut **tx)
            .await
            .map_err(unavailable)?;
    if version.namespaces.iter().any(|namespace| {
        owners
            .iter()
            .any(|(owner, held)| namespace_conflict(&version.plugin_id, owner, held, namespace))
    }) {
        return Err(PluginRegistryError::Conflict(
            "plugin capability namespace belongs to another plugin".into(),
        ));
    }
    let registered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM plugin_versions \
         WHERE plugin_id=$1 AND plugin_version=$2 AND NOT development)",
    )
    .bind(&version.plugin_id)
    .bind(&version.plugin_version)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if registered {
        return Err(PluginRegistryError::Conflict(
            "a registered release holds this version; give the manifest another one".into(),
        ));
    }
    let edited_elsewhere: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM plugin_app_states \
         WHERE plugin_id=$1 AND plugin_version=$2 AND manifest_digest<>$3 \
         AND NOT (app_id=$4 AND binding_id=$5))",
    )
    .bind(&version.plugin_id)
    .bind(&version.plugin_version)
    .bind(&version.manifest_digest)
    .bind(&release.app_id)
    .bind(&release.binding_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if edited_elsewhere {
        return Err(PluginRegistryError::Conflict(
            "another application runs this version with another manifest; change the version"
                .into(),
        ));
    }
    let deployment_taken: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM plugin_app_states \
         WHERE deployment_id=$1 AND NOT (app_id=$2 AND binding_id=$3)) \
         OR EXISTS(SELECT 1 FROM plugin_deployments d \
         JOIN plugin_versions v ON v.manifest_digest=d.manifest_digest \
         WHERE d.deployment_id=$1 AND NOT v.development)",
    )
    .bind(&release.deployment.deployment_id)
    .bind(&release.app_id)
    .bind(&release.binding_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if deployment_taken {
        return Err(PluginRegistryError::Conflict(
            "this deployment id is in use; choose another".into(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct PostgresPluginRegistry {
    pool: Arc<PgPool>,
    cipher: Arc<PluginConfigCipher>,
    manifests: Arc<ManifestCache>,
}

impl PostgresPluginRegistry {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            cipher: Arc::new(PluginConfigCipher::default()),
            manifests: Arc::new(ManifestCache::default()),
        }
    }

    pub fn with_cipher(mut self, cipher: Arc<PluginConfigCipher>) -> Self {
        self.cipher = cipher;
        self
    }

    async fn deployments(
        &self,
        filter: &DeploymentFilter,
        development: bool,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>> {
        let rows = sqlx::query(&format!(
            "SELECT {DEPLOYMENT_COLUMNS} FROM plugin_deployments \
             WHERE ($1::text IS NULL OR deployment_id=$1) AND ($2::text IS NULL OR plugin_id=$2) \
             AND ($3::text IS NULL OR plugin_version=$3) \
             AND ($4::text IS NULL OR manifest_digest=$4) \
             AND ($6 OR NOT EXISTS(SELECT 1 FROM plugin_versions v \
             WHERE v.manifest_digest=plugin_deployments.manifest_digest AND v.development)) \
             ORDER BY created_at DESC,deployment_id LIMIT $5"
        ))
        .bind(&filter.deployment_id)
        .bind(&filter.plugin_id)
        .bind(&filter.plugin_version)
        .bind(&filter.manifest_digest)
        .bind(filter.limit.unwrap_or(200).clamp(1, 200))
        .bind(development)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(deployment_row)
            .collect::<Result<_, _>>()
            .map_err(unavailable)
    }

    async fn cached_manifest(&self, digest: &str) -> RegistryResult<Arc<PluginManifest>> {
        if let Some(manifest) = self.manifests.get(digest) {
            return Ok(manifest);
        }
        let raw: String = sqlx::query_scalar(
            "SELECT manifest_json FROM plugin_versions WHERE manifest_digest=$1",
        )
        .bind(digest)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        self.manifests
            .insert(digest, raw.as_bytes())
            .map_err(|error| {
                tracing::error!(%digest, %error, "stored plugin manifest does not parse");
                PluginRegistryError::Unavailable
            })
    }

    /// Seals what the manifest marks secret and stores the state; answers it in the clear.
    async fn write_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        state: &AppPluginState,
        configuration: ConfigurationWrite,
    ) -> RegistryResult<AppPluginState> {
        let manifest = self.cached_manifest(&state.manifest_digest).await?;
        let mut sealed = state.clone();
        self.cipher
            .seal_state(&mut sealed, &manifest.secret_configuration_paths())?;
        let row = store_state(tx, &sealed, configuration).await?;
        let mut written = state_row(&row).map_err(unavailable)?;
        self.cipher.open_state(&mut written)?;
        Ok(written)
    }

    /// `desired` with the masked values a reader wrote back replaced by what is stored. An answer
    /// of Core never holds a secret, so an unchanged one arrives as the marker.
    async fn with_stored_secrets(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        desired: &DesiredAppState,
        current: &Option<AppPluginState>,
    ) -> RegistryResult<DesiredAppState> {
        let manifest = self.cached_manifest(&desired.manifest_digest).await?;
        let source = match &desired.secrets_from {
            Some(binding) if *binding != desired.binding_id => {
                let source = self
                    .locked_state(tx, &desired.app_id, binding)
                    .await?
                    .ok_or(PluginRegistryError::NotFound)?;
                if source.plugin_id != desired.plugin_id {
                    return Err(PluginRegistryError::Invalid(
                        "secrets come from a binding of the same plugin".into(),
                    ));
                }
                Some(source)
            }
            _ => None,
        };
        let stored = source.as_ref().or(current.as_ref());
        let mut desired = desired.clone();
        restore_secrets(
            &mut desired.configuration,
            stored.map(|state| &state.configuration),
            &manifest.secret_configuration_paths(),
        )
        .map_err(|_| PluginRegistryError::Invalid("a masked secret has no stored value".into()))?;
        Ok(desired)
    }

    async fn locked_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        app_id: &str,
        binding_id: &str,
    ) -> RegistryResult<Option<AppPluginState>> {
        let row = sqlx::query(&format!(
            "SELECT {STATE_COLUMNS} FROM plugin_app_states \
             WHERE app_id=$1 AND binding_id=$2 FOR UPDATE"
        ))
        .bind(app_id)
        .bind(binding_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut state = state_row(&row).map_err(unavailable)?;
        self.cipher.open_state(&mut state)?;
        Ok(Some(state))
    }

    async fn detail(
        &self,
        row: PgRow,
        created: Option<bool>,
    ) -> RegistryResult<PluginVersionDetail> {
        let version = version_row(&row).map_err(unavailable)?;
        let assets = sqlx::query_as::<_, (String, String, String, i64)>(
            "SELECT path,digest,media_type,octet_length(bytes)::bigint \
             FROM plugin_version_assets WHERE plugin_id=$1 AND plugin_version=$2 ORDER BY path",
        )
        .bind(&version.plugin_id)
        .bind(&version.plugin_version)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(|(path, digest, media_type, size)| PluginAssetRef {
            path,
            digest,
            media_type,
            size,
        })
        .collect();
        Ok(PluginVersionDetail {
            manifest_json: row.try_get("manifest_json").map_err(unavailable)?,
            key_id: row.try_get("key_id").map_err(unavailable)?,
            signature: row.try_get("signature").map_err(unavailable)?,
            readme: row.try_get("readme").map_err(unavailable)?,
            changelog: row.try_get("changelog").map_err(unavailable)?,
            version,
            assets,
            created,
        })
    }

    async fn release_detail(
        &self,
        key: &VersionKey,
        created: Option<bool>,
    ) -> RegistryResult<PluginVersionDetail> {
        let columns = format!("{VERSION_COLUMNS},manifest_json,key_id,signature,readme,changelog");
        let row = match key {
            VersionKey::Release {
                plugin_id,
                plugin_version,
            } => {
                sqlx::query(&format!(
                    "SELECT {columns} FROM plugin_versions \
                     WHERE plugin_id=$1 AND plugin_version=$2 AND NOT development"
                ))
                .bind(plugin_id)
                .bind(plugin_version)
                .fetch_optional(self.pool.as_ref())
                .await
            }
            VersionKey::Digest(digest) => {
                sqlx::query(&format!(
                    "SELECT {columns} FROM plugin_versions \
                     WHERE manifest_digest=$1 AND NOT development"
                ))
                .bind(digest)
                .fetch_optional(self.pool.as_ref())
                .await
            }
        }
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        self.detail(row, created).await
    }
}

#[async_trait::async_trait]
impl PluginRegistryRepository for PostgresPluginRegistry {
    async fn register_version(
        &self,
        version: &NewPluginVersion,
    ) -> RegistryResult<PluginVersionDetail> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        registry_lock(&mut tx).await?;
        let owner: Option<String> = sqlx::query_scalar(
            "SELECT publisher_id FROM plugin_versions \
             WHERE plugin_id=$1 AND NOT development LIMIT 1",
        )
        .bind(&version.plugin_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if owner.is_some_and(|owner| owner != version.publisher_id) {
            return Err(PluginRegistryError::Conflict(
                "plugin identity belongs to another publisher".into(),
            ));
        }
        let existing: Option<(String, bool)> = sqlx::query_as(
            "SELECT manifest_digest,development FROM plugin_versions \
             WHERE plugin_id=$1 AND plugin_version=$2",
        )
        .bind(&version.plugin_id)
        .bind(&version.plugin_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some((digest, development)) = existing {
            if development {
                return Err(PluginRegistryError::Conflict(
                    "a development binding holds this version; stop it first".into(),
                ));
            }
            if digest != version.manifest_digest {
                return Err(PluginRegistryError::Conflict(
                    "published plugin versions are immutable".into(),
                ));
            }
            drop(tx);
            return self
                .release_detail(
                    &VersionKey::Digest(version.manifest_digest.clone()),
                    Some(false),
                )
                .await;
        }
        let owners: Vec<(String, String)> =
            sqlx::query_as("SELECT plugin_id,namespace FROM plugin_namespaces")
                .fetch_all(&mut *tx)
                .await
                .map_err(unavailable)?;
        for namespace in &version.namespaces {
            if owners
                .iter()
                .any(|(owner, held)| namespace_conflict(&version.plugin_id, owner, held, namespace))
            {
                return Err(PluginRegistryError::Conflict(
                    "plugin capability namespace belongs to another plugin".into(),
                ));
            }
            sqlx::query(
                "INSERT INTO plugin_namespaces(namespace,plugin_id) VALUES($1,$2) \
                 ON CONFLICT(namespace) DO NOTHING",
            )
            .bind(namespace)
            .bind(&version.plugin_id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        sqlx::query(
            "INSERT INTO plugin_versions(plugin_id,plugin_version,display_name,publisher_id,\
             manifest_json,manifest_digest,key_id,signature,kind,readme,changelog) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&version.plugin_id)
        .bind(&version.plugin_version)
        .bind(&version.display_name)
        .bind(&version.publisher_id)
        .bind(&version.manifest_json)
        .bind(&version.manifest_digest)
        .bind(&version.key_id)
        .bind(&version.signature)
        .bind(version.kind)
        .bind(&version.readme)
        .bind(&version.changelog)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        for asset in &version.assets {
            sqlx::query(
                "INSERT INTO plugin_version_assets(plugin_id,plugin_version,path,digest,\
                 media_type,bytes) VALUES($1,$2,$3,$4,$5,$6)",
            )
            .bind(&version.plugin_id)
            .bind(&version.plugin_version)
            .bind(&asset.path)
            .bind(&asset.digest)
            .bind(asset.media_type)
            .bind(asset.bytes.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        self.release_detail(
            &VersionKey::Digest(version.manifest_digest.clone()),
            Some(true),
        )
        .await
    }

    async fn set_version_state(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        change: &VersionStateChange,
    ) -> RegistryResult<PluginVersion> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row = sqlx::query(&format!(
            "SELECT {VERSION_COLUMNS} FROM plugin_versions \
             WHERE plugin_id=$1 AND plugin_version=$2 AND NOT development FOR UPDATE"
        ))
        .bind(plugin_id)
        .bind(plugin_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        let current = version_row(&row).map_err(unavailable)?;
        let (published, revoked, suspended) = change.applied_to(&current)?;
        if (published, revoked, suspended)
            == (current.published, current.revoked, current.suspended)
        {
            return Ok(current);
        }
        let row = sqlx::query(&format!(
            "UPDATE plugin_versions SET published=$3,revoked=$4,suspended=$5,\
             updated_at=GREATEST(updated_at+1,{NOW_MS}) \
             WHERE plugin_id=$1 AND plugin_version=$2 RETURNING {VERSION_COLUMNS}"
        ))
        .bind(plugin_id)
        .bind(plugin_version)
        .bind(published)
        .bind(revoked)
        .bind(suspended)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        // Revocation and suspension reach the next read of every application.
        raise_revision(&mut tx, GLOBAL_SCOPE).await?;
        tx.commit().await.map_err(unavailable)?;
        version_row(&row).map_err(unavailable)
    }

    async fn list_versions(&self, filter: &VersionFilter) -> RegistryResult<Vec<PluginVersion>> {
        let rows = sqlx::query(&format!(
            "SELECT {VERSION_COLUMNS} FROM plugin_versions \
             WHERE ($1::text IS NULL OR plugin_id=$1) \
             AND ($2::text IS NULL OR plugin_id ILIKE '%'||$2||'%' OR display_name ILIKE '%'||$2||'%') \
             AND ($3::bigint IS NULL OR updated_at>=$3) AND NOT development \
             ORDER BY updated_at,plugin_id,plugin_version LIMIT $4 OFFSET $5"
        ))
        .bind(&filter.plugin_id)
        .bind(&filter.query)
        .bind(filter.updated_since)
        .bind(filter.limit)
        .bind(filter.offset)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(version_row)
            .collect::<Result<_, _>>()
            .map_err(unavailable)
    }

    async fn version(&self, key: &VersionKey) -> RegistryResult<PluginVersionDetail> {
        self.release_detail(key, None).await
    }

    async fn asset(
        &self,
        plugin_id: &str,
        plugin_version: &str,
        path: &str,
    ) -> RegistryResult<PluginAssetContent> {
        let (digest, media_type, bytes): (String, String, Vec<u8>) = sqlx::query_as(
            "SELECT digest,media_type,bytes FROM plugin_version_assets \
             WHERE plugin_id=$1 AND plugin_version=$2 AND path=$3",
        )
        .bind(plugin_id)
        .bind(plugin_version)
        .bind(path)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        Ok(PluginAssetContent {
            digest,
            media_type,
            bytes,
        })
    }

    async fn register_deployment(
        &self,
        registration: &DeploymentRegistration,
    ) -> RegistryResult<PluginDeploymentRecord> {
        if !registration.valid_id() {
            return Err(PluginRegistryError::Invalid("invalid deployment id".into()));
        }
        let (raw, digest): (String, String) = sqlx::query_as(
            "SELECT manifest_json,manifest_digest FROM plugin_versions \
             WHERE plugin_id=$1 AND plugin_version=$2 AND NOT revoked AND NOT development",
        )
        .bind(&registration.plugin_id)
        .bind(&registration.plugin_version)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        let manifest = PluginManifest::from_json(raw.as_bytes())
            .map_err(|_| PluginRegistryError::Unavailable)?;
        let deployment = PluginDeployment {
            deployment_id: registration.deployment_id.clone(),
            plugin_id: registration.plugin_id.clone(),
            plugin_version: registration.plugin_version.clone(),
            manifest_digest: digest,
            artifact_digest: manifest
                .artifact
                .as_ref()
                .map(|artifact| artifact.digest.clone())
                .unwrap_or_default(),
            grpc_authority: registration.grpc_authority.clone(),
        };
        deployment
            .validate(&manifest, &deployment.manifest_digest)
            .map_err(|_| {
                PluginRegistryError::Invalid("deployment does not match signed artifact".into())
            })?;
        let inserted = sqlx::query(&format!(
            "INSERT INTO plugin_deployments(deployment_id,plugin_id,plugin_version,manifest_digest,\
             artifact_digest,grpc_authority) VALUES($1,$2,$3,$4,$5,$6) \
             ON CONFLICT(deployment_id) DO NOTHING RETURNING {DEPLOYMENT_COLUMNS}"
        ))
        .bind(&deployment.deployment_id)
        .bind(&deployment.plugin_id)
        .bind(&deployment.plugin_version)
        .bind(&deployment.manifest_digest)
        .bind(&deployment.artifact_digest)
        .bind(&deployment.grpc_authority)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        if let Some(row) = inserted {
            return deployment_row(&row).map_err(unavailable);
        }
        let row = sqlx::query(&format!(
            "SELECT {DEPLOYMENT_COLUMNS} FROM plugin_deployments WHERE deployment_id=$1"
        ))
        .bind(&deployment.deployment_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let existing = deployment_row(&row).map_err(unavailable)?;
        if existing.plugin_id != deployment.plugin_id
            || existing.plugin_version != deployment.plugin_version
            || existing.manifest_digest != deployment.manifest_digest
            || existing.artifact_digest != deployment.artifact_digest
            || existing.grpc_authority != deployment.grpc_authority
        {
            return Err(PluginRegistryError::Conflict(
                "plugin deployment is immutable".into(),
            ));
        }
        Ok(existing)
    }

    async fn list_deployments(
        &self,
        filter: &DeploymentFilter,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>> {
        self.deployments(filter, false).await
    }

    async fn runtime_deployments(
        &self,
        filter: &DeploymentFilter,
    ) -> RegistryResult<Vec<PluginDeploymentRecord>> {
        self.deployments(filter, true).await
    }

    async fn report_deployment_health(
        &self,
        deployment_id: &str,
        healthy_until_ms: i64,
        last_error: Option<&str>,
    ) -> RegistryResult<()> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        // A renewal of a healthy deployment changes nothing a cached snapshot depends on: its
        // health lease expires it. Only coming back, or a new error, is news.
        let (was_until, was_error, now): (i64, Option<String>, i64) = sqlx::query_as(&format!(
            "SELECT healthy_until_ms,last_error,{NOW_MS} FROM plugin_deployments \
             WHERE deployment_id=$1 FOR UPDATE"
        ))
        .bind(deployment_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        sqlx::query(
            "UPDATE plugin_deployments SET healthy_until_ms=GREATEST(healthy_until_ms,$2),\
             last_error=$3 WHERE deployment_id=$1",
        )
        .bind(deployment_id)
        .bind(healthy_until_ms)
        .bind(last_error)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        if was_until <= now || was_error.as_deref() != last_error {
            raise_revision(&mut tx, GLOBAL_SCOPE).await?;
        }
        tx.commit().await.map_err(unavailable)
    }

    async fn put_config(
        &self,
        desired: &DesiredAppState,
        runtime: Option<RuntimePatch>,
    ) -> RegistryResult<AppPluginState> {
        desired.validate()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let development: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM plugin_versions WHERE manifest_digest=$1 AND development)",
        )
        .bind(&desired.manifest_digest)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if development {
            return Err(PluginRegistryError::Denied(
                "a development version binds through plugin_dev.register".into(),
            ));
        }
        let current = self
            .locked_state(&mut tx, &desired.app_id, &desired.binding_id)
            .await?;
        let desired = self.with_stored_secrets(&mut tx, desired, &current).await?;
        let configuration = configuration_write(&current, &desired.configuration);
        let mut state = desired.applied_to(current)?;
        if let Some(patch) = runtime {
            patch.apply(&mut state);
        }
        let written = self.write_state(&mut tx, &state, configuration).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(written)
    }

    async fn app_states(
        &self,
        apps: &[String],
        bindings: Option<&[String]>,
    ) -> RegistryResult<Vec<AppPluginState>> {
        let rows = sqlx::query(&format!(
            "SELECT {STATE_COLUMNS} FROM plugin_app_states WHERE app_id=ANY($1) \
             AND ($2::text[] IS NULL OR binding_id=ANY($2)) ORDER BY app_id,binding_id"
        ))
        .bind(apps)
        .bind(bindings)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(|row| {
                let mut state = state_row(row).map_err(unavailable)?;
                self.cipher.open_state(&mut state)?;
                Ok(state)
            })
            .collect()
    }

    async fn reconcile_page(
        &self,
        after: Option<ReconcileCursor>,
        limit: i64,
        development: bool,
    ) -> RegistryResult<Vec<ReconcileRow>> {
        let (at, app, binding) = match after {
            Some((at, app, binding)) => (Some(at), Some(app), Some(binding)),
            None => (None, None, None),
        };
        let rows = sqlx::query(&format!(
            "SELECT a.*,v.revoked AS version_revoked,v.suspended AS version_suspended,\
             {NOW_MS} AS now_ms FROM plugin_app_states a \
             JOIN plugin_versions v ON v.manifest_digest=a.manifest_digest \
             WHERE a.configured AND a.lifecycle<>'failed' AND a.organization_id<>'' \
             AND ($5 OR NOT v.development) AND ($1::bigint IS NULL OR \
             (a.updated_at,a.app_id,a.binding_id)>($1::bigint,$2::text,$3::text)) \
             ORDER BY a.updated_at,a.app_id,a.binding_id LIMIT $4"
        ))
        .bind(at)
        .bind(app)
        .bind(binding)
        .bind(limit)
        .bind(development)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        let mut page = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut state = state_row(row).map_err(unavailable)?;
            // A row that does not open waits; its neighbours do not.
            if self.cipher.open_state(&mut state).is_err() {
                tracing::error!(
                    app = %state.app_id,
                    binding = %state.binding_id,
                    "plugin configuration does not open with the configured keys"
                );
                continue;
            }
            page.push(ReconcileRow {
                state,
                version_revoked: row.try_get("version_revoked").map_err(unavailable)?,
                version_suspended: row.try_get("version_suspended").map_err(unavailable)?,
                now_ms: row.try_get("now_ms").map_err(unavailable)?,
            });
        }
        Ok(page)
    }

    async fn reconcile_lease(&self, holder: &str, ttl_ms: i64) -> RegistryResult<bool> {
        let taken: Option<i32> = sqlx::query_scalar(&format!(
            "INSERT INTO plugin_reconcile_lease(id,holder,until_ms) VALUES(1,$1,{NOW_MS}+$2) \
             ON CONFLICT(id) DO UPDATE SET holder=EXCLUDED.holder,until_ms=EXCLUDED.until_ms \
             WHERE plugin_reconcile_lease.holder=EXCLUDED.holder \
             OR plugin_reconcile_lease.until_ms<={NOW_MS} RETURNING id"
        ))
        .bind(holder)
        .bind(ttl_ms)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(taken.is_some())
    }

    async fn record_delivery(
        &self,
        seen: &AppPluginState,
        patch: RuntimePatch,
    ) -> RegistryResult<bool> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let Some(mut state) = self
            .locked_state(&mut tx, &seen.app_id, &seen.binding_id)
            .await?
        else {
            return Ok(false);
        };
        if state.revision != seen.revision
            || state.projected_revision != seen.projected_revision
            || state.updated_at != seen.updated_at
            || state.runtime_revision != seen.runtime_revision
            || state.runtime_attempts != seen.runtime_attempts
            || state.deployment_id != seen.deployment_id
        {
            return Ok(false);
        }
        patch.apply(&mut state);
        // Runtime facts cannot change the desired configuration. Preserve its exact
        // encrypted bytes so a fresh nonce cannot impersonate a configuration change.
        self.write_state(&mut tx, &state, ConfigurationWrite::Preserve)
            .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(true)
    }

    async fn manifest(&self, digest: &str) -> RegistryResult<Arc<PluginManifest>> {
        self.cached_manifest(digest).await
    }

    async fn attach_web_bundle(
        &self,
        approval: &WebBundleApproval,
    ) -> RegistryResult<WebBundleAttachment> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let digest: String = sqlx::query_scalar(
            "SELECT manifest_digest FROM plugin_versions \
             WHERE plugin_id=$1 AND plugin_version=$2 AND NOT development FOR SHARE",
        )
        .bind(&approval.plugin_id)
        .bind(&approval.plugin_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(PluginRegistryError::NotFound)?;
        let manifest = self.cached_manifest(&digest).await?;
        approval.check(&manifest)?;
        let size = i64::try_from(approval.size)
            .map_err(|_| PluginRegistryError::Invalid("web bundle size".into()))?;
        let existing = sqlx::query(&format!(
            "SELECT {WEB_BUNDLE_COLUMNS} FROM plugin_web_bundles \
             WHERE plugin_id=$1 AND plugin_version=$2 AND bundle_id=$3 FOR UPDATE"
        ))
        .bind(&approval.plugin_id)
        .bind(&approval.plugin_version)
        .bind(&approval.bundle_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some(row) = existing {
            let current = web_bundle_row(&row).map_err(unavailable)?;
            if current.withdrawn() {
                return Err(PluginRegistryError::Conflict(
                    "a withdrawn web bundle stays withdrawn".into(),
                ));
            }
            if current.sha256 != approval.sha256 || current.size != size {
                return Err(PluginRegistryError::Conflict(
                    "the web bundle of this version is attached with another digest".into(),
                ));
            }
            return Ok(current);
        }
        let row = sqlx::query(&format!(
            "INSERT INTO plugin_web_bundles(plugin_id,plugin_version,bundle_id,sha256,size) \
             VALUES($1,$2,$3,$4,$5) RETURNING {WEB_BUNDLE_COLUMNS}"
        ))
        .bind(&approval.plugin_id)
        .bind(&approval.plugin_version)
        .bind(&approval.bundle_id)
        .bind(&approval.sha256)
        .bind(size)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        raise_revision(&mut tx, GLOBAL_SCOPE).await?;
        tx.commit().await.map_err(unavailable)?;
        web_bundle_row(&row).map_err(unavailable)
    }

    async fn withdraw_web_bundle(
        &self,
        bundle: &WebBundleRef,
    ) -> RegistryResult<Option<WebBundleAttachment>> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row = sqlx::query(&format!(
            "SELECT {WEB_BUNDLE_COLUMNS} FROM plugin_web_bundles \
             WHERE plugin_id=$1 AND plugin_version=$2 AND bundle_id=$3 FOR UPDATE"
        ))
        .bind(&bundle.plugin_id)
        .bind(&bundle.plugin_version)
        .bind(&bundle.bundle_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let current = web_bundle_row(&row).map_err(unavailable)?;
        if current.withdrawn() {
            return Ok(Some(current));
        }
        let row = sqlx::query(&format!(
            "UPDATE plugin_web_bundles SET withdrawn_at=GREATEST(approved_at,{NOW_MS}),\
             updated_at=GREATEST(updated_at+1,{NOW_MS}) \
             WHERE plugin_id=$1 AND plugin_version=$2 AND bundle_id=$3 \
             RETURNING {WEB_BUNDLE_COLUMNS}"
        ))
        .bind(&bundle.plugin_id)
        .bind(&bundle.plugin_version)
        .bind(&bundle.bundle_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        // Every application stops serving it on its next read, before any re-projection.
        raise_revision(&mut tx, GLOBAL_SCOPE).await?;
        tx.commit().await.map_err(unavailable)?;
        web_bundle_row(&row).map(Some).map_err(unavailable)
    }

    async fn web_bundles(
        &self,
        filter: &WebBundleFilter,
    ) -> RegistryResult<Vec<WebBundleAttachment>> {
        let rows = sqlx::query(&format!(
            "SELECT {WEB_BUNDLE_COLUMNS} FROM plugin_web_bundles \
             WHERE plugin_id=$1 AND ($2::text IS NULL OR plugin_version=$2) \
             ORDER BY plugin_version,bundle_id LIMIT 512"
        ))
        .bind(&filter.plugin_id)
        .bind(&filter.plugin_version)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(web_bundle_row)
            .collect::<Result<_, _>>()
            .map_err(unavailable)
    }

    async fn register_development(&self, release: &DevRelease) -> RegistryResult<DevRegistered> {
        let version = &release.version;
        let deployment = &release.deployment;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        registry_lock(&mut tx).await?;
        let flagged: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM plugin_dev_apps WHERE app_id=$1)")
                .bind(&release.app_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        if !flagged {
            return Err(PluginRegistryError::Denied(
                "this application is not flagged for development".into(),
            ));
        }
        check_development_release(&mut tx, release).await?;
        let current = self
            .locked_state(&mut tx, &release.app_id, &release.binding_id)
            .await?;
        let authority: Option<String> = sqlx::query_scalar(
            "SELECT grpc_authority FROM plugin_deployments WHERE deployment_id=$1",
        )
        .bind(&deployment.deployment_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let registered = |revision, changed| DevRegistered {
            binding_id: release.binding_id.clone(),
            deployment_id: deployment.deployment_id.clone(),
            plugin_id: version.plugin_id.clone(),
            plugin_version: version.plugin_version.clone(),
            manifest_digest: version.manifest_digest.clone(),
            revision,
            changed,
        };
        if let Some(state) = &current
            && state.manifest_digest == version.manifest_digest
            && state.deployment_id.as_deref() == Some(deployment.deployment_id.as_str())
            && state.configuration == release.configuration
            && authority.as_deref() == Some(deployment.grpc_authority.as_str())
        {
            return Ok(registered(state.revision, false));
        }
        // The rows are immutable to everything else and reference each other: an edit replaces
        // them, children first.
        sqlx::query("DELETE FROM plugin_app_states WHERE app_id=$1 AND binding_id=$2")
            .bind(&release.app_id)
            .bind(&release.binding_id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        prune_development(&mut tx, &version.plugin_id).await?;
        sqlx::query(
            "INSERT INTO plugin_versions(plugin_id,plugin_version,display_name,publisher_id,\
             manifest_json,manifest_digest,key_id,signature,kind,development) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,true) \
             ON CONFLICT(plugin_id,plugin_version) DO NOTHING",
        )
        .bind(&version.plugin_id)
        .bind(&version.plugin_version)
        .bind(&version.display_name)
        .bind(&version.publisher_id)
        .bind(&version.manifest_json)
        .bind(&version.manifest_digest)
        .bind(&version.key_id)
        .bind(&version.signature)
        .bind(version.kind)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            "INSERT INTO plugin_deployments(deployment_id,plugin_id,plugin_version,manifest_digest,\
             artifact_digest,grpc_authority) VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(&deployment.deployment_id)
        .bind(&deployment.plugin_id)
        .bind(&deployment.plugin_version)
        .bind(&deployment.manifest_digest)
        .bind(&deployment.artifact_digest)
        .bind(&deployment.grpc_authority)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        let revision = current.map_or(1, |state| state.revision + 1);
        let mut state = DesiredAppState {
            app_id: release.app_id.clone(),
            binding_id: release.binding_id.clone(),
            plugin_id: version.plugin_id.clone(),
            plugin_version: version.plugin_version.clone(),
            manifest_digest: version.manifest_digest.clone(),
            revision,
            enabled: true,
            configured: true,
            configuration: release.configuration.clone(),
            secrets_from: None,
        }
        .applied_to(None)?;
        state.deployment_id = Some(deployment.deployment_id.clone());
        let manifest = PluginManifest::from_json(version.manifest_json.as_bytes())
            .map_err(|_| PluginRegistryError::Invalid("invalid manifest".into()))?;
        crate::domain::plugin::development::ready_to_converge(&mut state, &manifest);
        self.write_state(&mut tx, &state, ConfigurationWrite::Replace)
            .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(registered(revision, true))
    }

    async fn set_development_app(&self, app_id: &str, development: bool) -> RegistryResult<()> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        registry_lock(&mut tx).await?;
        if development {
            sqlx::query("INSERT INTO plugin_dev_apps(app_id) VALUES($1) ON CONFLICT DO NOTHING")
                .bind(app_id)
                .execute(&mut *tx)
                .await
                .map_err(|error| match &error {
                    sqlx::Error::Database(db) if db.is_check_violation() => {
                        PluginRegistryError::Invalid("invalid application id".into())
                    }
                    _ => unavailable(error),
                })?;
        } else {
            let plugins: Vec<String> = sqlx::query_scalar(
                "DELETE FROM plugin_app_states a USING plugin_versions v \
                 WHERE a.app_id=$1 AND v.manifest_digest=a.manifest_digest AND v.development \
                 RETURNING a.plugin_id",
            )
            .bind(app_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
            sqlx::query("DELETE FROM plugin_dev_apps WHERE app_id=$1")
                .bind(app_id)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            for plugin_id in &plugins {
                prune_development(&mut tx, plugin_id).await?;
            }
        }
        raise_revision(&mut tx, app_id).await?;
        tx.commit().await.map_err(unavailable)
    }
}
