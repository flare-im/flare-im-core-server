//! Read-only local-primary credential adapter. Gateways never import Capability's writer.
use flare_server_core::auth::credential_verifier::{
    CredentialReadError, LocalCredential, LocalCredentialReader,
};
use sqlx::{PgPool, Row};

/// Dedicated, read-only primary pool for credential entry. Not shared with end users,
/// Control writers or a replica; startup checks the exact local credential shape.
pub async fn connect_readonly_credential_pool(url: &str) -> Result<PgPool, &'static str> {
    let options = url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .map_err(|_| "Platform primary database configuration invalid")?;
    connect_readonly_credential_pool_with(options).await
}

/// Preserve typed socket/TCP configuration without formatting credentials into a URL.
pub async fn connect_readonly_credential_pool_with(
    options: sqlx::postgres::PgConnectOptions,
) -> Result<PgPool, &'static str> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_millis(250))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .map_err(|_| "Platform primary database unavailable")?;
    sqlx::query("SELECT k.key_id,k.app_id,k.payload,v.version FROM credential_keys k JOIN credential_projection_versions v USING(app_id) LIMIT 0")
        .fetch_all(&pool).await.map_err(|_| "Platform credential schema unavailable")?;
    Ok(pool)
}

#[derive(Clone)]
pub struct PostgresCredentialReader {
    pool: PgPool,
}
impl PostgresCredentialReader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
#[async_trait::async_trait]
impl LocalCredentialReader for PostgresCredentialReader {
    async fn lookup(&self, key_id: &str) -> Result<Option<LocalCredential>, CredentialReadError> {
        // One statement on the local primary: no Control RPC or caller-supplied binding.
        let row = sqlx::query(
            "SELECT k.app_id,k.payload,v.version FROM credential_keys k \
            JOIN credential_projection_versions v USING(app_id) WHERE k.key_id=$1",
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CredentialReadError::Unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let invalid = |_| CredentialReadError::InvalidProjection;
        let app_id: String = row.try_get("app_id").map_err(invalid)?;
        if app_id != flare_im_contracts::utils::normalize_tenant_id(&app_id) {
            return Err(CredentialReadError::InvalidProjection);
        }
        let version: i64 = row.try_get("version").map_err(invalid)?;
        let payload: serde_json::Value = row.try_get("payload").map_err(invalid)?;
        let record =
            serde_json::from_value(payload).map_err(|_| CredentialReadError::InvalidProjection)?;
        LocalCredential::new(key_id, app_id, version, record).map(Some)
    }
}
