//! Read-only local-primary credential adapter. Gateways never import Capability's writer.
use flare_server_core::auth::credential_verifier::{
    CredentialReadError, LocalCredential, LocalCredentialReader,
};
use sqlx::{PgPool, Row};

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
