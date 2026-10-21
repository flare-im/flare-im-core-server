//! `tenants` 表的投影仓储（与 `deploy/init.sql` 第 1 节对齐）。
//!
//! 写入是一条带 `WHERE tenants.version < EXCLUDED.version` 的 upsert：版本比较与写入在同一条
//! 语句里完成，并发重推同一租户也不会回退版本。

use std::sync::Arc;

use flare_im_contracts::domain::tenant::{TenantQuota, TenantStatus};
use flare_server_core::auth::credential_snapshot::CredentialSnapshot;
use flare_server_core::auth::credential_verifier::{
    CredentialReadError, LocalCredential, LocalCredentialReader,
};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use sqlx::{PgPool, Row};

use crate::domain::model::{TenantProjectionOutcome, TenantProjectionRecord, TenantRuntimeView};
use crate::domain::repository::{TenantProjectionRepository, TenantRuntimeReader};

pub struct PostgresTenantProjectionRepository {
    pool: Arc<PgPool>,
}

impl PostgresTenantProjectionRepository {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl LocalCredentialReader for PostgresTenantProjectionRepository {
    async fn lookup(
        &self,
        key_id: &str,
    ) -> std::result::Result<Option<LocalCredential>, CredentialReadError> {
        flare_im_service_kit::credential_projection::PostgresCredentialReader::new(
            (*self.pool).clone(),
        )
        .lookup(key_id)
        .await
    }
}

#[async_trait::async_trait]
impl TenantProjectionRepository for PostgresTenantProjectionRepository {
    async fn replace_plugins(
        &self,
        snapshot: &flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot,
    ) -> Result<TenantProjectionOutcome> {
        super::postgres_plugin_authorization::PostgresPluginAuthorization {
            pool: self.pool.clone(),
        }
        .replace(snapshot)
        .await
    }
    async fn plugin_versions(&self, app_ids: &[String]) -> Result<Vec<(String, u64)>> {
        super::postgres_plugin_authorization::PostgresPluginAuthorization {
            pool: self.pool.clone(),
        }
        .versions(app_ids)
        .await
    }
    async fn replace_credentials(&self, p: &CredentialSnapshot) -> Result<TenantProjectionOutcome> {
        // Never surface SQL row/detail text (it can contain credential material).
        let unavailable = |_| {
            flare_server_core::error::FlareError::system(
                "credential projection storage unavailable",
            )
        };
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let written: Option<i64> = sqlx::query_scalar(
            "INSERT INTO credential_projection_versions(app_id,version,projected_at) VALUES($1,$2,NOW()) \
             ON CONFLICT(app_id) DO UPDATE SET version=EXCLUDED.version,projected_at=NOW() \
             WHERE credential_projection_versions.version<EXCLUDED.version RETURNING version"
        ).bind(p.app_id()).bind(p.version()).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if written.is_none() {
            let current: i64 = sqlx::query_scalar(
                "SELECT version FROM credential_projection_versions WHERE app_id=$1",
            )
            .bind(p.app_id())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            tx.rollback().await.map_err(unavailable)?;
            return Ok(TenantProjectionOutcome {
                accepted: false,
                current_version: current as u64,
            });
        }
        sqlx::query("DELETE FROM credential_keys WHERE app_id=$1")
            .bind(p.app_id())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        for key in p.keys() {
            let payload = serde_json::to_value(key).map_err(|_| {
                flare_server_core::error::FlareError::system("credential storage encoding failed")
            })?;
            // Global key_id uniqueness refuses collisions instead of transferring
            // another application's grant. The entire version/key TX rolls back.
            sqlx::query("INSERT INTO credential_keys(key_id,app_id,payload) VALUES($1,$2,$3)")
                .bind(&key.key_id)
                .bind(p.app_id())
                .bind(payload)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(TenantProjectionOutcome {
            accepted: true,
            current_version: p.version() as u64,
        })
    }

    async fn credential_versions(&self, app_ids: &[String]) -> Result<Vec<(String, u64)>> {
        let rows=sqlx::query("SELECT app_id,version FROM credential_projection_versions WHERE cardinality($1::text[])=0 OR app_id=ANY($1) ORDER BY app_id")
            .bind(app_ids).fetch_all(&*self.pool).await
            .map_err(|_| flare_server_core::error::FlareError::system("credential version storage unavailable"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("app_id"), row.get::<i64, _>("version") as u64))
            .collect())
    }

    async fn upsert_if_newer(
        &self,
        record: &TenantProjectionRecord,
    ) -> Result<TenantProjectionOutcome> {
        let incoming = i64::try_from(record.version).unwrap_or(i64::MAX);
        let written: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO tenants (tenant_id, name, status, config, quota, version, projected_at, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, NOW(), NOW())
            ON CONFLICT (tenant_id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                config = EXCLUDED.config,
                quota = EXCLUDED.quota,
                version = EXCLUDED.version,
                projected_at = NOW(),
                updated_at = NOW()
            WHERE tenants.version < EXCLUDED.version
            RETURNING version
            "#,
        )
        .bind(&record.tenant_id)
        .bind(&record.name)
        .bind(record.status.as_str())
        .bind(&record.settings_json)
        .bind(record.quota_json())
        .bind(incoming)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to upsert tenant projection"))?;

        if let Some(version) = written {
            return Ok(TenantProjectionOutcome {
                accepted: true,
                current_version: u64::try_from(version).unwrap_or(0),
            });
        }

        let current: Option<i64> =
            sqlx::query_scalar("SELECT version FROM tenants WHERE tenant_id = $1")
                .bind(&record.tenant_id)
                .fetch_optional(&*self.pool)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "Failed to read tenant version")
                })?;
        Ok(TenantProjectionOutcome {
            accepted: false,
            current_version: current.and_then(|v| u64::try_from(v).ok()).unwrap_or(0),
        })
    }

    async fn versions(&self, tenant_ids: &[String]) -> Result<Vec<(String, u64)>> {
        let rows = if tenant_ids.is_empty() {
            sqlx::query("SELECT tenant_id, version FROM tenants ORDER BY tenant_id")
                .fetch_all(&*self.pool)
                .await
        } else {
            sqlx::query(
                "SELECT tenant_id, version FROM tenants WHERE tenant_id = ANY($1) ORDER BY tenant_id",
            )
            .bind(tenant_ids)
            .fetch_all(&*self.pool)
            .await
        }
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to list tenant versions"))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let version: i64 = row.get("version");
                (row.get("tenant_id"), u64::try_from(version).unwrap_or(0))
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl TenantRuntimeReader for PostgresTenantProjectionRepository {
    async fn tenant_runtime(&self, tenant_id: &str) -> Result<Option<TenantRuntimeView>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, name, status,
                   COALESCE(config, '{}'::jsonb)::text AS settings_json,
                   COALESCE(quota, '{}'::jsonb)::text AS quota_json,
                   version, projected_at, updated_at
            FROM tenants
            WHERE tenant_id = $1
            "#,
        )
        .bind(tenant_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "Failed to read tenant runtime")
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.get("status");
        let status = TenantStatus::parse(&status).ok_or_else(|| {
            flare_server_core::error::FlareError::system(format!(
                "tenant {tenant_id} has unknown status `{status}`"
            ))
        })?;
        let quota_json: String = row.get("quota_json");
        let quota = serde_json::from_str::<TenantQuota>(&quota_json).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "tenant {tenant_id} has malformed quota: {e}"
            ))
        })?;
        let version: i64 = row.get("version");
        Ok(Some(TenantRuntimeView {
            tenant_id: row.get("tenant_id"),
            name: row.get("name"),
            status,
            quota,
            settings_json: row.get("settings_json"),
            version: u64::try_from(version).unwrap_or(0),
            projected_at: row.get("projected_at"),
            updated_at: row.get("updated_at"),
        }))
    }
}
#[cfg(test)]
mod credential_postgres_tests {
    async fn exercise_credential_rpc(repo: Arc<PostgresTenantProjectionRepository>) {
        use ed25519_dalek::{
            SigningKey,
            pkcs8::{EncodePrivateKey, EncodePublicKey},
        };
        use flare_grpc_proto::control::{
            CredentialKey, CredentialsProjectionUpsert,
            tenant_projection_client::TenantProjectionClient,
            tenant_projection_server::TenantProjectionServer,
        };
        use tonic::{
            Request,
            transport::{Endpoint, Server},
        };
        let key = SigningKey::from_bytes(&[83; 32]);
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let signer = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            "flare-platform",
            60,
        )
        .unwrap();
        let handler = crate::interface::grpc::TenantProjectionGrpcServer::new(
            repo.clone(),
            Some(
                flare_im_service_kit::control_auth::PlatformAssertionVerifier::from_public_key(
                    &public,
                )
                .unwrap(),
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures_util::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(
            Server::builder()
                .add_service(TenantProjectionServer::new(handler))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stop_rx.await;
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap();
        let mut client = TenantProjectionClient::new(channel);
        use flare_server_core::auth::api_key::{ApiKeyKind, ApiKeyPepper, ApiKeyToken};
        let material = ApiKeyToken::generate(ApiKeyKind::Test).unwrap();
        let pepper =
            ApiKeyPepper::from_base64url("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let projected_key_id = material.key_id().to_string();
        let projected_hash = material.digest(&pepper);
        let payload = || CredentialsProjectionUpsert {
            app_id: "rpc-app".into(),
            version: 5,
            keys: vec![CredentialKey {
                key_id: projected_key_id.clone(),
                service_account_id: "sa-rpc".into(),
                mode: 2,
                hash: projected_hash.to_vec(),
                pepper_fingerprint: pepper.fingerprint().to_vec(),
                scopes: vec!["endpoint.read".into()],
                expires_at_ms: 0,
                ip_allow: vec![],
                revoked: false,
                service_account_enabled: true,
            }],
        };
        let signed = |p, subject, scope, audience| {
            let token = signer
                .generate_with_ed25519(subject, Some(scope), "control", audience)
                .unwrap();
            let mut r = Request::new(p);
            r.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            r
        };
        let worker = "flare-platform-worker";
        let audience = "flare-core-projection";
        let mut forged = Request::new(payload());
        forged
            .metadata_mut()
            .insert("x-tenant-id", "rpc-app".parse().unwrap());
        forged
            .metadata_mut()
            .insert("x-actor-roles", "control".parse().unwrap());
        assert_eq!(
            client.upsert_credentials(forged).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        // Only the two platform service subjects may sign a control assertion: the verifier
        // (`PlatformAssertionVerifier::verify_token`) refuses any other subject, an employee
        // included, as UNAUTHENTICATED before any tenant check.
        assert_eq!(
            client
                .upsert_credentials(signed(payload(), "employee-7", "rpc-app", audience))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            client
                .upsert_credentials(signed(payload(), worker, "rpc-app", "other-audience"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            client
                .upsert_credentials(signed(payload(), worker, "other-app", audience))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            client
                .upsert_credentials(signed(payload(), worker, "*", audience))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut request = signed(payload(), worker, "rpc-app", audience);
        request
            .metadata_mut()
            .insert("x-tenant-id", "other-app".parse().unwrap());
        let a = client
            .upsert_credentials(request)
            .await
            .unwrap()
            .into_inner();
        assert!(a.accepted);
        assert_eq!(a.current_version, 5);
        let a = client
            .upsert_credentials(signed(payload(), worker, "rpc-app", audience))
            .await
            .unwrap()
            .into_inner();
        assert!(!a.accepted);
        assert_eq!(a.current_version, 5);
        let mut invalid = payload();
        invalid.version = 6;
        invalid.keys = vec![CredentialKey::default()];
        assert_eq!(
            client
                .upsert_credentials(signed(invalid, worker, "rpc-app", audience))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let a = client
            .upsert_credentials(signed(payload(), worker, "rpc-app", audience))
            .await
            .unwrap()
            .into_inner();
        assert!(!a.accepted);
        assert_eq!(
            a.current_version, 5,
            "invalid newer snapshot must not alter state"
        );
        let stored: serde_json::Value = sqlx::query_scalar(
            "SELECT payload FROM credential_keys WHERE key_id=$1 AND app_id='rpc-app'",
        )
        .bind(material.key_id())
        .fetch_one(&*repo.pool)
        .await
        .unwrap();
        let stored: CredentialKeyRecord = serde_json::from_value(stored).unwrap();
        let identity = stored
            .grant("rpc-app")
            .unwrap()
            .authorize(
                &material,
                &pepper,
                Some("rpc-app"),
                "endpoint.read",
                "127.0.0.1".parse().unwrap(),
                1000,
            )
            .unwrap();
        assert_eq!(identity.app_id, "rpc-app");
        assert_eq!(identity.service_account_id, "sa-rpc");
        use flare_server_core::auth::credential_verifier::{
            CachedApiKeyVerifier, CredentialAuthError,
        };
        assert_eq!(
            repo.lookup(material.key_id())
                .await
                .unwrap()
                .unwrap()
                .version(),
            5
        );
        let encoded = material.into_issuance_token();
        let selector = stored.key_id.clone();
        let cache = CachedApiKeyVerifier::new(repo.clone());
        let ip = "127.0.0.1".parse().unwrap();
        assert_eq!(
            cache
                .verify(&encoded, &pepper, Some("rpc-app"), "endpoint.read", ip)
                .await
                .unwrap(),
            identity
        );
        assert_eq!(
            cache
                .verify(&encoded, &pepper, Some("other-app"), "endpoint.read", ip)
                .await,
            Err(CredentialAuthError::Forbidden)
        );

        // Corrupted local rows never acquire a default mode/account state.
        for corruption in [
            serde_json::json!({"mode": 0}),
            serde_json::json!({"key_id": "OTHERKEY2345"}),
        ] {
            let mut stored_json = serde_json::to_value(&stored).unwrap();
            stored_json
                .as_object_mut()
                .unwrap()
                .extend(corruption.as_object().unwrap().clone());
            sqlx::query("UPDATE credential_keys SET payload=$2 WHERE key_id=$1")
                .bind(&selector)
                .bind(stored_json)
                .execute(&*repo.pool)
                .await
                .unwrap();
            assert!(matches!(
                repo.lookup(&selector).await,
                Err(CredentialReadError::InvalidProjection)
            ));
            assert_eq!(
                CachedApiKeyVerifier::new(repo.clone())
                    .verify(&encoded, &pepper, None, "endpoint.read", ip)
                    .await,
                Err(CredentialAuthError::Unavailable)
            );
        }
        let mut stored_json = serde_json::to_value(&stored).unwrap();
        stored_json
            .as_object_mut()
            .unwrap()
            .remove("service_account_enabled");
        sqlx::query("UPDATE credential_keys SET payload=$2 WHERE key_id=$1")
            .bind(&selector)
            .bind(stored_json)
            .execute(&*repo.pool)
            .await
            .unwrap();
        assert!(matches!(
            repo.lookup(&selector).await,
            Err(CredentialReadError::InvalidProjection)
        ));
        let stored_json = serde_json::to_value(&stored).unwrap();
        sqlx::query("UPDATE credential_keys SET payload=$2 WHERE key_id=$1")
            .bind(&selector)
            .bind(stored_json)
            .execute(&*repo.pool)
            .await
            .unwrap();

        // An operator-corrupted alias binding must not normalize into tenant 0.
        sqlx::query(
            "INSERT INTO credential_projection_versions(app_id,version) VALUES('default',1)",
        )
        .execute(&*repo.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE credential_keys SET app_id='default' WHERE key_id=$1")
            .bind(&selector)
            .execute(&*repo.pool)
            .await
            .unwrap();
        assert!(matches!(
            repo.lookup(&selector).await,
            Err(CredentialReadError::InvalidProjection)
        ));
        sqlx::query("UPDATE credential_keys SET app_id='rpc-app' WHERE key_id=$1")
            .bind(&selector)
            .execute(&*repo.pool)
            .await
            .unwrap();
        let mut empty = payload();
        empty.version = 7;
        empty.keys.clear();
        let ack = client
            .upsert_credentials(signed(empty, worker, "rpc-app", audience))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 7);
        assert!(repo.lookup(&selector).await.unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            cache
                .verify(&encoded, &pepper, None, "endpoint.read", ip)
                .await,
            Err(CredentialAuthError::Unauthenticated)
        );
        let replay = client
            .upsert_credentials(signed(payload(), worker, "rpc-app", audience))
            .await
            .unwrap()
            .into_inner();
        assert!(!replay.accepted);
        assert_eq!(replay.current_version, 7);
        assert_eq!(
            CachedApiKeyVerifier::new(repo.clone())
                .verify(&encoded, &pepper, None, "endpoint.read", ip)
                .await,
            Err(CredentialAuthError::Unauthenticated)
        );
        stop_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    use super::*;
    use flare_server_core::auth::credential_snapshot::CredentialKeyRecord;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::time::Duration;

    fn snapshot(app: &str, version: u64, id: Option<&str>) -> CredentialSnapshot {
        CredentialSnapshot::new(
            app.into(),
            version,
            id.into_iter()
                .map(|id| CredentialKeyRecord {
                    key_id: id.into(),
                    service_account_id: "sa-a".into(),
                    mode: 1,
                    hash: [42; 32],
                    pepper_fingerprint: [43; 32],
                    scopes: vec!["endpoint.read".into()],
                    expires_at_ms: 0,
                    ip_allow: vec![],
                    revoked: false,
                    service_account_enabled: true,
                })
                .collect(),
        )
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires the exact private DB made by test_credential_projection.py"]
    async fn credential_postgres_atomic_regression() {
        let db = std::env::var("FLARE_CREDENTIAL_TEST_DB").expect("owned fixture required");
        let suffix = db
            .strip_prefix("flare_credential_test_")
            .expect("not an owned DB");
        assert!(suffix.len() == 12 && suffix.bytes().all(|b| b.is_ascii_hexdigit()));
        let options = PgConnectOptions::new()
            .database(&db)
            .host(&std::env::var("FLARE_CREDENTIAL_TEST_HOST").unwrap())
            .port(
                std::env::var("FLARE_CREDENTIAL_TEST_PORT")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
            .username(&std::env::var("FLARE_CREDENTIAL_TEST_USER").unwrap())
            .password(&std::env::var("PGPASSWORD").unwrap_or_default());
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options.clone())
            .await
            .unwrap();
        let marker: String = sqlx::query_scalar("SELECT token FROM flare_credential_test_fixture")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(marker, db, "never mutate installed DB");
        let readonly =
            flare_im_service_kit::credential_projection::connect_readonly_credential_pool_with(
                options.clone(),
            )
            .await
            .unwrap();
        let mut connections = Vec::new();
        for _ in 0..4 {
            let mut connection = readonly.acquire().await.unwrap();
            let mode: String = sqlx::query_scalar("SHOW default_transaction_read_only")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert_eq!(mode, "on");
            let error = sqlx::query("INSERT INTO credential_projection_versions(app_id,version) VALUES('readonly-probe',1)")
                .execute(&mut *connection).await.expect_err("Platform pool must never write");
            assert_eq!(
                error.as_database_error().and_then(|e| e.code()).as_deref(),
                Some("25006")
            );
            connections.push(connection);
        }
        assert!(
            readonly.acquire().await.is_err(),
            "fifth acquisition must be bounded/refused"
        );
        drop(connections);
        readonly.close().await;
        sqlx::query("ALTER TABLE credential_keys RENAME TO credential_fixture_keys")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            flare_im_service_kit::credential_projection::connect_readonly_credential_pool_with(
                options
            )
            .await,
            Err("Platform credential schema unavailable")
        ));
        sqlx::query("ALTER TABLE credential_fixture_keys RENAME TO credential_keys")
            .execute(&pool)
            .await
            .unwrap();
        let repo = Arc::new(PostgresTenantProjectionRepository::new(Arc::new(
            pool.clone(),
        )));
        let p = snapshot("app-a", 9, Some("ABCDEFGHIJKL"));
        exercise_credential_rpc(repo.clone()).await;
        assert!(repo.replace_credentials(&p).await.unwrap().accepted);
        for version in [8, 9] {
            let a = repo
                .replace_credentials(&snapshot("app-a", version, None))
                .await
                .unwrap();
            assert!(!a.accepted);
            assert_eq!(a.current_version, 9);
        }
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM credential_keys WHERE app_id='app-a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert!(
            repo.replace_credentials(&snapshot("app-b", 1, Some("ABCDEFGHIJKL")))
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM credential_projection_versions WHERE app_id='app-b'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "key collision must roll back its version");
        assert!(
            repo.replace_credentials(&snapshot("app-b", 4, Some("MNOPQRSTUVWX")))
                .await
                .unwrap()
                .accepted
        );
        assert!(
            repo.replace_credentials(&snapshot("app-a", 10, None))
                .await
                .unwrap()
                .accepted
        );
        assert!(
            !repo.replace_credentials(&p).await.unwrap().accepted,
            "old snapshot must not resurrect"
        );
        let mut tasks = tokio::task::JoinSet::new();
        for version in 11..=74 {
            let repo = repo.clone();
            tasks.spawn(async move {
                let p = snapshot("app-a", version, (version != 74).then_some("ABCDEFGHIJKL"));
                repo.replace_credentials(&p).await.unwrap()
            });
        }
        tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
        let versions = repo.credential_versions(&["app-a".into()]).await.unwrap()[0].1;
        assert_eq!(
            versions, 74,
            "highest empty version wins concurrent arrivals"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM credential_keys WHERE app_id='app-a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM credential_keys WHERE app_id='app-b'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "other app must stay unchanged");
        pool.close().await;
    }
}
