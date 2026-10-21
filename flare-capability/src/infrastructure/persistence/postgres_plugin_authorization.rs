use crate::domain::{model::TenantProjectionOutcome, repository::PluginAuthorizationReader};
use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use std::sync::Arc;

pub struct PostgresPluginAuthorization {
    pub pool: Arc<PgPool>,
}
fn unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("plugin authorization storage unavailable")
}
impl PostgresPluginAuthorization {
    pub async fn replace(
        &self,
        snapshot: &PluginRuntimeSnapshot,
    ) -> Result<TenantProjectionOutcome> {
        let now: i64 =
            sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(self.pool.as_ref())
                .await
                .map_err(unavailable)?;
        snapshot.validate(now).map_err(unavailable)?;
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
}
#[async_trait::async_trait]
impl PluginAuthorizationReader for PostgresPluginAuthorization {
    async fn plugin_snapshot(&self, tenant: &str) -> Result<Option<PluginRuntimeSnapshot>> {
        let row:Option<(serde_json::Value,i64)>=sqlx::query_as("SELECT snapshot,(extract(epoch FROM clock_timestamp())*1000)::bigint FROM plugin_authorization_snapshots WHERE app_id=$1 AND valid_until_ms>(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .bind(tenant).fetch_optional(self.pool.as_ref()).await.map_err(unavailable)?;
        row.map(|(value, now)| {
            let snapshot: PluginRuntimeSnapshot =
                serde_json::from_value(value).map_err(unavailable)?;
            snapshot.validate(now).map_err(unavailable)?;
            Ok(snapshot)
        })
        .transpose()
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
        let repo = PostgresPluginAuthorization { pool: pool.clone() };
        let now = chrono::Utc::now().timestamp_millis();
        let snapshot = PluginRuntimeSnapshot {
            tenant_id: "201".into(),
            version: 20,
            issued_at_ms: now,
            valid_until_ms: now + 60000,
            plugins: vec![],
        };
        assert!(repo.replace(&snapshot).await.unwrap().accepted);
        assert!(!repo.replace(&snapshot).await.unwrap().accepted);
        assert!(
            repo.replace(&PluginRuntimeSnapshot {
                valid_until_ms: now + 59000,
                ..snapshot.clone()
            })
            .await
            .is_err()
        );
        assert_eq!(
            repo.replace(&PluginRuntimeSnapshot {
                version: 19,
                ..snapshot.clone()
            })
            .await
            .unwrap()
            .current_version,
            20
        );
        let restart = PostgresPluginAuthorization { pool: pool.clone() };
        assert_eq!(
            restart.plugin_snapshot("201").await.unwrap(),
            Some(snapshot)
        );
        assert!(restart.plugin_snapshot("202").await.unwrap().is_none());
        sqlx::query("UPDATE plugin_authorization_snapshots SET valid_until_ms=1,snapshot=jsonb_set(snapshot,'{validUntilMs}','1') WHERE app_id='201'").execute(pool.as_ref()).await.unwrap();
        assert!(restart.plugin_snapshot("201").await.unwrap().is_none());
        assert_eq!(
            restart.versions(&[]).await.unwrap(),
            vec![("201".into(), 20)]
        );
        pool.close().await;
    }
}
