use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::resource_access::ResourceCallBudget;
use std::sync::Arc;

pub struct PostgresResourceBudget(pub Arc<sqlx::PgPool>);
impl PostgresResourceBudget {
    pub async fn check_schema(&self) -> Result<()> {
        sqlx::query("SELECT tenant_id,plugin_id,actor_id,request_id,calls,expires_at_ms FROM capability_plugin_resource_calls LIMIT 0")
            .execute(self.0.as_ref()).await
            .map(|_|()).map_err(|_|CapabilityError::System("resource budget schema unavailable".into()))
    }
}
#[async_trait::async_trait]
impl ResourceCallBudget for PostgresResourceBudget {
    async fn reserve(
        &self,
        tenant: &str,
        plugin: &str,
        actor: &str,
        request: &str,
        expires: i64,
    ) -> Result<bool> {
        let unavailable = |_| CapabilityError::System("resource budget unavailable".into());
        let mut tx = self.0.begin().await.map_err(unavailable)?;
        sqlx::query("SET LOCAL statement_timeout='1500ms'")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        sqlx::query("SET LOCAL lock_timeout='1s'")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        // Serialize admission per tenant, not the whole platform. The bounded
        // leased ledger is shared by every Capability instance.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,715))")
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let now: i64 =
            sqlx::query_scalar("SELECT (extract(epoch from clock_timestamp())*1000)::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        if expires <= now || expires > now.saturating_add(120000) {
            return Ok(false);
        }
        sqlx::query(
            "DELETE FROM capability_plugin_resource_calls WHERE tenant_id=$1 AND expires_at_ms<=$2",
        )
        .bind(tenant)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        // Also reclaim idle tenants' expired leases. Skip rows another replica
        // owns, and bound work per admission instead of a growing startup sweep.
        sqlx::query("DELETE FROM capability_plugin_resource_calls WHERE ctid IN (SELECT ctid FROM capability_plugin_resource_calls WHERE expires_at_ms<=$1 ORDER BY expires_at_ms LIMIT 128 FOR UPDATE SKIP LOCKED)")
            .bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        let result=sqlx::query("INSERT INTO capability_plugin_resource_calls(tenant_id,plugin_id,actor_id,request_id,calls,expires_at_ms) SELECT $1,$2,$3,$4,1,$5 WHERE (SELECT count(*) FROM capability_plugin_resource_calls WHERE tenant_id=$1)<1024 OR EXISTS(SELECT 1 FROM capability_plugin_resource_calls WHERE tenant_id=$1 AND plugin_id=$2 AND actor_id=$3 AND request_id=$4) ON CONFLICT(tenant_id,plugin_id,actor_id,request_id) DO UPDATE SET calls=capability_plugin_resource_calls.calls+1 WHERE capability_plugin_resource_calls.calls<8")
            .bind(tenant).bind(plugin).bind(actor).bind(request).bind(expires).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result.rows_affected() == 1)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub async fn run(pool: Arc<sqlx::PgPool>) {
        let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../deploy/init.sql"));
        let start = source
            .find("CREATE TABLE capability_plugin_resource_calls (")
            .unwrap();
        let end = start
            + source[start..]
                .find("CREATE TABLE capability_plugin_endpoints (")
                .unwrap();
        sqlx::raw_sql(&source[start..end])
            .execute(pool.as_ref())
            .await
            .unwrap();
        PostgresResourceBudget(pool.clone())
            .check_schema()
            .await
            .unwrap();
        let until = chrono::Utc::now().timestamp_millis() + 60000;
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                PostgresResourceBudget(pool)
                    .reserve("201", "example.echo", "u1", "same", until)
                    .await
                    .unwrap()
            }));
        }
        let mut accepted = 0;
        for task in tasks {
            accepted += usize::from(task.await.unwrap());
        }
        assert_eq!(accepted, 8, "all replicas share one invocation quota");
        let replica = PostgresResourceBudget(pool.clone());
        assert!(
            !replica
                .reserve("201", "example.echo", "u1", "same", until)
                .await
                .unwrap()
        );
        assert!(
            replica
                .reserve("202", "example.echo", "u1", "same", until)
                .await
                .unwrap()
        );
        assert!(
            replica
                .reserve("201", "example.echo", "u2", "same", until)
                .await
                .unwrap()
        );
        assert!(
            !replica
                .reserve("201", "example.echo", "u1", "expired", 1)
                .await
                .unwrap()
        );
        sqlx::query(
            "UPDATE capability_plugin_resource_calls SET expires_at_ms=1 WHERE tenant_id='201'",
        )
        .execute(pool.as_ref())
        .await
        .unwrap();
        assert!(
            replica
                .reserve("201", "example.echo", "u1", "same", until)
                .await
                .unwrap()
        );
    }
}
