//! Conditional tenant kicks use authoritative storage, never a TTL cache.
//! A shared row lock serializes the bounded command with Capability's projection UPSERT.
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

#[derive(Clone)]
pub struct TenantKickFence {
    pool: PgPool,
    admission: Arc<Semaphore>,
}
pub struct TenantKickLease {
    transaction: Transaction<'static, Postgres>,
    _permit: OwnedSemaphorePermit,
}
impl TenantKickFence {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            admission: Arc::new(Semaphore::new(1)),
        }
    }
    /// None means superseded. Missing/behind projections are retryable errors.
    /// One command/zero queue leaves the shared two-connection runtime pool a read slot.
    pub async fn acquire(
        &self,
        tenant: &str,
        version: u64,
    ) -> Result<Option<TenantKickLease>, Status> {
        let version = i64::try_from(version)
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| Status::invalid_argument("positive tenant version required"))?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("tenant kick busy"))?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| Status::unavailable("tenant fence unavailable"))?;
        sqlx::query("SELECT set_config('lock_timeout',(CASE WHEN setting::bigint=0 THEN 1000 ELSE LEAST(setting::bigint,1000) END)::text||'ms',true) FROM pg_settings WHERE name='lock_timeout'")
            .execute(&mut *tx).await.map_err(|_|Status::unavailable("tenant fence unavailable"))?;
        sqlx::query("SELECT set_config('statement_timeout',(CASE WHEN setting::bigint=0 THEN 2000 ELSE LEAST(setting::bigint,2000) END)::text||'ms',true) FROM pg_settings WHERE name='statement_timeout'")
            .execute(&mut *tx).await.map_err(|_|Status::unavailable("tenant fence unavailable"))?;
        let row: Option<(String, i64)> =
            sqlx::query_as("SELECT status,version FROM tenants WHERE tenant_id=$1 FOR SHARE")
                .bind(tenant)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Status::unavailable("tenant fence unavailable"))?;
        match row {
            Some((status, current))
                if current == version && matches!(status.as_str(), "suspended" | "deleting") =>
            {
                Ok(Some(TenantKickLease {
                    transaction: tx,
                    _permit: permit,
                }))
            }
            Some((status, current))
                if current > version || (current == version && status == "active") =>
            {
                tx.rollback()
                    .await
                    .map_err(|_| Status::unavailable("tenant fence unavailable"))?;
                Ok(None)
            }
            _ => {
                tx.rollback()
                    .await
                    .map_err(|_| Status::unavailable("tenant fence unavailable"))?;
                Err(Status::failed_precondition("tenant projection not ready"))
            }
        }
    }
}
impl TenantKickLease {
    pub async fn finish(self) -> Result<(), Status> {
        self.transaction
            .commit()
            .await
            .map_err(|_| Status::unavailable("tenant fence outcome unknown"))
    }
}
