//! `tenants` 表的投影仓储（与 `deploy/init.sql` 第 1 节对齐）。
//!
//! 写入是一条带 `WHERE tenants.version < EXCLUDED.version` 的 upsert：版本比较与写入在同一条
//! 语句里完成，并发重推同一租户也不会回退版本。

use std::sync::Arc;

use flare_im_contracts::domain::tenant::{TenantQuota, TenantStatus};
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
impl TenantProjectionRepository for PostgresTenantProjectionRepository {
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
