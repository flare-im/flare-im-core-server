//! `capability_plugin_endpoints`（迁移 0011）：经 `RegisterPluginEndpoint` 登记的插件端点。
//!
//! 每个 Capability 实例启动时整表载入、登记/注销先写表、按周期重读，于是多实例收敛到同一份
//! 登记，重启也不丢。`labels` 以 JSONB 存放（sqlx 未开 json 特性：按文本绑定、按文本读回）。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use sqlx::{PgPool, Row};

use crate::domain::model::{PluginEndpoint, PluginEndpointKey};
use crate::domain::repository::PluginEndpointRepository;

pub struct PostgresPluginEndpointRepository {
    pool: Arc<PgPool>,
}

impl PostgresPluginEndpointRepository {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

/// 存储错误只带固定说明与操作名；连接串、SQL 与行内容不进错误文本。
fn storage_error(operation: &'static str, _error: sqlx::Error) -> FlareError {
    tracing::warn!(operation, "plugin endpoint storage failed");
    ErrorBuilder::new(
        ErrorCode::DatabaseError,
        "plugin endpoint storage is unavailable",
    )
    .details(operation)
    .build_error()
}

#[async_trait::async_trait]
impl PluginEndpointRepository for PostgresPluginEndpointRepository {
    async fn upsert(&self, endpoint: &PluginEndpoint) -> Result<()> {
        let instance = &endpoint.instance;
        let labels = serde_json::to_string(&instance.labels).map_err(|_| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "labels are not encodable").build_error()
        })?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_error("upsert", e))?;
        sqlx::query(
            "INSERT INTO capability_plugin_endpoints (
                 tenant_id, plugin_id, capability_id, grpc_authority, labels, plugin_version,
                 api_version, manifest_sha256, declared_operations, unverified,
                 registered_at, updated_at)
             VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, now(), now())
             ON CONFLICT (tenant_id, plugin_id) DO UPDATE SET
                 capability_id = EXCLUDED.capability_id,
                 grpc_authority = EXCLUDED.grpc_authority,
                 labels = EXCLUDED.labels,
                 plugin_version = EXCLUDED.plugin_version,
                 api_version = EXCLUDED.api_version,
                 manifest_sha256 = EXCLUDED.manifest_sha256,
                 declared_operations = EXCLUDED.declared_operations,
                 unverified = EXCLUDED.unverified,
                 updated_at = now()",
        )
        .bind(&endpoint.tenant_id)
        .bind(&instance.plugin_id)
        .bind(&instance.capability_id)
        .bind(&instance.grpc_authority)
        .bind(labels)
        .bind(&instance.plugin_version)
        .bind(&instance.api_version)
        .bind(&instance.manifest_sha256)
        .bind(&instance.declared_operations)
        .bind(instance.unverified)
        .execute(&mut *tx)
        .await
        .map_err(|e| storage_error("upsert", e))?;
        flare_im_service_kit::audit_effects::durable::record(
            &mut tx,
            &endpoint.tenant_id,
            "plugin_endpoint",
            &instance.plugin_id,
            "register",
        )
        .await
        .map_err(|e| storage_error("upsert audit", e))?;
        tx.commit()
            .await
            .map_err(|e| storage_error("upsert commit", e))?;
        Ok(())
    }

    async fn delete(&self, tenant_id: &str, plugin_id: &str) -> Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_error("delete", e))?;
        let deleted = sqlx::query(
            "DELETE FROM capability_plugin_endpoints WHERE tenant_id = $1 AND plugin_id = $2",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| storage_error("delete", e))?
        .rows_affected();
        if deleted > 0 {
            flare_im_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant_id,
                "plugin_endpoint",
                plugin_id,
                "deregister",
            )
            .await
            .map_err(|e| storage_error("delete audit", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| storage_error("delete commit", e))?;
        Ok(deleted > 0)
    }

    async fn list_all(&self) -> Result<Vec<PluginEndpoint>> {
        let rows = sqlx::query(
            "SELECT tenant_id, plugin_id, capability_id, grpc_authority, labels::text AS labels,
                    plugin_version, api_version, manifest_sha256, declared_operations,
                    unverified
             FROM capability_plugin_endpoints
             ORDER BY tenant_id, plugin_id",
        )
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| storage_error("list", e))?;
        let mut endpoints = Vec::with_capacity(rows.len());
        for row in rows {
            let tenant_id: String = row.get("tenant_id");
            let plugin_id: String = row.get("plugin_id");
            let labels: String = row.get("labels");
            // The table guarantees an object; a value that is not a string makes the row
            // unusable as a route, so it is skipped (and logged) instead of failing the reload.
            // Rows written before plugin ids had to be identifiers are logged escaped (`?`).
            let Ok(labels) = serde_json::from_str::<HashMap<String, String>>(&labels) else {
                tracing::warn!(
                    tenant_id = ?tenant_id,
                    plugin_id = ?plugin_id,
                    "skipping plugin endpoint whose labels are not a string map"
                );
                continue;
            };
            endpoints.push(PluginEndpoint {
                tenant_id,
                instance: RegisteredPluginInstance {
                    plugin_id,
                    capability_id: row.get("capability_id"),
                    grpc_authority: row.get("grpc_authority"),
                    labels,
                    plugin_version: row.get("plugin_version"),
                    api_version: row.get("api_version"),
                    manifest_sha256: row.get("manifest_sha256"),
                    declared_operations: row.get("declared_operations"),
                    unverified: row.get("unverified"),
                },
            });
        }
        Ok(endpoints)
    }

    async fn record_healthy(&self, keys: &[PluginEndpointKey]) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let tenants: Vec<&str> = keys.iter().map(|k| k.tenant_id.as_str()).collect();
        let plugins: Vec<&str> = keys.iter().map(|k| k.plugin_id.as_str()).collect();
        let updated = sqlx::query(
            "UPDATE capability_plugin_endpoints e SET last_healthy_at = now()
             FROM unnest($1::text[], $2::text[]) AS k(tenant_id, plugin_id)
             WHERE e.tenant_id = k.tenant_id AND e.plugin_id = k.plugin_id",
        )
        .bind(tenants)
        .bind(plugins)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| storage_error("record_healthy", e))?
        .rows_affected();
        Ok(updated)
    }

    async fn delete_stale(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<PluginEndpointKey>> {
        let rows = sqlx::query(
            "DELETE FROM capability_plugin_endpoints
             WHERE updated_at < $1 AND (last_healthy_at IS NULL OR last_healthy_at < $1)
             RETURNING tenant_id, plugin_id",
        )
        .bind(cutoff)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| storage_error("delete_stale", e))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                PluginEndpointKey::new(
                    row.get::<String, _>("tenant_id"),
                    row.get::<String, _>("plugin_id"),
                )
            })
            .collect())
    }
}
