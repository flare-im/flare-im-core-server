//! # Hook配置PostgreSQL持久化
//!
//! 控制面的写入与查询。每次写入在同一事务里递增 `hook_config_versions`，Ingest 约每秒查一次、只取
//! 变了的范围（见 `flare_im_core_hooks::hooks::HookConfigReloader`）。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_server_core::error::{AnyhowContext, FlareError, Result};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use crate::domain::model::{HookConfigItem, HookSelectorConfig, HookTransportConfig};

const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// 写入串行化用的咨询锁键。版本号必须按提交顺序递增：Ingest 轮询「版本大于已加载版本」，后提交的
/// 小版本会被漏掉。Hook 配置写入很少，串行没有代价。
const HOOK_CONFIG_VERSION_LOCK: i64 = 0x686f_6f6b_5f63_6667;

/// 范围的版本加一：`scope` 为应用，空串为平台（`tenant_id` 为空的行）。在写入事务提交之前调用，锁
/// 持有到提交。
async fn bump_config_version(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Option<&str>,
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(HOOK_CONFIG_VERSION_LOCK)
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        r#"
        INSERT INTO hook_config_versions (scope, version)
        SELECT $1, COALESCE(max(version), 0) + 1 FROM hook_config_versions
        ON CONFLICT (scope) DO UPDATE
           SET version = EXCLUDED.version,
               changed_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(tenant_id.unwrap_or(""))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Hook配置数据库行
#[derive(Debug, Clone, FromRow)]
pub struct HookConfigRow {
    pub id: i64,
    pub hook_id: Option<String>,
    pub tenant_id: Option<String>,
    pub hook_type: String,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub hook_order: i32,
    pub timeout_ms: i64,
    pub require_success: bool,
    pub selector_config: Value,
    pub transport_config: Value,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<String>,
}

impl TryFrom<HookConfigRow> for HookConfigItem {
    type Error = FlareError;

    fn try_from(row: HookConfigRow) -> std::result::Result<Self, Self::Error> {
        // 解析选择器配置
        let selector: HookSelectorConfig = serde_json::from_value(row.selector_config)
            .context("failed to deserialize selector config")?;

        // 解析传输配置
        let transport: HookTransportConfig = serde_json::from_value(row.transport_config)
            .context("failed to deserialize transport config")?;

        // 解析元数据
        let metadata = match row.metadata {
            Some(value) => serde_json::from_value::<HashMap<String, String>>(value)
                .context("failed to deserialize metadata")?,
            None => HashMap::new(),
        };

        Ok(HookConfigItem {
            name: row.name,
            description: row.description,
            enabled: row.enabled,
            order: row.hook_order,
            timeout_ms: row.timeout_ms as u64,
            require_success: row.require_success,
            selector,
            transport,
            metadata,
        })
    }
}

/// `hook_deployment_summary` 的一行：一个服务装载的部署 Hook。
#[derive(Debug, Clone, FromRow)]
pub struct HookDeploymentSummaryRow {
    pub service: String,
    pub source: Option<String>,
    pub hooks: Value,
    pub instance_id: Option<String>,
    pub loaded_at: DateTime<Utc>,
}

/// Hook配置数据库仓储
#[derive(Debug)]
pub struct PostgresHookConfigRepository {
    pool: Arc<PgPool>,
}

fn write_error(_: sqlx::Error) -> FlareError {
    FlareError::system("hook write transaction unavailable")
}

impl PostgresHookConfigRepository {
    /// 与 [`crate::infrastructure::persistence::PostgresCapabilityPolicy`] 等共用连接池。
    pub fn connection_pool(&self) -> Arc<PgPool> {
        self.pool.clone()
    }

    /// 创建数据库连接池
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::new_with_pool_profile(database_url, DEFAULT_MAX_CONNECTIONS, 2, 10, 300, 1800).await
    }

    /// 创建数据库连接池（显式 pool profile）。
    pub async fn new_with_pool_profile(
        database_url: &str,
        max_connections: u32,
        min_connections: u32,
        acquire_timeout_seconds: u64,
        idle_timeout_seconds: u64,
        max_lifetime_seconds: u64,
    ) -> Result<Self> {
        let max_connections = max_connections.max(1);
        let min_connections = min_connections.min(max_connections);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(std::time::Duration::from_secs(
                acquire_timeout_seconds.max(1),
            ))
            .idle_timeout(std::time::Duration::from_secs(idle_timeout_seconds.max(1)))
            .max_lifetime(std::time::Duration::from_secs(max_lifetime_seconds.max(1)))
            .connect(database_url)
            .await
            .context("failed to create database connection pool")?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// 保存Hook配置
    pub async fn save(
        &self,
        tenant_id: Option<&str>,
        hook_type: &str,
        hook_item: &HookConfigItem,
        created_by: Option<&str>,
    ) -> Result<i64> {
        let selector_json = serde_json::to_value(&hook_item.selector)
            .context("failed to serialize selector config")?;
        let transport_json = serde_json::to_value(&hook_item.transport)
            .context("failed to serialize transport config")?;
        let metadata_json = if hook_item.metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&hook_item.metadata)
                    .context("failed to serialize metadata")?,
            )
        };

        // 平台行的 tenant_id 是 NULL，表约束管不到，唯一性在部分索引上。
        let conflict_target = if tenant_id.is_some() {
            "(tenant_id, hook_type, name)"
        } else {
            "(hook_type, name) WHERE tenant_id IS NULL"
        };
        let statement = format!(
            r#"
            INSERT INTO hook_configs (
                tenant_id, hook_type, name, description, enabled, hook_order, timeout_ms,
                require_success, selector_config, transport_config, metadata, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT {conflict_target}
            DO UPDATE SET
                description = EXCLUDED.description,
                enabled = EXCLUDED.enabled,
                hook_order = EXCLUDED.hook_order,
                timeout_ms = EXCLUDED.timeout_ms,
                require_success = EXCLUDED.require_success,
                selector_config = EXCLUDED.selector_config,
                transport_config = EXCLUDED.transport_config,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            "#
        );

        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let row = sqlx::query_as::<_, (i64,)>(&statement)
            .bind(tenant_id)
            .bind(hook_type)
            .bind(&hook_item.name)
            .bind(&hook_item.description)
            .bind(hook_item.enabled)
            .bind(hook_item.order)
            .bind(hook_item.timeout_ms as i64)
            .bind(hook_item.require_success)
            .bind(selector_json)
            .bind(transport_json)
            .bind(metadata_json)
            .bind(created_by)
            .fetch_one(&mut *tx)
            .await
            .map_err(write_error)?;

        flare_im_core_service_kit::audit_effects::durable::record(
            &mut tx,
            tenant_id.unwrap_or("platform"),
            "hook_config",
            &row.0.to_string(),
            "configure",
        )
        .await
        .map_err(write_error)?;
        bump_config_version(&mut tx, tenant_id)
            .await
            .map_err(write_error)?;
        tx.commit().await.map_err(write_error)?;
        Ok(row.0)
    }

    /// 根据ID查询Hook配置
    pub async fn get_by_id(&self, hook_id: i64) -> Result<Option<(HookConfigRow, HookConfigItem)>> {
        let row = sqlx::query_as::<_, HookConfigRow>(
            r#"
            SELECT * FROM hook_configs
            WHERE id = $1
            "#,
        )
        .bind(hook_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to fetch hook config by id: {}",
                e
            ))
        })?;

        match row {
            Some(r) => {
                let hook_item: HookConfigItem = r.clone().try_into()?;
                Ok(Some((r, hook_item)))
            }
            None => Ok(None),
        }
    }

    /// 根据租户ID、Hook类型和名称查询Hook配置
    pub async fn get_by_name(
        &self,
        tenant_id: Option<&str>,
        hook_type: &str,
        name: &str,
    ) -> Result<Option<(HookConfigRow, HookConfigItem)>> {
        let row = sqlx::query_as::<_, HookConfigRow>(
            r#"
            SELECT * FROM hook_configs
            WHERE (tenant_id IS NULL AND $1::TEXT IS NULL OR tenant_id = $1)
              AND hook_type = $2
              AND name = $3
            "#,
        )
        .bind(tenant_id)
        .bind(hook_type)
        .bind(name)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to fetch hook config by name: {}",
                e
            ))
        })?;

        match row {
            Some(r) => {
                let hook_item: HookConfigItem = r.clone().try_into()?;
                Ok(Some((r, hook_item)))
            }
            None => Ok(None),
        }
    }

    /// 这个类型、这个名字有没有被平台或部署占用：全局行（`tenant_id IS NULL`，停用的也算），或部署
    /// 摘要里的 Hook。
    pub async fn global_name_exists(&self, hook_type: &str, name: &str) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM hook_configs
                WHERE tenant_id IS NULL
                  AND hook_type = $1
                  AND name = $2
            ) OR EXISTS (
                SELECT 1
                FROM hook_deployment_summary AS summary,
                     jsonb_array_elements(summary.hooks) AS hook
                WHERE hook->>'hook_type' = $1
                  AND hook->>'name' = $2
            )
            "#,
        )
        .bind(hook_type)
        .bind(name)
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to check global hook name: {}",
                e
            ))
        })
    }

    /// 更新Hook配置
    pub async fn update(&self, hook_id: i64, hook_item: &HookConfigItem) -> Result<bool> {
        let selector_json = serde_json::to_value(&hook_item.selector)
            .context("failed to serialize selector config")?;
        let transport_json = serde_json::to_value(&hook_item.transport)
            .context("failed to serialize transport config")?;
        let metadata_json = if hook_item.metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&hook_item.metadata)
                    .context("failed to serialize metadata")?,
            )
        };

        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let tenant = sqlx::query_scalar::<_, Option<String>>(
            r#"
            UPDATE hook_configs
            SET description = $1,
                enabled = $2,
                hook_order = $3,
                timeout_ms = $4,
                require_success = $5,
                selector_config = $6,
                transport_config = $7,
                metadata = $8,
                name = $10,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $9
            RETURNING tenant_id
            "#,
        )
        .bind(&hook_item.description)
        .bind(hook_item.enabled)
        .bind(hook_item.order)
        .bind(hook_item.timeout_ms as i64)
        .bind(hook_item.require_success)
        .bind(selector_json)
        .bind(transport_json)
        .bind(metadata_json)
        .bind(hook_id)
        .bind(&hook_item.name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(write_error)?;

        if let Some(tenant) = &tenant {
            flare_im_core_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                "update",
            )
            .await
            .map_err(write_error)?;
            bump_config_version(&mut tx, tenant.as_deref())
                .await
                .map_err(write_error)?;
        }
        tx.commit().await.map_err(write_error)?;
        Ok(tenant.is_some())
    }

    /// 更新Hook状态（启用/禁用）
    pub async fn update_enabled(&self, hook_id: i64, enabled: bool) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let current = sqlx::query_scalar::<_, bool>(
            "SELECT enabled FROM hook_configs WHERE id=$1 FOR UPDATE",
        )
        .bind(hook_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(write_error)?;
        if current == Some(enabled) {
            tx.commit().await.map_err(write_error)?;
            return Ok(true);
        }
        let tenant = sqlx::query_scalar::<_, Option<String>>(
            r#"
            UPDATE hook_configs
            SET enabled = $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $2 AND enabled IS DISTINCT FROM $1
            RETURNING tenant_id
            "#,
        )
        .bind(enabled)
        .bind(hook_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(write_error)?;

        if let Some(tenant) = &tenant {
            flare_im_core_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                if enabled { "enable" } else { "disable" },
            )
            .await
            .map_err(write_error)?;
            bump_config_version(&mut tx, tenant.as_deref())
                .await
                .map_err(write_error)?;
        }
        tx.commit().await.map_err(write_error)?;
        Ok(tenant.is_some())
    }

    /// 删除Hook配置
    pub async fn delete(
        &self,
        tenant_id: Option<&str>,
        hook_type: &str,
        name: &str,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let id = sqlx::query_scalar::<_, i64>(
            r#"
            DELETE FROM hook_configs
            WHERE (tenant_id IS NULL AND $1::TEXT IS NULL OR tenant_id = $1)
              AND hook_type = $2
              AND name = $3
            RETURNING id
            "#,
        )
        .bind(tenant_id)
        .bind(hook_type)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(write_error)?;

        if let Some(id) = id {
            flare_im_core_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant_id.unwrap_or("platform"),
                "hook_config",
                &id.to_string(),
                "delete",
            )
            .await
            .map_err(write_error)?;
            bump_config_version(&mut tx, tenant_id)
                .await
                .map_err(write_error)?;
        }
        tx.commit().await.map_err(write_error)?;
        Ok(id.is_some())
    }

    /// Delete the immutable identity, not a name read before a concurrent rename/recreate.
    pub async fn delete_by_id(&self, hook_id: i64) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let tenant = sqlx::query_scalar::<_, Option<String>>(
            "DELETE FROM hook_configs WHERE id=$1 RETURNING tenant_id",
        )
        .bind(hook_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(write_error)?;
        if let Some(tenant) = &tenant {
            flare_im_core_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                "delete",
            )
            .await
            .map_err(write_error)?;
            bump_config_version(&mut tx, tenant.as_deref())
                .await
                .map_err(write_error)?;
        }
        tx.commit().await.map_err(write_error)?;
        Ok(tenant.is_some())
    }

    /// 一个范围自己的 Hook：`tenant_id` 为应用，`None` 为平台。应用的列表不含平台行，平台 Hook 的目标
    /// 不给应用看。
    pub async fn query(
        &self,
        tenant_id: Option<&str>,
        hook_type: Option<&str>,
        enabled_only: bool,
    ) -> Result<Vec<HookConfigRow>> {
        sqlx::query_as::<_, HookConfigRow>(
            r#"
            SELECT * FROM hook_configs
            WHERE tenant_id IS NOT DISTINCT FROM $1
              AND ($2::TEXT IS NULL OR hook_type = $2)
              AND (NOT $3 OR enabled)
            ORDER BY hook_type, hook_order ASC, id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(hook_type)
        .bind(enabled_only)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to query hook configs: {}",
                e
            ))
        })
    }

    /// 部署摘要，每个服务一行。
    pub async fn deployment_summaries(&self) -> Result<Vec<HookDeploymentSummaryRow>> {
        sqlx::query_as::<_, HookDeploymentSummaryRow>(
            r#"
            SELECT service, source, hooks, instance_id, loaded_at
            FROM hook_deployment_summary
            ORDER BY service
            "#,
        )
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to read hook deployment summary: {}",
                e
            ))
        })
    }
}

#[cfg(test)]
impl PostgresHookConfigRepository {
    /// 指向不监听端口的惰性连接池：只给在查库之前就该拒绝的测试用，真查库会很快失败。
    pub(crate) fn unconnected() -> Self {
        let pool = PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("postgres://flare@127.0.0.1:1/none")
            .expect("a lazy pool does not connect");
        Self {
            pool: Arc::new(pool),
        }
    }
}
