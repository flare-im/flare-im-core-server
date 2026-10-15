//! # Hook配置PostgreSQL持久化
//!
//! 提供Hook配置的数据库存储和查询能力

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_server_core::error::{AnyhowContext, FlareError, Result};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};

use crate::domain::model::{HookConfig, HookConfigItem, HookSelectorConfig, HookTransportConfig};

const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// Hook配置数据库行
#[derive(Debug, Clone, FromRow)]
pub struct HookConfigRow {
    pub id: i64,
    pub hook_id: Option<String>,
    pub tenant_id: Option<String>,
    pub hook_type: String,
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub priority: i32,
    pub group_name: Option<String>,
    pub timeout_ms: i64,
    pub max_retries: i32,
    pub error_policy: String,
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
            version: row.version,
            description: row.description,
            enabled: row.enabled,
            priority: row.priority,
            group: row.group_name,
            timeout_ms: row.timeout_ms as u64,
            max_retries: row.max_retries as u32,
            error_policy: row.error_policy,
            require_success: row.require_success,
            selector,
            transport,
            metadata,
        })
    }
}

/// 按租户分桶的 Hook 配置行（见 [`PostgresHookConfigRepository::load_by_tenant`]）。
#[derive(Debug, Clone, Default)]
pub struct TenantHookRows {
    /// `tenant_id IS NULL` 的行：对所有租户生效。
    pub global: HookConfig,
    /// 各租户自己登记的行，键为 `tenant_id`。
    pub tenants: HashMap<String, HookConfig>,
    /// 有行解析失败的租户：调用方沿用该租户上一版配置。
    pub failed_tenants: HashSet<String>,
}

/// 把一行按 `hook_type` 放进对应列表（[`HookConfig::list_mut`]，与字段名同一张类型表）；
/// 未知类型只告警。以前这里逐个列类型、漏了 `get_conversation_participants`，该类行被当成未知类型丢掉。
fn push_row(config: &mut HookConfig, row: HookConfigRow) -> Result<()> {
    let hook_type = row.hook_type.clone();
    let hook_item: HookConfigItem = row.try_into()?;
    match config.list_mut(&hook_type) {
        Some(list) => list.push(hook_item),
        None => {
            tracing::warn!(hook_type = %hook_type, "Unknown hook type");
        }
    }
    Ok(())
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

    /// 加载所有启用的Hook配置
    pub async fn load_all(&self, tenant_id: Option<&str>) -> Result<HookConfig> {
        let query = if let Some(tenant) = tenant_id {
            sqlx::query_as::<_, HookConfigRow>(
                r#"
                SELECT * FROM hook_configs
                WHERE enabled = true
                  AND (tenant_id IS NULL OR tenant_id = $1)
                ORDER BY hook_type, priority ASC
                "#,
            )
            .bind(tenant)
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to fetch hook configs: {}",
                    e
                ))
            })?
        } else {
            sqlx::query_as::<_, HookConfigRow>(
                r#"
                SELECT * FROM hook_configs
                WHERE enabled = true
                  AND tenant_id IS NULL
                ORDER BY hook_type, priority ASC
                "#,
            )
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to fetch hook configs: {}",
                    e
                ))
            })?
        };

        let mut config = HookConfig::default();

        for row in query {
            push_row(&mut config, row)?;
        }

        Ok(config)
    }

    /// 按租户分桶加载所有启用的 Hook 配置（多租户进程，未设 `TENANT_ID` 时用）。
    ///
    /// `tenant_id IS NULL` 的行进全局桶，对所有租户生效；其余每个租户一个桶，只含该租户登记
    /// 的行。全局行解析失败时整次加载失败（与 [`Self::load_all`] 相同）；某个租户的行解析失败
    /// 只记入 [`TenantHookRows::failed_tenants`]，不影响其他租户。
    pub async fn load_by_tenant(&self) -> Result<TenantHookRows> {
        let rows = sqlx::query_as::<_, HookConfigRow>(
            r#"
            SELECT * FROM hook_configs
            WHERE enabled = true
            ORDER BY hook_type, priority ASC, id ASC
            "#,
        )
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to fetch hook configs: {}",
                e
            ))
        })?;

        let mut loaded = TenantHookRows::default();
        for row in rows {
            match row.tenant_id.clone() {
                None => push_row(&mut loaded.global, row)?,
                Some(tenant) => {
                    if loaded.failed_tenants.contains(&tenant) {
                        continue;
                    }
                    let row_id = row.id;
                    let bucket = loaded.tenants.entry(tenant.clone()).or_default();
                    if let Err(error) = push_row(bucket, row) {
                        tracing::error!(
                            tenant_id = %tenant,
                            row_id,
                            %error,
                            "invalid tenant hook config row; keeping that tenant's previous hooks"
                        );
                        loaded.tenants.remove(&tenant);
                        loaded.failed_tenants.insert(tenant);
                    }
                }
            }
        }
        Ok(loaded)
    }

    /// 只加载一个租户自己登记的启用行（不含全局行），供 Administer 写入后按租户重载。
    pub async fn load_tenant(&self, tenant_id: &str) -> Result<HookConfig> {
        let rows = sqlx::query_as::<_, HookConfigRow>(
            r#"
            SELECT * FROM hook_configs
            WHERE enabled = true
              AND tenant_id = $1
            ORDER BY hook_type, priority ASC, id ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "failed to fetch tenant hook configs: {}",
                e
            ))
        })?;

        let mut config = HookConfig::default();
        for row in rows {
            push_row(&mut config, row)?;
        }
        Ok(config)
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

        let mut tx = self.pool.begin().await.map_err(write_error)?;
        let row = sqlx::query_as::<_, (i64,)>(
            r#"
            INSERT INTO hook_configs (
                tenant_id, hook_type, name, version, description, enabled,
                priority, group_name, timeout_ms, max_retries, error_policy,
                require_success, selector_config, transport_config, metadata, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (tenant_id, hook_type, name)
            DO UPDATE SET
                version = EXCLUDED.version,
                description = EXCLUDED.description,
                enabled = EXCLUDED.enabled,
                priority = EXCLUDED.priority,
                group_name = EXCLUDED.group_name,
                timeout_ms = EXCLUDED.timeout_ms,
                max_retries = EXCLUDED.max_retries,
                error_policy = EXCLUDED.error_policy,
                require_success = EXCLUDED.require_success,
                selector_config = EXCLUDED.selector_config,
                transport_config = EXCLUDED.transport_config,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            "#,
        )
        .bind(tenant_id)
        .bind(hook_type)
        .bind(&hook_item.name)
        .bind(&hook_item.version)
        .bind(&hook_item.description)
        .bind(hook_item.enabled)
        .bind(hook_item.priority)
        .bind(&hook_item.group)
        .bind(hook_item.timeout_ms as i64)
        .bind(hook_item.max_retries as i32)
        .bind(&hook_item.error_policy)
        .bind(hook_item.require_success)
        .bind(selector_json)
        .bind(transport_json)
        .bind(metadata_json)
        .bind(created_by)
        .fetch_one(&mut *tx)
        .await
        .map_err(write_error)?;

        flare_im_service_kit::audit_effects::durable::record(
            &mut tx,
            tenant_id.unwrap_or("platform"),
            "hook_config",
            &row.0.to_string(),
            "configure",
        )
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

    /// 有没有该类型、该名字的全局行（`tenant_id IS NULL`，停用的也算）。
    pub async fn global_name_exists(&self, hook_type: &str, name: &str) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM hook_configs
                WHERE tenant_id IS NULL
                  AND hook_type = $1
                  AND name = $2
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
            SET version = $1,
                description = $2,
                enabled = $3,
                priority = $4,
                group_name = $5,
                timeout_ms = $6,
                max_retries = $7,
                error_policy = $8,
                require_success = $9,
                selector_config = $10,
                transport_config = $11,
                metadata = $12,
                name = $14,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $13
            RETURNING tenant_id
            "#,
        )
        .bind(&hook_item.version)
        .bind(&hook_item.description)
        .bind(hook_item.enabled)
        .bind(hook_item.priority)
        .bind(&hook_item.group)
        .bind(hook_item.timeout_ms as i64)
        .bind(hook_item.max_retries as i32)
        .bind(&hook_item.error_policy)
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
            flare_im_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                "update",
            )
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
            flare_im_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                if enabled { "enable" } else { "disable" },
            )
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
            flare_im_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant_id.unwrap_or("platform"),
                "hook_config",
                &id.to_string(),
                "delete",
            )
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
            flare_im_service_kit::audit_effects::durable::record(
                &mut tx,
                tenant.as_deref().unwrap_or("platform"),
                "hook_config",
                &hook_id.to_string(),
                "delete",
            )
            .await
            .map_err(write_error)?;
        }
        tx.commit().await.map_err(write_error)?;
        Ok(tenant.is_some())
    }

    /// 查询Hook配置（支持过滤）
    pub async fn query(
        &self,
        tenant_id: Option<&str>,
        hook_type: Option<&str>,
        enabled_only: bool,
    ) -> Result<Vec<HookConfigRow>> {
        let rows = if let Some(tenant) = tenant_id {
            if enabled_only {
                sqlx::query_as::<_, HookConfigRow>(
                    r#"
                    SELECT * FROM hook_configs
                    WHERE enabled = true
                      AND (tenant_id IS NULL OR tenant_id = $1)
                      AND ($2::TEXT IS NULL OR hook_type = $2)
                    ORDER BY hook_type, priority ASC
                    "#,
                )
                .bind(tenant)
                .bind(hook_type)
                .fetch_all(&*self.pool)
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to query hook configs: {}",
                        e
                    ))
                })?
            } else {
                sqlx::query_as::<_, HookConfigRow>(
                    r#"
                    SELECT * FROM hook_configs
                    WHERE (tenant_id IS NULL OR tenant_id = $1)
                      AND ($2::TEXT IS NULL OR hook_type = $2)
                    ORDER BY hook_type, priority ASC
                    "#,
                )
                .bind(tenant)
                .bind(hook_type)
                .fetch_all(&*self.pool)
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to query hook configs: {}",
                        e
                    ))
                })?
            }
        } else {
            if enabled_only {
                sqlx::query_as::<_, HookConfigRow>(
                    r#"
                    SELECT * FROM hook_configs
                    WHERE enabled = true
                      AND tenant_id IS NULL
                      AND ($1::TEXT IS NULL OR hook_type = $1)
                    ORDER BY hook_type, priority ASC
                    "#,
                )
                .bind(hook_type)
                .fetch_all(&*self.pool)
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to query hook configs: {}",
                        e
                    ))
                })?
            } else {
                sqlx::query_as::<_, HookConfigRow>(
                    r#"
                    SELECT * FROM hook_configs
                    WHERE tenant_id IS NULL
                      AND ($1::TEXT IS NULL OR hook_type = $1)
                    ORDER BY hook_type, priority ASC
                    "#,
                )
                .bind(hook_type)
                .fetch_all(&*self.pool)
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to query hook configs: {}",
                        e
                    ))
                })?
            }
        };

        Ok(rows)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(hook_type: &str, name: &str) -> HookConfigRow {
        HookConfigRow {
            id: 1,
            hook_id: None,
            tenant_id: Some("tenant-a".into()),
            hook_type: hook_type.into(),
            name: name.into(),
            version: None,
            description: None,
            enabled: true,
            priority: 100,
            group_name: None,
            timeout_ms: 500,
            max_retries: 0,
            error_policy: "fail_fast".into(),
            require_success: true,
            selector_config: serde_json::json!({}),
            transport_config: serde_json::json!({"type": "local", "target": "noop"}),
            metadata: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            created_by: None,
        }
    }

    /// 修复前 `push_row` 逐个列类型、漏了 `get_conversation_participants`：这类行被当成未知类型
    /// 丢掉，库里登记的该类 Hook 永远加载不到。
    #[test]
    fn conversation_participants_rows_are_loaded() {
        let mut config = HookConfig::default();
        push_row(
            &mut config,
            row("get_conversation_participants", "participants"),
        )
        .unwrap();
        push_row(&mut config, row("pre_send", "gate")).unwrap();
        push_row(&mut config, row("message_read", "not-a-config-list")).unwrap();
        assert_eq!(config.get_conversation_participants.len(), 1);
        assert_eq!(config.get_conversation_participants[0].name, "participants");
        assert_eq!(config.pre_send.len(), 1);
        // 没有对应列表的类型仍只告警、不装。
        assert_eq!(config.hook_count(), 2);
    }
}
