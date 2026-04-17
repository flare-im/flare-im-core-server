//! # Hook配置PostgreSQL持久化
//!
//! 提供Hook配置的数据库存储和查询能力

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
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
    type Error = anyhow::Error;

    fn try_from(row: HookConfigRow) -> Result<Self, Self::Error> {
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

/// Hook配置数据库仓储
#[derive(Debug)]
pub struct PostgresHookConfigRepository {
    pool: Arc<PgPool>,
}

impl PostgresHookConfigRepository {
    /// 与 [`crate::infrastructure::persistence::PostgresCapabilityPolicy`] 等共用连接池。
    pub fn connection_pool(&self) -> Arc<PgPool> {
        self.pool.clone()
    }

    /// 创建数据库连接池
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
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
            .map_err(|e| anyhow::anyhow!("failed to fetch hook configs: {}", e))?
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
            .map_err(|e| anyhow::anyhow!("failed to fetch hook configs: {}", e))?
        };

        let mut config = HookConfig::default();

        for row in query {
            let hook_type = row.hook_type.clone();
            let hook_item: HookConfigItem = row.try_into()?;
            match hook_type.as_str() {
                "pre_send" => config.pre_send.push(hook_item),
                "post_send" => config.post_send.push(hook_item),
                "delivery" => config.delivery.push(hook_item),
                "recall" => config.recall.push(hook_item),
                "session_create" => config.session_create.push(hook_item),
                "session_update" => config.session_update.push(hook_item),
                "session_delete" => config.session_delete.push(hook_item),
                "user_login" => config.user_login.push(hook_item),
                "user_logout" => config.user_logout.push(hook_item),
                "user_online" => config.user_online.push(hook_item),
                "user_offline" => config.user_offline.push(hook_item),
                "push_pre_send" => config.push_pre_send.push(hook_item),
                "push_post_send" => config.push_post_send.push(hook_item),
                "push_delivery" => config.push_delivery.push(hook_item),
                _ => {
                    tracing::warn!(hook_type = %hook_type, "Unknown hook type");
                }
            }
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
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to save hook config: {}", e))?;

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
        .map_err(|e| anyhow::anyhow!("failed to fetch hook config by id: {}", e))?;

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
        .map_err(|e| anyhow::anyhow!("failed to fetch hook config by name: {}", e))?;

        match row {
            Some(r) => {
                let hook_item: HookConfigItem = r.clone().try_into()?;
                Ok(Some((r, hook_item)))
            }
            None => Ok(None),
        }
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

        let result = sqlx::query(
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
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $13
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
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update hook config: {}", e))?;

        Ok(result.rows_affected() > 0)
    }

    /// 更新Hook状态（启用/禁用）
    pub async fn update_enabled(&self, hook_id: i64, enabled: bool) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE hook_configs
            SET enabled = $1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
            "#,
        )
        .bind(enabled)
        .bind(hook_id)
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to update hook enabled status: {}", e))?;

        Ok(result.rows_affected() > 0)
    }

    /// 删除Hook配置
    pub async fn delete(
        &self,
        tenant_id: Option<&str>,
        hook_type: &str,
        name: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            DELETE FROM hook_configs
            WHERE (tenant_id IS NULL AND $1::TEXT IS NULL OR tenant_id = $1)
              AND hook_type = $2
              AND name = $3
            "#,
        )
        .bind(tenant_id)
        .bind(hook_type)
        .bind(name)
        .execute(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete hook config: {}", e))?;

        Ok(result.rows_affected() > 0)
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
                .map_err(|e| anyhow::anyhow!("failed to query hook configs: {}", e))?
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
                .map_err(|e| anyhow::anyhow!("failed to query hook configs: {}", e))?
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
                .map_err(|e| anyhow::anyhow!("failed to query hook configs: {}", e))?
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
                .map_err(|e| anyhow::anyhow!("failed to query hook configs: {}", e))?
            }
        };

        Ok(rows)
    }
}
