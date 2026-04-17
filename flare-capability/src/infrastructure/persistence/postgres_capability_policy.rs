//! PostgreSQL 实现的能力策略（`capability_*` 表，与 `init_v2.sql` 对齐）。

use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use crate::domain::capability::{
    CapabilityError, CapabilityPolicyBackend, Result, UserCapabilityGrant,
};

#[derive(Debug, Clone, FromRow)]
struct GrantRow {
    tenant_id: String,
    user_id: String,
    capability_id: String,
    granted_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    plan_code: Option<String>,
    source: Option<String>,
}

impl From<GrantRow> for UserCapabilityGrant {
    fn from(r: GrantRow) -> Self {
        UserCapabilityGrant {
            tenant_id: r.tenant_id,
            user_id: r.user_id,
            capability_id: r.capability_id,
            granted_at: r.granted_at,
            expires_at: r.expires_at,
            plan_code: r.plan_code,
            source: r.source,
        }
    }
}

/// 基于 PostgreSQL 的 [`CapabilityPolicyBackend`]
#[derive(Debug, Clone)]
pub struct PostgresCapabilityPolicy {
    pool: Arc<PgPool>,
}

impl PostgresCapabilityPolicy {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// 启动时校验 `public.capability_service_settings` 是否存在，避免首包 Dispatch 才报错。
    ///
    /// 典型误配：在库 A 执行了 `init_v2.sql`，但 `DATABASE_URL` 指向库 B；或未设置 `DATABASE_URL` 时
    /// 二进制默认连 `localhost:25432`，而脚本在 `5432` 执行。
    pub async fn assert_public_capability_schema(pool: &PgPool) -> anyhow::Result<()> {
        let exists: (bool,) = sqlx::query_as(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')
                  AND n.nspname = 'public'
                  AND c.relname = 'capability_service_settings'
            )
            "#,
        )
        .fetch_one(pool)
        .await
        .context("probe public.capability_service_settings")?;

        if exists.0 {
            return Ok(());
        }

        let row: (String, Option<String>) = sqlx::query_as(
            "SELECT current_database(), current_setting('search_path', true)",
        )
        .fetch_one(pool)
        .await
        .unwrap_or_else(|_| ("(unknown)".to_string(), None));

        anyhow::bail!(
            "missing table public.capability_service_settings on database {:?} (search_path={:?}). \
             Run flare-im-core/deploy/db/init_v2.sql section 9 on **this** database. \
             If you already ran init elsewhere, align DATABASE_URL host/port/dbname with that instance; \
             unset DATABASE_URL defaults to postgresql://*:*@localhost:25432/flare2 (see config base.toml [postgres.media]) in flare-capability cmd.",
            row.0,
            row.1
        );
    }

    /// 表结构已存在但无任何授权行时告警，避免首包 Dispatch 才看到 `user capability grant missing or expired`。
    pub async fn warn_if_user_grants_empty(pool: &PgPool) -> anyhow::Result<()> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*)::bigint FROM public.capability_user_grants",
        )
        .fetch_one(pool)
        .await
        .context("count public.capability_user_grants")?;

        if row.0 == 0 {
            tracing::warn!(
                target = "flare_capability::db",
                "public.capability_user_grants is empty: Dispatch will fail with \"user capability grant missing or expired\" until seeded. \
                 Apply flare-im-core/deploy/db/init_v2.sql section 9 (INSERT tenant_id='0', user_id='*', capability_id='rtc.*') on **this** database (host/db must match DATABASE_URL / startup log postgres_after_at)."
            );
        }
        Ok(())
    }

    async fn load_global_enabled(&self) -> Result<bool> {
        let row: Option<(bool,)> = sqlx::query_as(
            r#"SELECT global_enabled FROM public.capability_service_settings WHERE id = 1"#,
        )
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("capability global_enabled: {e}")))?;
        Ok(row.map(|r| r.0).unwrap_or(true))
    }

    async fn tenant_capability_disabled(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<bool> {
        let row: Option<(bool,)> = sqlx::query_as(
            r#"
            SELECT enabled FROM public.capability_tenant_switches
            WHERE tenant_id = $1 AND capability_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(capability_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("capability tenant switch: {e}")))?;
        Ok(matches!(row, Some((false,))))
    }

    async fn has_active_grant(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<bool> {
        // user_id = '*' 表示该租户下任意用户（与 init_v2 开发种子对齐；生产可改为按用户灌库并删除通配行）
        // 租户 `0` 与 `default` 视为同一默认租户（网关 JWT / SDK / 编排器历史上不一致，见 flare-capability 日志 capability.dispatch tenant_id）
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT 1::bigint FROM public.capability_user_grants
            WHERE (
                tenant_id = $1
                OR ($1 IN ('0', 'default') AND tenant_id IN ('0', 'default'))
            )
              AND (user_id = $2 OR user_id = '*')
              AND (capability_id = $3 OR capability_id = (split_part($3, '.', 1) || '.*'))
              AND (expires_at IS NULL OR expires_at > NOW())
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(capability_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("capability_user_grants: {e}")))?;
        Ok(row.is_some())
    }
}

#[async_trait]
impl CapabilityPolicyBackend for PostgresCapabilityPolicy {
    async fn ensure_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        if !self.load_global_enabled().await? {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        if self.tenant_capability_disabled(tenant_id, capability_id).await? {
            return Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} disabled for tenant {tenant_id}"
            )));
        }
        if !self
            .has_active_grant(tenant_id, user_id, capability_id)
            .await?
        {
            tracing::warn!(
                tenant_id = %tenant_id,
                capability_id = %capability_id,
                user_id_len = user_id.len(),
                "capability_user_grants: no matching row (tenant 0/default are equivalent in lookup; need rtc.* or exact capability + user or user_id='*')"
            );
            return Err(CapabilityError::PolicyDenied(
                "user capability grant missing or expired".into(),
            ));
        }
        Ok(())
    }

    async fn list_user_grants(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<UserCapabilityGrant>> {
        let rows = sqlx::query_as::<_, GrantRow>(
            r#"
            SELECT tenant_id, user_id, capability_id, granted_at, expires_at, plan_code, source
            FROM public.capability_user_grants
            WHERE tenant_id = $1 AND user_id = $2
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY capability_id
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("list_user_grants: {e}")))?;
        Ok(rows.into_iter().map(UserCapabilityGrant::from).collect())
    }

    async fn grant_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
        expires_at: Option<DateTime<Utc>>,
        plan_code: Option<String>,
        source: Option<String>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO public.capability_user_grants (
                tenant_id, user_id, capability_id, granted_at, expires_at, plan_code, source
            )
            VALUES ($1, $2, $3, NOW(), $4, $5, $6)
            ON CONFLICT (tenant_id, user_id, capability_id) DO UPDATE SET
                granted_at = NOW(),
                expires_at = EXCLUDED.expires_at,
                plan_code = EXCLUDED.plan_code,
                source = EXCLUDED.source
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(capability_id)
        .bind(expires_at)
        .bind(plan_code.as_deref())
        .bind(source.as_deref())
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("grant_user_capability: {e}")))?;
        Ok(())
    }

    async fn revoke_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM public.capability_user_grants
            WHERE tenant_id = $1 AND user_id = $2 AND capability_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(capability_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("revoke_user_capability: {e}")))?;
        Ok(())
    }

    async fn set_tenant_capability(
        &self,
        tenant_id: &str,
        capability_id: &str,
        enabled: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO public.capability_tenant_switches (tenant_id, capability_id, enabled, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (tenant_id, capability_id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                updated_at = NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(capability_id)
        .bind(enabled)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("set_tenant_capability: {e}")))?;
        Ok(())
    }
}
