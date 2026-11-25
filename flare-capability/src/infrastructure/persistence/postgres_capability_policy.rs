//! PostgreSQL 实现的能力策略（`capability_*` 表，与 `deploy/init.sql` 对齐）。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::error::AnyhowContext as _;
use sqlx::PgPool;

use crate::domain::capability::{
    CapabilityError, CapabilityPolicyBackend, Result, TenantCapabilitySwitch,
};

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
    /// 典型误配：在库 A 执行了 `deploy/init.sql`，但 `DATABASE_URL` 指向库 B；或未设置 `DATABASE_URL` 时
    /// 二进制默认连 `localhost:25432`，而脚本在 `5432` 执行。
    pub async fn assert_public_capability_schema(
        pool: &PgPool,
    ) -> flare_server_core::error::Result<()> {
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

        let row: (String, Option<String>) =
            sqlx::query_as("SELECT current_database(), current_setting('search_path', true)")
                .fetch_one(pool)
                .await
                .unwrap_or_else(|_| ("(unknown)".to_string(), None));

        Err(flare_server_core::error::FlareError::system(format!(
            "missing table public.capability_service_settings on database {:?} (search_path={:?}). \
             Run flare-core-server/deploy/init.sql Hook+Capability section on **this** database. \
             If you already ran init elsewhere, align DATABASE_URL host/port/dbname with that instance; \
             unset DATABASE_URL defaults to postgresql://*:*@localhost:25432/flare2 (see config base.toml [postgres.media]) in flare-capability cmd.",
            row.0, row.1
        )))
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

    /// 取租户开关的**原值**：`None` = 从未安装，`Some(false)` = 装过但停用。
    ///
    /// 与 `tenant_capability_disabled` 的区别正是这两者的区分 —— 后者把
    /// 「没装」和「装了没停用」都当成「不禁止」，那是旧语义。租户模型必须
    /// 分得清，否则「安装」这个动作没有意义。
    async fn tenant_capability_switch(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<Option<bool>> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let row: Option<(bool,)> = sqlx::query_as(
            r#"
            SELECT enabled FROM public.capability_tenant_switches
            WHERE tenant_id = $1 AND capability_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(capability_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(e.to_string()))?;
        Ok(row.map(|(enabled,)| enabled))
    }

    /// Whether a tenant switch that is off covers `capability_id`: its own, or a `<prefix>.*`
    /// switch whose prefix (with the dot) starts it (see
    /// `CapabilityPolicyBackend::ensure_not_suspended`).
    async fn tenant_capability_suspended(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<bool> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT 1::bigint FROM public.capability_tenant_switches
            WHERE tenant_id = $1 AND enabled = FALSE
              AND (capability_id = $2
                   OR (right(capability_id, 2) = '.*'
                       AND char_length(capability_id) > 2
                       AND starts_with($2, left(capability_id, -1))))
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(capability_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("capability tenant switch: {e}")))?;
        Ok(row.is_some())
    }
}

#[async_trait]
impl CapabilityPolicyBackend for PostgresCapabilityPolicy {
    async fn ensure_tenant_capability_enabled(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if !self.load_global_enabled().await? {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        match self
            .tenant_capability_switch(&tenant_id, capability_id)
            .await?
        {
            Some(true) => Ok(()),
            Some(false) => Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} is disabled for tenant {tenant_id}"
            ))),
            None => Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} is not installed for tenant {tenant_id}"
            ))),
        }
    }

    async fn ensure_not_suspended(&self, tenant_id: &str, capability_id: &str) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if !self.load_global_enabled().await? {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        if self
            .tenant_capability_suspended(&tenant_id, capability_id)
            .await?
        {
            return Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} disabled for tenant {tenant_id}"
            )));
        }
        Ok(())
    }

    async fn set_tenant_capability(
        &self,
        tenant_id: &str,
        capability_id: &str,
        enabled: bool,
    ) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let mut tx = self.pool.begin().await.map_err(write_error)?;
        sqlx::query(
            r#"
            INSERT INTO public.capability_tenant_switches (tenant_id, capability_id, enabled, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (tenant_id, capability_id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                updated_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(capability_id)
        .bind(enabled)
        .execute(&mut *tx)
        .await
        .map_err(write_error)?;
        flare_im_service_kit::audit_effects::durable::record(
            &mut tx,
            &tenant_id,
            "capability_switch",
            capability_id,
            if enabled { "enable" } else { "disable" },
        )
        .await
        .map_err(write_error)?;
        tx.commit().await.map_err(write_error)?;
        Ok(())
    }

    async fn list_tenant_switches(&self, tenant_id: &str) -> Result<Vec<TenantCapabilitySwitch>> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let rows: Vec<(String, bool, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT capability_id, enabled, updated_at
            FROM public.capability_tenant_switches
            WHERE tenant_id = $1
            ORDER BY capability_id
            "#,
        )
        .bind(&tenant_id)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| CapabilityError::System(format!("list_tenant_switches: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(capability_id, enabled, updated_at)| TenantCapabilitySwitch {
                    tenant_id: tenant_id.clone(),
                    capability_id,
                    enabled,
                    updated_at: Some(updated_at),
                },
            )
            .collect())
    }
}

fn write_error(_: sqlx::Error) -> CapabilityError {
    CapabilityError::System("capability write transaction unavailable".into())
}
