//! 能力策略变更审计（生产排障 / 合规；写入失败仅打日志，不阻断 RPC）。

use std::sync::Arc;

use serde_json::Value;
use sqlx::PgPool;

/// 写入 `capability_audit_log`（Grant / Revoke / SetTenantSwitch）。
#[derive(Debug, Clone)]
pub struct PostgresCapabilityAuditLog {
    pool: Arc<PgPool>,
}

impl PostgresCapabilityAuditLog {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// The durable Staff effect evidence of a governance call that changes no Core table (a
    /// plugin admin call): one `core_audit_outbox` row in its own transaction. Must run inside the
    /// Staff scope (`StaffAudit::staff_audit`); outside it nothing is written.
    pub async fn record_staff_effect(
        &self,
        app: &str,
        aggregate: &'static str,
        id: &str,
        action: &'static str,
    ) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;
        flare_im_service_kit::audit_effects::durable::record(&mut tx, app, aggregate, id, action)
            .await?;
        tx.commit().await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_policy_event(
        &self,
        action: &str,
        tenant_id: &str,
        actor_id: Option<&str>,
        target_user_id: Option<&str>,
        capability_id: Option<&str>,
        detail: Value,
        trace_id: Option<&str>,
    ) {
        let res = sqlx::query(
            r#"
            INSERT INTO public.capability_audit_log (
                action, tenant_id, actor_id, target_user_id, capability_id, detail, trace_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(action)
        .bind(tenant_id)
        .bind(actor_id)
        .bind(target_user_id)
        .bind(capability_id)
        .bind(detail)
        .bind(trace_id)
        .execute(self.pool.as_ref())
        .await;

        if let Err(e) = res {
            tracing::error!(error = %e, action, "capability_audit_log insert failed");
        }
    }
}
