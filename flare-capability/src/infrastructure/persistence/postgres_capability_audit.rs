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
