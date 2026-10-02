use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_server_core::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserSyncCompensationKind {
    EagerUserChanges,
    ConversationVersionBump,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSyncCompensationTask {
    pub task_id: String,
    pub tenant_id: String,
    pub kind: UserSyncCompensationKind,
    pub user_ids: Vec<String>,
    pub conversation_id: String,
    pub max_conversation_seq: u64,
    pub occurred_at_ms: i64,
    pub due_at_ms: i64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl UserSyncCompensationTask {
    pub fn due_now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    pub fn eager_user_changes(
        ctx: &Ctx,
        user_ids: &[String],
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
        due_at_ms: i64,
    ) -> Option<Self> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return None;
        }
        let user_ids = normalized_user_ids(user_ids);
        if user_ids.is_empty() {
            return None;
        }
        Some(Self::new(
            tenant_id(ctx, conversation_id)?,
            UserSyncCompensationKind::EagerUserChanges,
            user_ids,
            conversation_id.to_string(),
            max_conversation_seq,
            occurred_at_ms,
            due_at_ms,
        ))
    }

    pub fn conversation_version_bump(
        ctx: &Ctx,
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
        due_at_ms: i64,
    ) -> Option<Self> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return None;
        }
        Some(Self::new(
            tenant_id(ctx, conversation_id)?,
            UserSyncCompensationKind::ConversationVersionBump,
            Vec::new(),
            conversation_id.to_string(),
            max_conversation_seq,
            occurred_at_ms,
            due_at_ms,
        ))
    }

    fn new(
        tenant_id: String,
        kind: UserSyncCompensationKind,
        user_ids: Vec<String>,
        conversation_id: String,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
        due_at_ms: i64,
    ) -> Self {
        let kind_label = match kind {
            UserSyncCompensationKind::EagerUserChanges => "eager",
            UserSyncCompensationKind::ConversationVersionBump => "conversation_version",
        };
        let task_id = format!("{tenant_id}:{kind_label}:{conversation_id}:{max_conversation_seq}");
        Self {
            task_id,
            tenant_id,
            kind,
            user_ids,
            conversation_id,
            max_conversation_seq,
            occurred_at_ms,
            due_at_ms,
            attempts: 0,
            last_error: None,
        }
    }
}

#[async_trait]
pub trait UserSyncCompensationRepository: Send + Sync {
    async fn enqueue(&self, task: UserSyncCompensationTask) -> Result<()>;

    async fn claim_due(&self, limit: usize) -> Result<Vec<UserSyncCompensationTask>>;

    async fn mark_completed(&self, task_id: &str) -> Result<()>;

    async fn mark_failed(
        &self,
        task: UserSyncCompensationTask,
        error: &str,
        retry_after_ms: i64,
    ) -> Result<()>;
}

/// 补偿任务的租户：缺租户时不建任务（记 ERROR），不再落到租户 "0"（显式的 "0" 照常）。
fn tenant_id(ctx: &Ctx, conversation_id: &str) -> Option<String> {
    let tenant_id = normalize_optional_tenant_id(ctx.tenant_id());
    if tenant_id.is_none() {
        tracing::error!(
            conversation_id = %conversation_id,
            "user_sync compensation skipped: the context has no tenant"
        );
    }
    tenant_id
}

fn normalized_user_ids(user_ids: &[String]) -> Vec<String> {
    user_ids
        .iter()
        .map(|user_id| user_id.trim())
        .filter(|user_id| !user_id.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::Context;
    use std::sync::Arc;

    /// 缺租户的上下文不建补偿任务，不再落到租户 "0"（W11-T）；显式租户 "0" 照常。
    #[test]
    fn compensation_tasks_require_a_context_tenant() {
        let users = vec!["user-a".to_string()];
        for missing in [
            Context::with_request_id("req"),
            Context::with_request_id("req").with_tenant_id(""),
        ] {
            let ctx: Ctx = Arc::new(missing);
            assert!(
                UserSyncCompensationTask::eager_user_changes(&ctx, &users, "conv-a", 1, 1, 1)
                    .is_none()
            );
            assert!(
                UserSyncCompensationTask::conversation_version_bump(&ctx, "conv-a", 1, 1, 1)
                    .is_none()
            );
        }
        for tenant in ["tenant-a", "0"] {
            let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id(tenant));
            let eager =
                UserSyncCompensationTask::eager_user_changes(&ctx, &users, "conv-a", 1, 1, 1)
                    .expect("task");
            assert_eq!(eager.tenant_id, tenant);
            let bump = UserSyncCompensationTask::conversation_version_bump(&ctx, "conv-a", 1, 1, 1)
                .expect("task");
            assert_eq!(bump.tenant_id, tenant);
            assert!(bump.task_id.starts_with(&format!("{tenant}:")));
        }
    }
}
