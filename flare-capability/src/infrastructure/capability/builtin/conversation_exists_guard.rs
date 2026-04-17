//! 内建：会话存在性 Guard。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;

use crate::domain::capability::{
    GuardDecision, GuardRejection, PreSendEvaluateInput, PreSendGuard, Result,
};

#[async_trait]
pub trait ConversationExistenceChecker: Send + Sync {
    async fn conversation_exists(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<bool>;
}

pub struct ConversationExistsGuard {
    id: String,
    checker: Option<Arc<dyn ConversationExistenceChecker>>,
}

impl ConversationExistsGuard {
    pub fn new(checker: Option<Arc<dyn ConversationExistenceChecker>>) -> Self {
        Self {
            id: "builtin.conversation_exists".into(),
            checker,
        }
    }
}

#[async_trait]
impl PreSendGuard for ConversationExistsGuard {
    fn id(&self) -> &str {
        &self.id
    }

    async fn evaluate(&self, ctx: &Ctx, input: &PreSendEvaluateInput) -> Result<GuardDecision> {
        let tid = input.meta.tenant_id.as_str();
        let cid = input.conversation_id.as_str();
        if cid.trim().is_empty() {
            return Ok(GuardDecision::Reject(GuardRejection {
                guard_id: self.id.clone(),
                code: "conversation_id_empty".into(),
                message: "conversation_id 不能为空".into(),
                tenant_id: Some(tid.to_string()),
                ext: serde_json::Value::Null,
            }));
        }
        if let Some(chk) = &self.checker {
            let ok = chk.conversation_exists(ctx, tid, cid).await?;
            if !ok {
                return Ok(GuardDecision::Reject(GuardRejection {
                    guard_id: self.id.clone(),
                    code: "conversation_not_found".into(),
                    message: "会话不存在或无权访问".into(),
                    tenant_id: Some(tid.to_string()),
                    ext: serde_json::Value::Null,
                }));
            }
        }
        Ok(GuardDecision::Allow)
    }
}

#[allow(dead_code)]
pub struct AlwaysPresentConversationChecker;

#[async_trait]
impl ConversationExistenceChecker for AlwaysPresentConversationChecker {
    async fn conversation_exists(
        &self,
        _ctx: &Ctx,
        _tenant_id: &str,
        _conversation_id: &str,
    ) -> Result<bool> {
        Ok(true)
    }
}
