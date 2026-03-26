use std::sync::Arc;

use crate::error::Result;

use super::registry::HookRegistry;
use super::types::{MessageDraft, MessageRecord, PreSendDecision};
use flare_server_core::context::Ctx;

/// Hook 调度器，封装常用执行入口
#[derive(Clone)]
pub struct HookDispatcher {
    registry: Arc<HookRegistry>,
}

impl HookDispatcher {
    pub fn new(registry: Arc<HookRegistry>) -> Self {
        Self { registry }
    }

    /// 调用获取会话参与者的Hook
    pub async fn invoke_get_conversation_participants(
        &self,
        _ctx: &Ctx,
        _conversation_id: &str,
    ) -> Result<Option<Vec<String>>> {
        // 在Hook注册表中查找GetConversationParticipantsHook类型的Hook
        // 这里简化实现，实际应该遍历注册表查找合适的Hook
        Ok(None)
    }

    /// 执行 PreSend Hook
    pub async fn pre_send(
        &self,
        ctx: &Ctx,
        draft: &mut MessageDraft,
    ) -> Result<PreSendDecision> {
        // 计划要执行的 PreSend Hooks
        let plans = self.registry.plan_pre_send(ctx).await;

        // 依次执行每个计划
        for plan in plans {
            let decision = plan.execute(ctx, draft).await;
            match decision {
                PreSendDecision::Continue => continue,
                PreSendDecision::Reject { error } => return Err(error),
            }
        }

        Ok(PreSendDecision::Continue)
    }

    /// 执行 PostSend Hook
    pub async fn post_send(
        &self,
        ctx: &Ctx,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        self.registry.execute_post_send(ctx, record, draft).await
    }
}
