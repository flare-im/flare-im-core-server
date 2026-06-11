//! Hook **Command** 编排：将用例委托给领域 [`crate::domain::service::HookOrchestrationService`]。

use std::sync::Arc;

use flare_im_hooks::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::context::{Context, Ctx};
use flare_server_core::error::Result;

use crate::domain::model::HookExecutionPlan;
use crate::domain::service::HookOrchestrationService;

/// Hook 命令处理器（应用层仅做上下文适配与委托）。
pub struct HookCommandHandler {
    orchestration_service: Arc<HookOrchestrationService>,
}

impl HookCommandHandler {
    pub fn new(orchestration_service: Arc<HookOrchestrationService>) -> Self {
        Self {
            orchestration_service,
        }
    }

    /// 处理 PreSend Hook 命令
    pub async fn handle_pre_send(
        &self,
        ctx: &Context,
        draft: &mut MessageDraft,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<PreSendDecision> {
        let ctx_arc: Ctx = Arc::new(ctx.clone());
        self.orchestration_service
            .execute_pre_send(&ctx_arc, draft, hooks)
            .await
    }

    /// 处理 PostSend Hook 命令
    pub async fn handle_post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<()> {
        let ctx_arc: Ctx = Arc::new(ctx.clone());
        self.orchestration_service
            .execute_post_send(&ctx_arc, record, draft, hooks)
            .await
    }

    /// 处理 Delivery Hook 命令
    pub async fn handle_delivery(
        &self,
        ctx: &Context,
        event: &DeliveryEvent,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<()> {
        let ctx_arc: Ctx = Arc::new(ctx.clone());
        self.orchestration_service
            .execute_delivery(&ctx_arc, event, hooks)
            .await
    }

    /// 处理 Recall Hook 命令
    pub async fn handle_recall(
        &self,
        ctx: &Context,
        event: &RecallEvent,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<PreSendDecision> {
        let ctx_arc: Ctx = Arc::new(ctx.clone());
        self.orchestration_service
            .execute_recall(&ctx_arc, event, hooks)
            .await
    }
}
