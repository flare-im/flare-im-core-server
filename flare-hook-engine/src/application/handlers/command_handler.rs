//! # Hook命令处理器（编排层）
//!
//! 负责处理命令，调用领域服务

use std::sync::Arc;

use anyhow::Result;

use crate::domain::model::HookExecutionPlan;
use crate::domain::service::HookOrchestrationService;
use flare_im_core::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::context::{Context, Ctx};

/// Hook命令处理器（编排层）
pub struct HookCommandHandler {
    orchestration_service: Arc<HookOrchestrationService>,
}

impl HookCommandHandler {
    pub fn new(orchestration_service: Arc<HookOrchestrationService>) -> Self {
        Self {
            orchestration_service,
        }
    }

    /// 处理PreSend Hook命令
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

    /// 处理PostSend Hook命令
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

    /// 处理Delivery Hook命令
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

    /// 处理Recall Hook命令
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
