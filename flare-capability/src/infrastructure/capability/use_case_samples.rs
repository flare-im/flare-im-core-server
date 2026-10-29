//! 参考用例（基础设施侧示例）：发消息前 Guard + 收信人解析。
//!
//! 依赖 [`CapabilityExtensionRegistry`]，供集成或其它服务对照；**非**应用层核心路径。

use flare_core_base::context::Ctx;
use serde_json::Value;

use crate::domain::capability::{
    CapabilityError, CapabilityInvokeMeta, ConversationKind, GuardDecision, PreSendEvaluateInput,
    PreSendGuardPipeline, RecipientResolveRequest, ResolveTrigger, Result,
};

use super::CapabilityExtensionRegistry;

async fn require_pre_send_allow(
    registry: &CapabilityExtensionRegistry,
    ctx: &Ctx,
    input: PreSendEvaluateInput,
) -> Result<()> {
    let pipeline = registry.pre_send().await;
    match pipeline.evaluate(ctx, &input).await? {
        GuardDecision::Allow => Ok(()),
        GuardDecision::Reject(r) => Err(CapabilityError::Rejected(Box::new(r))),
    }
}

pub struct SendMessageUseCaseExample {
    registry: CapabilityExtensionRegistry,
}

impl SendMessageUseCaseExample {
    pub fn new(registry: CapabilityExtensionRegistry) -> Self {
        Self { registry }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        request_id: &str,
        sender_user_id: &str,
        conversation_id: &str,
        conversation_kind: ConversationKind,
    ) -> Result<Vec<String>> {
        let meta = CapabilityInvokeMeta::new(tenant_id, request_id);
        let guard_input = PreSendEvaluateInput {
            meta: meta.clone(),
            sender_user_id: sender_user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            conversation_kind: conversation_kind.clone(),
            payload_type: Some("text".into()),
            payload: None,
            ext: Value::Null,
        };
        require_pre_send_allow(&self.registry, ctx, guard_input).await?;

        let resolve_req = RecipientResolveRequest {
            meta,
            conversation_id: conversation_id.to_string(),
            conversation_kind,
            trigger: ResolveTrigger::MessageDelivery,
            sender_user_id: sender_user_id.to_string(),
            direct_peer_user_id: None,
            ext: Value::Null,
        };
        let resolver_rt = self.registry.recipient().await;
        let resolved = resolver_rt.resolve(ctx, &resolve_req).await?;
        Ok(resolved.recipient_user_ids)
    }
}
