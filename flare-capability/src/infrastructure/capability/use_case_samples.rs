//! 参考用例（基础设施侧示例）：发消息前 Guard + 收信人解析；起呼前 Guard + RTC + 解析。
//!
//! 依赖 [`CapabilityExtensionRegistry`]，供集成或其它服务对照；**非**应用层核心路径。

use flare_core_base::context::Ctx;
use serde_json::Value;

use crate::domain::capability::{
    CapabilityError, CapabilityInvokeMeta, ConversationKind, CreateCallRequest, GuardDecision,
    PreSendEvaluateInput, PreSendGuardPipeline, RecipientResolveRequest, ResolveTrigger, Result,
    RtcCapability,
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
        GuardDecision::Reject(r) => Err(CapabilityError::Rejected(r)),
    }
}

pub struct SendMessageUseCaseExample {
    registry: CapabilityExtensionRegistry,
}

impl SendMessageUseCaseExample {
    pub fn new(registry: CapabilityExtensionRegistry) -> Self {
        Self { registry }
    }

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
            rtc_intent: false,
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

pub struct StartCallUseCaseExample {
    registry: CapabilityExtensionRegistry,
}

impl StartCallUseCaseExample {
    pub fn new(registry: CapabilityExtensionRegistry) -> Self {
        Self { registry }
    }

    pub async fn execute(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        request_id: &str,
        initiator_user_id: &str,
        conversation_id: &str,
        conversation_kind: ConversationKind,
        direct_peer_user_id: Option<String>,
    ) -> Result<(String, Vec<String>)> {
        let meta = CapabilityInvokeMeta::new(tenant_id, request_id);
        let guard_input = PreSendEvaluateInput {
            meta: meta.clone(),
            sender_user_id: initiator_user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            conversation_kind: conversation_kind.clone(),
            rtc_intent: true,
            payload_type: Some("rtc.call".into()),
            payload: None,
            ext: Value::Null,
        };
        require_pre_send_allow(&self.registry, ctx, guard_input).await?;

        let rtc = self.registry.rtc_router().await;
        let create_req = CreateCallRequest {
            tenant_id: tenant_id.to_string(),
            request_id: request_id.to_string(),
            conversation_id: conversation_id.to_string(),
            initiator_user_id: initiator_user_id.to_string(),
            media: Some("video".into()),
            ext: Value::Null,
        };
        let created = rtc.create_call(ctx, &create_req).await?;

        let resolve_req = RecipientResolveRequest {
            meta,
            conversation_id: conversation_id.to_string(),
            conversation_kind,
            trigger: ResolveTrigger::RtcInvite,
            sender_user_id: initiator_user_id.to_string(),
            direct_peer_user_id,
            ext: Value::Null,
        };
        let resolver_rt = self.registry.recipient().await;
        let resolved = resolver_rt.resolve(ctx, &resolve_req).await?;
        Ok((created.call_id, resolved.recipient_user_ids))
    }
}
