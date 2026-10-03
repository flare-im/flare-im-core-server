//! PreSend Hook → Guard Pipeline 桥接：将 `HookInvocationContext` + `MessageDraft` 转为 `PreSendEvaluateInput`。

use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::HookInvocationContext;
use flare_im_hooks::MessageDraft;
use serde_json::{Value, json};

use crate::domain::capability::{
    CapabilityError, CapabilityInvokeMeta, ConversationKind, GuardDecision, PreSendEvaluateInput,
    PreSendGuardPipeline, Result,
};
use crate::infrastructure::capability::CapabilityExtensionRegistry;

fn conversation_kind_from_label(label: &str) -> ConversationKind {
    match label.trim().to_ascii_lowercase().as_str() {
        "single" => ConversationKind::Direct,
        "group" => ConversationKind::Group,
        other if other.chars().all(|c| c.is_ascii_digit()) => {
            ConversationKind::Custom("unknown".to_string())
        }
        other
            if !other.is_empty()
                && other != "unspecified"
                && other != "unknown"
                && !other.starts_with("conversation_type_") =>
        {
            ConversationKind::Custom(other.to_string())
        }
        _ => ConversationKind::Custom("unknown".to_string()),
    }
}

fn first_nonempty(values: impl IntoIterator<Item = String>) -> Option<String> {
    values
        .into_iter()
        .map(|v| v.trim().to_string())
        .find(|v| !v.is_empty())
}

/// 由 Hook 调用上下文与草稿组装 PreSend 守卫的输入。
///
/// 租户只认 Hook 上下文（HookPlugin 换成了签名租户，见 `HookCaller::signed_invocation_context`）：
/// 缺租户时返回错误。以前缺租户时取草稿元数据（即消息 attributes）里的 `tenant_id`，再兜底成
/// `"0"`，客户端写进消息的租户声明就能决定守卫按哪个租户判定（W11-H 余项）。
pub fn build_pre_send_evaluate_input(
    hook_context: &HookInvocationContext,
    draft: &MessageDraft,
) -> Result<PreSendEvaluateInput> {
    let tenant_id = first_nonempty([hook_context.tenant_id.clone()]).ok_or_else(|| {
        CapabilityError::PolicyDenied("pre-send guard requires the hook context tenant".into())
    })?;
    let request_id = first_nonempty([
        hook_context.request_id.clone(),
        draft
            .metadata
            .get("x-request-id")
            .cloned()
            .unwrap_or_default(),
    ])
    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let sender_user_id = first_nonempty([
        hook_context.operator_user_id.clone(),
        draft.metadata.get("sender_id").cloned().unwrap_or_default(),
    ])
    .unwrap_or_default();
    let conversation_id = first_nonempty([
        draft.conversation_id.clone().unwrap_or_default(),
        hook_context.conversation_id.clone(),
        draft
            .metadata
            .get("conversation_id")
            .cloned()
            .unwrap_or_default(),
    ])
    .unwrap_or_default();
    let conversation_type = first_nonempty([
        hook_context.conversation_type.clone(),
        draft
            .metadata
            .get("conversation_type")
            .cloned()
            .unwrap_or_default(),
    ])
    .unwrap_or_default();
    let peer_user_id = first_nonempty([
        hook_context
            .attributes
            .get("receiver_id")
            .cloned()
            .unwrap_or_default(),
        draft
            .metadata
            .get("receiver_id")
            .cloned()
            .unwrap_or_default(),
        hook_context
            .tags
            .get("receiver_id")
            .cloned()
            .unwrap_or_default(),
    ]);
    let payload_type = draft
        .metadata
        .get("message_type")
        .or_else(|| draft.metadata.get("content_type"))
        .cloned();
    let group_id = first_nonempty([
        hook_context
            .attributes
            .get("group_id")
            .cloned()
            .unwrap_or_default(),
        draft.metadata.get("group_id").cloned().unwrap_or_default(),
        draft
            .metadata
            .get("channel_id")
            .cloned()
            .unwrap_or_default(),
    ]);
    let mention_all = first_nonempty([
        draft
            .metadata
            .get("mention_all")
            .cloned()
            .unwrap_or_default(),
        draft
            .headers
            .get("mention_all")
            .cloned()
            .unwrap_or_default(),
    ]);
    let mut ext = Value::Object(Default::default());
    if let Some(peer) = peer_user_id {
        ext["direct_peer_user_id"] = json!(peer);
        ext["peer_user_id"] = json!(peer);
    }
    if let Some(gid) = group_id {
        ext["group_id"] = json!(gid);
        ext["channel_id"] = json!(gid);
    }
    if let Some(flag) = mention_all {
        ext["mention_all"] = json!(flag);
    }
    let payload = if draft.payload.is_empty() {
        None
    } else {
        Some(draft.payload.clone())
    };
    Ok(PreSendEvaluateInput {
        meta: CapabilityInvokeMeta::new(tenant_id, request_id),
        sender_user_id,
        conversation_id,
        conversation_kind: conversation_kind_from_label(&conversation_type),
        rtc_intent: false,
        payload_type,
        payload,
        ext,
    })
}

pub async fn evaluate_pre_send_guards(
    registry: &CapabilityExtensionRegistry,
    ctx: &Ctx,
    hook_context: &HookInvocationContext,
    draft: &MessageDraft,
) -> Result<GuardDecision> {
    let input = build_pre_send_evaluate_input(hook_context, draft)?;
    let pipeline = registry.pre_send().await;
    pipeline.evaluate(ctx, &input).await
}

#[cfg(test)]
mod tests {
    use super::{build_pre_send_evaluate_input, conversation_kind_from_label};
    use crate::domain::capability::ConversationKind;
    use flare_grpc_proto::capability::HookInvocationContext;
    use flare_im_hooks::MessageDraft;

    fn draft_claiming(tenant: &str) -> MessageDraft {
        let mut draft = MessageDraft::new(b"hello".to_vec());
        draft.metadata("tenant_id", tenant);
        draft
    }

    /// 守卫的租户只取 Hook 上下文；消息里自报的租户既不能顶替，也不能在上下文缺租户时补上。
    #[test]
    fn the_guard_tenant_comes_only_from_the_hook_context() {
        let context = HookInvocationContext {
            tenant_id: "tenant-a".into(),
            operator_user_id: "u1".into(),
            request_id: "req-1".into(),
            ..Default::default()
        };
        let input = build_pre_send_evaluate_input(&context, &draft_claiming("tenant-b")).unwrap();
        assert_eq!(input.meta.tenant_id, "tenant-a");

        for tenant in ["", "  "] {
            let context = HookInvocationContext {
                tenant_id: tenant.into(),
                ..context.clone()
            };
            assert!(
                build_pre_send_evaluate_input(&context, &draft_claiming("tenant-b")).is_err(),
                "a draft-declared tenant filled in for {tenant:?}"
            );
            assert!(
                build_pre_send_evaluate_input(&context, &MessageDraft::new(Vec::new())).is_err(),
                "a missing tenant became a default tenant"
            );
        }
    }

    #[test]
    fn conversation_kind_from_label_accepts_only_canonical_conversation_type_names() {
        assert_eq!(
            conversation_kind_from_label("single"),
            ConversationKind::Direct
        );
        assert_eq!(
            conversation_kind_from_label("group"),
            ConversationKind::Group
        );
        assert_eq!(
            conversation_kind_from_label("1"),
            ConversationKind::Custom("unknown".to_string())
        );
        assert_eq!(
            conversation_kind_from_label("direct"),
            ConversationKind::Custom("direct".to_string())
        );
        assert_eq!(
            conversation_kind_from_label("conversation_type_single"),
            ConversationKind::Custom("unknown".to_string())
        );
    }
}
