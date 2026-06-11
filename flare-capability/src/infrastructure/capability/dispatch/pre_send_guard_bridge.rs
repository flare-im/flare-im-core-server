//! PreSend Hook → Guard Pipeline 桥接：将 `HookInvocationContext` + `MessageDraft` 转为 `PreSendEvaluateInput`。

use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::HookInvocationContext;
use flare_im_hooks::MessageDraft;
use serde_json::{Value, json};

use crate::domain::capability::{
    CapabilityInvokeMeta, ConversationKind, GuardDecision, PreSendEvaluateInput,
    PreSendGuardPipeline, Result,
};
use crate::infrastructure::capability::CapabilityExtensionRegistry;

fn conversation_kind_from_label(label: &str) -> ConversationKind {
    match label.trim().to_ascii_lowercase().as_str() {
        "single" | "direct" | "1" => ConversationKind::Direct,
        "group" | "2" => ConversationKind::Group,
        other if !other.is_empty() && other != "unspecified" && other != "unknown" => {
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

pub fn build_pre_send_evaluate_input(
    hook_context: &HookInvocationContext,
    draft: &MessageDraft,
) -> PreSendEvaluateInput {
    let tenant_id = first_nonempty([
        hook_context.tenant_id.clone(),
        draft.metadata.get("tenant_id").cloned().unwrap_or_default(),
    ])
    .unwrap_or_else(|| "0".to_string());
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
    PreSendEvaluateInput {
        meta: CapabilityInvokeMeta::new(tenant_id, request_id),
        sender_user_id,
        conversation_id,
        conversation_kind: conversation_kind_from_label(&conversation_type),
        rtc_intent: false,
        payload_type,
        payload,
        ext,
    }
}

pub async fn evaluate_pre_send_guards(
    registry: &CapabilityExtensionRegistry,
    ctx: &Ctx,
    hook_context: &HookInvocationContext,
    draft: &MessageDraft,
) -> Result<GuardDecision> {
    let input = build_pre_send_evaluate_input(hook_context, draft);
    let pipeline = registry.pre_send().await;
    pipeline.evaluate(ctx, &input).await
}
