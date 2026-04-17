use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Ctx;
use crate::error::{ErrorBuilder, ErrorCode, Result};
use async_trait::async_trait;
use prost_types::Timestamp;
use tonic::IntoRequest;
use tonic::transport::{Channel, Endpoint};
use flare_grpc_proto::hooks::hook_extension_client::HookExtensionClient;
use flare_proto::common::Message as ProtoStorageMessage;
use flare_grpc_proto::{
    ProtoDeliveryHookRequest, ProtoDeliveryHookResponse, ProtoHookDeliveryEvent,
    ProtoHookInvocationContext, ProtoHookMessageDraft, ProtoHookMessageRecord,
    ProtoHookRecallEvent, ProtoPostSendHookRequest, ProtoPreSendHookRequest,
    ProtoRecallHookRequest, ProtoRecallHookResponse,
};

use super::super::config::HookDefinition;
use super::super::types::{
    DeliveryEvent, DeliveryHook, HookOutcome, MessageDraft, MessageRecord, PostSendHook,
    PreSendDecision, PreSendHook, RecallEvent, RecallHook,
};

#[derive(Clone)]
pub struct GrpcHookFactory;

impl GrpcHookFactory {
    pub fn new() -> Self {
        Self
    }

    fn build_channel(endpoint: &str) -> Result<Channel> {
        let endpoint = Endpoint::from_shared(endpoint.to_string()).map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "invalid gRPC hook endpoint")
                .details(err.to_string())
                .build_error()
        })?;
        Ok(endpoint.connect_lazy())
    }

    pub fn build_pre_send(
        &self,
        metadata: HashMap<String, String>,
        channel: Channel,
    ) -> Arc<dyn PreSendHook> {
        Arc::new(GrpcPreSendHook {
            channel,
            static_metadata: metadata,
        })
    }

    pub fn build_post_send(
        &self,
        metadata: HashMap<String, String>,
        channel: Channel,
    ) -> Arc<dyn PostSendHook> {
        Arc::new(GrpcPostSendHook {
            channel,
            static_metadata: metadata,
        })
    }

    pub fn build_delivery(
        &self,
        metadata: HashMap<String, String>,
        channel: Channel,
    ) -> Arc<dyn DeliveryHook> {
        Arc::new(GrpcDeliveryHook {
            channel,
            static_metadata: metadata,
        })
    }

    pub fn build_recall(
        &self,
        metadata: HashMap<String, String>,
        channel: Channel,
    ) -> Arc<dyn RecallHook> {
        Arc::new(GrpcRecallHook {
            channel,
            static_metadata: metadata,
        })
    }

    pub fn channel_for(&self, def: &HookDefinition) -> Result<Channel> {
        match &def.transport {
            super::super::config::HookTransportConfig::Grpc { endpoint, .. } => {
                Self::build_channel(endpoint)
            }
            _ => Err(
                ErrorBuilder::new(ErrorCode::ConfigurationError, "transport is not gRPC")
                    .details(format!("hook={}", def.name))
                    .build_error(),
            ),
        }
    }
}

#[derive(Clone)]
struct GrpcPreSendHook {
    channel: Channel,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl PreSendHook for GrpcPreSendHook {
    async fn handle(&self, ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        let mut client = HookExtensionClient::new(self.channel.clone());
        let mut request = ProtoPreSendHookRequest::default();
        request.context = Some(build_context(ctx, &self.static_metadata));
        request.draft = Some(build_draft(draft));

        let response = client.invoke_pre_send(request.into_request()).await;
        match response {
            Ok(resp) => {
                let inner = resp.into_inner();
                if !inner.allow {
                    let err = ErrorBuilder::new(ErrorCode::OperationFailed, "pre-send hook rejected")
                        .details("allow=false")
                        .build_error();
                    return PreSendDecision::Reject { error: err };
                }
                if let Some(draft_resp) = inner.draft {
                    apply_draft(draft, draft_resp);
                }
                PreSendDecision::Continue
            }
            Err(status) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "pre-send hook failed")
                    .details(status.to_string())
                    .build_error();
                PreSendDecision::Reject { error: err }
            }
        }
    }
}

#[derive(Clone)]
struct GrpcPostSendHook {
    channel: Channel,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl PostSendHook for GrpcPostSendHook {
    async fn handle(&self, ctx: &Ctx, record: &MessageRecord, draft: &MessageDraft) -> HookOutcome {
        let mut client = HookExtensionClient::new(self.channel.clone());
        let mut request = ProtoPostSendHookRequest::default();
        request.context = Some(build_context(ctx, &self.static_metadata));
        request.record = Some(build_record(record));
        request.draft = Some(build_draft(draft));

        match client.invoke_post_send(request).await {
            Ok(resp) => {
                let inner = resp.into_inner();
                if inner.success {
                    HookOutcome::Completed
                } else {
                    HookOutcome::Failed(
                        ErrorBuilder::new(ErrorCode::OperationFailed, "post-send hook reported failure")
                            .build_error(),
                    )
                }
            }
            Err(status) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "post-send hook failed")
                    .details(status.to_string())
                    .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

#[derive(Clone)]
struct GrpcDeliveryHook {
    channel: Channel,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl DeliveryHook for GrpcDeliveryHook {
    async fn handle(&self, ctx: &Ctx, event: &DeliveryEvent) -> HookOutcome {
        let mut client = HookExtensionClient::new(self.channel.clone());
        let mut request = ProtoDeliveryHookRequest::default();
        request.context = Some(build_context(ctx, &self.static_metadata));
        request.event = Some(build_delivery_event(event));

        match client.notify_delivery(request).await {
            Ok(resp) => {
                let inner: ProtoDeliveryHookResponse = resp.into_inner();
                if inner.success {
                    HookOutcome::Completed
                } else {
                    HookOutcome::Failed(
                        ErrorBuilder::new(ErrorCode::OperationFailed, "delivery hook reported failure")
                            .build_error(),
                    )
                }
            }
            Err(status) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "delivery hook failed")
                    .details(status.to_string())
                    .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

#[derive(Clone)]
struct GrpcRecallHook {
    channel: Channel,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl RecallHook for GrpcRecallHook {
    async fn handle(&self, ctx: &Ctx, event: &RecallEvent) -> HookOutcome {
        let mut client = HookExtensionClient::new(self.channel.clone());
        let mut request = ProtoRecallHookRequest::default();
        request.context = Some(build_context(ctx, &self.static_metadata));
        request.event = Some(build_recall_event(event));

        match client.notify_recall(request).await {
            Ok(resp) => {
                let inner: ProtoRecallHookResponse = resp.into_inner();
                if inner.allow {
                    HookOutcome::Completed
                } else {
                    HookOutcome::Failed(
                        ErrorBuilder::new(ErrorCode::OperationFailed, "recall hook rejected")
                            .build_error(),
                    )
                }
            }
            Err(status) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "recall hook failed")
                    .details(status.to_string())
                    .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

fn build_context(
    ctx: &Ctx,
    static_metadata: &HashMap<String, String>,
) -> ProtoHookInvocationContext {
    // 从 Context 中提取 Hook 特定的数据
    // 注意：这里需要访问 HookContextData，但它在 flare-capability 服务 crate 中
    // 为了简化，我们使用 Context 的基本字段
    use crate::hooks::hook_context_data::get_hook_context_data;

    let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
    let corridor = hook_data
        .attributes
        .get("corridor")
        .cloned()
        .or_else(|| hook_data.conversation_type.clone())
        .unwrap_or_else(|| "messaging".to_string());

    let mut attributes = hook_data.attributes.clone();
    for (key, value) in static_metadata {
        attributes
            .entry(key.clone())
            .or_insert_with(|| value.clone());
    }
    for (key, value) in &hook_data.request_metadata {
        attributes
            .entry(format!("request.{key}"))
            .or_insert_with(|| value.clone());
    }

    ProtoHookInvocationContext {
        conversation_id: hook_data.conversation_id.clone().unwrap_or_default(),
        conversation_type: hook_data.conversation_type.clone().unwrap_or_default(),
        corridor,
        tags: hook_data.tags.clone(),
        attributes,
    }
}

fn build_draft(draft: &MessageDraft) -> ProtoHookMessageDraft {
    ProtoHookMessageDraft {
        message_id: draft.message_id.clone().unwrap_or_default(),
        client_message_id: draft.client_message_id.clone().unwrap_or_default(),
        conversation_id: draft.conversation_id.clone().unwrap_or_default(),
        payload: draft.payload.clone(),
        headers: draft.headers.clone(),
        metadata: draft.metadata.clone(),
    }
}

fn apply_draft(target: &mut MessageDraft, source: ProtoHookMessageDraft) {
    if !source.message_id.is_empty() {
        target.message_id = Some(source.message_id);
    }
    if !source.client_message_id.is_empty() {
        target.client_message_id = Some(source.client_message_id);
    }
    if !source.conversation_id.is_empty() {
        target.conversation_id = Some(source.conversation_id);
    }
    target.payload = source.payload;
    target.headers = source.headers;
    target.metadata = source.metadata;
}

fn build_record(record: &MessageRecord) -> ProtoHookMessageRecord {
    let persisted_ts = system_time_to_timestamp(record.persisted_at);

    let mut message = ProtoStorageMessage::default();
    message.server_id = record.message_id.clone();
    message.conversation_id = record.conversation_id.clone();
    message.sender_id = record.sender_id.clone();
    message.conversation_type = record
        .conversation_type
        .as_deref()
        .map(|t| match t.to_ascii_lowercase().as_str() {
            "single" | "conversation_type_single" | "1" => {
                flare_proto::common::ConversationType::Single as i32
            }
            "group" | "conversation_type_group" | "2" => {
                flare_proto::common::ConversationType::Group as i32
            }
            "channel" | "conversation_type_channel" | "3" => {
                // Channel is treated as Group for now
                flare_proto::common::ConversationType::Group as i32
            }
            "ai" | "conversation_type_ai" | "4" => {
                flare_proto::common::ConversationType::Ai as i32
            }
            "customer" | "conversation_type_customer" | "5" => {
                flare_proto::common::ConversationType::Customer as i32
            }
            "system" | "conversation_type_system" | "6" => {
                flare_proto::common::ConversationType::System as i32
            }
            "temp" | "conversation_type_temp" | "7" => {
                flare_proto::common::ConversationType::Temp as i32
            }
            _ => flare_proto::common::ConversationType::Unspecified as i32,
        })
        .unwrap_or(flare_proto::common::ConversationType::Unspecified as i32);
    message.extra = record.metadata.clone();
    message.timestamp = Some(persisted_ts.clone());
    message.message_type = record
        .message_type
        .as_deref()
        .map(|kind| match kind.to_ascii_lowercase().as_str() {
            "text" | "message_type_text" => flare_proto::common::MessageType::Text as i32,
            "image" => flare_proto::common::MessageType::Image as i32,
            "video" => flare_proto::common::MessageType::Video as i32,
            "audio" => flare_proto::common::MessageType::Audio as i32,
            "file" => flare_proto::common::MessageType::File as i32,
            "location" => flare_proto::common::MessageType::Location as i32,
            "card" => flare_proto::common::MessageType::Card as i32,
            "notification" => flare_proto::common::MessageType::Notification as i32,
            "binary" | "attachment" | "message_type_binary" => {
                flare_proto::common::MessageType::Custom as i32
            } // 二进制消息映射到 Custom
            "custom" | "message_type_custom" => flare_proto::common::MessageType::Custom as i32,
            _ => flare_proto::common::MessageType::Unspecified as i32,
        })
        .unwrap_or(flare_proto::common::MessageType::Unspecified as i32);
    if let Some(message_type) = &record.message_type {
        message
            .extra
            .entry("message_type".into())
            .or_insert_with(|| message_type.clone());
    }

    if let Some(client_id) = record.client_message_id.as_ref() {
        message
            .extra
            .entry("client_message_id".into())
            .or_insert_with(|| client_id.clone());
    }

    ProtoHookMessageRecord {
        message: Some(message),
        persisted_at: Some(persisted_ts),
        metadata: record.metadata.clone(),
    }
}

fn build_delivery_event(event: &DeliveryEvent) -> ProtoHookDeliveryEvent {
    ProtoHookDeliveryEvent {
        message_id: event.message_id.clone(),
        user_id: event.user_id.clone(),
        channel: event.channel.clone(),
        delivered_at: Some(system_time_to_timestamp(event.delivered_at)),
        metadata: event.metadata.clone(),
    }
}

fn build_recall_event(event: &RecallEvent) -> ProtoHookRecallEvent {
    ProtoHookRecallEvent {
        message_id: event.message_id.clone(),
        operator_id: event.operator_id.clone(),
        recalled_at: Some(system_time_to_timestamp(event.recalled_at)),
        metadata: event.metadata.clone(),
    }
}

fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}
