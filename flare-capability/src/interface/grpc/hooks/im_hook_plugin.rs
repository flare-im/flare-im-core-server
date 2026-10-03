//! `HookPlugin` gRPC：IM 主链与周边事件（`flare.capability.v1.HookPlugin.Call`）。

use flare_grpc_proto::capability::hook_plugin_server::HookPlugin;
use flare_grpc_proto::capability::{
    ConversationLifecycleHookRequest, ConversationLifecycleHookResponse,
    ConversationMemberHookRequest, ConversationMemberHookResponse, CustomHookRequest,
    CustomHookResponse, DeliveryHookRequest, DeliveryHookResponse, GenericRequest, GenericResponse,
    HookDeliveryEvent, HookMessageRecord, HookRecallEvent, MessageReactionHookRequest,
    MessageReactionHookResponse, MessageReadHookRequest, MessageReadHookResponse,
    PostSendHookRequest, PostSendHookResponse, PreSendHookRequest, PreSendHookResponse,
    PresenceHookRequest, PresenceHookResponse, PushDeliveryHookRequest, PushDeliveryHookResponse,
    PushPostSendHookRequest, PushPostSendHookResponse, PushPreSendHookRequest,
    PushPreSendHookResponse, RecallHookRequest, RecallHookResponse, UserLoginHookRequest,
    UserLoginHookResponse, UserLogoutHookRequest, UserLogoutHookResponse, UserOfflineHookRequest,
    UserOfflineHookResponse, UserOnlineHookRequest, UserOnlineHookResponse,
};
use flare_server_core::error::Result as FlareResult;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::caller::{HookCaller, PRE_SEND_OPERATION};
use crate::application::commands::materialize_hook_execution_plan;
use crate::application::handler::HookCommandHandler;
use crate::composition::hook_registry::CoreHookRegistry;
use crate::domain::capability::GuardDecision;
use crate::domain::model::{HookExecutionPlan, is_hook_unavailable};
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::adapters::conversion::{
    message_draft_to_proto, proto_to_message_draft, timestamp_to_system_time,
};
use crate::infrastructure::capability::{CapabilityExtensionRegistry, evaluate_pre_send_guards};
use flare_im_hooks::{DeliveryEvent, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::SignedContextVerifier;

fn proto_conversation_type_wire_name(value: i32) -> Option<&'static str> {
    use flare_proto::common::ConversationType;

    match ConversationType::try_from(value) {
        Ok(ConversationType::Unspecified) => None,
        Ok(ConversationType::Single) => Some("single"),
        Ok(ConversationType::Group) => Some("group"),
        Ok(ConversationType::Ai) => Some("ai"),
        Ok(ConversationType::System) => Some("system"),
        Ok(ConversationType::Customer) => Some("customer"),
        Ok(ConversationType::Temp) => Some("temp"),
        Ok(ConversationType::Channel) => Some("channel"),
        Ok(ConversationType::Broadcast) => Some("broadcast"),
        Err(_) => Some("unspecified"),
    }
}

/// Hook 编排因 Hook 不可用（必需的 Hook 连不上、对端报错、超时）而拒绝时回的 gRPC 状态：
/// 不可达为 `UNAVAILABLE`，超时为 `DEADLINE_EXCEEDED`，说明只有固定原因与 Hook 名。
///
/// 不能回 `allow=false`：调用方（Message Ingest 的 gRPC Hook 适配器）把 `allow=false` 当成策略
/// 拒绝，HTTP 发送回 422 `MESSAGE_REJECTED`（不要重试）；而这里是暂时性故障，调用方据状态码得到
/// 可重试的 `SERVICE_UNAVAILABLE`（HTTP 503）。
fn hook_unavailable_status(error: flare_server_core::error::FlareError) -> Status {
    Status::from(error)
}

/// PostSend、Delivery、Recall 的 Hook 编排返回错误时回的 gRPC 状态：Hook 不可用（必需的 Hook
/// 超时，带 [`crate::domain::model::HOOK_UNAVAILABLE_PARAM`] 标记）回可重试的状态（见
/// [`hook_unavailable_status`]：`UNAVAILABLE` 或 `DEADLINE_EXCEEDED`）；其余错误仍是 `INTERNAL`，
/// 说明照旧。
///
/// 以前一律 `INTERNAL`：必需的事件类 Hook 超时在 Capability 侧与内部故障分不开（W13-D 复审）。
/// Hook 的明确决定不经这里：Recall 被拒仍是 `allow=false`（见 [`recall_response`]）。
fn hook_execution_status(error: flare_server_core::error::FlareError) -> Status {
    if is_hook_unavailable(&error) {
        hook_unavailable_status(error)
    } else {
        Status::internal(format!("Failed to execute hooks: {error}"))
    }
}

/// PreSend 编排结果 → 应答：放行带（可能被改写的）草稿；Hook 明确拒绝回 `allow=false` 与拒绝
/// 原因；Hook 不可用造成的拒绝回可重试的 gRPC 状态（见 [`hook_unavailable_status`]）。
fn pre_send_response(
    decision: PreSendDecision,
    draft: flare_grpc_proto::capability::HookMessageDraft,
) -> Result<PreSendHookResponse, Status> {
    match decision {
        PreSendDecision::Continue => Ok(PreSendHookResponse {
            allow: true,
            draft: Some(draft),
            routing: None,
            annotations: std::collections::HashMap::new(),
            outcome_extensions: None,
            deny_reason_code: String::new(),
            deny_reason_message: String::new(),
        }),
        PreSendDecision::Reject { error } if is_hook_unavailable(&error) => {
            Err(hook_unavailable_status(error))
        }
        PreSendDecision::Reject { error } => {
            let (deny_reason_code, deny_reason_message) = match error {
                flare_server_core::error::FlareError::Localized {
                    reason, details, ..
                } => (reason, details.unwrap_or_default()),
                other => ("HOOK_REJECTED".to_string(), other.to_string()),
            };
            Ok(PreSendHookResponse {
                allow: false,
                draft: None,
                routing: None,
                annotations: std::collections::HashMap::new(),
                outcome_extensions: None,
                deny_reason_code,
                deny_reason_message,
            })
        }
    }
}

/// Recall 编排结果 → 应答，规则同 [`pre_send_response`]。
fn recall_response(decision: PreSendDecision) -> Result<RecallHookResponse, Status> {
    match decision {
        PreSendDecision::Continue => Ok(RecallHookResponse {
            allow: true,
            routing: None,
            annotations: std::collections::HashMap::new(),
            outcome_extensions: None,
            deny_reason_code: String::new(),
            deny_reason_message: String::new(),
        }),
        PreSendDecision::Reject { error } if is_hook_unavailable(&error) => {
            Err(hook_unavailable_status(error))
        }
        PreSendDecision::Reject { .. } => Ok(RecallHookResponse {
            allow: false,
            routing: None,
            annotations: std::collections::HashMap::new(),
            outcome_extensions: None,
            deny_reason_code: String::new(),
            deny_reason_message: String::new(),
        }),
    }
}

/// IM `HookPlugin` gRPC 适配器（接口层 → 应用命令 / 编排）。
///
/// 每个 `Call` 先验调用方的签名上下文（见 [`HookCaller`]）：没有验签公钥、没有或验不过签名都回
/// `UNAUTHENTICATED`；执行哪些 Hook、Hook 看到的租户与用户都只按签名。
pub struct ImHookPluginServer {
    command_handler: Arc<HookCommandHandler>,
    registry: Arc<CoreHookRegistry>,
    adapter_factory: Arc<HookAdapterFactory>,
    capability_registry: CapabilityExtensionRegistry,
    verifier: Option<SignedContextVerifier>,
}

impl ImHookPluginServer {
    /// `verifier` 验 Cell 签名的上下文（`FLARE_INTERNAL_CTX_PUBLIC_KEY`）；为 `None` 时所有调用都被拒绝。
    pub fn new(
        command_handler: Arc<HookCommandHandler>,
        registry: Arc<CoreHookRegistry>,
        adapter_factory: Arc<HookAdapterFactory>,
        capability_registry: CapabilityExtensionRegistry,
        verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            command_handler,
            registry,
            adapter_factory,
            capability_registry,
            verifier,
        }
    }

    /// 将 protobuf HookMessageRecord 转换为 MessageRecord
    fn proto_to_message_record(proto: &HookMessageRecord) -> FlareResult<MessageRecord> {
        let message = proto.message.as_ref().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "Message is required").build_error()
        })?;

        let persisted_at = proto
            .persisted_at
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or_else(std::time::SystemTime::now);

        Ok(MessageRecord {
            message_id: message.server_id.clone(),
            client_message_id: None,
            conversation_id: message.conversation_id.clone(),
            sender_id: message.sender_id.clone(),
            conversation_type: proto_conversation_type_wire_name(message.conversation_type)
                .map(str::to_string),
            message_type: None,
            persisted_at,
            metadata: proto.metadata.clone(),
        })
    }

    /// 将 protobuf HookDeliveryEvent 转换为 DeliveryEvent
    fn proto_to_delivery_event(proto: &HookDeliveryEvent) -> FlareResult<DeliveryEvent> {
        let delivered_at = proto
            .delivered_at
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or_else(std::time::SystemTime::now);

        Ok(DeliveryEvent {
            message_id: proto.message_id.clone(),
            user_id: proto.user_id.clone(),
            channel: proto.channel.clone(),
            delivered_at,
            metadata: proto.metadata.clone(),
        })
    }

    /// 将 protobuf HookRecallEvent 转换为 RecallEvent
    fn proto_to_recall_event(proto: &HookRecallEvent) -> FlareResult<RecallEvent> {
        let recalled_at = proto
            .recalled_at
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or_else(std::time::SystemTime::now);

        Ok(RecallEvent {
            message_id: proto.message_id.clone(),
            conversation_id: None,
            operator_id: proto.operator_id.clone(),
            recalled_at,
            metadata: proto.metadata.clone(),
        })
    }

    /// 从 HookConfigItem 创建 HookExecutionPlan（包含适配器）
    ///
    /// # 参数
    /// * `config` - Hook配置项
    /// * `hook_type` - Hook类型（pre_send, post_send, delivery, recall等）
    async fn create_execution_plan(
        &self,
        config: crate::domain::model::HookConfigItem,
        hook_type: &str,
    ) -> FlareResult<HookExecutionPlan> {
        materialize_hook_execution_plan(self.adapter_factory.as_ref(), config, hook_type).await
    }

    // proto 响应不再携带 `status` 字段；失败通过 gRPC `Status` 或业务字段表达。
}

impl ImHookPluginServer {
    pub(crate) async fn grpc_invoke_pre_send(
        &self,
        caller: &HookCaller,
        request: Request<PreSendHookRequest>,
    ) -> Result<Response<PreSendHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let mut draft = req
            .draft
            .ok_or_else(|| Status::invalid_argument("draft is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, PRE_SEND_OPERATION)?;
        let context = caller.signed_invocation_context(&context);
        let mut message_draft = proto_to_message_draft(&draft);

        // PreSend Guard（好友关系等能力扩展校验）
        let guard_ctx: Arc<flare_server_core::Context> = Arc::new(ctx.clone());
        match evaluate_pre_send_guards(
            &self.capability_registry,
            &guard_ctx,
            &context,
            &message_draft,
        )
        .await
        {
            Ok(GuardDecision::Allow) => {}
            Ok(GuardDecision::Reject(rejection)) => {
                return Ok(Response::new(PreSendHookResponse {
                    allow: false,
                    draft: None,
                    routing: None,
                    annotations: std::collections::HashMap::new(),
                    outcome_extensions: None,
                    deny_reason_code: rejection.code,
                    deny_reason_message: rejection.message,
                }));
            }
            Err(e) => {
                return Err(Status::internal(format!("PreSend guard failed: {e}")));
            }
        }

        // 获取PreSend Hook列表
        let hooks = self
            .registry
            .get_pre_send_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "pre_send").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook
        let ctx_arc: Arc<flare_server_core::Context> = Arc::new(ctx.clone());
        let decision = self
            .command_handler
            .handle_pre_send(&ctx_arc, &mut message_draft, execution_plans)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        // 更新 draft（如果被 Hook 修改）
        draft = message_draft_to_proto(&message_draft);

        pre_send_response(decision, draft).map(Response::new)
    }

    pub(crate) async fn grpc_invoke_post_send(
        &self,
        caller: &HookCaller,
        request: Request<PostSendHookRequest>,
    ) -> Result<Response<PostSendHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let record = req
            .record
            .ok_or_else(|| Status::invalid_argument("record is required"))?;
        let draft = req
            .draft
            .ok_or_else(|| Status::invalid_argument("draft is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.post_send")?;
        let message_record = Self::proto_to_message_record(&record)
            .map_err(|e| Status::invalid_argument(format!("Invalid record: {}", e)))?;
        let message_draft = proto_to_message_draft(&draft);

        // 获取PostSend Hook列表
        let hooks = self
            .registry
            .get_post_send_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "post_send").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook
        let ctx_arc: Arc<flare_server_core::Context> = Arc::new(ctx.clone());
        self.command_handler
            .handle_post_send(&ctx_arc, &message_record, &message_draft, execution_plans)
            .await
            .map_err(hook_execution_status)?;

        Ok(Response::new(PostSendHookResponse {
            success: true,
            routing: None,
            outcome_extensions: None,
            error_code: String::new(),
            error_message: String::new(),
        }))
    }

    pub(crate) async fn grpc_notify_delivery(
        &self,
        caller: &HookCaller,
        request: Request<DeliveryHookRequest>,
    ) -> Result<Response<DeliveryHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.delivery")?;
        let delivery_event = Self::proto_to_delivery_event(&event)
            .map_err(|e| Status::invalid_argument(format!("Invalid event: {}", e)))?;

        // 获取Delivery Hook列表
        let hooks = self
            .registry
            .get_delivery_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "delivery").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook
        let ctx_arc: Arc<flare_server_core::Context> = Arc::new(ctx.clone());
        self.command_handler
            .handle_delivery(&ctx_arc, &delivery_event, execution_plans)
            .await
            .map_err(hook_execution_status)?;

        Ok(Response::new(DeliveryHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_recall(
        &self,
        caller: &HookCaller,
        request: Request<RecallHookRequest>,
    ) -> Result<Response<RecallHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.recall")?;
        let recall_event = Self::proto_to_recall_event(&event)
            .map_err(|e| Status::invalid_argument(format!("Invalid event: {}", e)))?;

        // 获取Recall Hook列表
        let hooks = self
            .registry
            .get_recall_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "recall").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook
        let ctx_arc: Arc<flare_server_core::Context> = Arc::new(ctx.clone());
        let decision = self
            .command_handler
            .handle_recall(&ctx_arc, &recall_event, execution_plans)
            .await
            .map_err(hook_execution_status)?;

        recall_response(decision).map(Response::new)
    }

    pub(crate) async fn grpc_notify_conversation_lifecycle(
        &self,
        caller: &HookCaller,
        request: Request<ConversationLifecycleHookRequest>,
    ) -> Result<Response<ConversationLifecycleHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.conversation_lifecycle")?;

        // 获取ConversationLifecycle Hook列表
        let hooks = self
            .registry
            .get_conversation_lifecycle_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self
                    .create_execution_plan(hook_config, "conversation_lifecycle")
                    .await
                {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以根据Hook类型实现具体逻辑）
        use crate::infrastructure::adapters::hook_context_data::get_hook_context_data;
        let conversation_id = get_hook_context_data(&ctx).and_then(|d| d.conversation_id.as_ref());
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                conversation_id = ?conversation_id,
                event_type = event.event,
                "Executing ConversationLifecycle hook"
            );
        }

        Ok(Response::new(ConversationLifecycleHookResponse {
            success: true,
            routing: None,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_presence(
        &self,
        caller: &HookCaller,
        request: Request<PresenceHookRequest>,
    ) -> Result<Response<PresenceHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let _event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.presence")?;

        // Presence Hook 目前没有专门的配置，记录日志
        use crate::infrastructure::adapters::hook_context_data::get_hook_context_data;
        let conversation_id = get_hook_context_data(&ctx).and_then(|d| d.conversation_id.as_ref());
        tracing::trace!(
            user_id = ?conversation_id,
            "Presence hook notification received"
        );

        Ok(Response::new(PresenceHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_invoke_custom(
        &self,
        caller: &HookCaller,
        request: Request<CustomHookRequest>,
    ) -> Result<Response<CustomHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let hook_type = req.r#type;
        let _payload = req.payload;

        // 转换为内部类型
        let ctx = caller.hook_context(&context, "flare.hook.v1.custom")?;

        // Custom Hook 目前没有专门的配置，记录日志
        tracing::trace!(
            hook_type = %hook_type,
            tenant_id = %ctx.tenant_id().unwrap_or(""),
            "Custom hook invocation received"
        );

        Ok(Response::new(CustomHookResponse {
            success: true,
            response_payload: Vec::new(),
            outcome_extensions: None,
            error_code: String::new(),
            error_message: String::new(),
        }))
    }

    pub(crate) async fn grpc_invoke_push_pre_send(
        &self,
        caller: &HookCaller,
        request: Request<PushPreSendHookRequest>,
    ) -> Result<Response<PushPreSendHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let draft = req
            .draft
            .ok_or_else(|| Status::invalid_argument("draft is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&context, "flare.hook.v1.push_pre_send")?;

        // 获取PushPreSend Hook列表
        let hooks = self
            .registry
            .get_push_pre_send_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self
                    .create_execution_plan(hook_config, "push_pre_send")
                    .await
                {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PreSend 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %draft.user_id,
                task_id = %draft.task_id,
                "Executing PushPreSend hook"
            );
        }

        Ok(Response::new(PushPreSendHookResponse {
            allow: true,
            draft: Some(draft),
            routing: None,
            annotations: std::collections::HashMap::new(),
            outcome_extensions: None,
            deny_reason_code: String::new(),
            deny_reason_message: String::new(),
        }))
    }

    pub(crate) async fn grpc_invoke_push_post_send(
        &self,
        caller: &HookCaller,
        request: Request<PushPostSendHookRequest>,
    ) -> Result<Response<PushPostSendHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let _record = req
            .record
            .ok_or_else(|| Status::invalid_argument("record is required"))?;
        let _draft = req
            .draft
            .ok_or_else(|| Status::invalid_argument("draft is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.push_post_send")?;

        // 获取PushPostSend Hook列表
        let hooks = self
            .registry
            .get_push_post_send_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self
                    .create_execution_plan(hook_config, "push_post_send")
                    .await
                {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PostSend 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                "Executing PushPostSend hook"
            );
        }

        Ok(Response::new(PushPostSendHookResponse {
            success: true,
            routing: None,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_push_delivery(
        &self,
        caller: &HookCaller,
        request: Request<PushDeliveryHookRequest>,
    ) -> Result<Response<PushDeliveryHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.push_delivery")?;

        // 获取PushDelivery Hook列表
        let hooks = self
            .registry
            .get_push_delivery_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self
                    .create_execution_plan(hook_config, "push_delivery")
                    .await
                {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 Delivery 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %event.user_id,
                task_id = %event.task_id,
                channel = %event.channel,
                "Executing PushDelivery hook"
            );
        }

        Ok(Response::new(PushDeliveryHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_user_login(
        &self,
        caller: &HookCaller,
        request: Request<UserLoginHookRequest>,
    ) -> Result<Response<UserLoginHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.user_login")?;

        // 获取UserLogin Hook列表
        let hooks = self
            .registry
            .get_user_login_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "user_login").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PreSend 的逻辑，可以拒绝登录）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserLogin hook"
            );
        }

        Ok(Response::new(UserLoginHookResponse {
            allow: true,
            routing: None,
            annotations: std::collections::HashMap::new(),
            outcome_extensions: None,
            deny_reason_code: String::new(),
            deny_reason_message: String::new(),
        }))
    }

    pub(crate) async fn grpc_notify_user_logout(
        &self,
        caller: &HookCaller,
        request: Request<UserLogoutHookRequest>,
    ) -> Result<Response<UserLogoutHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.user_logout")?;

        // 获取UserLogout Hook列表
        let hooks = self
            .registry
            .get_user_logout_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "user_logout").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PostSend 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserLogout hook"
            );
        }

        Ok(Response::new(UserLogoutHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_user_online(
        &self,
        caller: &HookCaller,
        request: Request<UserOnlineHookRequest>,
    ) -> Result<Response<UserOnlineHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.user_online")?;

        // 获取UserOnline Hook列表
        let hooks = self
            .registry
            .get_user_online_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self.create_execution_plan(hook_config, "user_online").await {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PostSend 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserOnline hook"
            );
        }

        Ok(Response::new(UserOnlineHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_notify_user_offline(
        &self,
        caller: &HookCaller,
        request: Request<UserOfflineHookRequest>,
    ) -> Result<Response<UserOfflineHookResponse>, Status> {
        let req = request.into_inner();
        let _context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;

        // 转换为内部类型
        let _ctx = caller.hook_context(&_context, "flare.hook.v1.user_offline")?;

        // 获取UserOffline Hook列表
        let hooks = self
            .registry
            .get_user_offline_hooks(caller.tenant_id())
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans: Vec<HookExecutionPlan> = Vec::new();
        for hook_config in hooks {
            if hook_config.enabled {
                match self
                    .create_execution_plan(hook_config, "user_offline")
                    .await
                {
                    Ok(plan) => execution_plans.push(plan),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to create execution plan, skipping hook");
                        continue;
                    }
                }
            }
        }

        // 执行Hook（目前只记录日志，后续可以实现类似 PostSend 的逻辑）
        for plan in execution_plans {
            tracing::trace!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                reason = %event.reason,
                "Executing UserOffline hook"
            );
        }

        Ok(Response::new(UserOfflineHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub(crate) async fn grpc_on_message_read(
        &self,
        _caller: &HookCaller,
        request: Request<MessageReadHookRequest>,
    ) -> Result<Response<MessageReadHookResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(MessageReadHookResponse {
            success: true,
            ..Default::default()
        }))
    }

    pub(crate) async fn grpc_on_message_reaction(
        &self,
        _caller: &HookCaller,
        request: Request<MessageReactionHookRequest>,
    ) -> Result<Response<MessageReactionHookResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(MessageReactionHookResponse {
            allow: true,
            ..Default::default()
        }))
    }

    pub(crate) async fn grpc_on_conversation_member(
        &self,
        _caller: &HookCaller,
        request: Request<ConversationMemberHookRequest>,
    ) -> Result<Response<ConversationMemberHookResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(ConversationMemberHookResponse {
            allow: true,
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl HookPlugin for ImHookPluginServer {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        use prost::Message;

        let caller = HookCaller::verify(request.metadata(), self.verifier.as_ref())?;
        let outer = request.into_inner();
        let operation = outer.operation.clone();
        let request_id = outer.request_id.clone();
        let payload = outer
            .payload
            .ok_or_else(|| Status::invalid_argument("payload required"))?;

        fn pack_response(
            request_id: String,
            response_type_url: &str,
            msg: &impl prost::Message,
        ) -> Result<Response<GenericResponse>, Status> {
            let any = prost_types::Any {
                type_url: response_type_url.to_string(),
                value: msg.encode_to_vec(),
            };
            Ok(Response::new(GenericResponse {
                ok: true,
                request_id,
                payload: Some(any),
                error_code: String::new(),
                error_message: String::new(),
            }))
        }

        if operation == "flare.hook.v1.pre_send" {
            let inner = PreSendHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_invoke_pre_send(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PreSendHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.post_send" {
            let inner = PostSendHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_invoke_post_send(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PostSendHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.delivery" {
            let inner = DeliveryHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_delivery(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.DeliveryHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.recall" {
            let inner = RecallHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_recall(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.RecallHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.message_read" {
            let inner = MessageReadHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_on_message_read(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.MessageReadHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.message_reaction" {
            let inner = MessageReactionHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_on_message_reaction(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.MessageReactionHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.conversation_lifecycle" {
            let inner = ConversationLifecycleHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_conversation_lifecycle(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.ConversationLifecycleHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.conversation_member" {
            let inner = ConversationMemberHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_on_conversation_member(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.ConversationMemberHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.presence" {
            let inner = PresenceHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_presence(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PresenceHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.custom" {
            let inner = CustomHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_invoke_custom(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.CustomHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.push_pre_send" {
            let inner = PushPreSendHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_invoke_push_pre_send(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PushPreSendHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.push_post_send" {
            let inner = PushPostSendHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_invoke_push_post_send(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PushPostSendHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.push_delivery" {
            let inner = PushDeliveryHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_push_delivery(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.PushDeliveryHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.user_login" {
            let inner = UserLoginHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_user_login(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.UserLoginHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.user_logout" {
            let inner = UserLogoutHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_user_logout(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.UserLogoutHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.user_online" {
            let inner = UserOnlineHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_user_online(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.UserOnlineHookResponse",
                &rsp,
            );
        }
        if operation == "flare.hook.v1.user_offline" {
            let inner = UserOfflineHookRequest::decode(payload.value.as_slice())
                .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
            let rsp = self
                .grpc_notify_user_offline(&caller, Request::new(inner))
                .await?
                .into_inner();
            return pack_response(
                request_id,
                "type.googleapis.com/flare.capability.v1.UserOfflineHookResponse",
                &rsp,
            );
        }

        Err(Status::unimplemented(format!(
            "unknown hook operation: {operation}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::proto_conversation_type_wire_name;
    use flare_proto::common::ConversationType;

    mod hook_unavailable {
        //! Hook 不可用与 Hook 拒绝在 HookPlugin 应答上分得开：前者回可重试的 gRPC 状态，后者回
        //! `allow=false`。Message Ingest 的 gRPC Hook 适配器把 `allow=false` 当成策略拒绝（HTTP
        //! 422，不要重试），把 gRPC 失败当成暂时不可用（HTTP 503，可重试）。
        use super::super::*;
        use crate::application::handler::HookCommandHandler;
        use crate::domain::model::{
            HOOK_UNAVAILABLE_PARAM, HookConfig, HookConfigItem, HookSelectorConfig,
            HookTransportConfig,
        };
        use crate::domain::service::HookOrchestrationService;
        use crate::infrastructure::config::ConfigWatcher;
        use crate::interface::grpc::hooks::test_support::{
            CellKeys, cell_keys, ingest_pre_send as ingest_pre_send_with, serve_hook_plugin,
            signed_ctx,
        };
        use flare_grpc_proto::capability::hook_plugin_server::HookPluginServer;
        use flare_grpc_proto::capability::{GenericResponse, HookMessageDraft};
        use flare_im_hooks::is_pre_send_rejection;
        use flare_server_core::SignedActorType;
        use flare_server_core::error::FlareError;
        use prost::Message as _;
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        fn unavailable(code: ErrorCode, reason: &str) -> FlareError {
            ErrorBuilder::new(code, reason)
                .details("hook=required-closed")
                .param(HOOK_UNAVAILABLE_PARAM, "true")
                .build_error()
        }

        #[test]
        fn unavailable_rejections_are_retryable_statuses_not_allow_false() {
            let status = pre_send_response(
                PreSendDecision::Reject {
                    error: unavailable(ErrorCode::ServiceUnavailable, "pre-send hook unavailable"),
                },
                HookMessageDraft::default(),
            )
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");

            let status = pre_send_response(
                PreSendDecision::Reject {
                    error: unavailable(ErrorCode::OperationTimeout, "pre-send hook timed out"),
                },
                HookMessageDraft::default(),
            )
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::DeadlineExceeded, "{status:?}");

            let status = recall_response(PreSendDecision::Reject {
                error: unavailable(ErrorCode::ServiceUnavailable, "recall hook unavailable"),
            })
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        }

        #[test]
        fn hook_decisions_stay_in_the_response_body() {
            let denied = pre_send_response(
                PreSendDecision::Reject {
                    error: ErrorBuilder::new(ErrorCode::PermissionDenied, "SOCIAL_PRESEND_DENIED")
                        .details("not friends")
                        .build_error(),
                },
                HookMessageDraft::default(),
            )
            .unwrap();
            assert!(!denied.allow);
            assert_eq!(denied.deny_reason_code, "SOCIAL_PRESEND_DENIED");
            assert_eq!(denied.deny_reason_message, "not friends");

            let allowed =
                pre_send_response(PreSendDecision::Continue, HookMessageDraft::default()).unwrap();
            assert!(allowed.allow && allowed.draft.is_some());

            let recall_denied = recall_response(PreSendDecision::Reject {
                error: ErrorBuilder::new(ErrorCode::OperationFailed, "recall denied").build_error(),
            })
            .unwrap();
            assert!(!recall_denied.allow);
            assert!(recall_response(PreSendDecision::Continue).unwrap().allow);
        }

        /// 下游 Hook：对 PreSend 一律回 `allow=false`（策略拒绝）。
        struct DenyAll;

        #[tonic::async_trait]
        impl HookPlugin for DenyAll {
            async fn call(
                &self,
                request: Request<GenericRequest>,
            ) -> Result<Response<GenericResponse>, Status> {
                let response = PreSendHookResponse {
                    allow: false,
                    deny_reason_code: "SOCIAL_PRESEND_DENIED".into(),
                    ..Default::default()
                };
                Ok(Response::new(GenericResponse {
                    ok: true,
                    request_id: request.into_inner().request_id,
                    payload: Some(prost_types::Any {
                        type_url: "type.googleapis.com/flare.capability.v1.PreSendHookResponse"
                            .into(),
                        value: response.encode_to_vec(),
                    }),
                    error_code: String::new(),
                    error_message: String::new(),
                }))
            }
        }

        async fn serve<S>(service: S) -> String
        where
            S: HookPlugin,
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(HookPluginServer::new(service))
                    .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
            );
            format!("http://{address}")
        }

        async fn closed_port_endpoint() -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);
            format!("http://{address}")
        }

        fn required_pre_send_hook(endpoint: &str) -> HookConfigItem {
            HookConfigItem {
                name: "required-pre-send".into(),
                version: None,
                description: None,
                enabled: true,
                priority: 110,
                group: None,
                timeout_ms: 10_000,
                max_retries: 0,
                error_policy: "fail_fast".into(),
                require_success: true,
                selector: HookSelectorConfig::default(),
                transport: HookTransportConfig::Grpc {
                    endpoint: Some(endpoint.into()),
                    service_name: None,
                    registry_type: None,
                    namespace: None,
                    load_balance: None,
                    metadata: HashMap::new(),
                },
                metadata: HashMap::new(),
            }
        }

        fn keys() -> CellKeys {
            cell_keys(61)
        }

        /// Capability 的 HookPlugin 服务，PreSend 只挂 `hook` 一条；验 [`keys`] 的签名。
        async fn capability_with(hook: HookConfigItem) -> String {
            let watcher = Arc::new(ConfigWatcher::with_config(HookConfig {
                pre_send: vec![hook],
                ..Default::default()
            }));
            let server = ImHookPluginServer::new(
                Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
                Arc::new(CoreHookRegistry::new(watcher)),
                Arc::new(HookAdapterFactory::new()),
                CapabilityExtensionRegistry::new(),
                Some(keys().verifier),
            );
            serve_hook_plugin(server).await
        }

        /// 以 Message Ingest 的 gRPC Hook 适配器调 Capability 的 PreSend（Cell 签名的用户身份）。
        async fn ingest_pre_send(capability: &str) -> PreSendDecision {
            let ctx = signed_ctx(&keys(), SignedActorType::User, "u1", "0");
            ingest_pre_send_with(capability, &ctx, "m-hook-unavailable").await
        }

        /// 必需的下游 Hook 不可达：Message Ingest 得到的是不带策略拒绝标记的
        /// `SERVICE_UNAVAILABLE`（发送方 503、可重试），不是策略拒绝（422）。修复前 Capability 回
        /// `allow=false`，Ingest 把它当成策略拒绝。
        #[tokio::test]
        async fn ingest_sees_an_unreachable_required_hook_as_unavailable_not_a_policy_rejection() {
            let capability =
                capability_with(required_pre_send_hook(&closed_port_endpoint().await)).await;
            match ingest_pre_send(&capability).await {
                PreSendDecision::Reject { error } => {
                    assert!(!is_pre_send_rejection(&error), "{error}");
                    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable), "{error}");
                }
                PreSendDecision::Continue => panic!("an unreachable required hook must not allow"),
            }
        }

        /// 对照：下游 Hook 明确拒绝时，Message Ingest 仍得到带标记的策略拒绝（422）。
        #[tokio::test]
        async fn ingest_still_sees_a_hook_denial_as_a_policy_rejection() {
            let downstream = serve(DenyAll).await;
            let capability = capability_with(required_pre_send_hook(&downstream)).await;
            match ingest_pre_send(&capability).await {
                PreSendDecision::Reject { error } => {
                    assert!(is_pre_send_rejection(&error), "{error}");
                }
                PreSendDecision::Continue => panic!("a denying hook must not allow"),
            }
        }
    }

    mod event_hook_timeouts {
        //! 必需的事件类 Hook（PostSend、Delivery、Recall）在 Capability 里超时，经 `flare-im-hooks`
        //! 的 gRPC Hook 适配器（Message Ingest 等调 Capability `HookPlugin` 用的那一个）端到端：
        //! Capability 回可重试的 gRPC 状态（PostSend、Delivery 为 `UNAVAILABLE`，Recall 同 PreSend
        //! 为 `DEADLINE_EXCEEDED`），适配器交给调用方的是可重试的 `ServiceUnavailable`。下游明确
        //! 报失败仍是 `INTERNAL`，撤回被拒仍是 `allow=false`，可选 Hook 超时照常放行。
        //!
        //! 适配器把 `HookPlugin` 的任何失败状态都记成 `ServiceUnavailable`（`flare-im-hooks`
        //! `adapters/grpc.rs` 的 `Err(status)` 分支），所以适配器一侧的断言修复前后都成立；Capability
        //! 回的状态码用同一个签名令牌直接调 `HookPlugin` 看。
        use super::super::*;
        use crate::application::handler::HookCommandHandler;
        use crate::domain::model::{
            HookConfig, HookConfigItem, HookSelectorConfig, HookTransportConfig,
        };
        use crate::domain::service::HookOrchestrationService;
        use crate::infrastructure::config::ConfigWatcher;
        use crate::interface::grpc::hooks::test_support::{
            CellKeys, cell_keys, serve_hook_plugin, signed_ctx,
        };
        use flare_grpc_proto::capability::hook_plugin_client::HookPluginClient;
        use flare_grpc_proto::capability::hook_plugin_server::HookPluginServer;
        use flare_grpc_proto::capability::{HookInvocationContext, HookMessageDraft};
        use flare_im_hooks::hooks::adapters::GrpcHookFactory;
        use flare_im_hooks::{HookOutcome, MessageDraft};
        use flare_server_core::SignedActorType;
        use flare_server_core::context::Ctx;
        use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
        use prost::Message as _;
        use std::collections::HashMap;
        use std::time::{Duration, SystemTime};
        use tokio::net::TcpListener;

        const POST_SEND: &str = "flare.hook.v1.post_send";
        const DELIVERY: &str = "flare.hook.v1.delivery";
        const RECALL: &str = "flare.hook.v1.recall";

        fn keys() -> CellKeys {
            cell_keys(93)
        }

        /// 下游 Hook：永不回应答（被暂停、只收连接不回应答的扩展）。
        struct Hanging;

        #[tonic::async_trait]
        impl HookPlugin for Hanging {
            async fn call(
                &self,
                _request: Request<GenericRequest>,
            ) -> Result<Response<GenericResponse>, Status> {
                std::future::pending().await
            }
        }

        /// 下游 Hook：PostSend、Delivery 回 `success=false`，Recall 回 `allow=false`（明确的决定）。
        struct Refusing;

        #[tonic::async_trait]
        impl HookPlugin for Refusing {
            async fn call(
                &self,
                request: Request<GenericRequest>,
            ) -> Result<Response<GenericResponse>, Status> {
                let request = request.into_inner();
                let (type_url, value) = match request.operation.as_str() {
                    POST_SEND => (
                        "type.googleapis.com/flare.capability.v1.PostSendHookResponse",
                        PostSendHookResponse::default().encode_to_vec(),
                    ),
                    DELIVERY => (
                        "type.googleapis.com/flare.capability.v1.DeliveryHookResponse",
                        DeliveryHookResponse::default().encode_to_vec(),
                    ),
                    RECALL => (
                        "type.googleapis.com/flare.capability.v1.RecallHookResponse",
                        RecallHookResponse::default().encode_to_vec(),
                    ),
                    other => return Err(Status::unimplemented(other.to_string())),
                };
                Ok(Response::new(GenericResponse {
                    ok: true,
                    request_id: request.request_id,
                    payload: Some(prost_types::Any {
                        type_url: type_url.into(),
                        value,
                    }),
                    error_code: String::new(),
                    error_message: String::new(),
                }))
            }
        }

        async fn serve<S: HookPlugin>(service: S) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(HookPluginServer::new(service))
                    .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
            );
            format!("http://{address}")
        }

        fn event_hook(endpoint: &str, require_success: bool) -> HookConfigItem {
            HookConfigItem {
                name: "event-hook".into(),
                version: None,
                description: None,
                enabled: true,
                priority: 110,
                group: None,
                timeout_ms: 200,
                max_retries: 0,
                error_policy: "fail_fast".into(),
                require_success,
                selector: HookSelectorConfig::default(),
                transport: HookTransportConfig::Grpc {
                    endpoint: Some(endpoint.into()),
                    service_name: None,
                    registry_type: None,
                    namespace: None,
                    load_balance: None,
                    metadata: HashMap::new(),
                },
                metadata: HashMap::new(),
            }
        }

        /// Capability 的 HookPlugin 服务，PostSend、Delivery、Recall 各挂一条指向 `downstream` 的
        /// Hook（validation 组，200 ms 超时）。
        async fn capability(downstream: &str, require_success: bool) -> String {
            let hook = event_hook(downstream, require_success);
            let watcher = Arc::new(ConfigWatcher::with_config(HookConfig {
                post_send: vec![hook.clone()],
                delivery: vec![hook.clone()],
                recall: vec![hook],
                ..Default::default()
            }));
            let server = ImHookPluginServer::new(
                Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
                Arc::new(CoreHookRegistry::new(watcher)),
                Arc::new(HookAdapterFactory::new()),
                CapabilityExtensionRegistry::new(),
                Some(keys().verifier),
            );
            serve_hook_plugin(server).await
        }

        /// 服务身份（事件类 Hook 由服务代用户调用）的签名上下文。
        fn service_ctx() -> Ctx {
            signed_ctx(
                &keys(),
                SignedActorType::Service,
                "flare-orchestrator",
                "tenant-a",
            )
        }

        const GUARD: Duration = Duration::from_secs(10);

        /// 以 `flare-im-hooks` 的 gRPC Hook 适配器依次调 `capability` 的三类事件 Hook。
        async fn via_adapter(capability: &str, ctx: &Ctx) -> Vec<(&'static str, HookOutcome)> {
            let factory = GrpcHookFactory::new();
            let now = SystemTime::now();
            let record = MessageRecord {
                message_id: "m1".into(),
                client_message_id: None,
                conversation_id: "conv-1".into(),
                sender_id: "u1".into(),
                conversation_type: Some("single".into()),
                message_type: None,
                persisted_at: now,
                metadata: HashMap::new(),
            };
            let draft = MessageDraft::new(b"hello".to_vec());
            let delivery = DeliveryEvent {
                message_id: "m1".into(),
                user_id: "u2".into(),
                channel: "ws".into(),
                delivered_at: now,
                metadata: HashMap::new(),
            };
            let recall = RecallEvent {
                message_id: "m1".into(),
                conversation_id: Some("conv-1".into()),
                operator_id: "u1".into(),
                recalled_at: now,
                metadata: HashMap::new(),
            };
            let post_send = factory.build_post_send(HashMap::new(), capability.into());
            let delivery_hook = factory.build_delivery(HashMap::new(), capability.into());
            let recall_hook = factory.build_recall(HashMap::new(), capability.into());
            vec![
                (
                    "post_send",
                    tokio::time::timeout(GUARD, post_send.handle(ctx, &record, &draft))
                        .await
                        .expect("post_send: the adapter call returns"),
                ),
                (
                    "delivery",
                    tokio::time::timeout(GUARD, delivery_hook.handle(ctx, &delivery))
                        .await
                        .expect("delivery: the adapter call returns"),
                ),
                (
                    "recall",
                    tokio::time::timeout(GUARD, recall_hook.handle(ctx, &recall))
                        .await
                        .expect("recall: the adapter call returns"),
                ),
            ]
        }

        /// 与适配器同一个签名令牌、同样的 `GenericRequest` 直接调 HookPlugin，返回 Capability 的
        /// 应答或状态。
        async fn raw_call(
            capability: &str,
            ctx: &Ctx,
            operation: &'static str,
        ) -> Result<GenericResponse, Status> {
            let verified = ctx.get_data::<VerifiedContext>().unwrap();
            let claims = verified.claims().unwrap();
            let context = Some(HookInvocationContext {
                conversation_id: "conv-1".into(),
                conversation_type: "single".into(),
                tenant_id: claims.tenant_id.clone(),
                request_id: claims.request_id.clone(),
                ..Default::default()
            });
            let (type_url, value) = match operation {
                POST_SEND => (
                    "type.googleapis.com/flare.capability.v1.PostSendHookRequest",
                    PostSendHookRequest {
                        context,
                        record: Some(HookMessageRecord {
                            message: Some(flare_proto::common::Message {
                                server_id: "m1".into(),
                                conversation_id: "conv-1".into(),
                                sender_id: "u1".into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        draft: Some(HookMessageDraft::default()),
                    }
                    .encode_to_vec(),
                ),
                DELIVERY => (
                    "type.googleapis.com/flare.capability.v1.DeliveryHookRequest",
                    DeliveryHookRequest {
                        context,
                        event: Some(HookDeliveryEvent {
                            message_id: "m1".into(),
                            user_id: "u2".into(),
                            channel: "ws".into(),
                            ..Default::default()
                        }),
                    }
                    .encode_to_vec(),
                ),
                RECALL => (
                    "type.googleapis.com/flare.capability.v1.RecallHookRequest",
                    RecallHookRequest {
                        context,
                        event: Some(HookRecallEvent {
                            message_id: "m1".into(),
                            operator_id: "u1".into(),
                            ..Default::default()
                        }),
                    }
                    .encode_to_vec(),
                ),
                other => panic!("not an event hook operation: {other}"),
            };
            let mut client = HookPluginClient::connect(capability.to_string())
                .await
                .unwrap();
            let mut request = Request::new(GenericRequest {
                operation: operation.into(),
                metadata: HashMap::new(),
                payload: Some(prost_types::Any {
                    type_url: type_url.into(),
                    value,
                }),
                request_id: claims.request_id.clone(),
            });
            request
                .metadata_mut()
                .insert("x-flare-ctx", verified.token().unwrap().parse().unwrap());
            tokio::time::timeout(GUARD, client.call(request))
                .await
                .expect("the HookPlugin call returns")
                .map(Response::into_inner)
        }

        /// 必需的事件类 Hook 在 Capability 里超时：Capability 回可重试的 gRPC 状态，适配器的调用方
        /// 得到可重试的 `ServiceUnavailable`。修复前 PostSend 与 Delivery 回 `INTERNAL`。
        #[tokio::test]
        async fn required_event_hook_timeouts_are_retryable_through_the_adapter() {
            let downstream = serve(Hanging).await;
            let capability = capability(&downstream, true).await;
            let ctx = service_ctx();

            for (operation, expected) in [
                (POST_SEND, tonic::Code::Unavailable),
                (DELIVERY, tonic::Code::Unavailable),
                (RECALL, tonic::Code::DeadlineExceeded),
            ] {
                let status = raw_call(&capability, &ctx, operation)
                    .await
                    .expect_err(operation);
                assert_eq!(status.code(), expected, "{operation}: {status:?}");
                assert!(
                    !status.message().contains("Failed to execute hooks"),
                    "{operation}: {status:?}"
                );
            }

            for (phase, outcome) in via_adapter(&capability, &ctx).await {
                match outcome {
                    HookOutcome::Failed(error) => {
                        assert_eq!(
                            error.code(),
                            Some(ErrorCode::ServiceUnavailable),
                            "{phase}: {error}"
                        );
                        assert!(error.is_retryable(), "{phase}: {error}");
                    }
                    HookOutcome::Completed => {
                        panic!("{phase}: a timed-out required hook must not complete")
                    }
                }
            }
        }

        /// 对照：下游明确报失败时 PostSend 与 Delivery 仍是 `INTERNAL`，撤回被拒仍是 `allow=false`
        /// （适配器得到策略拒绝 `OPERATION_FAILED`）；可选 Hook 超时照常放行。
        #[tokio::test]
        async fn refusals_keep_their_answers_and_optional_timeouts_complete() {
            let ctx = service_ctx();
            let refusing = capability(&serve(Refusing).await, true).await;
            for operation in [POST_SEND, DELIVERY] {
                let status = raw_call(&refusing, &ctx, operation)
                    .await
                    .expect_err(operation);
                assert_eq!(
                    status.code(),
                    tonic::Code::Internal,
                    "{operation}: {status:?}"
                );
                assert!(
                    status.message().starts_with("Failed to execute hooks"),
                    "{operation}: {status:?}"
                );
            }
            let response = raw_call(&refusing, &ctx, RECALL).await.unwrap();
            let recall =
                RecallHookResponse::decode(response.payload.unwrap().value.as_slice()).unwrap();
            assert!(!recall.allow);
            let outcomes = via_adapter(&refusing, &ctx).await;
            match &outcomes[2] {
                ("recall", HookOutcome::Failed(error)) => {
                    assert_eq!(error.code(), Some(ErrorCode::OperationFailed), "{error}");
                    assert!(!error.is_retryable(), "{error}");
                }
                other => panic!("recall refusal: {other:?}"),
            }

            let optional = capability(&serve(Hanging).await, false).await;
            for operation in [POST_SEND, DELIVERY, RECALL] {
                let response = raw_call(&optional, &ctx, operation).await;
                assert!(response.is_ok_and(|r| r.ok), "{operation}");
            }
            for (phase, outcome) in via_adapter(&optional, &ctx).await {
                assert!(outcome.is_completed(), "{phase}: {outcome:?}");
            }
        }

        /// 编排错误的状态映射本身：带 Hook 不可用标记的回可重试状态，其余仍是 `INTERNAL`。
        #[test]
        fn hook_execution_errors_map_to_retryable_statuses_only_when_unavailable() {
            let unavailable =
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "post-send hook timed out")
                    .details("hook=h")
                    .param(crate::domain::model::HOOK_UNAVAILABLE_PARAM, "true")
                    .build_error();
            assert_eq!(
                hook_execution_status(unavailable).code(),
                tonic::Code::Unavailable
            );
            let internal = ErrorBuilder::new(
                ErrorCode::InternalError,
                "remote PostSend hook reported failure",
            )
            .build_error();
            let status = hook_execution_status(internal);
            assert_eq!(status.code(), tonic::Code::Internal);
            assert!(
                status.message().starts_with("Failed to execute hooks"),
                "{status:?}"
            );
            let unmarked =
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "downstream unavailable")
                    .build_error();
            assert_eq!(
                hook_execution_status(unmarked).code(),
                tonic::Code::Internal,
                "only the hook-unavailable marker selects a retryable status"
            );
        }
    }

    mod signed_caller_and_tenants {
        //! HookPlugin 只认 Cell 签名的调用方（W10-S2 F3），并按签名租户选 Hook：租户登记的 Webhook
        //! 只对本租户的事件触发，全局配置对所有租户生效（F1、F4）。
        use super::super::*;
        use crate::application::handler::HookCommandHandler;
        use crate::domain::model::HookConfig;
        use crate::domain::service::HookOrchestrationService;
        use crate::infrastructure::config::{ConfigWatcher, HookConfigSnapshot};
        use crate::interface::grpc::hooks::test_support::{
            CellKeys, WebhookReceiver, cell_keys, ingest_pre_send, serve_hook_plugin, signed_ctx,
            webhook_hook,
        };
        use flare_grpc_proto::capability::hook_plugin_client::HookPluginClient;
        use flare_grpc_proto::capability::{HookInvocationContext, HookMessageDraft};
        use flare_server_core::{SignedActorType, SignedContextIdentity};
        use prost::Message as _;
        use std::collections::HashMap;

        fn keys() -> CellKeys {
            cell_keys(81)
        }

        fn pre_send(hooks: Vec<crate::domain::model::HookConfigItem>) -> HookConfig {
            HookConfig {
                pre_send: hooks,
                ..Default::default()
            }
        }

        /// 全局一条 Webhook，租户 a、b 各登记一条，都投到同一个接收端的不同路径。
        fn two_tenants(receiver: &WebhookReceiver) -> HookConfigSnapshot {
            HookConfigSnapshot::new(
                pre_send(vec![webhook_hook("global-audit", &receiver.url("/global"))]),
                HashMap::from([
                    (
                        "tenant-a".to_string(),
                        pre_send(vec![webhook_hook("wh-a", &receiver.url("/a"))]),
                    ),
                    (
                        "tenant-b".to_string(),
                        pre_send(vec![webhook_hook("wh-b", &receiver.url("/b"))]),
                    ),
                ]),
            )
        }

        async fn capability(
            snapshot: HookConfigSnapshot,
            verifier: Option<SignedContextVerifier>,
        ) -> String {
            let server = ImHookPluginServer::new(
                Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
                Arc::new(CoreHookRegistry::new(Arc::new(
                    ConfigWatcher::with_snapshot(snapshot),
                ))),
                Arc::new(HookAdapterFactory::new()),
                CapabilityExtensionRegistry::new(),
                verifier,
            );
            serve_hook_plugin(server).await
        }

        /// 与网内探针的触发请求同形：PreSend，请求体自报租户与用户。
        fn pre_send_call(tenant: &str, user: &str, marker: &str) -> GenericRequest {
            let inner = PreSendHookRequest {
                context: Some(HookInvocationContext {
                    conversation_id: "conv-1".into(),
                    conversation_type: "single".into(),
                    tenant_id: tenant.into(),
                    operator_user_id: user.into(),
                    request_id: "req-body".into(),
                    ..Default::default()
                }),
                draft: Some(HookMessageDraft {
                    client_message_id: marker.into(),
                    conversation_id: "conv-1".into(),
                    payload: b"hello".to_vec(),
                    ..Default::default()
                }),
            };
            GenericRequest {
                operation: "flare.hook.v1.pre_send".into(),
                metadata: HashMap::new(),
                payload: Some(prost_types::Any {
                    type_url: "type.googleapis.com/flare.capability.v1.PreSendHookRequest".into(),
                    value: inner.encode_to_vec(),
                }),
                request_id: "req-body".into(),
            }
        }

        fn token(keys: &CellKeys, actor: SignedActorType, id: &str, tenant: &str) -> String {
            keys.signer
                .sign(&SignedContextIdentity::new(actor, id, tenant, "req-signed"))
                .unwrap()
        }

        async fn raw_call(
            endpoint: &str,
            body: GenericRequest,
            token: Option<String>,
        ) -> Result<GenericResponse, Status> {
            let mut client = HookPluginClient::connect(endpoint.to_string())
                .await
                .unwrap();
            let mut request = Request::new(body);
            if let Some(token) = token {
                request
                    .metadata_mut()
                    .insert("x-flare-ctx", token.parse().unwrap());
            }
            client.call(request).await.map(Response::into_inner)
        }

        fn allowed(response: &GenericResponse) -> bool {
            let payload = response.payload.as_ref().unwrap();
            response.ok
                && PreSendHookResponse::decode(payload.value.as_slice())
                    .unwrap()
                    .allow
        }

        /// 无签名的调用不执行任何 Hook。修复前 HookPlugin 只套 `ContextLayer::allow_missing`，
        /// 这里得到业务 `OK` 并按请求体的租户外发 Webhook。
        #[tokio::test]
        async fn unsigned_calls_are_unauthenticated_and_deliver_nothing() {
            let receiver = WebhookReceiver::start().await;
            let endpoint = capability(two_tenants(&receiver), Some(keys().verifier)).await;
            let status = raw_call(
                &endpoint,
                pre_send_call("tenant-a", "u1", "m-unsigned"),
                None,
            )
            .await
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert_eq!(receiver.count(), 0);
        }

        #[tokio::test]
        async fn other_keys_and_a_missing_verifier_are_unauthenticated() {
            let receiver = WebhookReceiver::start().await;
            let endpoint = capability(two_tenants(&receiver), Some(keys().verifier)).await;
            let forged = token(&cell_keys(82), SignedActorType::User, "u1", "tenant-a");
            let status = raw_call(
                &endpoint,
                pre_send_call("tenant-a", "u1", "m-forged"),
                Some(forged),
            )
            .await
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");

            let unconfigured = capability(two_tenants(&receiver), None).await;
            let signed = token(&keys(), SignedActorType::User, "u1", "tenant-a");
            let status = raw_call(
                &unconfigured,
                pre_send_call("tenant-a", "u1", "m-unconfigured"),
                Some(signed),
            )
            .await
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert_eq!(receiver.count(), 0);
        }

        /// 签名是租户 a 的用户 u1：请求体换成租户 b 或别的用户都被拒，PreSend 也不收服务身份。
        #[tokio::test]
        async fn a_body_that_disagrees_with_the_signature_is_denied() {
            let receiver = WebhookReceiver::start().await;
            let endpoint = capability(two_tenants(&receiver), Some(keys().verifier)).await;
            let signed = || Some(token(&keys(), SignedActorType::User, "u1", "tenant-a"));
            for body in [
                pre_send_call("tenant-b", "u1", "m-other-tenant"),
                pre_send_call("tenant-a", "u2", "m-other-user"),
            ] {
                let status = raw_call(&endpoint, body, signed()).await.unwrap_err();
                assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
            }
            let service = token(&keys(), SignedActorType::Service, "svc", "tenant-a");
            let status = raw_call(
                &endpoint,
                pre_send_call("tenant-a", "u1", "m-svc"),
                Some(service),
            )
            .await
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert_eq!(receiver.count(), 0);
        }

        /// 正确签名的调用通过；空的请求体租户按签名租户执行（不落到 `"0"`）。
        #[tokio::test]
        async fn a_signed_call_runs_the_signed_tenants_hooks() {
            let receiver = WebhookReceiver::start().await;
            let endpoint = capability(two_tenants(&receiver), Some(keys().verifier)).await;
            let response = raw_call(
                &endpoint,
                pre_send_call("", "", "m-signed"),
                Some(token(&keys(), SignedActorType::User, "u1", "tenant-b")),
            )
            .await
            .unwrap();
            assert!(allowed(&response));
            let mut paths: Vec<_> = receiver
                .wait_for("m-signed", 2)
                .await
                .into_iter()
                .map(|delivery| {
                    assert_eq!(delivery.tenant(), Some("tenant-b"));
                    delivery.path
                })
                .collect();
            paths.sort();
            assert_eq!(paths, ["/b", "/global"]);
        }

        /// 按 Message Ingest 的方式调用：每个租户的事件只投到本租户登记的 Webhook，外加全局的。
        /// 修复前进程只有一份不分租户的配置，且多租户进程根本不加载租户登记的行。
        #[tokio::test]
        async fn each_tenant_runs_only_its_own_webhooks_and_the_global_ones() {
            let receiver = WebhookReceiver::start().await;
            let endpoint = capability(two_tenants(&receiver), Some(keys().verifier)).await;
            for (tenant, own, other) in [("tenant-a", "/a", "/b"), ("tenant-b", "/b", "/a")] {
                let marker = format!("m-{tenant}");
                let ctx = signed_ctx(&keys(), SignedActorType::User, "u1", tenant);
                assert!(matches!(
                    ingest_pre_send(&endpoint, &ctx, &marker).await,
                    PreSendDecision::Continue
                ));
                receiver.wait_for(&marker, 2).await;
                // 多等一会儿，确认没有第三次（投到别的租户）的投递。
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let deliveries = receiver.with_marker(&marker);
                assert_eq!(deliveries.len(), 2, "{deliveries:?}");
                assert!(
                    deliveries.iter().all(|d| d.tenant() == Some(tenant)),
                    "{deliveries:?}"
                );
                assert!(deliveries.iter().any(|d| d.path == own), "{deliveries:?}");
                assert!(
                    deliveries.iter().any(|d| d.path == "/global"),
                    "{deliveries:?}"
                );
                assert!(deliveries.iter().all(|d| d.path != other), "{deliveries:?}");
            }
            let ctx = signed_ctx(&keys(), SignedActorType::User, "u1", "tenant-c");
            assert!(matches!(
                ingest_pre_send(&endpoint, &ctx, "m-tenant-c").await,
                PreSendDecision::Continue
            ));
            receiver.wait_for("m-tenant-c", 1).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let deliveries = receiver.with_marker("m-tenant-c");
            assert_eq!(deliveries.len(), 1, "{deliveries:?}");
            assert_eq!(deliveries[0].path, "/global");
        }
    }

    #[test]
    fn maps_proto_conversation_types_to_hook_wire_names() {
        let cases = [
            (ConversationType::Unspecified, None),
            (ConversationType::Single, Some("single")),
            (ConversationType::Group, Some("group")),
            (ConversationType::Ai, Some("ai")),
            (ConversationType::System, Some("system")),
            (ConversationType::Customer, Some("customer")),
            (ConversationType::Temp, Some("temp")),
            (ConversationType::Channel, Some("channel")),
            (ConversationType::Broadcast, Some("broadcast")),
        ];

        for (conversation_type, expected) in cases {
            assert_eq!(
                proto_conversation_type_wire_name(conversation_type as i32),
                expected
            );
        }
    }

    #[test]
    fn maps_unknown_proto_conversation_type_to_unspecified_label() {
        assert_eq!(
            proto_conversation_type_wire_name(i32::MAX),
            Some("unspecified")
        );
    }
}
