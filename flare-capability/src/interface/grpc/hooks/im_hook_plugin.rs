//! `HookPlugin` gRPC：IM 主链与周边事件（`flare.capability.v1.HookPlugin.Call`）。

use flare_grpc_proto::capability::hook_plugin_server::HookPlugin;
use flare_grpc_proto::capability::{
    ConversationLifecycleHookRequest, ConversationLifecycleHookResponse,
    ConversationMemberHookRequest, ConversationMemberHookResponse, CustomHookRequest,
    CustomHookResponse, DeliveryHookRequest, DeliveryHookResponse, GenericRequest, GenericResponse,
    HookDeliveryEvent, HookInvocationContext, HookMessageRecord, HookRecallEvent,
    MessageReactionHookRequest, MessageReactionHookResponse, MessageReadHookRequest,
    MessageReadHookResponse, PostSendHookRequest, PostSendHookResponse, PreSendHookRequest,
    PreSendHookResponse, PresenceHookRequest, PresenceHookResponse, PushDeliveryHookRequest,
    PushDeliveryHookResponse, PushPostSendHookRequest, PushPostSendHookResponse,
    PushPreSendHookRequest, PushPreSendHookResponse, RecallHookRequest, RecallHookResponse,
    UserLoginHookRequest, UserLoginHookResponse, UserLogoutHookRequest, UserLogoutHookResponse,
    UserOfflineHookRequest, UserOfflineHookResponse, UserOnlineHookRequest, UserOnlineHookResponse,
};
use flare_server_core::error::Result as FlareResult;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use std::sync::Arc;
use tonic::{Request, Response, Status};

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
use flare_server_core::context::Context;

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
pub struct ImHookPluginServer {
    command_handler: Arc<HookCommandHandler>,
    registry: Arc<CoreHookRegistry>,
    adapter_factory: Arc<HookAdapterFactory>,
    capability_registry: CapabilityExtensionRegistry,
}

impl ImHookPluginServer {
    pub fn new(
        command_handler: Arc<HookCommandHandler>,
        registry: Arc<CoreHookRegistry>,
        adapter_factory: Arc<HookAdapterFactory>,
        capability_registry: CapabilityExtensionRegistry,
    ) -> Self {
        Self {
            command_handler,
            registry,
            adapter_factory,
            capability_registry,
        }
    }

    /// 将 protobuf HookInvocationContext 转换为 flare_server_core::Context
    fn proto_to_context(proto: &HookInvocationContext) -> Context {
        crate::infrastructure::adapters::conversion::proto_to_context(proto)
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
    pub async fn grpc_invoke_pre_send(
        &self,
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
        let ctx = Self::proto_to_context(&context);
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
            .get_pre_send_hooks()
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

    pub async fn grpc_invoke_post_send(
        &self,
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
        let ctx = Self::proto_to_context(&context);
        let message_record = Self::proto_to_message_record(&record)
            .map_err(|e| Status::invalid_argument(format!("Invalid record: {}", e)))?;
        let message_draft = proto_to_message_draft(&draft);

        // 获取PostSend Hook列表
        let hooks = self
            .registry
            .get_post_send_hooks()
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
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        Ok(Response::new(PostSendHookResponse {
            success: true,
            routing: None,
            outcome_extensions: None,
            error_code: String::new(),
            error_message: String::new(),
        }))
    }

    pub async fn grpc_notify_delivery(
        &self,
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
        let ctx = Self::proto_to_context(&context);
        let delivery_event = Self::proto_to_delivery_event(&event)
            .map_err(|e| Status::invalid_argument(format!("Invalid event: {}", e)))?;

        // 获取Delivery Hook列表
        let hooks = self
            .registry
            .get_delivery_hooks()
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
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        Ok(Response::new(DeliveryHookResponse {
            success: true,
            outcome_extensions: None,
        }))
    }

    pub async fn grpc_notify_recall(
        &self,
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
        let ctx = Self::proto_to_context(&context);
        let recall_event = Self::proto_to_recall_event(&event)
            .map_err(|e| Status::invalid_argument(format!("Invalid event: {}", e)))?;

        // 获取Recall Hook列表
        let hooks = self
            .registry
            .get_recall_hooks()
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
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        recall_response(decision).map(Response::new)
    }

    pub async fn grpc_notify_conversation_lifecycle(
        &self,
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
        let ctx = Self::proto_to_context(&context);

        // 获取ConversationLifecycle Hook列表
        let hooks = self
            .registry
            .get_conversation_lifecycle_hooks()
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

    pub async fn grpc_notify_presence(
        &self,
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
        let ctx = Self::proto_to_context(&context);

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

    pub async fn grpc_invoke_custom(
        &self,
        request: Request<CustomHookRequest>,
    ) -> Result<Response<CustomHookResponse>, Status> {
        let req = request.into_inner();
        let context = req
            .context
            .ok_or_else(|| Status::invalid_argument("context is required"))?;
        let hook_type = req.r#type;
        let _payload = req.payload;

        // 转换为内部类型
        let ctx = Self::proto_to_context(&context);

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

    pub async fn grpc_invoke_push_pre_send(
        &self,
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
        let _ctx = Self::proto_to_context(&context);

        // 获取PushPreSend Hook列表
        let hooks = self
            .registry
            .get_push_pre_send_hooks()
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

    pub async fn grpc_invoke_push_post_send(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取PushPostSend Hook列表
        let hooks = self
            .registry
            .get_push_post_send_hooks()
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

    pub async fn grpc_notify_push_delivery(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取PushDelivery Hook列表
        let hooks = self
            .registry
            .get_push_delivery_hooks()
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

    pub async fn grpc_notify_user_login(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取UserLogin Hook列表
        let hooks = self
            .registry
            .get_user_login_hooks()
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

    pub async fn grpc_notify_user_logout(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取UserLogout Hook列表
        let hooks = self
            .registry
            .get_user_logout_hooks()
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

    pub async fn grpc_notify_user_online(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取UserOnline Hook列表
        let hooks = self
            .registry
            .get_user_online_hooks()
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

    pub async fn grpc_notify_user_offline(
        &self,
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
        let _ctx = Self::proto_to_context(&_context);

        // 获取UserOffline Hook列表
        let hooks = self
            .registry
            .get_user_offline_hooks()
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

    pub async fn grpc_on_message_read(
        &self,
        request: Request<MessageReadHookRequest>,
    ) -> Result<Response<MessageReadHookResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(MessageReadHookResponse {
            success: true,
            ..Default::default()
        }))
    }

    pub async fn grpc_on_message_reaction(
        &self,
        request: Request<MessageReactionHookRequest>,
    ) -> Result<Response<MessageReactionHookResponse>, Status> {
        let _ = request.into_inner();
        Ok(Response::new(MessageReactionHookResponse {
            allow: true,
            ..Default::default()
        }))
    }

    pub async fn grpc_on_conversation_member(
        &self,
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
                .grpc_invoke_pre_send(Request::new(inner))
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
                .grpc_invoke_post_send(Request::new(inner))
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
                .grpc_notify_delivery(Request::new(inner))
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
                .grpc_notify_recall(Request::new(inner))
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
                .grpc_on_message_read(Request::new(inner))
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
                .grpc_on_message_reaction(Request::new(inner))
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
                .grpc_notify_conversation_lifecycle(Request::new(inner))
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
                .grpc_on_conversation_member(Request::new(inner))
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
                .grpc_notify_presence(Request::new(inner))
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
                .grpc_invoke_custom(Request::new(inner))
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
                .grpc_invoke_push_pre_send(Request::new(inner))
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
                .grpc_invoke_push_post_send(Request::new(inner))
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
                .grpc_notify_push_delivery(Request::new(inner))
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
                .grpc_notify_user_login(Request::new(inner))
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
                .grpc_notify_user_logout(Request::new(inner))
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
                .grpc_notify_user_online(Request::new(inner))
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
                .grpc_notify_user_offline(Request::new(inner))
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
        use ed25519_dalek::SigningKey;
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        use flare_grpc_proto::capability::hook_plugin_server::HookPluginServer;
        use flare_grpc_proto::capability::{GenericResponse, HookMessageDraft};
        use flare_im_hooks::hooks::adapters::GrpcHookFactory;
        use flare_im_hooks::{MessageDraft, PreSendHook, is_pre_send_rejection};
        use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
        use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
        use flare_server_core::{SignedContextVerifier, error::FlareError};
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

        /// Capability 的 HookPlugin 服务，PreSend 只挂 `hook` 一条。
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
            );
            serve(server).await
        }

        /// Message Ingest 调 Hook 时的上下文：Cell 签名的用户身份（gRPC Hook 适配器要求）。
        fn signed_user_ctx() -> flare_server_core::context::Ctx {
            let key = SigningKey::from_bytes(&[61; 32]);
            let private = key.to_pkcs8_pem(Default::default()).unwrap();
            let public = key
                .verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap();
            let signer = SignedContextSigner::from_private_key(&private).unwrap();
            let verifier = SignedContextVerifier::from_public_key(&public).unwrap();
            let token = signer
                .sign(&SignedContextIdentity::new(
                    SignedActorType::User,
                    "u1",
                    "0",
                    "req-hook-unavailable",
                ))
                .unwrap();
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("x-flare-ctx", token.parse().unwrap());
            verify_signed_context_metadata(&metadata, &verifier).unwrap()
        }

        /// 以 Message Ingest 的 gRPC Hook 适配器调 Capability 的 PreSend。
        async fn ingest_pre_send(capability: &str) -> PreSendDecision {
            let hook = GrpcHookFactory::new().build_pre_send(HashMap::new(), capability.into());
            let mut draft = MessageDraft::new(b"hello".to_vec());
            hook.handle(&signed_user_ctx(), &mut draft).await
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
