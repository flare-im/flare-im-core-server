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
use crate::domain::model::HookExecutionPlan;
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

        // 转换响应
        let response = match decision {
            PreSendDecision::Continue => PreSendHookResponse {
                allow: true,
                draft: Some(draft),
                routing: None,
                annotations: std::collections::HashMap::new(),
                outcome_extensions: None,
                deny_reason_code: String::new(),
                deny_reason_message: String::new(),
            },
            PreSendDecision::Reject { error } => {
                let (deny_reason_code, deny_reason_message) = match error {
                    flare_server_core::error::FlareError::Localized {
                        reason, details, ..
                    } => (reason, details.unwrap_or_default()),
                    other => ("HOOK_REJECTED".to_string(), other.to_string()),
                };
                PreSendHookResponse {
                    allow: false,
                    draft: None,
                    routing: None,
                    annotations: std::collections::HashMap::new(),
                    outcome_extensions: None,
                    deny_reason_code,
                    deny_reason_message,
                }
            }
        };

        Ok(Response::new(response))
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

        // 转换响应
        let response = match decision {
            PreSendDecision::Continue => RecallHookResponse {
                allow: true,
                routing: None,
                annotations: std::collections::HashMap::new(),
                outcome_extensions: None,
                deny_reason_code: String::new(),
                deny_reason_message: String::new(),
            },
            PreSendDecision::Reject { .. } => RecallHookResponse {
                allow: false,
                routing: None,
                annotations: std::collections::HashMap::new(),
                outcome_extensions: None,
                deny_reason_code: String::new(),
                deny_reason_message: String::new(),
            },
        };

        Ok(Response::new(response))
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
