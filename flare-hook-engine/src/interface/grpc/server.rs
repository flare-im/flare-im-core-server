//! # Hook引擎gRPC服务实现
//!
//! 实现HookExtension服务的所有gRPC接口

use anyhow::Result;
use flare_proto::common::{ErrorCode as ProtoErrorCode, ErrorContext, RpcStatus};
use flare_proto::hooks::hook_extension_server::HookExtension;
use flare_proto::hooks::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::application::handlers::HookCommandHandler;
use crate::domain::model::HookExecutionPlan;
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::adapters::conversion::{
    context_to_proto, delivery_event_to_proto, message_draft_to_proto,
    message_record_to_proto, proto_to_message_draft, proto_to_pre_send_decision,
    proto_to_recall_decision, recall_event_to_proto, timestamp_to_system_time,
};
use crate::service::registry::CoreHookRegistry;
use flare_im_core::{
    DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent,
};
use flare_server_core::context::Context;

/// HookExtension gRPC服务实现
pub struct HookExtensionServer {
    command_handler: Arc<HookCommandHandler>,
    registry: Arc<CoreHookRegistry>,
    adapter_factory: Arc<HookAdapterFactory>,
}

impl HookExtensionServer {
    pub fn new(
        command_handler: Arc<HookCommandHandler>,
        registry: Arc<CoreHookRegistry>,
        adapter_factory: Arc<HookAdapterFactory>,
    ) -> Self {
        Self {
            command_handler,
            registry,
            adapter_factory,
        }
    }

    /// 将 protobuf HookInvocationContext 转换为 flare_server_core::Context
    fn proto_to_context(proto: &HookInvocationContext) -> Context {
        crate::infrastructure::adapters::conversion::proto_to_context(proto)
    }

    /// 将 protobuf HookMessageRecord 转换为 MessageRecord
    fn proto_to_message_record(proto: &HookMessageRecord) -> Result<MessageRecord> {
        let message = proto
            .message
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Message is required in HookMessageRecord"))?;

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
            conversation_type: match message.conversation_type {
                v if v == flare_proto::common::ConversationType::Unspecified as i32 => None,
                1 => Some("single".to_string()),
                2 => Some("group".to_string()),
                3 => Some("channel".to_string()),
                _ => Some("unspecified".to_string()),
            },
            message_type: None,
            persisted_at,
            metadata: proto.metadata.clone(),
        })
    }

    /// 将 protobuf HookDeliveryEvent 转换为 DeliveryEvent
    fn proto_to_delivery_event(proto: &HookDeliveryEvent) -> Result<DeliveryEvent> {
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
    fn proto_to_recall_event(proto: &HookRecallEvent) -> Result<RecallEvent> {
        let recalled_at = proto
            .recalled_at
            .as_ref()
            .map(timestamp_to_system_time)
            .unwrap_or_else(std::time::SystemTime::now);

        Ok(RecallEvent {
            message_id: proto.message_id.clone(),
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
    ) -> Result<HookExecutionPlan> {
        let mut plan = HookExecutionPlan::from_hook_config(config.clone(), hook_type);

        // 如果配置已启用且不是 Local Plugin，创建适配器
        if config.enabled {
            if !matches!(
                config.transport,
                crate::domain::model::HookTransportConfig::Local { .. }
            ) {
                let adapter = self
                    .adapter_factory
                    .create_adapter(&config.transport)
                    .await?;
                plan = plan.with_adapter(adapter);
            }
        }

        Ok(plan)
    }

    /// 构建 RpcStatus
    fn build_rpc_status(code: i32, message: &str) -> RpcStatus {
        RpcStatus {
            code,
            message: message.to_string(),
            details: vec![],
            context: Some(ErrorContext {
                service: "hook-engine".to_string(),
                instance: "default".to_string(),
                region: String::new(),
                zone: String::new(),
                attributes: std::collections::HashMap::new(),
            }),
        }
    }

    /// 从 PreSendDecision 构建 RpcStatus
    fn decision_to_rpc_status(decision: &PreSendDecision) -> RpcStatus {
        match decision {
            PreSendDecision::Continue => Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK"),
            PreSendDecision::Reject { error } => {
                // 从错误中提取错误码，如果无法提取则使用默认值
                let code = ProtoErrorCode::FailedPrecondition as i32;
                Self::build_rpc_status(code, &error.to_string())
            }
        }
    }
}

#[tonic::async_trait]
impl HookExtension for HookExtensionServer {
    async fn invoke_pre_send(
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

        // 获取PreSend Hook列表
        let hooks = self
            .registry
            .get_pre_send_hooks()
            .await
            .map_err(|e| Status::internal(format!("Failed to get hooks: {}", e)))?;

        // 创建HookExecutionPlan（包含适配器）
        let mut execution_plans = Vec::new();
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
        let decision = self
            .command_handler
            .handle_pre_send(&ctx, &mut message_draft, execution_plans)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        // 更新 draft（如果被 Hook 修改）
        draft = message_draft_to_proto(&message_draft);

        // 转换响应
        let status = Self::decision_to_rpc_status(&decision);
        let response = match decision {
            PreSendDecision::Continue => PreSendHookResponse {
                allow: true,
                draft: Some(draft),
                status: Some(status),
                annotations: std::collections::HashMap::new(),
            },
            PreSendDecision::Reject { .. } => PreSendHookResponse {
                allow: false,
                draft: None,
                status: Some(status),
                annotations: std::collections::HashMap::new(),
            },
        };

        Ok(Response::new(response))
    }

    async fn invoke_post_send(
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
        let mut execution_plans = Vec::new();
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
        self.command_handler
            .handle_post_send(&ctx, &message_record, &message_draft, execution_plans)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        Ok(Response::new(PostSendHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_delivery(
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
        let mut execution_plans = Vec::new();
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
        self.command_handler
            .handle_delivery(&ctx, &delivery_event, execution_plans)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        Ok(Response::new(DeliveryHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_recall(
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
        let mut execution_plans = Vec::new();
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
        let decision = self
            .command_handler
            .handle_recall(&ctx, &recall_event, execution_plans)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute hooks: {}", e)))?;

        // 转换响应
        let status = Self::decision_to_rpc_status(&decision);
        let response = match decision {
            PreSendDecision::Continue => RecallHookResponse {
                allow: true,
                status: Some(status),
                annotations: std::collections::HashMap::new(),
            },
            PreSendDecision::Reject { .. } => RecallHookResponse {
                allow: false,
                status: Some(status),
                annotations: std::collections::HashMap::new(),
            },
        };

        Ok(Response::new(response))
    }

    async fn notify_conversation_lifecycle(
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
        let mut execution_plans = Vec::new();
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
        let conversation_id = get_hook_context_data(&ctx)
            .and_then(|d| d.conversation_id.as_ref());
        for plan in execution_plans {
            tracing::debug!(
                hook = %plan.name(),
                conversation_id = ?conversation_id,
                event_type = event.event,
                "Executing ConversationLifecycle hook"
            );
        }

        Ok(Response::new(ConversationLifecycleHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_presence(
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
        let conversation_id = get_hook_context_data(&ctx)
            .and_then(|d| d.conversation_id.as_ref());
        tracing::debug!(
            user_id = ?conversation_id,
            "Presence hook notification received"
        );

        Ok(Response::new(PresenceHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn invoke_custom(
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
        tracing::debug!(
            hook_type = %hook_type,
            tenant_id = %ctx.tenant_id().unwrap_or(""),
            "Custom hook invocation received"
        );

        Ok(Response::new(CustomHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn invoke_push_pre_send(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %draft.user_id,
                task_id = %draft.task_id,
                "Executing PushPreSend hook"
            );
        }

        Ok(Response::new(PushPreSendHookResponse {
            allow: true,
            draft: Some(draft),
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
            annotations: std::collections::HashMap::new(),
        }))
    }

    async fn invoke_push_post_send(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                "Executing PushPostSend hook"
            );
        }

        Ok(Response::new(PushPostSendHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_push_delivery(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %event.user_id,
                task_id = %event.task_id,
                channel = %event.channel,
                "Executing PushDelivery hook"
            );
        }

        Ok(Response::new(PushDeliveryHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_user_login(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserLogin hook"
            );
        }

        Ok(Response::new(UserLoginHookResponse {
            allow: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
            annotations: std::collections::HashMap::new(),
        }))
    }

    async fn notify_user_logout(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserLogout hook"
            );
        }

        Ok(Response::new(UserLogoutHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_user_online(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                "Executing UserOnline hook"
            );
        }

        Ok(Response::new(UserOnlineHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }

    async fn notify_user_offline(
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
        let mut execution_plans = Vec::new();
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
            tracing::debug!(
                hook = %plan.name(),
                user_id = %event.user_id,
                device_id = %event.device_id,
                reason = %event.reason,
                "Executing UserOffline hook"
            );
        }

        Ok(Response::new(UserOfflineHookResponse {
            success: true,
            status: Some(Self::build_rpc_status(ProtoErrorCode::Ok as i32, "OK")),
        }))
    }
}
