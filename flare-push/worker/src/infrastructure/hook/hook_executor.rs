//! Hook 执行器 - 负责调用 flare-hook-engine 执行 Hook

use std::sync::Arc;
use std::time::Duration;

use flare_im_core::hooks::{DeliveryEvent, MessageDraft, MessageRecord};
use flare_server_core::context::{Context, ContextExt};
use flare_proto::hooks::hook_extension_client::HookExtensionClient;
use flare_proto::hooks::{
    DeliveryHookRequest, DeliveryHookResponse, HookDeliveryEvent as ProtoHookDeliveryEvent,
    HookInvocationContext, PushPostSendHookRequest, PushPostSendHookResponse,
    PushPreSendHookRequest, PushPreSendHookResponse,
};
use flare_server_core::error::Result;
use tracing::{debug, error, instrument, warn};

/// Hook 执行器 - 封装 flare-hook-engine 客户端
///
/// 架构设计：
/// - 通过 gRPC 调用 flare-hook-engine
/// - 支持多种 Hook 类型：PreSend、PostSend、Delivery 等
/// - 提供降级策略，保证推送流程不被 Hook 阻塞
/// - 支持超时控制和错误处理
pub struct HookExecutor {
    client: Option<Arc<HookExtensionClient<tonic::transport::Channel>>>,
    timeout: Duration,
}

impl HookExecutor {
    pub fn new(client: Option<HookExtensionClient<tonic::transport::Channel>>) -> Self {
        Self {
            client: client.map(Arc::new),
            timeout: Duration::from_secs(5), // 默认5秒超时
        }
    }

    /// 设置超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 检查 Hook 引擎是否可用
    pub fn is_available(&self) -> bool {
        self.client.is_some()
    }

    /// 构建 Hook 调用上下文
    fn build_hook_context(&self, ctx: &Context) -> HookInvocationContext {
        use flare_im_core::hooks::hook_context_data::get_hook_context_data;
        
        let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
        
        HookInvocationContext {
            conversation_id: hook_data.conversation_id.clone().unwrap_or_default(),
            conversation_type: hook_data.conversation_type.clone().unwrap_or_default(),
            corridor: hook_data
                .attributes
                .get("corridor")
                .cloned()
                .unwrap_or_else(|| "messaging".to_string()),
            tags: hook_data.tags.clone(),
            attributes: hook_data.attributes.clone(),
        }
    }

    /// 转换领域模型到 Proto 模型
    fn delivery_event_to_proto(event: &DeliveryEvent) -> ProtoHookDeliveryEvent {
        ProtoHookDeliveryEvent {
            message_id: event.message_id.clone(),
            user_id: event.user_id.clone(),
            channel: event.channel.clone(),
            delivered_at: Some(prost_types::Timestamp {
                seconds: event
                    .delivered_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                nanos: 0,
            }),
            metadata: event.metadata.clone(),
        }
    }

    /// 执行 PostDelivery Hook
    ///
    /// 策略：
    /// 1. 优先调用 Hook 引擎
    /// 2. Hook 引擎不可用或超时时，降级为日志记录
    /// 3. Hook 失败不影响推送结果，只记录错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %event.message_id,
        user_id = %event.user_id
    ))]
    pub async fn post_delivery(&self, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        ctx.ensure_not_cancelled().map_err(|e| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InternalError,
                "Request cancelled",
            )
            .details(e.to_string())
            .build_error()
        })?;
        if let Some(client) = &self.client {
            let hook_context = self.build_hook_context(ctx);
            let delivery_event = Self::delivery_event_to_proto(event);

            let request = DeliveryHookRequest {
                context: Some(hook_context),
                event: Some(delivery_event),
            };

            // 创建新的客户端实例来避免借用问题
            let endpoint = "http://localhost:50051"; // 这应该从配置中获取
            match HookExtensionClient::connect(endpoint).await {
                Ok(mut new_client) => {
                    // 调用 Hook 引擎，设置超时
                    match tokio::time::timeout(self.timeout, new_client.notify_delivery(request))
                        .await
                    {
                        Ok(Ok(response)) => {
                            let response = response.into_inner();

                            // 检查 Hook 执行结果
                            if let Some(status) = response.status {
                                if status.code != 0 {
                                    // 0 = Success
                                    warn!(
                                        "PostDelivery hook failed: {} (code: {})",
                                        status.message, status.code
                                    );
                                } else {
                                    debug!("PostDelivery hook executed successfully");
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "PostDelivery hook gRPC call failed");
                        }
                        Err(_) => {
                            warn!("PostDelivery hook timed out after {:?}", self.timeout);
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to connect to Hook engine");
                }
            }
        } else {
            debug!("Hook engine not available, skipping PostDelivery hook");
        }

        // Hook 失败不影响主流程
        Ok(())
    }

    /// 执行 PushPreSend Hook
    ///
    /// 注意：这是同步 Hook，可能修改推送内容或拒绝推送
    /// 需要等待 Hook 执行完成才能继续推送
    #[instrument(skip(self, ctx, push_request), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = ?push_request.message.as_ref().map(|m| &m.server_id)
    ))]
    pub async fn push_pre_send(
        &self,
        ctx: &Context,
        push_request: &mut flare_proto::push::PushMessageRequest,
    ) -> Result<bool> {
        ctx.ensure_not_cancelled().map_err(|e| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InternalError,
                "Request cancelled",
            )
            .details(e.to_string())
            .build_error()
        })?;
        if let Some(client) = &self.client {
            let hook_context = self.build_hook_context(ctx);

            // 构建 HookPushDraft
            let hook_draft = flare_proto::hooks::HookPushDraft {
                task_id: String::new(), // 任务ID将在Hook引擎中生成
                user_id: push_request.user_ids.first().cloned().unwrap_or_default(),
                title: String::new(), // 标题需要从消息内容中提取
                content: push_request
                    .message
                    .as_ref()
                    .and_then(|m| m.content.as_ref())
                    .map(|c| format!("{:?}", c)) // 简单格式化内容
                    .unwrap_or_default(),
                channel: "push".to_string(), // 默认渠道
                message_id: push_request
                    .message
                    .as_ref()
                    .map(|m| m.server_id.clone())
                    .unwrap_or_default(),
                metadata: std::collections::HashMap::new(), // 暂时没有元数据
            };

            let request = PushPreSendHookRequest {
                context: Some(hook_context),
                draft: Some(hook_draft),
            };

            // 创建新的客户端实例来避免借用问题
            let endpoint = "http://localhost:50051"; // 这应该从配置中获取
            match HookExtensionClient::connect(endpoint).await {
                Ok(mut new_client) => {
                    // 调用 Hook 引擎，设置超时
                    match tokio::time::timeout(
                        self.timeout,
                        new_client.invoke_push_pre_send(request),
                    )
                    .await
                    {
                        Ok(Ok(response)) => {
                            let response = response.into_inner();

                            // 检查 Hook 执行结果
                            if let Some(status) = response.status {
                                if status.code != 0 {
                                    // 非 Success 状态
                                    warn!(
                                        "PushPreSend hook rejected: {} (code: {})",
                                        status.message, status.code
                                    );
                                    return Ok(false); // 拒绝推送
                                }
                            }

                            // 应用 Hook 修改的内容（如果有）
                            if let Some(modified_draft) = response.draft {
                                // 如果Hook修改了推送草稿，我们可以应用一些修改
                                // 例如更新消息内容或其他字段
                                debug!("PushPreSend hook modified push draft");
                            } else {
                                debug!("PushPreSend hook executed successfully");
                            }

                            Ok(true) // 允许推送
                        }
                        Ok(Err(e)) => {
                            warn!(error = %e, "PushPreSend hook gRPC call failed");
                            // 推送前 Hook 失败，为安全起见拒绝推送
                            Ok(false)
                        }
                        Err(_) => {
                            warn!("PushPreSend hook timed out after {:?}", self.timeout);
                            // 超时为安全起见拒绝推送
                            Ok(false)
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to connect to Hook engine");
                    // 连接失败，为安全起见拒绝推送
                    Ok(false)
                }
            }
        } else {
            debug!("Hook engine not available, allowing push");
            Ok(true) // Hook 引擎不可用，允许推送
        }
    }

    /// 执行 PushPostSend Hook
    ///
    /// 用于处理推送任务入队后的通知 Hook 调用
    #[instrument(skip(self, ctx, record, draft), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
    ))]
    pub async fn push_post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        ctx.ensure_not_cancelled().map_err(|e| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InternalError,
                "Request cancelled",
            )
            .details(e.to_string())
            .build_error()
        })?;
        if let Some(client) = &self.client {
            let hook_context = self.build_hook_context(ctx);

            // 构建 HookPushRecord
            let hook_record = flare_proto::hooks::HookPushRecord {
                task_id: record.message_id.clone(),
                user_id: record.sender_id.clone(),
                title: String::new(),        // 从record中无法直接提取标题
                content: String::new(),      // 从record中无法直接提取内容
                channel: "push".to_string(), // 默认渠道
                enqueued_at: Some(prost_types::Timestamp {
                    seconds: record
                        .persisted_at
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0),
                    nanos: 0,
                }),
                metadata: record.metadata.clone(),
            };

            // 构建 HookPushDraft
            let hook_draft = flare_proto::hooks::HookPushDraft {
                task_id: draft.message_id.clone().unwrap_or_default(),
                user_id: String::new(), // 从draft中无法直接提取用户ID
                title: String::new(),   // 从draft中无法直接提取标题
                content: String::from_utf8_lossy(&draft.payload).to_string(),
                channel: "push".to_string(), // 默认渠道
                message_id: draft.message_id.clone().unwrap_or_default(),
                metadata: draft.metadata.clone(),
            };

            let request = PushPostSendHookRequest {
                context: Some(hook_context),
                record: Some(hook_record),
                draft: Some(hook_draft),
            };

            // 异步调用，不等待结果
            let endpoint = "http://localhost:50051"; // 这应该从配置中获取
            tokio::spawn(async move {
                match HookExtensionClient::connect(endpoint).await {
                    Ok(mut new_client) => match new_client.invoke_push_post_send(request).await {
                        Ok(response) => {
                            let response = response.into_inner();
                            if let Some(status) = response.status {
                                if status.code != 0 {
                                    warn!(
                                        "PushPostSend hook failed: {} (code: {})",
                                        status.message, status.code
                                    );
                                } else {
                                    debug!("PushPostSend hook executed successfully");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "PushPostSend hook gRPC call failed");
                        }
                    },
                    Err(e) => {
                        warn!(error = %e, "Failed to connect to Hook engine");
                    }
                }
            });
        }

        Ok(())
    }
}
