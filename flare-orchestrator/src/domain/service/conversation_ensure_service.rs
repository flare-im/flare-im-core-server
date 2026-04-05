//! 会话生成公共服务
//!
//! 提供统一的会话确保能力，被消息和事件处理共同使用。
//!
//! ## 设计
//! - 支持同步（gRPC）和异步（事件）两种模式
//! - 幂等性保证：多次调用不会重复创建
//! - 降级策略：失败后由 Storage Writer 兜底

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_core::Ctx;
use flare_server_core::context::Context;

use crate::error::Result;
use crate::config::SessionCreationMode;
use crate::domain::repository::ConversationClient;
use crate::infrastructure::rpc::ConversationRpcClient;

/// 会话生成服务
///
/// ## 职责
/// 1. 确保会话存在（不存在则创建）
/// 2. 支持同步/异步两种模式
/// 3. 提供降级策略
pub struct ConversationEnsureService {
    /// 会话仓储（可选，用于同步模式）
    conversation_repository: Option<Arc<ConversationClient>>,
    /// 会话生成模式
    session_creation_mode: SessionCreationMode,
    /// 事件发布器（用于异步模式）
    event_publisher: Option<Arc<dyn ConversationEventPublisher>>,
}

/// 会话事件发布器（用于异步模式）
/// 
/// ## Rust 2024 兼容性
/// 使用 `Pin<Box<dyn Future>>` 返回类型以支持 `dyn Trait`
pub trait ConversationEventPublisher: Send + Sync {
    /// 发布会话确保事件
    fn publish_conversation_ensure<'a>(
        &'a self,
        conversation_id: &'a str,
        tenant_id: &'a str,
        conversation_type: i32,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// 会话确保请求
pub struct ConversationEnsureRequest {
    pub conversation_id: String,
    pub conversation_type: i32,
    pub business_type: String,
    pub participants: Vec<String>,
    pub stored_channel_id: String,
    pub tenant_id: String,
}

impl ConversationEnsureService {
    /// 创建会话生成服务
    pub fn new(
        conversation_repository: Option<Arc<ConversationClient>>,
        session_creation_mode: SessionCreationMode,
        event_publisher: Option<Arc<dyn ConversationEventPublisher>>,
    ) -> Self {
        Self {
            conversation_repository,
            session_creation_mode,
            event_publisher,
        }
    }

    /// 确保会话存在
    ///
    /// ## 策略
    /// - Sync: 同步调用 Conversation 服务，强一致
    /// - Async: 发布 conversation.ensure 事件，最终一致
    ///
    /// ## 降级
    /// 失败后不阻塞消息发送，由 Storage Writer 兜底创建
    pub async fn ensure_conversation(
        &self,
        ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<()> {
        match self.session_creation_mode {
            SessionCreationMode::Sync => {
                self.ensure_conversation_sync(ctx, request).await
            }
            SessionCreationMode::Async => {
                self.ensure_conversation_async(ctx, request).await
            }
        }
    }

    /// 同步模式：调用 Conversation 服务
    async fn ensure_conversation_sync(
        &self,
        ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<()> {
        let Some(conversation_repo) = &self.conversation_repository else {
            tracing::debug!(
                conversation_id = %request.conversation_id,
                "Conversation repository not configured, skip sync ensure"
            );
            return Ok(());
        };

        // 构建上下文
        let mut ensure_ctx = (**ctx).clone();
        if ensure_ctx.tenant_id().is_none() {
            ensure_ctx = ensure_ctx.with_tenant_id(request.tenant_id.clone());
        }
        if ensure_ctx.request_id().is_empty() {
            use uuid::Uuid;
            let new_request_id = Uuid::new_v4().to_string();
            let trace_id = ensure_ctx.trace_id().to_string();
            ensure_ctx = Context::with_request_id(new_request_id);
            if !trace_id.is_empty() {
                ensure_ctx = ensure_ctx.with_trace_id(trace_id);
            }
            if let Some(t) = ctx.tenant_id() {
                ensure_ctx = ensure_ctx.with_tenant_id(t.to_string());
            } else {
                ensure_ctx = ensure_ctx.with_tenant_id(request.tenant_id.clone());
            }
        }

        // 调用会话服务（带超时）
        let ensure_result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            {
                use crate::domain::model::ConversationType;
                let conversation_type = ConversationType::from_proto(request.conversation_type);
                
                conversation_repo.ensure_conversation(
                    &ensure_ctx,
                    &request.conversation_id,
                    conversation_type,
                    &request.business_type,
                    request.participants.clone(),
                    request.stored_channel_id.clone(),
                )
            },
        )
        .await;

        match ensure_result {
            Ok(Ok(_)) => {
                tracing::debug!(
                    conversation_id = %request.conversation_id,
                    "Conversation ensured (sync)"
                );
                Ok(())
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e,
                    conversation_id = %request.conversation_id,
                    "Failed to ensure conversation (sync), Storage Writer will use UPSERT as fallback"
                );
                // 不返回错误，允许消息继续发送
                Ok(())
            }
            Err(_) => {
                tracing::warn!(
                    conversation_id = %request.conversation_id,
                    "Timeout ensuring conversation (2s), Storage Writer will use UPSERT as fallback"
                );
                // 不返回错误，允许消息继续发送
                Ok(())
            }
        }
    }

    /// 异步模式：发布 conversation.ensure 事件
    async fn ensure_conversation_async(
        &self,
        _ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<()> {
        let Some(publisher) = &self.event_publisher else {
            tracing::debug!(
                conversation_id = %request.conversation_id,
                "Event publisher not configured, skip async ensure"
            );
            return Ok(());
        };

        if let Err(e) = publisher
            .publish_conversation_ensure(
                &request.conversation_id,
                &request.tenant_id,
                request.conversation_type,
                &request.business_type,
                request.participants.clone(),
                request.stored_channel_id.clone(),
            )
            .await
        {
            tracing::warn!(
                error = %e,
                conversation_id = %request.conversation_id,
                "Failed to publish conversation.ensure event (async), Conversation service may create on demand"
            );
        } else {
            tracing::debug!(
                conversation_id = %request.conversation_id,
                "Published conversation.ensure event (async)"
            );
        }

        Ok(())
    }
}

/// 辅助函数：从消息构建会话确保请求
pub fn build_conversation_ensure_request_from_message(
    message: &flare_proto::common::Message,
    tenant_id: &str,
) -> ConversationEnsureRequest {
    let mut participants = vec![message.sender_id.clone()];
    
    // 单聊时，channel_id 是对方 user_id
    if message.conversation_type == flare_proto::common::ConversationType::Single as i32
        && !message.channel_id.is_empty()
    {
        participants.push(message.channel_id.clone());
    }

    let business_type = message
        .extra
        .get("business_type")
        .cloned()
        .unwrap_or_default();
    let stored_channel_id = persisted_conversation_channel_id(
        message.conversation_type,
        message.channel_id.as_str(),
    );

    ConversationEnsureRequest {
        conversation_id: message.conversation_id.clone(),
        conversation_type: message.conversation_type,
        business_type,
        participants,
        stored_channel_id,
        tenant_id: tenant_id.to_string(),
    }
}

/// 辅助函数：从事件构建会话确保请求
pub fn build_conversation_ensure_request_from_event(
    event: &flare_proto::common::Event,
    tenant_id: &str,
    operator_id: &str,
) -> ConversationEnsureRequest {
    // 事件通常不需要创建会话，但为了统一接口，提供默认实现
    ConversationEnsureRequest {
        conversation_id: event.conversation_id.clone(),
        conversation_type: flare_proto::common::ConversationType::Single as i32,
        business_type: String::new(),
        participants: vec![operator_id.to_string()],
        stored_channel_id: String::new(),
        tenant_id: tenant_id.to_string(),
    }
}

/// 写入会话表 `channel_id`：单聊须空；群/频道等取消息 `channel_id`
fn persisted_conversation_channel_id(conversation_type: i32, message_channel_id: &str) -> String {
    match flare_proto::common::ConversationType::try_from(conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => String::new(),
        _ => message_channel_id.to_string(),
    }
}


