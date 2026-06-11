//! 消息发送链路的会话生成服务
//!
//! 提供统一的会话确保能力，被 `MessageIngestHandler` 在上行消息发送主链中使用。
//!
//! ## 设计
//! - 支持同步（gRPC）和异步（事件）两种模式
//! - 幂等性保证：多次调用不会重复创建

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::Ctx;
use flare_im_message_pipeline::rpc::ConversationRpcClient;
use flare_server_core::context::Context;
use moka::future::Cache;

use crate::config::SessionCreationMode;
use crate::domain::repository::ConversationClient;
use flare_server_core::error::{FlareError, Result};

/// 会话生成服务
///
/// ## 职责
/// 1. 确保会话存在（不存在则创建）
/// 2. 支持同步/异步两种模式
pub struct ConversationEnsureService {
    /// 会话仓储（可选，用于同步模式）
    conversation_repository: Option<Arc<ConversationClient>>,
    /// 会话生成模式
    session_creation_mode: SessionCreationMode,
    /// 事件发布器（用于异步模式）
    event_publisher: Option<Arc<dyn ConversationEventPublisher>>,
    /// 高频单聊 ensure 成功缓存，避免每条单聊消息都打 Conversation 服务或 MQ。
    ensure_cache: Option<Cache<String, ()>>,
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
        Self::with_cache(
            conversation_repository,
            session_creation_mode,
            event_publisher,
            100_000,
            Duration::from_secs(30),
        )
    }

    pub fn with_cache(
        conversation_repository: Option<Arc<ConversationClient>>,
        session_creation_mode: SessionCreationMode,
        event_publisher: Option<Arc<dyn ConversationEventPublisher>>,
        cache_capacity: u64,
        cache_ttl: Duration,
    ) -> Self {
        let ensure_cache = if cache_capacity == 0 || cache_ttl.is_zero() {
            None
        } else {
            Some(
                Cache::builder()
                    .max_capacity(cache_capacity)
                    .time_to_live(cache_ttl)
                    .build(),
            )
        };
        Self {
            conversation_repository,
            session_creation_mode,
            event_publisher,
            ensure_cache,
        }
    }

    /// 确保会话存在
    ///
    /// ## 策略
    /// - Sync: 同步调用 Conversation 服务，强一致
    /// - Async: 发布 conversation.ensure 事件，最终一致
    ///
    pub async fn ensure_conversation(
        &self,
        ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<()> {
        let cache_key = self.cache_key(request);
        if let (Some(cache), Some(key)) = (&self.ensure_cache, cache_key.as_ref())
            && cache.get(key).await.is_some()
        {
            tracing::trace!(
                conversation_id = %request.conversation_id,
                "Conversation ensure skipped by single-chat hot cache"
            );
            return Ok(());
        }

        let ensured = match self.session_creation_mode {
            SessionCreationMode::Sync => self.ensure_conversation_sync(ctx, request).await?,
            SessionCreationMode::Async => self.ensure_conversation_async(ctx, request).await?,
        };
        if ensured && let (Some(cache), Some(key)) = (&self.ensure_cache, cache_key) {
            cache.insert(key, ()).await;
        }
        Ok(())
    }

    fn cache_key(&self, request: &ConversationEnsureRequest) -> Option<String> {
        if request.conversation_type != flare_proto::common::ConversationType::Single as i32 {
            return None;
        }
        if request.conversation_id.trim().is_empty() || request.participants.len() < 2 {
            return None;
        }
        let mut participants = request.participants.clone();
        participants.retain(|id| !id.trim().is_empty());
        participants.sort();
        participants.dedup();
        if participants.len() < 2 {
            return None;
        }
        Some(format!(
            "{}|{}|{}|{}",
            request.tenant_id,
            request.conversation_id,
            request.business_type,
            participants.join(",")
        ))
    }

    /// 同步模式：调用 Conversation 服务
    async fn ensure_conversation_sync(
        &self,
        ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<bool> {
        let Some(conversation_repo) = &self.conversation_repository else {
            tracing::trace!(
                conversation_id = %request.conversation_id,
                "Conversation repository not configured, cannot sync ensure"
            );
            return Err(FlareError::system(
                "conversation repository not configured for sync ensure",
            ));
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
                ensure_ctx = ensure_ctx.with_tenant_id(t);
            } else {
                ensure_ctx = ensure_ctx.with_tenant_id(request.tenant_id.clone());
            }
        }

        // 调用会话服务（带超时）
        let ensure_result = tokio::time::timeout(std::time::Duration::from_secs(2), {
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
        })
        .await;

        match ensure_result {
            Ok(Ok(_)) => {
                tracing::trace!(
                    conversation_id = %request.conversation_id,
                    "Conversation ensured (sync)"
                );
                Ok(true)
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e,
                    conversation_id = %request.conversation_id,
                    "Failed to ensure conversation (sync), block message send"
                );
                Err(e)
            }
            Err(_) => {
                tracing::warn!(
                    conversation_id = %request.conversation_id,
                    "Timeout ensuring conversation (2s), block message send"
                );
                Err(FlareError::system(format!(
                    "timeout ensuring conversation {}",
                    request.conversation_id
                )))
            }
        }
    }

    /// 异步模式：发布 conversation.ensure 事件
    async fn ensure_conversation_async(
        &self,
        _ctx: &Ctx,
        request: &ConversationEnsureRequest,
    ) -> Result<bool> {
        let Some(publisher) = &self.event_publisher else {
            tracing::trace!(
                conversation_id = %request.conversation_id,
                "Event publisher not configured, skip async ensure"
            );
            return Ok(false);
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
                "Failed to publish conversation.ensure event (async), block message send"
            );
            Err(e)
        } else {
            tracing::trace!(
                conversation_id = %request.conversation_id,
                "Published conversation.ensure event (async)"
            );
            Ok(true)
        }
    }
}

/// 辅助函数：从消息构建会话确保请求
pub fn build_conversation_ensure_request_from_message(
    message: &flare_proto::common::Message,
    tenant_id: &str,
) -> ConversationEnsureRequest {
    let mut participants = vec![message.sender_id.clone()];

    // 单聊时，channel_id 是对方 user_id；如果客户端误传成自己，不能创建单人成员会话。
    if message.conversation_type == flare_proto::common::ConversationType::Single as i32
        && !message.channel_id.is_empty()
        && message.channel_id != message.sender_id
    {
        participants.push(message.channel_id.clone());
    }
    if message.conversation_type == flare_proto::common::ConversationType::Group as i32
        && let Some(member_ids) = message.attributes.get("group_member_ids")
    {
        participants.extend(parse_group_member_ids(member_ids));
    }
    participants.retain(|id| !id.trim().is_empty());
    participants.sort();
    participants.dedup();

    let business_type = message
        .attributes
        .get("business_type")
        .cloned()
        .unwrap_or_default();
    let stored_channel_id =
        persisted_conversation_channel_id(message.conversation_type, message.channel_id.as_str());

    ConversationEnsureRequest {
        conversation_id: message.conversation_id.clone(),
        conversation_type: message.conversation_type,
        business_type,
        participants,
        stored_channel_id,
        tenant_id: tenant_id.to_string(),
    }
}

fn parse_group_member_ids(raw: &str) -> Vec<String> {
    raw.split([',', ';', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// 写入会话表 `channel_id`：单聊须空；群/频道等取消息 `channel_id`
fn persisted_conversation_channel_id(conversation_type: i32, message_channel_id: &str) -> String {
    match flare_proto::common::ConversationType::try_from(conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => String::new(),
        _ => message_channel_id.to_string(),
    }
}
