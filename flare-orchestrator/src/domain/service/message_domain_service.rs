//! 消息领域服务 - 包含所有业务逻辑实现
//!
//! ## 会话生成（Session/Conversation Ensure）
//!
//! 若会话不存在则创建，支持两种模式（见 [crate::config::SessionCreationMode] 与 docs/SESSION_CREATION_DESIGN.md）：
//! - **Sync（默认）**：同步调用 Conversation 服务 ensure_conversation，强一致；失败/超时后继续发消息，Storage Writer 兜底。
//! - **Async**：发布 conversation.ensure 事件到 Kafka，由 Conversation 服务消费并幂等创建，低延迟、最终一致。

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as AnyhowContext, Result};
use flare_im_core::abstractions::builders::PushMessageRequestBuilder;
use flare_im_core::abstractions::decorator::{MessageDecorator, NoopMessageDecorator};
use flare_im_core::abstractions::storage_payload::StorageMessagePayload;
use flare_im_core::hooks::HookDispatcher;
use flare_im_core::tracing::create_span;
use flare_server_core::context::{Context, Ctx};
use flare_proto::push::{PushMessageRequest, PushOptions};
use flare_proto::common::Message;
use prost::Message as ProstMessage;
use tracing::{Span, instrument};

use crate::domain::model::MessageProfile;
use crate::domain::model::{MessageDefaults, MessageSubmission};
use crate::domain::repository::{
    ConversationRepository, ConversationRepositoryItem, OrchestratorPublisher, WalRepository,
    WalRepositoryItem,
};
use crate::domain::service::hook_builder::{
    build_hook_context_from_ctx,
    apply_draft_to_request, build_draft_from_request, build_hook_context, build_message_record,
    draft_from_submission, merge_context,
};
use crate::config::SessionCreationMode;
use crate::domain::service::message_publish_strategy::{MessagePublishStrategyRegistry, PublishContext};
use crate::domain::service::sequence_allocator::SequenceAllocator;

/// 消息领域服务 - 包含所有业务逻辑
pub struct MessageDomainService {
    publisher: Arc<OrchestratorPublisher>,
    wal_repository: Arc<WalRepositoryItem>,
    conversation_repository: Option<Arc<ConversationRepositoryItem>>,
    /// 序列号分配器（核心能力：保证同会话消息顺序）
    sequence_allocator: Arc<SequenceAllocator>,
    defaults: MessageDefaults,
    hooks: Arc<HookDispatcher>,
    /// 按消息类别可插拔的发布策略（Strategy 模式）
    publish_strategy_registry: MessagePublishStrategyRegistry,
    /// Decorator 模式：消息增强（已读标记、@提及等），默认 Noop
    message_decorator: Arc<dyn MessageDecorator>,
    /// 会话生成模式：Sync 同步 gRPC / Async 发布 conversation.ensure 事件
    session_creation_mode: SessionCreationMode,
}

impl MessageDomainService {
    pub fn new(
        publisher: Arc<OrchestratorPublisher>,
        wal_repository: Arc<WalRepositoryItem>,
        conversation_repository: Option<Arc<ConversationRepositoryItem>>,
        sequence_allocator: Arc<SequenceAllocator>,
        defaults: MessageDefaults,
        hooks: Arc<HookDispatcher>,
        message_decorator: Option<Arc<dyn MessageDecorator>>,
        session_creation_mode: SessionCreationMode,
    ) -> Self {
        Self {
            publisher,
            wal_repository,
            conversation_repository,
            sequence_allocator,
            defaults,
            hooks,
            publish_strategy_registry: MessagePublishStrategyRegistry::new(),
            message_decorator: message_decorator.unwrap_or_else(|| Arc::new(NoopMessageDecorator)),
            session_creation_mode,
        }
    }

    /// 编排消息存储流程（业务逻辑）
    /// 按照"PreSend Hook → WAL → Kafka → PostSend Hook"的顺序编排消息写入流程；请求为 common.Message，envelope 在 extra。
    #[instrument(skip(self), fields(tenant_id, message_id, message_type))]
    pub async fn orchestrate_message_storage(
        &self,
        ctx: &Ctx,
        mut request: Message,
        execute_pre_send: bool,
    ) -> Result<(String, u64)> {
        let _start = Instant::now();
        let _span = Span::current();
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        if !request.server_id.is_empty() {
            _span.record("message_id", &request.server_id);
        }
        if !request.sender_id.is_empty() {}
        _span.record("message_type", request.message_type);

        // 从Context构建hook_context（确保tenant_id从Context获取）
        let original_context = build_hook_context_from_ctx(ctx, &request);
        let mut draft =
            build_draft_from_request(&request).with_context(|| "Failed to build draft from request")?;

        // 执行 PreSend Hook（如果启用）
        if execute_pre_send {
            let _hook_span = create_span("message-orchestrator", "pre_send_hook");

            self.hooks
                .pre_send(&original_context, &mut draft)
                .await
                .with_context(|| "PreSend hook failed")?;

            // 让 _hook_span 离开作用域以结束 span

            apply_draft_to_request(&mut request, &draft);
        }

        let updated_context =
            build_hook_context(&request, self.defaults.default_tenant_id.as_ref());
        let hook_context = merge_context(&original_context, updated_context);

        let submission = MessageSubmission::prepare(request, &self.defaults)
            .context("Failed to prepare message")?;

        // 🔹 核心能力：分配 session_seq（保证消息顺序）
        // 参考微信 MsgService 设计：每个会话维护独立的递增序列号
        let session_seq = self
            .sequence_allocator
            .allocate_seq(&submission.message.conversation_id, &tenant_id)
            .await
            .with_context(|| {
                format!(
                    "allocate seq failed for conversation_id={}",
                    submission.message.conversation_id
                )
            })?;
        tracing::debug!(
            conversation_id = %submission.message.conversation_id,
            seq = session_seq,
            "Allocated session sequence"
        );

        let mut submission = submission;
        submission.message.seq = session_seq;
        submission.kafka_payload.seq = session_seq;

        // 获取消息类型信息（用于判断是否需要持久化）
        // 注意：MessageProfile::ensure 会修改 message，所以需要 clone
        let mut message_for_profile = submission.message.clone();
        let profile = MessageProfile::ensure(&mut message_for_profile);
        let processing_type = profile.processing_type(); // 保留变量名，因为在后面会使用

        let _message_type = match processing_type {
            // 添加下划线前缀表示故意未使用
            crate::domain::model::message_kind::MessageProcessingType::Normal => "normal",
            crate::domain::model::message_kind::MessageProcessingType::Notification => {
                "notification"
            }
        };

        // 仅普通消息需要写入WAL
        if profile.needs_wal() {
            let _wal_span = create_span("message-orchestrator", "wal_write");

            self.wal_repository
                .append(&submission)
                .await
                .context("Failed to append WAL entry")?;

            // 让 _wal_span 离开作用域以结束 span
        }

        // 会话生成：如会话不存在则创建（Sync 同步 gRPC / Async 发布 conversation.ensure 事件）
        let mut participants = vec![submission.message.sender_id.clone()];
        if submission.message.conversation_type == flare_proto::common::ConversationType::Single as i32
            && !submission.message.channel_id.is_empty()
        {
            participants.push(submission.message.channel_id.clone());
        }
        let conversation_id = submission.message.conversation_id.clone();
        let conversation_type =
            match flare_proto::common::ConversationType::try_from(submission.message.conversation_type) {
                Ok(st) => match st {
                    flare_proto::common::ConversationType::Single => "single".to_string(),
                    flare_proto::common::ConversationType::Group => "group".to_string(),
                    flare_proto::common::ConversationType::Channel => "channel".to_string(),
                    _ => "unknown".to_string(),
                },
                Err(_) => "unknown".to_string(),
            };
        let business_type = submission
            .message
            .extra
            .get("business_type")
            .cloned()
            .unwrap_or_default();

        match self.session_creation_mode {
            SessionCreationMode::Sync => {
                if let Some(conversation_repo) = &self.conversation_repository {
                    let mut ensure_ctx = (**ctx).clone();
                    if ensure_ctx.tenant_id().is_none() {
                        if let Some(tenant_id_from_message) = submission.message.extra.get("x-tenant-id").or_else(|| submission.message.extra.get("tenant_id")) {
                            ensure_ctx = ensure_ctx.with_tenant_id(tenant_id_from_message.clone());
                        } else {
                            ensure_ctx = ensure_ctx.with_tenant_id(tenant_id.clone());
                        }
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
                        } else if let Some(t) = submission.message.extra.get("x-tenant-id").or_else(|| submission.message.extra.get("tenant_id")) {
                            ensure_ctx = ensure_ctx.with_tenant_id(t.clone());
                        } else {
                            ensure_ctx = ensure_ctx.with_tenant_id(tenant_id.clone());
                        }
                    }
                    let ensure_result = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        conversation_repo.ensure_conversation(
                            &ensure_ctx,
                            &conversation_id,
                            &conversation_type,
                            &business_type,
                            participants.clone(),
                        ),
                    )
                    .await;
                    match ensure_result {
                        Ok(Ok(_)) => {
                            tracing::debug!(conversation_id = %conversation_id, "Conversation ensured (sync)");
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                error = %e,
                                conversation_id = %conversation_id,
                                "Failed to ensure conversation (sync), Storage Writer will use UPSERT as fallback"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                conversation_id = %conversation_id,
                                "Timeout ensuring conversation (2s), Storage Writer will use UPSERT as fallback"
                            );
                        }
                    }
                }
            }
            SessionCreationMode::Async => {
                let tenant_id_str = ctx
                    .tenant_id()
                    .or_else(|| submission.message.extra.get("x-tenant-id").or_else(|| submission.message.extra.get("tenant_id")).map(|s| s.as_str()))
                    .unwrap_or("0");
                if let Err(e) = self
                    .publisher
                    .publish_conversation_ensure(
                        &conversation_id,
                        tenant_id_str,
                        &conversation_type,
                        &business_type,
                        participants,
                    )
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        conversation_id = %conversation_id,
                        "Failed to publish conversation.ensure event (async), Conversation service may create on demand"
                    );
                } else {
                    tracing::debug!(
                        conversation_id = %conversation_id,
                        "Published conversation.ensure event (async)"
                    );
                }
            }
        }

        // Decorator 模式：对消息做增强（已读标记、@提及等）后再构建推送
        submission.message = self
            .message_decorator
            .decorate(submission.message.clone())
            .await
            .context("Message decorator failed")?;

        // 构建推送任务
        let push_request = self.build_push_request(&submission, &profile)?;

        // Strategy 模式（ARCHITECTURE_REFACTOR §2）：按 MessageProfile 的 category/processing_type 取策略并执行
        let _kafka_span = create_span("message-orchestrator", "kafka_produce");
        let category = profile.category();
        tracing::info!(
            message_id = %submission.message_id,
            message_type = submission.message.message_type,
            category = ?category,
            "准备按策略发布到事件总线"
        );
        let strategy = self
            .publish_strategy_registry
            .get(category, profile.processing_type());
        let storage_payload = StorageMessagePayload::from(&submission.kafka_payload);
        strategy
            .publish(PublishContext {
                request_ctx: ctx,
                publisher: self.publisher.as_ref(),
                storage_payload,
                push_request,
            })
            .await
            .context("Failed to publish message")?;

        // 让 _kafka_span 离开作用域以结束 span

        let record = build_message_record(&submission, &submission.message);
        let post_draft =
            draft_from_submission(&submission).context("Failed to build draft from submission")?;

        // 执行 PostSend Hook（使用hook_context，确保tenant_id正确）
        self.hooks
            .post_send(&hook_context, &record, &post_draft)
            .await
            .context("PostSend hook failed")?;

        Ok((submission.message_id, submission.message.seq))
    }

    /// 构建推送请求
    ///
    /// 优化：优先使用 receiver_id 和 channel_id，避免查询会话服务
    fn build_push_request(
        &self,
        submission: &MessageSubmission,
        profile: &MessageProfile,
    ) -> Result<PushMessageRequest> {
        // 提取接收者ID列表（优先使用 receiver_id 和 channel_id）
        let mut user_ids = Vec::new();

        if let Ok(conversation_type) =
            flare_proto::common::ConversationType::try_from(submission.message.conversation_type)
        {
            match conversation_type {
                flare_proto::common::ConversationType::Single => {
                    // 单聊：优先使用 receiver_id，性能最优
                    if !submission.message.channel_id.is_empty() {
                        user_ids.push(submission.message.channel_id.clone());
                        tracing::debug!(
                            "Single chat message using channel_id: conversation_id={}, sender_id={}, channel_id={}",
                            submission.message.conversation_id,
                            submission.message.sender_id,
                            submission.message.channel_id
                        );
                    } else {
                        // receiver_id 为空，降级到从 conversation_id 提取（向后兼容）
                        tracing::warn!(
                            "Single chat message missing receiver_id, falling back to conversation_id extraction. conversation_id={}, sender_id={}",
                            submission.message.conversation_id,
                            submission.message.sender_id
                        );
                        if let Some(participants) = self.extract_participants_from_conversation_id(
                            &submission.message.conversation_id,
                            &submission.message.sender_id,
                        ) {
                            user_ids = participants;
                        }
                    }
                }
                flare_proto::common::ConversationType::Group
                | flare_proto::common::ConversationType::Channel => {
                    // 群聊、频道：使用 conversation_id 查询成员，user_ids 留空由推送服务查询
                    tracing::debug!(
                        "Group/channel message. Push worker will query members. conversation_id={}",
                        submission.message.conversation_id
                    );
                }
                _ => {}
            }
        }

        // 克隆消息并清理字段，确保所有字符串字段都是有效的 UTF-8
        // 这是为了避免 Protobuf 解码错误
        let mut message_for_push = submission.message.clone();

        // 验证单聊时 channel_id（对方 user_id）在克隆后仍然存在
        if message_for_push.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            if message_for_push.channel_id.is_empty() {
                tracing::error!(
                    message_id = %message_for_push.server_id,
                    conversation_id = %message_for_push.conversation_id,
                    sender_id = %message_for_push.sender_id,
                    "Single chat message missing channel_id after clone"
                );
                anyhow::bail!(
                    "Single chat message must provide channel_id (receiver). message_id={}, conversation_id={}, sender_id={}",
                    message_for_push.server_id,
                    message_for_push.conversation_id,
                    message_for_push.sender_id
                );
            }
        }

        // 清理字符串字段，确保它们是有效的 UTF-8 字符串
        // 注意：新版 Message 结构已移除 sender_platform_id、sender_nickname、sender_avatar_url、group_id 等字段
        // 这些信息现在通过 attributes 或 extra 字段存储
        // 但 channel_id、conversation_id 等仍是 Message 的字段，必须保留
        message_for_push.client_msg_id =
            String::from_utf8_lossy(message_for_push.client_msg_id.as_bytes()).to_string();
        if let Some(rr) = message_for_push.extra.get("recall_reason").cloned() {
            message_for_push.extra.insert(
                "recall_reason".to_string(),
                String::from_utf8_lossy(rr.as_bytes()).to_string(),
            );
        }

        // 验证消息大小，防止异常大的消息
        // 先序列化消息以计算大小
        let message_bytes = message_for_push.encode_to_vec();
        const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB
        if message_bytes.len() > MAX_MESSAGE_SIZE {
            anyhow::bail!(
                "Message size {} bytes exceeds maximum allowed size {} bytes",
                message_bytes.len(),
                MAX_MESSAGE_SIZE
            );
        }

        // Builder 模式（ARCHITECTURE_REFACTOR §3）：使用 flare_im_core::abstractions::builders::PushMessageRequestBuilder 组装
        let push_options = PushOptions {
            require_online: profile.processing_type()
                == crate::domain::model::message_kind::MessageProcessingType::Notification,
            persist_if_offline: profile.processing_type()
                == crate::domain::model::message_kind::MessageProcessingType::Normal,
            priority: 5, // 默认优先级
            metadata: std::collections::HashMap::new(),
            channel: String::new(),
            mute_when_quiet: false,
        };

        Ok(PushMessageRequestBuilder::new()
            .user_ids(user_ids)
            .message(Some(message_for_push))
            .options(Some(push_options))
            .build())
    }

    /// 从会话ID中提取参与者
    ///
    /// 注意：新格式（1-{hash}）无法从哈希反推用户ID，需要查询会话服务获取参与者
    ///
    /// # 参数
    /// * `conversation_id` - 会话ID（格式：1-{hash}）
    /// * `sender_id` - 发送者ID（用于过滤）
    ///
    /// # 返回
    /// * `None` - 新格式无法直接解析，需要查询会话服务
    fn extract_participants_from_conversation_id(
        &self,
        _conversation_id: &str,
        _sender_id: &str,
    ) -> Option<Vec<String>> {
        // 新格式（1-{hash}）无法从哈希反推用户ID，返回None
        // 调用方需要查询会话服务获取参与者
        None
    }
}
