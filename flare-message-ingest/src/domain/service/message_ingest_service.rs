//! 消息摄入服务。
//!
//! ## 核心职责
//! 1. 消息校验（使用策略模式）
//! 2. 序列号分配（使用公共 SequenceAllocator）
//! 3. WAL 写入
//! 4. 消息装饰
//! 5. fanout 到存储流与推送流（使用 PushRepository）
//!
//! ## 设计原则
//! - 写入入口边界：承接 send/system send/WAL replay/storage fanout 的消息摄入主链
//! - 依赖注入：通过构造函数注入依赖
//! - 不包含 Hook 执行、会话 ensure、gRPC 适配或存储实现

use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_im_contracts::Ctx;
use flare_im_contracts::abstractions::decorator::{MessageDecorator, NoopMessageDecorator};
use flare_im_seq::SequenceAllocator;
use flare_proto::common::Message;
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::domain::model::{MessageDefaults, MessageSubmission};
use crate::domain::repository::{
    PushRepository, RecipientRepository, SeqFloorProvider, WalRepository, WalRepositoryItem,
    needs_member_lookup,
};
use crate::domain::{MessageProfile, PersistenceMode};
use flare_im_message_pipeline::MqPushRepository;
use flare_im_message_pipeline::{
    CompositeMessageValidationStrategy, MessageValidationStrategy, ValidationContext,
};
use flare_server_core::error::{ErrorCode, Result};

/// 消息摄入服务。
pub struct MessageIngestService {
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    push_repository: Arc<MqPushRepository>,
    /// 接收者仓储
    recipient_repository: Arc<dyn RecipientRepository>,
    /// WAL 仓储
    wal_repository: Arc<WalRepositoryItem>,
    /// 序列号分配器
    sequence_allocator: Arc<SequenceAllocator>,
    /// 默认值配置
    defaults: MessageDefaults,
    /// 消息装饰器
    message_decorator: Arc<dyn MessageDecorator>,
    /// 消息校验策略
    validation_strategy: Arc<dyn MessageValidationStrategy>,
    /// 会话持久化 max_seq 提供者（seq 回退自愈）。为 `None` 时退化为普通 `INCR` 分配。
    seq_floor_provider: Option<Arc<dyn SeqFloorProvider>>,
    /// 进程内 floor 校验状态（key = `tenant::conversation`）。
    ///
    /// 每个会话在本进程内**首次触达**时查一次持久化权威 max_seq 作 floor 修复；之后温路径
    /// 直接走快 `INCR`。查询失败进入退避期，期间不再重试（避免存储故障时每消息打点）。
    /// 有界缓存：逐出仅意味着该会话多付一次 floor 查询，语义安全。
    seq_floor_state: moka::future::Cache<String, SeqFloorState>,
}

/// 会话 floor 校验状态。
#[derive(Clone)]
enum SeqFloorState {
    /// 已完成 floor 校验，温路径走快 `INCR`。
    Checked,
    /// 上次校验失败的时刻；退避期内不重试。
    FailedAt(Instant),
}

/// floor 查询失败后的重试退避窗口。
const SEQ_FLOOR_FAILURE_BACKOFF: Duration = Duration::from_secs(30);
/// floor 查询 RPC 上限：超时即降级普通 `INCR`，发送主链绝不被存储黑洞拖死。
const SEQ_FLOOR_RPC_TIMEOUT: Duration = Duration::from_millis(800);
/// floor 状态缓存容量上限（约数百万会话进程的内存保险丝）。
const SEQ_FLOOR_CACHE_CAPACITY: u64 = 200_000;

#[derive(Default)]
pub struct MessageIngestServiceOptions {
    pub message_decorator: Option<Arc<dyn MessageDecorator>>,
    pub validation_strategy: Option<Arc<dyn MessageValidationStrategy>>,
    /// 会话持久化 max_seq 提供者；注入后启用 seq 回退自愈。
    pub seq_floor_provider: Option<Arc<dyn SeqFloorProvider>>,
}

impl MessageIngestServiceOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

/// seq 分配计划（纯决策，便于单测）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeqAllocPlan {
    /// 温路径：普通 `INCR`（已校验过 floor，或未配置 provider）。
    PlainIncr,
    /// 首次触达：查持久化权威 max_seq 作 floor 修复回退。
    FirstTouchFloor,
}

/// 决定本次 seq 分配走哪条路径。
///
/// - 未配置 provider → 永远 `PlainIncr`（保持既有行为，绝不比现状更差）。
/// - 本进程已对该会话做过 floor 校验 → `PlainIncr`（温路径快 INCR）。
/// - 否则 → `FirstTouchFloor`（首次触达查存储权威 max_seq）。
pub(crate) fn plan_seq_alloc(has_provider: bool, already_checked: bool) -> SeqAllocPlan {
    if has_provider && !already_checked {
        SeqAllocPlan::FirstTouchFloor
    } else {
        SeqAllocPlan::PlainIncr
    }
}

impl MessageIngestService {
    pub fn new(
        push_repository: Arc<MqPushRepository>,
        recipient_repository: Arc<dyn RecipientRepository>,
        wal_repository: Arc<WalRepositoryItem>,
        sequence_allocator: Arc<SequenceAllocator>,
        defaults: MessageDefaults,
        options: MessageIngestServiceOptions,
    ) -> Self {
        Self {
            push_repository,
            recipient_repository,
            wal_repository,
            sequence_allocator,
            defaults,
            message_decorator: options
                .message_decorator
                .unwrap_or_else(|| Arc::new(NoopMessageDecorator)),
            validation_strategy: options.validation_strategy.unwrap_or_else(|| {
                Arc::new(CompositeMessageValidationStrategy::default_composite())
            }),
            seq_floor_provider: options.seq_floor_provider,
            seq_floor_state: moka::future::Cache::new(SEQ_FLOOR_CACHE_CAPACITY),
        }
    }

    /// 校验消息
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `message`: 消息
    ///
    /// # 返回
    /// - `Ok(())`: 校验通过
    /// - `Err`: 校验失败
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_type = message.message_type,
    ))]
    pub async fn validate_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message: &Message,
    ) -> Result<()> {
        let validation_context = ValidationContext {
            ctx,
            tenant_id,
            conversation_id: &message.conversation_id,
        };

        let validation_result = self
            .validation_strategy
            .validate(&validation_context, message)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InvalidParameter,
                    &format!("Message validation failed: {}", e)
                )
            })?;

        if !validation_result.is_valid {
            return Err(flare_err_details!(
                ErrorCode::InvalidParameter,
                "Message validation failed",
                format!("{:?}", validation_result.errors)
            ));
        }

        Ok(())
    }

    /// 准备消息提交（不分配序列号）。
    ///
    /// # 参数
    /// - `message`: 消息
    ///
    /// # 返回
    /// - `Ok(submission)`: 已填充默认值和消息 ID 的提交
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_type = message.message_type,
    ))]
    pub async fn prepare_submission(&self, message: Message) -> Result<MessageSubmission> {
        // 准备消息提交
        MessageSubmission::prepare(message, &self.defaults).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                &format!("Failed to prepare message: {}", e)
            )
        })
    }

    /// 分配会话序列号，并在本进程首次触达该会话时以持久化权威 `max_seq` 作 floor 自愈回退。
    ///
    /// - 温路径（已校验、退避期内或无 provider）：普通 `INCR`，热路径仅一次缓存读。
    /// - 首次触达：查存储权威 `max_seq` 作 floor（RPC 有超时上限），`allocate_seq_with_floor`
    ///   消除回退；查询失败/超时则降级为普通 `INCR`（绝不比现状更差）并进入退避期，
    ///   退避结束后下次发送重试 floor 校验。
    async fn allocate_conversation_seq(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<u64> {
        let cache_key = format!("{tenant_id}::{conversation_id}");
        let already_checked = match self.seq_floor_state.get(&cache_key).await {
            Some(SeqFloorState::Checked) => true,
            Some(SeqFloorState::FailedAt(at)) => at.elapsed() < SEQ_FLOOR_FAILURE_BACKOFF,
            None => false,
        };

        match plan_seq_alloc(self.seq_floor_provider.is_some(), already_checked) {
            SeqAllocPlan::PlainIncr => {
                let seq = self
                    .sequence_allocator
                    .allocate_seq(conversation_id, tenant_id)
                    .await?;
                // 运行中 Redis flush 检测：已完成 floor 校验的会话不可能再分到 1
                //（首触路径至少返回 floor+1，温路径单调递增）。seq==1 几乎必是
                // 计数器丢失 → 立即重做 floor 校验重新分配（seq 1 作废成洞，无害）。
                if seq == 1
                    && matches!(
                        self.seq_floor_state.get(&cache_key).await,
                        Some(SeqFloorState::Checked)
                    )
                {
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        tenant_id = %tenant_id,
                        "checked conversation allocated seq=1; counter likely lost (redis flush), re-running floor check"
                    );
                    return self
                        .first_touch_floor_alloc(ctx, tenant_id, conversation_id, cache_key)
                        .await;
                }
                Ok(seq)
            }
            SeqAllocPlan::FirstTouchFloor => {
                self.first_touch_floor_alloc(ctx, tenant_id, conversation_id, cache_key)
                    .await
            }
        }
    }

    /// 首触/疑似丢计数器路径：查存储权威 `max_seq` 作 floor 分配，失败降级 + 退避。
    async fn first_touch_floor_alloc(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        cache_key: String,
    ) -> Result<u64> {
        // 无 provider 时降级为普通 INCR 而非 panic：本函数是发送主链上的长驻服务路径，
        // 调用方（plan_seq_alloc / flush 检测）已保证 provider 存在，但不靠断言兜底。
        let Some(provider) = self.seq_floor_provider.as_ref() else {
            return self
                .sequence_allocator
                .allocate_seq(conversation_id, tenant_id)
                .await;
        };
        let lookup = tokio::time::timeout(
            SEQ_FLOOR_RPC_TIMEOUT,
            provider.persisted_max_seq(ctx, tenant_id, conversation_id),
        )
        .await;
        let floor = match lookup {
            Ok(Ok(floor)) => floor,
            Ok(Err(error)) => {
                // 存储读不可用：降级普通 INCR + 退避，存储恢复后自愈。
                tracing::warn!(
                    conversation_id = %conversation_id,
                    tenant_id = %tenant_id,
                    error = %error,
                    "persisted max_seq lookup failed; falling back to plain INCR (backoff before retry)"
                );
                self.seq_floor_state
                    .insert(cache_key, SeqFloorState::FailedAt(Instant::now()))
                    .await;
                return self
                    .sequence_allocator
                    .allocate_seq(conversation_id, tenant_id)
                    .await;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    conversation_id = %conversation_id,
                    tenant_id = %tenant_id,
                    timeout_ms = SEQ_FLOOR_RPC_TIMEOUT.as_millis() as u64,
                    "persisted max_seq lookup timed out; falling back to plain INCR (backoff before retry)"
                );
                self.seq_floor_state
                    .insert(cache_key, SeqFloorState::FailedAt(Instant::now()))
                    .await;
                return self
                    .sequence_allocator
                    .allocate_seq(conversation_id, tenant_id)
                    .await;
            }
        };

        let seq = self
            .sequence_allocator
            .allocate_seq_with_floor(conversation_id, tenant_id, floor)
            .await?;

        self.seq_floor_state
            .insert(cache_key, SeqFloorState::Checked)
            .await;
        Ok(seq)
    }

    /// 为已准备好的提交分配会话序列号。
    #[instrument(skip(self, submission), fields(
        conversation_id = %submission.message.conversation_id,
        message_id = %submission.message_id,
    ))]
    pub async fn allocate_seq_for_submission(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        mut submission: MessageSubmission,
    ) -> Result<(MessageSubmission, MessageProfile)> {
        // 分配序列号（首次触达某会话时以持久化权威 max_seq 作 floor，自愈 seq 回退）。
        let session_seq = self
            .allocate_conversation_seq(ctx, tenant_id, &submission.message.conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!(
                        "allocate seq failed for conversation_id={}: {}",
                        submission.message.conversation_id, e
                    )
                )
            })?;

        tracing::trace!(
            conversation_id = %submission.message.conversation_id,
            conversation_seq = session_seq,
            "Allocated session sequence"
        );

        submission.message.conversation_seq = session_seq;

        // 获取消息类型信息
        let mut message_for_profile = submission.message.clone();
        let profile = MessageProfile::ensure(&mut message_for_profile);

        Ok((submission, profile))
    }

    /// 准备消息提交并分配序列号。
    ///
    /// 保留给已有测试和直接调用方；发送主链应在会话 ensure / decorate 成功后再调用
    /// [`Self::allocate_seq_for_submission`]，减少失败路径消耗会话序列号。
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_type = message.message_type,
    ))]
    pub async fn prepare_and_allocate_seq(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message: Message,
    ) -> Result<(MessageSubmission, MessageProfile)> {
        let submission = self.prepare_submission(message).await?;
        self.allocate_seq_for_submission(ctx, tenant_id, submission)
            .await
    }

    /// 写入 WAL（如果需要）
    ///
    /// # 参数
    /// - `submission`: 消息提交
    /// - `profile`: 消息配置
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %submission.message.conversation_id,
        message_id = %submission.message_id,
    ))]
    pub async fn write_wal_if_needed(
        &self,
        submission: &MessageSubmission,
        profile: &MessageProfile,
        persistence_mode: PersistenceMode,
        tenant_id: &str,
    ) -> Result<()> {
        if should_write_wal(profile, persistence_mode) {
            self.wal_repository
                .append(submission, tenant_id)
                .await
                .map_err(|e| {
                    flare_err!(
                        ErrorCode::InternalError,
                        &format!("Failed to append WAL entry: {}", e)
                    )
                })?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(
        conversation_id = %submission.message.conversation_id,
        message_id = %submission.message_id,
    ))]
    pub async fn remove_wal_after_broker_accept(
        &self,
        submission: &MessageSubmission,
    ) -> Result<()> {
        self.wal_repository
            .remove(&submission.message_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to remove WAL entry after broker accept: {}", e)
                )
            })
    }

    /// 装饰消息
    ///
    /// # 参数
    /// - `message`: 原始消息
    ///
    /// # 返回
    /// - `Ok(decorated_message)`: 装饰后的消息
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
    ))]
    pub async fn decorate_message(&self, message: Message) -> Result<Message> {
        self.message_decorator.decorate(message).await.map_err(|e| {
            flare_err!(
                ErrorCode::InternalError,
                &format!("Message decorator failed: {}", e)
            )
        })
    }

    /// 获取接收者用户 ID 列表
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `message`: 消息
    ///
    /// # 返回
    /// - `Ok(recipient_user_ids)`: 接收者用户 ID 列表
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
    ))]
    pub async fn get_recipient_user_ids(
        &self,
        ctx: &Ctx,
        message: &Message,
    ) -> Result<Vec<String>> {
        use crate::domain::model::ConversationType;
        let conversation_type = ConversationType::from_proto(message.conversation_type);

        let mut recipients = self
            .recipient_repository
            .get_message_recipients(
                ctx,
                &message.conversation_id,
                conversation_type,
                if message.channel_id.is_empty() {
                    None
                } else {
                    Some(&message.channel_id)
                },
                &message.sender_id,
            )
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to get message recipients: {}", e)
                )
            })?;

        if recipients.is_empty() && conversation_type != ConversationType::Single {
            recipients = recipient_hints_from_message_attributes(message);
            if !recipients.is_empty() {
                tracing::warn!(
                    conversation_id = %message.conversation_id,
                    conversation_type = ?conversation_type,
                    recipient_count = recipients.len(),
                    "Resolved non-single message recipients from message attributes fallback"
                );
            }
        }

        Ok(recipients)
    }

    async fn resolve_persistent_message_recipients(
        &self,
        ctx: &Ctx,
        message: &Message,
    ) -> Result<(Vec<String>, bool)> {
        use crate::domain::model::ConversationType;

        let conversation_type = ConversationType::from_proto(message.conversation_type);
        // 统一读扩散：成员制会话（群/频道/系统/AI/客服/广播）**永不物化收件人** → (vec![], large=true)。
        // 投递经会话级 publish + 网关在线订阅（O(在线/节点)，与群人数无关）；离线成员靠 conversation 版本号增量拉。
        // 10 万群热路径不再做 O(成员) 物化。
        if needs_member_lookup(conversation_type) {
            return Ok((Vec::new(), true));
        }
        // 1:1(Single) / Temp：解析对端用于 channel 归一化与小会话内联投递（非 large）。
        self.get_recipient_user_ids(ctx, message)
            .await
            .map(|recipients| (recipients, false))
    }

    fn normalize_single_chat_routing(
        &self,
        mut message: Message,
        recipient_user_ids: &[String],
    ) -> Message {
        use crate::domain::model::ConversationType;

        if ConversationType::from_proto(message.conversation_type) != ConversationType::Single {
            return message;
        }

        let sender_id = message.sender_id.trim();
        let Some(peer_id) = recipient_user_ids
            .iter()
            .map(|id| id.trim())
            .find(|id| !id.is_empty() && *id != sender_id)
        else {
            return message;
        };

        if message.channel_id.trim() != peer_id {
            tracing::warn!(
                conversation_id = %message.conversation_id,
                message_id = %message.server_id,
                sender_id = %message.sender_id,
                old_channel_id = %message.channel_id,
                normalized_channel_id = %peer_id,
                "Normalized single chat message channel_id from resolved recipient"
            );
            message.channel_id = peer_id.to_string();
        }

        message
    }

    /// 仅推送消息（不持久化），接收者由调用方显式提供
    ///
    /// 适用于上游已完成路由决策的场景，避免重复成员查询。
    #[instrument(skip(self, recipient_user_ids), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn push_only_with_recipients(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %message.server_id,
            recipient_count = recipient_user_ids.len(),
            "Pushing message only (no persistence)"
        );

        let message = self.normalize_single_chat_routing(message, &recipient_user_ids);
        let conversation_id = message.conversation_id.clone();
        self.push_repository
            .push_only_message(ctx, message, recipient_user_ids, conversation_id)
            .await
    }

    /// 仅推送消息（不持久化），由服务内部自动解析接收者
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
    ))]
    pub async fn push_only(&self, ctx: &Ctx, message: Message) -> Result<()> {
        let recipient_user_ids = self.get_recipient_user_ids(ctx, &message).await?;
        let message = self.normalize_single_chat_routing(message, &recipient_user_ids);
        self.push_only_with_recipients(ctx, message, recipient_user_ids)
            .await
    }

    /// 仅持久化消息（不推送）
    ///
    /// 用于主队列二段处理：
    /// - 已在上游完成收件人路由决策
    /// - 只需投递到存储队列落库
    /// - 不应再次做实时推送，避免重复下行
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `message`: 消息
    /// - `recipient_user_ids`: 接收者用户 ID 列表
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
    ))]
    pub async fn persistence_only(
        &self,
        ctx: &Ctx,
        message: Message,
        _recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %message.server_id,
            "Persisting message only (no push)"
        );

        let conversation_id = message.conversation_id.clone();
        self.push_repository
            .persistence_only_message(ctx, message, conversation_id)
            .await
    }

    /// 持久消息主流 fanout：先写入存储 topic，再写入推送 topic。
    ///
    /// 主消息队列只承载一次可靠输入；这里把它拆成存储与实时投递两个专用流，
    /// 保持 Storage Writer 和 Push Server 的职责清晰。
    #[instrument(skip(self, recipient_user_ids), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn persist_and_push_with_recipients(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %message.server_id,
            recipient_count = recipient_user_ids.len(),
            "Fanout persistent message to storage and push topics"
        );

        let message = self.normalize_single_chat_routing(message, &recipient_user_ids);
        let conversation_id = message.conversation_id.clone();
        self.push_repository
            .persistence_only_message(ctx, message.clone(), conversation_id.clone())
            .await?;
        self.push_repository
            .push_only_message(ctx, message, recipient_user_ids, conversation_id)
            .await
    }

    /// 推送消息
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `submission`: 消息提交
    /// - `profile`: 消息配置
    /// - `persistence_mode`: 持久化模式
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %submission.message.conversation_id,
        message_id = %submission.message_id,
        message_type = profile.message_type_label(),
    ))]
    pub async fn push_message(
        &self,
        ctx: &Ctx,
        submission: &MessageSubmission,
        profile: &MessageProfile,
        persistence_mode: PersistenceMode,
    ) -> Result<()> {
        let should_push_only = persistence_mode.should_push_only(profile.is_temporary());
        if should_push_only {
            return self.push_only(ctx, submission.message.clone()).await;
        }

        let (recipient_user_ids, large_conversation) = self
            .resolve_persistent_message_recipients(ctx, &submission.message)
            .await?;
        let message = if large_conversation {
            submission.message.clone()
        } else {
            self.normalize_single_chat_routing(submission.message.clone(), &recipient_user_ids)
        };
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %submission.message_id,
            message_type = profile.message_type_label(),
            persistence_mode = ?persistence_mode,
            large_conversation,
            "Publishing message (persistence + push)"
        );

        self.push_repository
            .publish_message(
                ctx,
                message.clone(),
                recipient_user_ids,
                message.conversation_id.clone(),
                large_conversation,
            )
            .await
    }
}

fn should_write_wal(profile: &MessageProfile, persistence_mode: PersistenceMode) -> bool {
    !persistence_mode.should_push_only(profile.is_temporary())
}

fn recipient_hints_from_message_attributes(message: &Message) -> Vec<String> {
    let Some(raw) = message.attributes.get("group_member_ids") else {
        return Vec::new();
    };
    let mut recipients = raw
        .split([',', ';', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|id| !id.is_empty() && *id != message.sender_id)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    recipients.sort();
    recipients.dedup();
    recipients
}

#[cfg(test)]
mod tests {
    use super::{
        SeqAllocPlan, plan_seq_alloc, recipient_hints_from_message_attributes, should_write_wal,
    };
    use crate::domain::PersistenceMode;
    use crate::domain::model::{MessageProfile, notification_persistent};
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{Message, MessageContent, NotificationContent, TextContent};

    #[test]
    fn no_provider_always_plain_incr() {
        // 未配置 provider：保持既有行为，永远走普通 INCR。
        assert_eq!(plan_seq_alloc(false, false), SeqAllocPlan::PlainIncr);
        assert_eq!(plan_seq_alloc(false, true), SeqAllocPlan::PlainIncr);
    }

    #[test]
    fn first_touch_consults_floor_then_warm_path_is_fast() {
        // 有 provider 且本进程首次触达该会话 → 查存储权威 max_seq 作 floor。
        assert_eq!(plan_seq_alloc(true, false), SeqAllocPlan::FirstTouchFloor);
        // 已校验过（温路径）→ 快 INCR，不再查存储。
        assert_eq!(plan_seq_alloc(true, true), SeqAllocPlan::PlainIncr);
    }

    fn text_profile() -> MessageProfile {
        let mut message = Message {
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "hello".to_string(),
                    mentions: vec![],
                })),
            }),
            ..Message::default()
        };
        MessageProfile::ensure(&mut message)
    }

    fn notification_profile(persistent: bool) -> (MessageProfile, Message) {
        let mut message = Message {
            content: Some(MessageContent {
                content: Some(Content::Notification(NotificationContent {
                    notification_type: "general".to_string(),
                    title: "title".to_string(),
                    body: "body".to_string(),
                    attributes: Default::default(),
                    target_user_ids: vec![],
                    target_role_id: String::new(),
                    notify_all: false,
                    persistent,
                    show_in_list: true,
                    show_badge: true,
                    play_sound: true,
                })),
            }),
            ..Message::default()
        };
        let profile = MessageProfile::ensure(&mut message);
        (profile, message)
    }

    #[test]
    fn durable_message_modes_require_wal() {
        let profile = text_profile();
        assert!(should_write_wal(&profile, PersistenceMode::Auto));
        assert!(should_write_wal(
            &profile,
            PersistenceMode::ForcePersistence
        ));
        assert!(!should_write_wal(&profile, PersistenceMode::ForcePushOnly));
    }

    #[test]
    fn persistent_notifications_require_wal_when_they_enter_storage_path() {
        let (profile, message) = notification_profile(true);
        assert_eq!(notification_persistent(&message), Some(true));
        assert!(should_write_wal(&profile, PersistenceMode::Auto));

        let (profile, message) = notification_profile(false);
        assert_eq!(notification_persistent(&message), Some(false));
        assert!(!should_write_wal(&profile, PersistenceMode::ForcePushOnly));
    }

    #[test]
    fn recipient_attribute_fallback_excludes_sender_and_deduplicates_members() {
        let mut message = Message {
            sender_id: "u1".to_string(),
            ..Message::default()
        };
        message.attributes.insert(
            "group_member_ids".to_string(),
            " u1, u2;u2 u3\n\t".to_string(),
        );

        assert_eq!(
            recipient_hints_from_message_attributes(&message),
            vec!["u2".to_string(), "u3".to_string()]
        );
    }
}
