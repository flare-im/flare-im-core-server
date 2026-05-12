//! 消息领域服务 - 重构版
//!
//! ## 核心职责
//! 1. 消息校验（使用策略模式）
//! 2. 序列号分配（使用公共 SequenceAllocator）
//! 3. WAL 写入
//! 4. 消息装饰
//! 5. 消息推送（使用 PushRepository）
//!
//! ## 设计原则
//! - 单一职责：每个服务只负责一件事
//! - 依赖注入：通过构造函数注入依赖
//! - 纯领域逻辑：不包含 Hook 执行

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_im_core::abstractions::decorator::{MessageDecorator, NoopMessageDecorator};
use flare_im_core::tracing::create_span;
use flare_proto::common::Message;
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::domain::model::{MessageDefaults, MessageSubmission};
use crate::domain::repository::{
    PushRepository, RecipientRepository, WalRepository, WalRepositoryItem,
};
use crate::domain::service::sequence_allocator::SequenceAllocator;
use crate::domain::service::validation_strategy::{
    CompositeMessageValidationStrategy, MessageValidationStrategy, ValidationContext,
};
use crate::domain::{MessageProfile, PersistenceMode};
use crate::error::{ErrorCode, Result};
use crate::infrastructure::messaging::push_repository::MqPushRepository;

/// 消息领域服务
pub struct MessageDomainService {
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
}

impl MessageDomainService {
    pub fn new(
        push_repository: Arc<MqPushRepository>,
        recipient_repository: Arc<dyn RecipientRepository>,
        wal_repository: Arc<WalRepositoryItem>,
        sequence_allocator: Arc<SequenceAllocator>,
        defaults: MessageDefaults,
        message_decorator: Option<Arc<dyn MessageDecorator>>,
        validation_strategy: Option<Arc<dyn MessageValidationStrategy>>,
    ) -> Self {
        Self {
            push_repository,
            recipient_repository,
            wal_repository,
            sequence_allocator,
            defaults,
            message_decorator: message_decorator.unwrap_or_else(|| Arc::new(NoopMessageDecorator)),
            validation_strategy: validation_strategy.unwrap_or_else(|| {
                Arc::new(CompositeMessageValidationStrategy::default_composite())
            }),
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

    /// 准备消息提交并分配序列号
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `message`: 消息
    ///
    /// # 返回
    /// - `Ok((submission, profile))`: 消息提交和消息配置
    /// - `Err`: 错误
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
        // 准备消息提交
        let submission = MessageSubmission::prepare(message, &self.defaults).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                &format!("Failed to prepare message: {}", e)
            )
        })?;

        // 分配序列号
        let session_seq = self
            .sequence_allocator
            .allocate_seq(&submission.message.conversation_id, tenant_id)
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
            seq = session_seq,
            "Allocated session sequence"
        );

        let mut submission = submission;
        submission.message.seq = session_seq;

        // 获取消息类型信息
        let mut message_for_profile = submission.message.clone();
        let profile = MessageProfile::ensure(&mut message_for_profile);

        Ok((submission, profile))
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
    ) -> Result<()> {
        if profile.needs_wal() {
            let _wal_span = create_span("message-domain", "wal_write");
            self.wal_repository.append(submission).await.map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to append WAL entry: {}", e)
                )
            })?;
        }
        Ok(())
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

        self.recipient_repository
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
            })
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

        let conversation_id = message.conversation_id.clone();
        self.push_repository
            .push_only_message(ctx, message, recipient_user_ids, conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish push-only message to MQ: {}", e)
                )
            })
    }

    /// 仅推送消息（不持久化），由服务内部自动解析接收者
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
    ))]
    pub async fn push_only(&self, ctx: &Ctx, message: Message) -> Result<()> {
        let recipient_user_ids = self.get_recipient_user_ids(ctx, &message).await?;
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
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish persistence-only message to MQ: {}", e)
                )
            })
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

        let recipient_user_ids = self
            .get_recipient_user_ids(ctx, &submission.message)
            .await?;
        tracing::trace!(
            conversation_id = %submission.message.conversation_id,
            message_id = %submission.message_id,
            message_type = profile.message_type_label(),
            persistence_mode = ?persistence_mode,
            "Publishing message (persistence + push)"
        );

        self.push_repository
            .publish_message(
                ctx,
                submission.message.clone(),
                recipient_user_ids,
                submission.message.conversation_id.clone(),
            )
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish message to MQ: {}", e)
                )
            })
    }
}
