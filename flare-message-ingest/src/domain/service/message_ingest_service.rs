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

use flare_im_contracts::Ctx;
use flare_im_contracts::abstractions::decorator::{MessageDecorator, NoopMessageDecorator};
use flare_im_seq::SequenceAllocator;
use flare_proto::common::Message;
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::domain::model::{MessageDefaults, MessageSubmission};
use crate::domain::repository::{
    PushRepository, RecipientRepository, WalRepository, WalRepositoryItem, needs_member_lookup,
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
}

#[derive(Default)]
pub struct MessageIngestServiceOptions {
    pub message_decorator: Option<Arc<dyn MessageDecorator>>,
    pub validation_strategy: Option<Arc<dyn MessageValidationStrategy>>,
}

impl MessageIngestServiceOptions {
    pub fn new() -> Self {
        Self::default()
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

    /// 为已准备好的提交分配会话序列号。
    #[instrument(skip(self, submission), fields(
        conversation_id = %submission.message.conversation_id,
        message_id = %submission.message_id,
    ))]
    pub async fn allocate_seq_for_submission(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        mut submission: MessageSubmission,
    ) -> Result<(MessageSubmission, MessageProfile)> {
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
    use super::{recipient_hints_from_message_attributes, should_write_wal};
    use crate::domain::PersistenceMode;
    use crate::domain::model::{MessageProfile, notification_persistent};
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{Message, MessageContent, NotificationContent, TextContent};

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
