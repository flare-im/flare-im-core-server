//! 接收者仓储实现：结合会话服务获取消息接收者。
//!
//! ## 核心逻辑
//! 1. **消息接收者**：根据会话类型确定接收者列表
//!    - 单聊：优先使用会话成员表，排除发送者；仅在成员缺失时降级到 channel_id
//!    - 群聊/频道：从会话服务获取成员列表，排除发送者
//! 2. **事件接收者**：直接使用 conversation_id 获取会话成员列表
//! 3. **会话成员**：调用会话服务获取详情，提取参与者列表

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use flare_im_core::Ctx;
use flare_proto::common::EventType;
use tracing::{debug, warn};

use crate::domain::model::ConversationType;
use crate::domain::repository::RecipientRepository;
use crate::infrastructure::rpc::{ConversationClient, ConversationRpcClient};

/// 接收者仓储实现，依赖会话服务。
pub struct RecipientRepositoryImpl {
    conversation_repo: Arc<ConversationClient>,
}

impl std::fmt::Debug for RecipientRepositoryImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecipientRepositoryImpl")
            .field("conversation_repo", &self.conversation_repo)
            .finish()
    }
}

impl RecipientRepositoryImpl {
    pub fn new(conversation_repo: Arc<ConversationClient>) -> Self {
        Self { conversation_repo }
    }
}

impl RecipientRepository for RecipientRepositoryImpl {
    /// 获取消息接收者列表
    ///
    /// ## 逻辑
    /// - 单聊：优先从会话成员解析对方，防止客户端误把 channel_id 传成自己导致自投递
    /// - 其他类型：从会话服务获取成员列表，排除发送者
    fn get_message_recipients<'a>(
        &'a self,
        ctx: &'a Ctx,
        conversation_id: &'a str,
        conversation_type: ConversationType,
        channel_id: Option<&'a str>,
        sender_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(async move {
            match conversation_type {
                ConversationType::Single => {
                    let members = self.get_conversation_members(ctx, conversation_id).await?;
                    let mut recipients: Vec<String> = members
                        .into_iter()
                        .map(|uid| uid.trim().to_string())
                        .filter(|uid| !uid.is_empty() && uid != sender_id)
                        .collect();
                    recipients.sort();
                    recipients.dedup();

                    if !recipients.is_empty() {
                        debug!(
                            conversation_id = %conversation_id,
                            sender_id = %sender_id,
                            recipient_count = recipients.len(),
                            "Single chat recipients resolved from conversation members"
                        );
                        return Ok(recipients);
                    }

                    if let Some(cid) = channel_id.map(str::trim).filter(|cid| !cid.is_empty()) {
                        if cid == sender_id {
                            warn!(
                                conversation_id = %conversation_id,
                                sender_id = %sender_id,
                                channel_id = %cid,
                                "Single chat channel_id points to sender and no peer member was found"
                            );
                            return Ok(vec![]);
                        }
                        debug!(
                            conversation_id = %conversation_id,
                            channel_id = %cid,
                            "Single chat recipient fallback: channel_id"
                        );
                        return Ok(vec![cid.to_string()]);
                    }

                    warn!(
                        conversation_id = %conversation_id,
                        sender_id = %sender_id,
                        "Single chat missing recipients"
                    );
                    Ok(vec![])
                }
                _ => {
                    // 其他类型：从会话服务获取成员列表，排除发送者
                    let members = self.get_conversation_members(ctx, conversation_id).await?;
                    let recipients: Vec<String> =
                        members.into_iter().filter(|uid| uid != sender_id).collect();
                    debug!(
                        conversation_id = %conversation_id,
                        conversation_type = ?conversation_type,
                        sender_id = %sender_id,
                        recipient_count = recipients.len(),
                        "Non-single chat recipients (excluding sender)"
                    );
                    Ok(recipients)
                }
            }
        })
    }

    /// 获取事件接收者列表
    ///
    /// ## 逻辑
    /// 直接使用 conversation_id 获取会话成员列表
    fn get_event_recipients<'a>(
        &'a self,
        ctx: &'a Ctx,
        message_id: &'a str,
        conversation_id: &'a str,
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(async move {
            debug!(
                message_id = %message_id,
                conversation_id = %conversation_id,
                event_type = ?event_type,
                "Getting event recipients by conversation_id"
            );

            // 直接使用 conversation_id 获取成员列表
            let mut members = self.get_conversation_members(ctx, conversation_id).await?;
            if event_type == EventType::EventTyping {
                if let Some(current_uid) = ctx.user_id() {
                    members.retain(|uid| uid != current_uid);
                }
            }

            debug!(
                message_id = %message_id,
                conversation_id = %conversation_id,
                event_type = ?event_type,
                member_count = members.len(),
                "Retrieved event recipients from conversation service"
            );

            Ok(members)
        })
    }

    /// 获取会话成员列表
    ///
    /// ## 逻辑
    /// 调用会话服务的 GetConversationMembers 获取参与者列表
    fn get_conversation_members<'a>(
        &'a self,
        ctx: &'a Ctx,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(async move {
            match self
                .conversation_repo
                .get_conversation_members(ctx, conversation_id)
                .await
            {
                Ok(members) => {
                    debug!(
                        conversation_id = %conversation_id,
                        member_count = members.len(),
                        "Retrieved conversation members from conversation service"
                    );
                    Ok(members)
                }
                Err(e) => {
                    warn!(
                        conversation_id = %conversation_id,
                        error = %e,
                        "Failed to get conversation members, returning empty list"
                    );
                    Ok(vec![])
                }
            }
        })
    }
}
