//! 接收者仓储实现：结合会话服务获取消息接收者。
//!
//! ## 核心逻辑
//! 1. **消息接收者**：根据会话类型确定接收者列表
//!    - 单聊：优先使用显式对端 channel_id，缺失或异常时再降级到会话成员表
//!    - 群聊/频道：从会话服务获取成员列表，排除发送者
//! 2. **事件接收者**：直接使用 conversation_id 获取会话成员列表
//! 3. **会话成员**：调用会话服务获取详情，提取参与者列表

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::constants::sync_inbox::sync_inbox_recipient;
use flare_proto::common::EventType;
use flare_server_core::error::Result;
use tracing::{debug, warn};

use crate::model::ConversationType;
use crate::repository::RecipientRepository;
use crate::rpc::{ConversationClient, ConversationRpcClient};

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

    fn explicit_single_chat_recipient(channel_id: Option<&str>, sender_id: &str) -> Option<String> {
        let cid = channel_id.map(str::trim).filter(|cid| !cid.is_empty())?;
        (cid != sender_id).then(|| cid.to_string())
    }
}

impl RecipientRepository for RecipientRepositoryImpl {
    /// 获取消息接收者列表
    ///
    /// ## 逻辑
    /// - 单聊：优先使用显式对端 channel_id，避免依赖会话服务热路径
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
            // sync 收件箱的收件人就写在 ID 里（sync:<user_id>）。
            // 必须在这里短路：它不是真实会话、没有参与者行，落到下面的
            // get_conversation_members 必然 NOT_FOUND，消息随之被丢弃——
            // 线上实测建 499 人群时有 8 个成员因此收不到系统通知。
            // 顺带省掉一次会话服务的 RPC。
            if let Some(recipient) = sync_inbox_recipient(conversation_id) {
                debug!(
                    conversation_id = %conversation_id,
                    "Sync inbox recipient resolved from conversation id"
                );
                return Ok(vec![recipient.to_string()]);
            }

            match conversation_type {
                ConversationType::Single => {
                    if let Some(recipient_id) =
                        Self::explicit_single_chat_recipient(channel_id, sender_id)
                    {
                        debug!(
                            conversation_id = %conversation_id,
                            channel_id = %recipient_id,
                            "Single chat recipient resolved from channel_id"
                        );
                        return Ok(vec![recipient_id]);
                    }

                    if let Some(cid) = channel_id.map(str::trim).filter(|cid| !cid.is_empty())
                        && cid == sender_id
                    {
                        warn!(
                            conversation_id = %conversation_id,
                            sender_id = %sender_id,
                            channel_id = %cid,
                            "Single chat channel_id points to sender, falling back to conversation members"
                        );
                    }

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
            let members = self.get_conversation_members(ctx, conversation_id).await?;

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

    fn get_conversation_member_count<'a>(
        &'a self,
        ctx: &'a Ctx,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            let member_count = self
                .conversation_repo
                .get_conversation_member_count(ctx, conversation_id)
                .await?;
            debug!(
                conversation_id = %conversation_id,
                member_count,
                "Retrieved conversation member count from conversation service"
            );
            Ok(member_count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RecipientRepositoryImpl;

    #[test]
    fn explicit_single_chat_recipient_prefers_non_self_channel_id() {
        assert_eq!(
            RecipientRepositoryImpl::explicit_single_chat_recipient(Some(" user-b "), "user-a"),
            Some("user-b".to_string())
        );
    }

    #[test]
    fn explicit_single_chat_recipient_ignores_empty_or_self_channel_id() {
        assert_eq!(
            RecipientRepositoryImpl::explicit_single_chat_recipient(Some(""), "user-a"),
            None
        );
        assert_eq!(
            RecipientRepositoryImpl::explicit_single_chat_recipient(Some("user-a"), "user-a"),
            None
        );
        assert_eq!(
            RecipientRepositoryImpl::explicit_single_chat_recipient(None, "user-a"),
            None
        );
    }

    /// sync 收件箱的收件人必须从会话 ID 直接解析，绝不能去查会话参与者。
    ///
    /// 它不是真实会话、没有参与者行，一旦落到 get_conversation_members 就是
    /// NOT_FOUND，消息被静默丢弃——线上实测建 499 人群时有 8 个成员因此
    /// 收不到系统通知。这个测试锁住「ID 即收件人」这条捷径。
    #[test]
    fn sync_inbox_recipient_comes_from_the_id_not_from_participants() {
        use flare_im_contracts::constants::sync_inbox::{
            sync_inbox_conversation_id, sync_inbox_recipient,
        };
        let cid = sync_inbox_conversation_id("351364692347715584");
        assert_eq!(sync_inbox_recipient(&cid), Some("351364692347715584"));
        // 真实会话不能被误判成 sync 收件箱，否则会跳过真正的成员解析
        assert_eq!(sync_inbox_recipient("2AXK6MVC000H827SKV"), None);
    }
}
