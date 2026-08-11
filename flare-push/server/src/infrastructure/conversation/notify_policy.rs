//! 通知偏好读取：经会话服务的参与者分页接口取 `muted`。
//!
//! 不直连会话服务的库：`conversation_participants` 归会话服务所有，推送侧绕过它
//! 直读会把两个服务焊死在同一份表结构上。多一次 RPC 换清晰的所有权边界，
//! 且这条路径只在离线推送时走，本来就在等网络。

use std::collections::HashSet;
use std::sync::Arc;

use flare_grpc_proto::conversation::ListConversationParticipantsRequest;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_server_core::client::request_with_context;
use flare_server_core::error::FlareError;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::NotifyPolicyRepository;

/// 单次拉取的参与者页大小。单聊只有两人；群聊本就没有离线推送
/// （成员制会话走读扩散、推送侧只解析在线用户），所以这里不需要翻页规模。
const PAGE_LIMIT: i32 = 200;

#[derive(Default)]
pub struct ConversationNotifyPolicy {
    channel: Arc<Mutex<Option<Channel>>>,
}

impl ConversationNotifyPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self) -> Result<ConversationReadServiceClient<Channel>, FlareError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(ConversationReadServiceClient::new(channel.clone()));
        }
        let name = get_service_name(CONVERSATION);
        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(&name);
        let channel =
            flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(&name, fallback)
                .await
                .map_err(|e| {
                    FlareError::localized(
                        flare_server_core::error::ErrorCode::ServiceUnavailable,
                        format!("connect {name}: {e}"),
                    )
                })?;
        *guard = Some(channel.clone());
        Ok(ConversationReadServiceClient::new(channel))
    }
}

#[async_trait::async_trait]
impl NotifyPolicyRepository for ConversationNotifyPolicy {
    async fn muted_users(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashSet<String>, FlareError> {
        if user_ids.is_empty() || conversation_id.trim().is_empty() {
            return Ok(HashSet::new());
        }
        let mut client = self.client().await?;
        let resp = client
            .list_conversation_participants(request_with_context(
                ListConversationParticipantsRequest {
                    conversation_id: conversation_id.to_string(),
                    cursor: String::new(),
                    limit: PAGE_LIMIT,
                    include_removed: false,
                    ext: Default::default(),
                },
                ctx,
            ))
            .await
            .map_err(|e| {
                FlareError::localized(
                    flare_server_core::error::ErrorCode::ServiceUnavailable,
                    format!("list conversation participants: {e}"),
                )
            })?
            .into_inner();

        let wanted: HashSet<&str> = user_ids.iter().map(String::as_str).collect();
        Ok(resp
            .participants
            .into_iter()
            .filter(|p| p.muted && wanted.contains(p.user_id.as_str()))
            .map(|p| p.user_id)
            .collect())
    }
}
