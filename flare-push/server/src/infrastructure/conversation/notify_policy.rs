//! 通知偏好读取：经会话服务的参与者分页接口取 `muted`。
//!
//! 不直连会话服务的库：`conversation_participants` 归会话服务所有，推送侧绕过它
//! 直读会把两个服务焊死在同一份表结构上。多一次 RPC 换清晰的所有权边界，
//! 且这条路径只在离线推送时走，本来就在等网络。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flare_grpc_proto::conversation::ListConversationParticipantsRequest;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_server_core::client::request_with_context;
use flare_server_core::error::FlareError;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::{NotifyPolicyRepository, NotifyPreference};

/// 单次拉取的参与者页大小。
const PAGE_LIMIT: i32 = 200;

/// 最多翻多少页。群聊现在也有离线推送了，成员数可以远超一页——只看第一页会让
/// 靠后的成员即使设了免打扰照样被推。加个上限是防止超大会话把这条本该轻量的
/// 旁路查询拖成翻页风暴；真到了上限就按「查不到即未静音」放行（见下方 fail-open）。
const MAX_PAGES: usize = 16;

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

impl ConversationNotifyPolicy {
    /// 翻页枚举参与者并交给 `sink` 收集。
    ///
    /// 免打扰、「只接收@我」、大群成员列表读的都是同一批行——各写一次翻页会让
    /// 每条消息的 RPC 按用途翻倍，也迟早在页数上限、游标终止条件这些细节上分叉。
    ///
    /// `sink` 返回 `false` 表示「要的都拿到了，别再翻」。
    async fn walk_participants<F>(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        mut sink: F,
    ) -> Result<bool, FlareError>
    where
        F: FnMut(&flare_proto::common::ConversationParticipant) -> bool,
    {
        let mut client = self.client().await?;
        let mut cursor = String::new();

        for _ in 0..MAX_PAGES {
            let resp = client
                .list_conversation_participants(request_with_context(
                    ListConversationParticipantsRequest {
                        conversation_id: conversation_id.to_string(),
                        cursor: cursor.clone(),
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

            let has_more = resp.has_more && !resp.next_cursor.trim().is_empty();
            for participant in &resp.participants {
                if !sink(participant) {
                    return Ok(true);
                }
            }
            if !has_more {
                return Ok(true);
            }
            cursor = resp.next_cursor;
        }
        // 翻到页数上限还没完：告诉调用方这次枚举不完整。
        Ok(false)
    }
}

fn preference_of(p: &flare_proto::common::ConversationParticipant) -> NotifyPreference {
    NotifyPreference {
        muted: p.muted,
        mention_only: p.mention_only,
    }
}

#[async_trait::async_trait]
impl NotifyPolicyRepository for ConversationNotifyPolicy {
    async fn preferences_for(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, NotifyPreference>, FlareError> {
        if user_ids.is_empty() || conversation_id.trim().is_empty() {
            return Ok(HashMap::new());
        }
        let mut wanted: HashSet<&str> = user_ids.iter().map(String::as_str).collect();
        let mut out = HashMap::new();
        self.walk_participants(ctx, conversation_id, |participant| {
            if wanted.remove(participant.user_id.as_str()) {
                out.insert(participant.user_id.clone(), preference_of(participant));
            }
            // 关心的人都判定完就停：单聊/小范围收件人不必把大群整册翻完。
            !wanted.is_empty()
        })
        .await?;
        Ok(out)
    }

    async fn all_participants(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cap: usize,
    ) -> Result<Option<HashMap<String, NotifyPreference>>, FlareError> {
        if conversation_id.trim().is_empty() || cap == 0 {
            return Ok(Some(HashMap::new()));
        }
        let mut out = HashMap::new();
        let mut over_cap = false;
        let complete = self
            .walk_participants(ctx, conversation_id, |participant| {
                if out.len() >= cap {
                    over_cap = true;
                    return false;
                }
                out.insert(participant.user_id.clone(), preference_of(participant));
                true
            })
            .await?;
        if over_cap || !complete {
            return Ok(None);
        }
        Ok(Some(out))
    }
}
