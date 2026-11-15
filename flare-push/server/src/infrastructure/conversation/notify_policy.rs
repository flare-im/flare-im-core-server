//! 通知偏好读取：经会话服务的参与者读接口取 `muted` 与 `mention_only`。
//!
//! 不直连会话服务的库：`conversation_participants` 归会话服务所有，推送侧绕过它
//! 直读会把两个服务焊死在同一份表结构上。多一次 RPC 换清晰的所有权边界，
//! 且这条路径只在离线推送时走，本来就在等网络。
//!
//! 几种读法各有用处：指定一批人时按 id 查成员（一次主键查找，与群规模无关，查不到就是
//! 确定不是成员）；要全体成员时分页枚举；超过成员上限的群只要订阅者时，经内部读接口
//! `ListNotificationSubscribers` 以服务身份读。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flare_grpc_proto::conversation::conversation_internal_read_service_client::ConversationInternalReadServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::{
    ListConversationParticipantsRequest, ListNotificationSubscribersRequest,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_server_core::error::FlareError;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::{NotifyPolicyRepository, NotifyPreference, ParticipantPage};
use crate::infrastructure::read_request::request_with_context;

/// 订阅者每页取多少（会话服务的单页上限）。
const SUBSCRIBER_PAGE_SIZE: i32 = 1000;

/// 订阅者最多翻几页。会话服务整个列表封顶 5000 人，五页就翻完；多一页余量，游标不终止时也停得下来。
const MAX_SUBSCRIBER_PAGES: usize = 6;

/// 单次拉取的参与者页大小。
const PAGE_LIMIT: i32 = 200;

/// 全员枚举最多翻多少页。加个上限是防止超大会话把这条本该轻量的旁路查询拖成翻页风暴；
/// 翻到上限还没完就告诉调用方这次枚举不完整，由它放弃这次全员扇出。
const MAX_PAGES: usize = 16;

/// 会话服务按 id 查成员的 `ext` 键（`flare-conversation` 的 `PARTICIPANT_MEMBERSHIP_EXT_KEY`）：
/// 值是逗号分隔的 user id，只返回其中的成员——有参与者行且未退出，隐藏了会话的也算。
const PARTICIPANT_MEMBERSHIP_EXT_KEY: &str = "member_ids";

/// 一次按 id 查成员最多带多少个 id（会话服务的上限）。
const MEMBERSHIP_LOOKUP_BATCH: usize = 500;

/// 把要查的人整理成会话服务接受的 id 列表：去重，保持原顺序。
///
/// 空白 id 或带逗号的 id 没法放进逗号分隔的列表里，硬查只会得到「不是成员」这个错误答案，
/// 所以直接报错，让调用方按查询失败放行。
fn membership_lookup_ids(user_ids: &[String]) -> Result<Vec<&str>, FlareError> {
    let mut seen = HashSet::with_capacity(user_ids.len());
    let mut ids = Vec::with_capacity(user_ids.len());
    for user_id in user_ids {
        let id = user_id.as_str();
        if id.trim().is_empty() || id.trim() != id || id.contains(',') {
            return Err(FlareError::localized(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "user id cannot be looked up as a conversation member",
            ));
        }
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

/// all_participants 单次有界拉取的 cap 上界:cap+1 必须能装进服务端单页(clamp 上限 5000),
/// 否则单页无法判定是否超限,回退多页 walk。与 conversation 侧 MAX_PARTICIPANT_PAGE_LIMIT 对齐。
const MAX_SINGLE_FETCH_CAP: usize = 5000;

#[derive(Default)]
pub struct ConversationNotifyPolicy {
    channel: Arc<Mutex<Option<Channel>>>,
}

impl ConversationNotifyPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    async fn channel(&self) -> Result<Channel, FlareError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(channel.clone());
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
        Ok(channel)
    }

    async fn client(&self) -> Result<ConversationReadServiceClient<Channel>, FlareError> {
        Ok(ConversationReadServiceClient::new(self.channel().await?))
    }

    /// 内部读接口与参与者读接口在会话服务的同一个监听上，共用一条连接。
    async fn internal_client(
        &self,
    ) -> Result<ConversationInternalReadServiceClient<Channel>, FlareError> {
        Ok(ConversationInternalReadServiceClient::new(
            self.channel().await?,
        ))
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
                        // 只按 sink 消费 participants(偏好),不看 total/version →
                        // 让服务端首页跳过 COUNT(*)+MAX(updated_at) 两次 O(成员) 聚合。
                        ext: std::collections::HashMap::from([(
                            "skip_meta".to_string(),
                            "1".to_string(),
                        )]),
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
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }
        if conversation_id.trim().is_empty() {
            // 没有会话就谈不上谁是成员；返回空表会被读成「这些人都不是成员」。
            return Err(FlareError::localized(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "conversation id required for notify preferences",
            ));
        }
        // 按 id 查而不是翻页找：翻页在大群里要么翻不到靠后的人，要么把整册成员翻完，
        // 而且「没翻到」分不清是不在会话里还是没翻够。按 id 查的结果是确定的。
        let ids = membership_lookup_ids(user_ids)?;
        let mut client = self.client().await?;
        let mut out = HashMap::with_capacity(ids.len());
        for batch in ids.chunks(MEMBERSHIP_LOOKUP_BATCH) {
            let resp = client
                .list_conversation_participants(request_with_context(
                    ListConversationParticipantsRequest {
                        conversation_id: conversation_id.to_string(),
                        ext: HashMap::from([(
                            PARTICIPANT_MEMBERSHIP_EXT_KEY.to_string(),
                            batch.join(","),
                        )]),
                        ..Default::default()
                    },
                    ctx,
                ))
                .await
                .map_err(|e| {
                    FlareError::localized(
                        flare_server_core::error::ErrorCode::ServiceUnavailable,
                        format!("look up conversation members: {e}"),
                    )
                })?
                .into_inner();
            for participant in &resp.participants {
                out.insert(participant.user_id.clone(), preference_of(participant));
            }
        }
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
        // 大群短路:单次有界拉取 cap+1 行即可判定是否超限,超限直接返回 None 跳过离线扇出,
        // 不再逐条消息多页翻册 + 累积到 cap 的 HashMap 去重新发现"超限"(成员数不随消息变,
        // 十万群原本每条消息 5 次分页往返)。配合服务端 skip_meta,单次调用零 O(成员) 聚合。
        // cap+1 超过服务端单页上限时回退多页 walk 以保正确性。
        if cap < MAX_SINGLE_FETCH_CAP {
            let mut client = self.client().await?;
            let resp = client
                .list_conversation_participants(request_with_context(
                    ListConversationParticipantsRequest {
                        conversation_id: conversation_id.to_string(),
                        cursor: String::new(),
                        limit: (cap + 1) as i32,
                        include_removed: false,
                        ext: std::collections::HashMap::from([(
                            "skip_meta".to_string(),
                            "1".to_string(),
                        )]),
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
            // 多取的第 cap+1 行出现,或还有更多页 → 超限,跳过离线扇出。
            if resp.participants.len() > cap
                || (resp.has_more && !resp.next_cursor.trim().is_empty())
            {
                return Ok(None);
            }
            let out = resp
                .participants
                .iter()
                .map(|p| (p.user_id.clone(), preference_of(p)))
                .collect();
            return Ok(Some(out));
        }
        // cap 极大(≥ 服务端单页上限):回退多页 walk。
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

    async fn notification_subscribers(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>, FlareError> {
        if conversation_id.trim().is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.internal_client().await?;
        let mut out = Vec::new();
        let mut page_token = String::new();
        for _ in 0..MAX_SUBSCRIBER_PAGES {
            // 租户不在请求体里：会话服务只认签名上下文里的租户，且只放行服务身份。
            let response = client
                .list_notification_subscribers(request_with_context(
                    ListNotificationSubscribersRequest {
                        conversation_id: conversation_id.to_string(),
                        page_token: page_token.clone(),
                        page_size: SUBSCRIBER_PAGE_SIZE,
                    },
                    ctx,
                ))
                .await
                .map_err(|status| {
                    FlareError::localized(
                        flare_server_core::error::ErrorCode::ServiceUnavailable,
                        format!("list notification subscribers: {:?}", status.code()),
                    )
                })?
                .into_inner();
            out.extend(response.user_ids);
            if response.next_page_token.trim().is_empty() {
                return Ok(out);
            }
            page_token = response.next_page_token;
        }
        Ok(out)
    }

    async fn participant_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<ParticipantPage, FlareError> {
        if conversation_id.trim().is_empty() {
            return Ok(ParticipantPage::default());
        }
        let mut client = self.client().await?;
        let response = client
            .list_conversation_participants(request_with_context(
                ListConversationParticipantsRequest {
                    conversation_id: conversation_id.to_string(),
                    cursor: cursor.to_string(),
                    limit: limit.clamp(1, MAX_SINGLE_FETCH_CAP) as i32,
                    include_removed: false,
                    ext: HashMap::from([("skip_meta".to_string(), "1".to_string())]),
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
        let members = response
            .participants
            .iter()
            .map(|participant| (participant.user_id.clone(), preference_of(participant)))
            .collect();
        let next_cursor = (response.has_more && !response.next_cursor.trim().is_empty())
            .then_some(response.next_cursor);
        Ok(ParticipantPage {
            members,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::error::ErrorCode;

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    /// 会话服务要求 id 互不相同；同一个人在收件人里出现两次不该让整次查询被拒。
    #[test]
    fn lookup_ids_are_deduplicated_in_request_order() {
        let requested = ids(&["u2", "u1", "u2", "u3", "u1"]);
        assert_eq!(
            membership_lookup_ids(&requested).unwrap(),
            vec!["u2", "u1", "u3"]
        );
    }

    /// 放不进逗号分隔列表的 id 必须报错而不是悄悄漏查：漏查的人会被读成「确定不是成员」。
    #[test]
    fn ids_that_cannot_be_listed_fail_the_lookup_instead_of_being_dropped() {
        for bad in ["", "  ", " u1", "u1 ", "u1,u2"] {
            let requested = ids(&["u0", bad]);
            assert_eq!(
                membership_lookup_ids(&requested).unwrap_err().code(),
                Some(ErrorCode::InvalidParameter),
                "{bad:?}"
            );
        }
    }

    /// 超过单次上限的收件人按批查，每批都在会话服务的上限之内，合起来不丢人。
    #[test]
    fn large_recipient_lists_split_into_bounded_batches() {
        let requested: Vec<String> = (0..1201).map(|i| format!("u{i}")).collect();
        let lookup = membership_lookup_ids(&requested).unwrap();
        let batches: Vec<_> = lookup.chunks(MEMBERSHIP_LOOKUP_BATCH).collect();
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.len() <= 500));
        assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 1201);
    }
}
