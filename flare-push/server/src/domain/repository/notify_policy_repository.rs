//! 用户会话级通知偏好（Port）。
//!
//! 只在**即将生成离线推送任务**时才会被问到：在线用户的事件走长连接，客户端自己
//! 决定要不要提示；只有离线推送是「设备上会真的响一声」的那一路，服务端不替用户
//! 拦，就没有任何人能拦了。
//!
//! 放在离线分支而不是发信热路径，是因为成本结构完全不同：发信是每条消息都走，
//! 而离线推送本就要发起 APNs/FCM 网络调用，多一次查询是噪音。

use std::collections::HashMap;

use flare_im_contracts::Ctx;
use flare_server_core::error::FlareError;

/// 一个成员在某会话下的通知偏好。
///
/// 两个布尔正交：`muted` 是「一条都别响」，`mention_only` 是「只有点名才响」。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotifyPreference {
    pub muted: bool,
    pub mention_only: bool,
}

#[async_trait::async_trait]
pub trait NotifyPolicyRepository: Send + Sync {
    /// 取 `user_ids` 这批人在该会话里的通知偏好。
    ///
    /// **一次取回全部偏好，而不是每个偏好查一遍**：它们读的是同一批参与者行，
    /// 分成多次查询会让每条消息的 RPC 按偏好种类翻倍——加第二种偏好时就踩过一次。
    ///
    /// `Ok` 的结果是确定的：表里有的人是成员（未退出，隐藏了会话的也算），表里没有的人
    /// 确定不是成员。补提示靠这一点不给已经不在会话里的人推送，所以实现拿不准时
    /// （自身不可用、只读到一部分）必须返回错误，不能返回一张不完整的表。
    ///
    /// 调用方对错误 fail-open：通知偏好是偏好不是安全边界，「该静音的响了一声」远好过
    /// 「该到的推送没到」。
    async fn preferences_for(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, NotifyPreference>, FlareError>;

    /// 会话全体成员及其通知偏好，供群会话的波次枚举使用。
    ///
    /// 返回 `None` 表示成员数超过 `cap`：调用方改为只枚举订阅者
    /// （[`Self::notification_subscribers`]），点名的人另行按人推送。不返回截断的一部分——
    /// 那会变成「随机挑一部分人推送」。
    ///
    /// 成员列表与偏好是同一次枚举的产物，过滤时不必再查一遍。
    async fn all_participants(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cap: usize,
    ) -> Result<Option<HashMap<String, NotifyPreference>>, FlareError>;

    /// 会话里显式开启了「全部消息通知」的成员（超过成员上限的群的波次只枚举他们）：在席、
    /// 没设免打扰与仅 @我，至多 5000 人（会话服务的上限，超出的不列）。
    async fn notification_subscribers(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>, FlareError>;

    /// 按页枚举会话的全体成员及其偏好，供超过成员上限的群的全员 @所有人 波次分页流式推送。
    /// `cursor` 为空取第一页；页里至多 `limit` 人。
    async fn participant_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<ParticipantPage, FlareError>;
}

/// 一页会话成员及其通知偏好。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParticipantPage {
    pub members: Vec<(String, NotifyPreference)>,
    /// 下一页的游标；`None` 表示没有下一页。
    pub next_cursor: Option<String>,
}
