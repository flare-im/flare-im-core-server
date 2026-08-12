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
    /// 取 `user_ids` 这批人的通知偏好。
    ///
    /// **一次取回全部偏好，而不是每个偏好查一遍**：它们读的是同一批参与者行，
    /// 分成多次查询会让每条消息的翻页 RPC 按偏好种类翻倍——加第二种偏好时就踩过一次。
    ///
    /// 实现应当在自身不可用时返回空表而不是错误：通知偏好是偏好不是安全边界，
    /// 「该静音的响了一声」远好过「该到的推送没到」。调用方据此 fail-open。
    async fn preferences_for(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, NotifyPreference>, FlareError>;

    /// 会话全体成员及其通知偏好，供大群离线扇出使用。
    ///
    /// 返回 `None` 表示成员数超过 `cap`，调用方应放弃本次离线扇出——
    /// 截断会变成「随机挑一部分人推送」，比统一不推更难解释。
    ///
    /// 与 `preferences_for` 共用同一次枚举的产物：大群路径拿到成员列表的同时
    /// 就拿到了偏好，不必为了过滤再查一遍。
    async fn all_participants(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cap: usize,
    ) -> Result<Option<HashMap<String, NotifyPreference>>, FlareError>;
}
