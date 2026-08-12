//! 用户会话级通知偏好（Port）。
//!
//! 只在**即将生成离线推送任务**时才会被问到：在线用户的事件走长连接，客户端自己
//! 决定要不要提示；只有离线推送是「设备上会真的响一声」的那一路，服务端不替用户
//! 拦，就没有任何人能拦了。
//!
//! 放在离线分支而不是发信热路径，是因为成本结构完全不同：发信是每条消息都走，
//! 而离线推送本就要发起 APNs/FCM 网络调用，多一次查询是噪音。

use std::collections::HashSet;

use flare_im_contracts::Ctx;
use flare_server_core::error::FlareError;

#[async_trait::async_trait]
pub trait NotifyPolicyRepository: Send + Sync {
    /// 返回 `user_ids` 中**已对该会话设置免打扰**的子集。
    ///
    /// 实现应当在自身不可用时返回空集而不是错误：免打扰是偏好不是安全边界，
    /// 「该静音的响了一声」远好过「该到的推送没到」。调用方据此 fail-open。
    async fn muted_users(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashSet<String>, FlareError>;

    /// 返回 `user_ids` 中**只接收提到我的消息**的子集。
    ///
    /// 与 `muted_users` 分开返回而不是合成一个「该不该推」的布尔：是否该推还取决于
    /// 这条消息有没有提到本人，而那是消息侧的信息，不该让偏好仓储去解析消息内容。
    ///
    /// 同样 fail-open：查不到就当没设，宁可多响一声也别把该到的推送吞掉。
    async fn mention_only_users(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<HashSet<String>, FlareError>;
}
