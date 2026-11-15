//! 补提示的取数（Port）。
//!
//! 设备交接只带会话与 seq，要渲染一条正常的离线任务还得把那条消息读回来；网关失联的交接
//! 连会话都不带，得向会话服务问出窗口内有新未读的会话。两者都只在交接时才读，
//! 不在消息路由的路径上。

use flare_im_contracts::Ctx;
use flare_proto::common::Message;
use flare_server_core::error::FlareError;

#[async_trait::async_trait]
pub trait MessageLookupRepository: Send + Sync {
    /// 读某会话里 seq 对应的那条消息，按 `user_id` 的可见性过滤（入群前的、为自己删除的
    /// 消息不返回）。
    ///
    /// `Ok(None)` 表示这个 seq 上没有这个用户可见的消息：可能是还没落库，也可能已被撤回或
    /// 删除。调用方分不清这两种情况，所以两种都按「读不到」处理，用通用文案提示。
    async fn message_at(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        seq: u64,
        user_id: &str,
    ) -> Result<Option<Message>, FlareError>;
}

/// 用户有新未读的一个会话，只保留补提示要看的字段。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecentConversation {
    pub conversation_id: String,
    /// 最新一条消息的会话 seq。
    pub last_seq: u64,
    /// 最新一条消息的服务端 ID。
    pub last_message_id: String,
    /// 最新一条消息的发送者。
    pub last_sender_id: String,
    /// 最新一条消息的时间，Unix epoch 毫秒。
    pub last_message_at: i64,
    /// 这个用户在该会话的未读数。
    pub unread: u32,
}

#[async_trait::async_trait]
pub trait RecentConversationRepository: Send + Sync {
    /// `user_id` 在 `since`（Unix epoch 毫秒）之后有新未读的会话：他仍在其中、最后一条消息晚于
    /// `since`、不是他自己发的、且他还有未读。按最后一条消息的时间从新到旧，至多 `limit` 个；
    /// 实现可以给得更少（会话服务单次至多 20 个）。
    ///
    /// 筛选由会话服务做，调用方不再筛一遍：未读怎么算只有它知道，规则只留一份。
    /// 实现以服务身份按用户读取，不冒用用户身份。错误的可重试性如实反映原因：
    /// 暂时不可用可重试，被拒绝不可重试。
    async fn recent_conversations(
        &self,
        ctx: &Ctx,
        user_id: &str,
        since: i64,
        limit: usize,
    ) -> Result<Vec<RecentConversation>, FlareError>;
}
