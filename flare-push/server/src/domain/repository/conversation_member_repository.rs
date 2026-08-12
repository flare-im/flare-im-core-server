//! 会话成员读取：给「大群离线推送」用。
//!
//! 大群走读扩散——推送侧拿到的是一条**不带收件人**的会话广播，只能通过在线索引
//! 找到在线的人。离线成员因此完全收不到推送通知，只能等下次打开 app 自己拉。
//! 要补上这条，就得把成员枚举出来再减去在线的那批。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_server_core::error::FlareError;

#[async_trait]
pub trait ConversationMemberReader: Send + Sync {
    /// 会话成员 ID。返回 `None` 表示**成员数超过 `cap`**，调用方应放弃本次离线扇出。
    ///
    /// 用 `None` 而不是截断列表：截断会变成「随机挑一部分人推送」，
    /// 比统一不推更难解释，也更难在日志里看出发生了什么。
    async fn member_ids(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        cap: usize,
    ) -> Result<Option<Vec<String>>, FlareError>;
}
