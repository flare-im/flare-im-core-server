//! 管理员只读视图：租户由接口层从已验证的 Staff 上下文取得，读取不要求调用者是会话成员。

use super::Conversation;

/// 一条会话（`participants` 恒为空）与它当前的有效成员数。
#[derive(Clone, Debug)]
pub struct AdminConversation {
    pub conversation: Conversation,
    pub member_count: i64,
}

/// 按 `updated_at` 倒序的一页会话。
#[derive(Clone, Debug)]
pub struct AdminConversationPage {
    pub conversations: Vec<AdminConversation>,
    /// 不透明游标；`has_more=false` 时为 `None`。
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
