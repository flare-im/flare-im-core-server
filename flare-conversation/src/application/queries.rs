use std::collections::HashMap;

use crate::domain::model::{ConversationFilter, ConversationSort};

/// 列出会话查询
#[derive(Debug, Clone)]
pub struct ListConversationsQuery {
    pub cursor: Option<String>,
    pub limit: i32,
}

/// 搜索会话查询
#[derive(Debug, Clone)]
pub struct SearchConversationsQuery {
    pub filters: Vec<ConversationFilter>,
    pub sort: Vec<ConversationSort>,
    pub limit: usize,
    pub offset: usize,
}

/// 会话引导查询
#[derive(Debug, Clone)]
pub struct ConversationBootstrapQuery {
    pub client_cursor: HashMap<String, i64>,
    pub include_recent: bool,
    pub recent_limit: Option<i32>,
    /// 增量过滤边界（毫秒）：只返回 effective_updated_at 晚于该时刻的会话；0=全量。
    pub updated_after_ms: i64,
    /// 返回会话数上限：0=服务默认；>0 受硬上限钳制（编排层快照分页用高值覆盖大账号）。
    pub max_conversations: i32,
}

/// 单会话详情（读模型）
#[derive(Debug, Clone)]
pub struct GetConversationDetailQuery {
    pub conversation_id: String,
}

#[derive(Debug, Clone)]
pub struct ListConversationParticipantsQuery {
    pub conversation_id: String,
    pub cursor: Option<String>,
    pub limit: i32,
    pub include_removed: bool,
}

/// 同步消息查询
#[derive(Debug, Clone)]
pub struct SyncMessagesQuery {
    pub conversation_id: String,
    pub since_ts: i64,
    pub cursor: Option<String>,
    pub limit: i32,
}
