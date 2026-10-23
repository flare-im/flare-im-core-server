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
    /// 跳过首页 total/version 元数据计算(COUNT(*) + MAX(updated_at) 各聚合整张成员表)。
    /// 群消息扇出只取 user_id、不看 total/version,置真可省去十万群每条消息两次 O(成员) 聚合。
    pub skip_metadata: bool,
    /// 在席（投递）还是成员（名单）口径，见 [`crate::domain::model::ParticipantScope`]。
    pub scope: crate::domain::model::ParticipantScope,
}

/// 一次查出会话里哪些 id 是成员（未退出；隐藏了会话的也算；不分页）：上行事件的操作者核对、
/// 插件资源桥的成员核对等按 id 判成员的读。
#[derive(Debug, Clone)]
pub struct FindConversationMembersQuery {
    pub conversation_id: String,
    /// `1..=MAX_PARTICIPANT_LOOKUP_IDS` 个互不相同的 user id。
    pub user_ids: Vec<String>,
}

/// [`FindConversationMembersQuery::user_ids`] 的上限。
pub const MAX_PARTICIPANT_LOOKUP_IDS: usize = 500;

/// 同步消息查询
#[derive(Debug, Clone)]
pub struct SyncMessagesQuery {
    pub conversation_id: String,
    pub since_ts: i64,
    pub cursor: Option<String>,
    pub limit: i32,
}

/// 群聊里调用者自己发送的一批消息的已读摘要。
#[derive(Debug, Clone)]
pub struct GetMessageReadSummariesQuery {
    pub conversation_id: String,
    /// `1..=MAX_READ_SUMMARY_MESSAGES` 个互不相同的服务端消息 ID。
    pub server_msg_ids: Vec<String>,
}

/// 群聊里调用者自己发送的一条消息的已读或未读成员（分页）。
#[derive(Debug, Clone)]
pub struct ListMessageReadersQuery {
    pub conversation_id: String,
    pub server_msg_id: String,
    pub state: crate::domain::model::ReaderState,
    pub cursor: String,
    /// 0 取默认页大小；上限 `MAX_READER_PAGE`。
    pub limit: i32,
}
