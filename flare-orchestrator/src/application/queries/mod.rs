//! 查询结构体定义（Query DTO）

use serde::{Deserialize, Serialize};

/// 查询消息请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMessageQuery {
    /// 消息ID
    pub message_id: String,
    /// 会话ID
    pub conversation_id: String,
}

/// 查询消息列表请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMessagesQuery {
    /// 会话ID
    pub conversation_id: String,
    /// 分页参数
    pub limit: Option<i32>,
    /// 游标（用于分页）
    pub cursor: Option<String>,
    /// 开始时间戳（可选）
    pub start_time: Option<i64>,
    /// 结束时间戳（可选）
    pub end_time: Option<i64>,
}

/// 搜索消息请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMessagesQuery {
    /// 会话ID（可选）
    pub conversation_id: Option<String>,
    /// 搜索关键词
    pub keyword: String,
    /// 分页参数
    pub limit: Option<i32>,
    /// 游标（用于分页）
    pub cursor: Option<String>,
}

/// 查询消息结果（带分页信息）
#[derive(Debug, Clone)]
pub struct QueryMessagesResult {
    /// 消息列表
    pub messages: Vec<flare_proto::common::Message>,
    /// 下一页游标
    pub next_cursor: String,
    /// 是否还有更多数据
    pub has_more: bool,
}
