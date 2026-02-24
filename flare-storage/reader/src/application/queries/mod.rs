//! 查询结构体定义（Query DTO）

/// 查询消息列表
#[derive(Debug, Clone)]
pub struct QueryMessagesQuery {
    pub conversation_id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub limit: i32,
    pub cursor: Option<String>,
}

/// 获取单条消息
#[derive(Debug, Clone)]
pub struct GetMessageQuery {
    pub message_id: String,
}

/// 搜索消息
#[derive(Debug, Clone)]
pub struct SearchMessagesQuery {
    pub filters: Vec<flare_proto::common::FilterExpression>,
    pub start_time: i64, // 改为 i64，与 QueryMessagesQuery 保持一致
    pub end_time: i64,
    pub limit: i32,
}

/// 列出所有标签
#[derive(Debug, Clone)]
pub struct ListMessageTagsQuery {
    // 无参数
}

/// 基于 seq 查询消息列表
#[derive(Debug, Clone)]
pub struct QueryMessagesBySeqQuery {
    pub conversation_id: String,
    pub after_seq: i64,
    pub before_seq: Option<i64>,
    pub limit: i32,
    pub user_id: Option<String>,
}

/// 查询消息操作历史
#[derive(Debug, Clone)]
pub struct QueryMessageOperationsQuery {
    pub message_id: String,
}