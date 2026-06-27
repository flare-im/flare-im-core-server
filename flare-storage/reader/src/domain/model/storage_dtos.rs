//! Storage 读模型 DTO（与 storage.proto 响应类型语义对齐，供接口层转换为 proto）

use chrono::{DateTime, Utc};

/// 编辑历史单条（对应 storage.proto MessageEditHistoryEntry）
#[derive(Debug, Clone)]
pub struct EditHistoryEntry {
    pub edit_version: i32,
    pub content_bytes: Vec<u8>,
    pub edited_at: Option<DateTime<Utc>>,
    pub editor_id: String,
    pub reason: Option<String>,
    pub show_edited_mark: bool,
}

/// 已读列表单条（对应 storage.proto MessageReadListEntry）
#[derive(Debug, Clone)]
pub struct ReadListEntry {
    pub user_id: String,
    pub read_at: Option<DateTime<Utc>>,
    pub burned_at: Option<DateTime<Utc>>,
}

/// 标记单条（对应 storage.proto MessageMarkEntry）
#[derive(Debug, Clone)]
pub struct MarkEntry {
    pub user_id: String,
    pub mark_type: i32, // MarkType 枚举值
    pub color: Option<String>,
    pub marked_at: Option<DateTime<Utc>>,
}

/// 反应单条（对应 storage.proto MessageReactionItem）
#[derive(Debug, Clone)]
pub struct ReactionItem {
    pub emoji: String,
    pub user_ids: Vec<String>,
    pub count: i32,
    pub last_updated: Option<DateTime<Utc>>,
}

/// 可见性状态（领域枚举，与 proto VisibilityStatus 对齐）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum VisibilityStatus {
    Visible = 0,
    Hidden = 1,
    Deleted = 2,
}

/// 置顶消息信息（对应 storage.proto PinnedMessageInfo）
#[derive(Debug, Clone)]
pub struct PinnedMessageInfo {
    pub message_id: String,
    pub user_id: String,
    pub scope: i32,
    pub owner_user_id: String,
    pub pinned_at: Option<DateTime<Utc>>,
    pub expire_at: Option<DateTime<Utc>>,
    pub reason: Option<String>,
}

/// 过滤表达式（对应 storage.proto FilterExpression）
#[derive(Debug, Clone)]
pub struct FilterExpression {
    pub field: String,
    pub operator: String,
    pub value: String,
}

/// 管理面消息导出任务草案。Storage Reader 只创建任务，真实文件生成由后续 export worker 执行。
#[derive(Debug, Clone)]
pub struct MessageExportTaskDraft {
    pub task_id: String,
    pub tenant_id: String,
    pub conversation_id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub filters: serde_json::Value,
    pub requested_by: Option<String>,
    pub request_id: String,
    pub trace_id: String,
}

/// 消息写链路账本过滤条件（Admin/运维查询）。
#[derive(Debug, Clone)]
pub struct MessageWriteLedgerQuery {
    pub tenant_id: String,
    pub server_id: Option<String>,
    pub conversation_id: Option<String>,
    pub write_state: Option<String>,
    pub failed_only: bool,
    pub updated_after: Option<DateTime<Utc>>,
    pub updated_before: Option<DateTime<Utc>>,
    pub limit: i64,
    pub offset: i64,
}

/// 消息写链路账本单条记录。
#[derive(Debug, Clone)]
pub struct MessageWriteLedgerEntry {
    pub tenant_id: String,
    pub server_id: String,
    pub conversation_id: String,
    pub seq: i64,
    pub write_state: String,
    pub archive_persisted_at: Option<DateTime<Utc>>,
    pub storage_persisted_at: Option<DateTime<Utc>>,
    pub wal_cleaned_at: Option<DateTime<Utc>>,
    pub ack_published_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 同步游标（对应 storage.proto SyncCursor）
#[derive(Debug, Clone)]
pub struct SyncCursor {
    pub user_id: String,
    pub conversation_id: String,
    pub last_seq: i64,
    pub last_message_id: i64,
    pub last_timestamp: i64,
}
