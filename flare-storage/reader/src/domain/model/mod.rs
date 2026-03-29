//! 领域模型定义（与 proto 解耦，application/domain/infrastructure 仅使用本模块类型）

mod conversation_head;
mod event;
mod message;
mod storage_dtos;

pub use conversation_head::ConversationMessageHead;
pub use event::{Event, EventType};
pub use message::{Attachment, Message};
pub use storage_dtos::{
    EditHistoryEntry, FilterExpression, MarkEntry, PinnedMessageInfo, ReactionItem, ReadListEntry,
    SyncCursor, VisibilityStatus,
};

/// 消息更新结构（读模型侧，写操作如已读/撤回/反应更新）
#[derive(Default)]
pub struct MessageUpdate {
    pub is_recalled: Option<bool>,
    pub recalled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub visibility: Option<std::collections::HashMap<String, VisibilityStatus>>,
    pub read_by: Option<Vec<ReadListEntry>>,
    pub operations: Option<Vec<Event>>,
    pub attributes: Option<std::collections::HashMap<String, String>>,
    pub tags: Option<Vec<String>>,
    pub reactions: Option<Vec<ReactionItem>>,
    pub status: Option<i32>,
}
