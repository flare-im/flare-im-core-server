//! 消息操作领域事件（Domain Event）
//!
//! 根据 DDD 架构，Domain Event 用于通知状态变更，驱动读模型更新

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::model::MessageFsmState;
use crate::domain::model::message_fsm::EditHistoryEntry;

/// 消息操作事件基类
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageOperationEvent {
    /// 消息ID
    pub message_id: String,
    /// 会话ID
    pub conversation_id: String,
    /// 操作者ID
    pub operator_id: String,
    /// 事件时间戳
    pub timestamp: DateTime<Utc>,
    /// 租户ID
    pub tenant_id: String,
}

/// 消息撤回事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRecalledEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 撤回原因（可选）
    pub reason: Option<String>,
    /// 新的FSM状态
    pub new_state: MessageFsmState,
}

/// 消息编辑事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEditedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 编辑版本号
    pub edit_version: i32,
    /// 新的FSM状态
    pub new_state: MessageFsmState,
    /// 编辑原因（可选）
    pub reason: Option<String>,
    /// 新的消息内容
    pub new_content: Vec<u8>,
    /// 完整的编辑历史
    pub edit_history: Vec<EditHistoryEntry>,
}

/// 消息删除事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDeletedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 删除类型（软删除/硬删除）
    pub delete_type: String,
    /// 新的FSM状态（硬删除时）
    pub new_state: Option<MessageFsmState>,
    /// 目标用户ID（软删除时使用）
    pub target_user_id: Option<String>,
}

/// 消息已读事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReadEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 已读的消息ID列表
    pub message_ids: Vec<String>,
    /// 已读时间戳
    pub read_at: DateTime<Utc>,
}

/// 消息反应添加事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReactionAddedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 表情符号
    pub emoji: String,
    /// 反应计数
    pub count: i32,
}

/// 消息反应移除事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReactionRemovedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 表情符号
    pub emoji: String,
    /// 反应计数
    pub count: i32,
}

/// 消息置顶事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePinnedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
}

/// 消息取消置顶事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageUnpinnedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
}

/// 消息收藏事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageFavoritedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
    /// 收藏标签
    pub tags: Vec<String>,
}

/// 消息取消收藏事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageUnfavoritedEvent {
    /// 基础事件字段
    #[serde(flatten)]
    pub base: MessageOperationEvent,
}

// -----------------------------------------------------------------------------
// 统一领域事件枚举（事件驱动：一次派发完成 Kafka Event + Push）
// -----------------------------------------------------------------------------

/// 消息操作领域事件枚举
///
/// 用于事件驱动：操作服务产生此事件后由 [crate::domain::service::operation_event_dispatcher::OperationEventDispatcher]
/// 统一派发（写 Kafka 操作流 + 推送），避免多处重复发布与冗余 trait。
#[derive(Debug, Clone)]
pub enum MessageOperationDomainEvent {
    Recalled(MessageRecalledEvent),
    Edited(MessageEditedEvent),
    Deleted(MessageDeletedEvent),
    Read(MessageReadEvent),
    ReactionAdded(MessageReactionAddedEvent),
    ReactionRemoved(MessageReactionRemovedEvent),
    Pinned(MessagePinnedEvent),
    Unpinned(MessageUnpinnedEvent),
    Favorited(MessageFavoritedEvent),
    Unfavorited(MessageUnfavoritedEvent),
}

