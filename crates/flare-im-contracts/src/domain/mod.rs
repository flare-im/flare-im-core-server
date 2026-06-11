//! 跨服务领域契约（Gateway ↔ Orchestrator、Online、Route、Hook 等）
//!
//! **不要删除**：`MessageCommandHandler` / `SyncQueryHandler` 与 `ConnectionEvent` 等被多个 workspace crate 直接依赖；
//! 若迁入 `flare-server-core` 会把 IM 语义绑进通用库，反而污染分层。Orchestrator 内部另有应用层 `SendMessageCommand`，勿混淆。

use crate::Ctx;
use std::collections::HashMap;

pub type ConversationId = String;
pub type UserId = String;
pub type MessageId = String;
pub type ClientMessageId = String;
pub type Seq = u64;
pub type DeviceId = String;
pub type ConnectionId = String;
pub type GatewayId = String;

/// 连接事件占位（signaling/online 等使用）
#[derive(Debug, Clone)]
pub struct ConnectionEvent {
    pub user_id: String,
    pub device_id: Option<String>,
    pub state: String,
}

/// 事件元数据占位（与 signaling/online 等使用处对齐）
#[derive(Debug, Clone, Default)]
pub struct EventMeta {
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub tenant_id: Option<String>,
    pub operator_id: Option<String>,
    pub occurred_at_ms: Option<i64>,
}

/// 发送消息命令（供 Gateway 长连接上行 → Orchestrator 的场景）
#[derive(Debug, Clone)]
pub struct SendMessageCommand {
    pub conversation_id: ConversationId,
    pub client_msg_id: ClientMessageId,
    pub sender_id: UserId,
    pub message_type: i32,
    pub content: Vec<u8>,
    pub receiver_id: Option<String>,
    pub extra: HashMap<String, String>,
}

/// 发送消息 ACK 结果
#[derive(Debug, Clone)]
pub struct SendAckResult {
    pub success: bool,
    pub server_msg_id: Option<String>,
    pub seq: Option<u64>,
    pub error_code: Option<i32>,
    pub error_message: Option<String>,
}

/// 删除类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteType {
    Soft,
    Hard,
}

/// 标记类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkType {
    Important,
    Todo,
    Done,
    Custom,
}

/// Reaction 动作
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactionAction {
    Add,
    Remove,
}

/// 消息命令（操作事件统一入口）
#[derive(Debug, Clone)]
pub enum MessageCommand {
    Send(SendMessageCommand),
    Recall {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        operator_id: UserId,
        reason: Option<String>,
        request_id: Option<String>,
    },
    Edit {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        operator_id: UserId,
        new_content: Vec<u8>,
        request_id: Option<String>,
    },
    Delete {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        operator_id: UserId,
        delete_type: DeleteType,
        request_id: Option<String>,
    },
    ReadReceipt {
        conversation_id: ConversationId,
        user_id: UserId,
        read_seq: Option<i64>,
        message_ids: Option<Vec<String>>,
        request_id: Option<String>,
    },
    Reaction {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        user_id: UserId,
        emoji: String,
        action: ReactionAction,
        request_id: Option<String>,
    },
    Pin {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        pinned_by: UserId,
        request_id: Option<String>,
    },
    Unpin {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        request_id: Option<String>,
    },
    Mark {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        user_id: UserId,
        mark_type: MarkType,
        request_id: Option<String>,
    },
    Unmark {
        conversation_id: ConversationId,
        server_msg_id: MessageId,
        user_id: UserId,
        mark_type: MarkType,
        request_id: Option<String>,
    },
}

/// 操作结果
#[derive(Debug, Clone)]
pub struct OperationResult {
    pub request_id: Option<String>,
    pub success: bool,
    pub error_code: Option<i32>,
    pub error_message: Option<String>,
}

/// 消息命令处理 trait（Gateway 通过此 trait 走本地编排器发消息/操作）
pub trait MessageCommandHandler: Send + Sync {
    async fn handle_send_message(
        &self,
        ctx: &Ctx,
        cmd: &SendMessageCommand,
    ) -> flare_core_base::error::Result<SendAckResult>;

    async fn handle_message_operation(
        &self,
        ctx: &Ctx,
        cmd: &MessageCommand,
    ) -> flare_core_base::error::Result<Option<OperationResult>>;
}

/// 同步查询结果
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    pub events: Vec<Vec<u8>>,
    pub max_seq: u64,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub window_id: Option<String>,
}

/// 多设备同步结果中的单会话切片
#[derive(Debug, Clone, Default)]
pub struct ConversationSyncSlice {
    pub conversation_id: String,
    pub events: Vec<Vec<u8>>,
    pub max_seq: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// 多设备同步结果
#[derive(Debug, Clone, Default)]
pub struct MultiDeviceSyncResult {
    pub slices: Vec<ConversationSyncSlice>,
    pub max_seq_per_conversation: HashMap<String, u64>,
    pub has_more: bool,
}

/// 同步查询处理 trait（Gateway 通过此 trait 走本地查询服务做会话同步）
pub trait SyncQueryHandler: Send + Sync {
    async fn handle_sync_conversation(
        &self,
        ctx: &Ctx,
        conversation_id: &ConversationId,
        last_seq: u64,
        cursor: Option<&str>,
        limit: i32,
        device_id: Option<&DeviceId>,
    ) -> flare_core_base::error::Result<SyncResult>;

    async fn handle_multi_device_sync(
        &self,
        ctx: &Ctx,
        user_id: &UserId,
        device_id: &DeviceId,
        conversation_ids: &[ConversationId],
        last_seq_per_conversation: &HashMap<String, u64>,
        limit: i32,
    ) -> flare_core_base::error::Result<MultiDeviceSyncResult>;
}
