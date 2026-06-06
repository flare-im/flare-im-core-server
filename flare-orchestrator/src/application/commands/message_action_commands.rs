//! 消息操作命令：撤回、编辑、删除、已读、反应、置顶、标记及对应 App* DTO。
//!
//! Command 仅表达写路径；读模型由 Storage Reader 提供。

use chrono::{DateTime, Utc};
use flare_proto::common::Pagination as ProtoPagination;
use flare_proto::message_content_ext::MessageContentExt;
use serde::{Deserialize, Serialize};

fn operator_id_from_ctx(ctx: &flare_server_core::context::Ctx) -> String {
    ctx.actor()
        .map(|a| a.actor_id().to_string())
        .or_else(|| ctx.user_id().map(|u| u.to_string()))
        .unwrap_or_default()
}

/// 本地分页结构体（用于序列化/反序列化）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalPagination {
    /// 下一页游标（空表示没有更多数据）
    pub cursor: Option<String>,
    /// 每页数量限制（1-1000，建议20-100）
    pub limit: Option<i32>,
    /// 是否有更多数据
    pub has_more: Option<bool>,
    /// 上一页游标（可选）
    pub previous_cursor: Option<String>,
    /// 总记录数（可选，可能影响性能）
    pub total_size: Option<i64>,
}

impl From<&ProtoPagination> for LocalPagination {
    fn from(pagination: &ProtoPagination) -> Self {
        Self {
            cursor: if pagination.cursor.is_empty() {
                None
            } else {
                Some(pagination.cursor.clone())
            },
            limit: if pagination.limit > 0 {
                Some(pagination.limit)
            } else {
                None
            },
            has_more: Some(pagination.has_more),
            previous_cursor: if pagination.previous_cursor.is_empty() {
                None
            } else {
                Some(pagination.previous_cursor.clone())
            },
            total_size: if pagination.total_size > 0 {
                Some(pagination.total_size)
            } else {
                None
            },
        }
    }
}

/// 消息操作命令基类
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageOperationCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 操作者ID
    pub operator_id: String,
    /// 操作时间戳
    pub timestamp: DateTime<Utc>,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

/// 撤回消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 撤回原因（可选）
    pub reason: Option<String>,
    /// 撤回时间限制（秒，可选）
    pub time_limit_seconds: Option<i32>,
    /// 是否允许管理员权限覆盖发送者限制
    pub allow_admin_override: bool,
}

impl RecallMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::RecallMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = operator_id_from_ctx(ctx);

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: request.conversation_id.clone(),
            },
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            time_limit_seconds: if request.recall_time_limit_seconds > 0 {
                Some(request.recall_time_limit_seconds)
            } else {
                None
            },
            allow_admin_override: false,
        }
    }
}

/// 编辑消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 编辑后的内容（二进制）
    pub new_content: Vec<u8>,
    /// 编辑原因（可选）
    pub reason: Option<String>,
    /// 是否允许管理员权限覆盖发送者限制
    pub allow_admin_override: bool,
}

impl EditMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::EditMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = operator_id_from_ctx(ctx);

        let new_content = request
            .new_content
            .as_ref()
            .and_then(|content| content.encode_to_bytes().ok())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            new_content,
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            allow_admin_override: false,
        }
    }
}

/// 删除消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 删除类型（软删除/硬删除）
    pub delete_type: DeleteType,
    /// 删除作用域（用户私有/会话全局）
    pub delete_scope: DeleteScope,
    /// 删除原因（可选）
    pub reason: Option<String>,

    /// 要删除的消息ID列表（批量删除）
    pub message_ids: Vec<String>,
    /// 是否通知其他用户
    pub notify_others: bool,
    /// 目标用户ID（用户私有删除时用于多端同步）
    pub target_user_id: Option<String>,
    /// 是否允许管理员权限覆盖发送者限制
    pub allow_admin_override: bool,
}

impl DeleteMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::DeleteMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = operator_id_from_ctx(ctx);
        let target_user_id = operator_id.clone();

        let delete_type = if request.delete_type == 2 {
            DeleteType::Hard
        } else {
            DeleteType::Soft
        };
        let delete_scope = request
            .scope
            .and_then(DeleteScope::from_proto_value)
            .unwrap_or_else(|| DeleteScope::default_for_type(delete_type));

        Self {
            base: MessageOperationCommand {
                message_id: String::new(), // 批量删除不需要特定消息ID
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: request.conversation_id.clone(),
            },
            delete_type,
            delete_scope,
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            message_ids: request.message_ids.clone(),
            notify_others: request.notify_others,
            target_user_id: Some(target_user_id),
            allow_admin_override: false,
        }
    }
}

/// 删除类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeleteType {
    /// 软删除（仅对当前用户隐藏）
    Soft,
    /// 硬删除（永久删除，仅管理员）
    Hard,
}

/// 删除作用域
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeleteScope {
    /// 仅操作人账号多端生效
    UserPrivate,
    /// 会话全员生效
    ConversationGlobal,
}

impl DeleteScope {
    pub fn from_proto_value(v: i32) -> Option<Self> {
        match v {
            1 => Some(Self::UserPrivate),
            2 => Some(Self::ConversationGlobal),
            _ => None,
        }
    }

    pub fn to_proto_value(self) -> i32 {
        match self {
            Self::UserPrivate => 1,
            Self::ConversationGlobal => 2,
        }
    }

    pub fn default_for_type(delete_type: DeleteType) -> Self {
        match delete_type {
            DeleteType::Soft => Self::UserPrivate,
            DeleteType::Hard => Self::ConversationGlobal,
        }
    }
}

/// 已读消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 已读的消息ID列表（批量已读）
    pub message_ids: Vec<String>,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 是否阅后即焚
    pub burn_after_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBurnMessageCommand {
    pub tenant_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub reader_id: String,
    pub read_at: DateTime<Utc>,
    pub burn_after_read_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurnDueMessagesCommand {
    pub tenant_id: String,
    pub now: i64,
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardDeleteBurnedMessagesCommand {
    pub tenant_id: String,
    pub now: i64,
    pub limit: i64,
}

impl ReadMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::MarkMessageReadRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = operator_id_from_ctx(ctx);

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: request.conversation_id.clone(),
            },
            message_ids: vec![request.message_id.clone()],
            read_at: request.read_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            burn_after_read: false,
        }
    }
}

/// 添加反应命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddReactionCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 表情符号
    pub emoji: String,
}

impl AddReactionCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::AddReactionRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            emoji: request.emoji.clone(),
        }
    }
}

/// 移除反应命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveReactionCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 表情符号
    pub emoji: String,
}

impl RemoveReactionCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::RemoveReactionRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            emoji: request.emoji.clone(),
        }
    }
}

/// 置顶消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 置顶原因（可选）
    pub reason: Option<String>,
    /// 置顶到期时间（可选）
    pub expire_at: Option<DateTime<Utc>>,
}

impl PinMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::PinMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            expire_at: request.expire_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
        }
    }
}

/// 取消置顶消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnpinMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
}

impl UnpinMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::UnpinMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
        }
    }
}

/// 标记消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 标记类型（0: Important, 1: Todo, 2: Done, 3: Custom）
    pub mark_type: i32,
}

impl MarkMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::MarkMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            mark_type: request.mark_type,
        }
    }
}

/// 取消标记消息命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmarkMessageCommand {
    /// 基础命令字段
    #[serde(flatten)]
    pub base: MessageOperationCommand,
    /// 标记类型（可选，如果为 None 则取消所有标记）
    pub mark_type: Option<i32>,
    /// 用户ID（执行取消标记操作的用户）
    pub user_id: String,
}

impl UnmarkMessageCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::UnmarkMessageRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();

        Self {
            base: MessageOperationCommand {
                message_id: request.message_id.clone(),
                operator_id,
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: String::new(), // 会在后续填充
            },
            mark_type: if request.mark_type < 0 {
                None
            } else {
                Some(request.mark_type)
            },
            user_id: String::new(), // 由 handler 从 context 填充
        }
    }
}

/// 批量标记已读命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMarkMessageReadCommand {
    /// 会话ID
    pub conversation_id: String,
    /// 用户ID
    pub user_id: String,
    /// 要标记已读的消息ID列表（如果为空，则标记会话中所有未读消息）
    pub message_ids: Vec<String>,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
}

impl BatchMarkMessageReadCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::BatchMarkMessageReadRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let read_at = request
            .read_at
            .map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            })
            .unwrap_or_else(chrono::Utc::now);

        Self {
            conversation_id: request.conversation_id.clone(),
            user_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            message_ids: request.message_ids.clone(),
            read_at: Some(read_at),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        }
    }
}

/// 标记会话已读命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkConversationReadCommand {
    /// 会话ID
    pub conversation_id: String,
    /// 用户ID
    pub user_id: String,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
}

impl MarkConversationReadCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        request: &flare_grpc_proto::message::MarkConversationReadRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        let read_at = request
            .read_at
            .map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            })
            .unwrap_or_else(chrono::Utc::now);

        Self {
            conversation_id: request.conversation_id.clone(),
            user_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            read_at: Some(read_at),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        }
    }
}

/// 标记全部会话已读命令
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkAllConversationsReadCommand {
    /// 用户ID
    pub user_id: String,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 会话类型筛选（可选，如果为空则标记所有类型的会话）
    pub conversation_types: Vec<String>,
    /// 租户ID
    pub tenant_id: String,
}

impl MarkAllConversationsReadCommand {
    /// 从protobuf请求创建命令
    pub fn from_request(
        _request: &flare_grpc_proto::message::MarkAllConversationsReadRequest,
        ctx: &flare_server_core::context::Ctx,
    ) -> Self {
        Self {
            user_id: String::new(), // 从context获取
            read_at: Some(chrono::Utc::now()),
            conversation_types: Vec::new(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        }
    }
}

// 定义独立的应用层命令结构，完全与protobuf解耦
// 这些是真正的应用层命令，不依赖protobuf结构

/// 应用层撤回消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecallMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 撤回原因（可选）
    pub reason: Option<String>,
    /// 撤回时间限制（秒，可选）
    pub time_limit_seconds: Option<i32>,
    /// 操作者ID
    pub operator_id: String,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::RecallMessageRequest> for AppRecallMessageCommand {
    fn from(request: &flare_grpc_proto::message::RecallMessageRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            time_limit_seconds: if request.recall_time_limit_seconds > 0 {
                Some(request.recall_time_limit_seconds)
            } else {
                None
            },
            operator_id: String::new(), // 从context填充
            tenant_id: String::new(),   // 从context填充
            conversation_id: request.conversation_id.clone(), // 客户端可选提供，便于无 StorageReader 时解析
        }
    }
}

/// 应用层编辑消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppEditMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 新内容（字节形式）
    pub new_content: Vec<u8>,
    /// 编辑原因（可选）
    pub reason: Option<String>,
    /// 显示编辑标记
    pub show_edited_mark: bool,
    /// 编辑版本
    pub edit_version: i32,
    /// 操作者ID
    pub operator_id: String,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::EditMessageRequest> for AppEditMessageCommand {
    fn from(request: &flare_grpc_proto::message::EditMessageRequest) -> Self {
        let new_content = request
            .new_content
            .as_ref()
            .and_then(|content| content.encode_to_bytes().ok())
            .unwrap_or_default();

        Self {
            message_id: request.message_id.clone(),
            new_content,
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            show_edited_mark: request.show_edited_mark,
            edit_version: request.edit_version,
            operator_id: String::new(), // 从context填充
            tenant_id: String::new(),   // 从context填充
            conversation_id: request.conversation_id.clone(), // 客户端可选提供，便于无 StorageReader 时解析
        }
    }
}

/// 应用层删除消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppDeleteMessageCommand {
    /// 消息ID列表（批量删除）
    pub message_ids: Vec<String>,
    /// 会话ID
    pub conversation_id: String,
    /// 删除类型
    pub delete_type: DeleteType,
    /// 删除作用域
    pub delete_scope: DeleteScope,
    /// 删除原因（可选）
    pub reason: Option<String>,

    /// 通知其他人
    pub notify_others: bool,
    /// 目标用户ID（用户私有删除）
    pub target_user_id: Option<String>,
    /// 硬删除标志
    pub hard_delete: bool,
    /// 操作者ID
    pub operator_id: String,
    /// 租户ID
    pub tenant_id: String,
}

impl From<&flare_grpc_proto::message::DeleteMessageRequest> for AppDeleteMessageCommand {
    fn from(request: &flare_grpc_proto::message::DeleteMessageRequest) -> Self {
        let delete_type = if request.delete_type == 2 {
            DeleteType::Hard
        } else {
            DeleteType::Soft
        };
        let delete_scope = request
            .scope
            .and_then(DeleteScope::from_proto_value)
            .unwrap_or_else(|| DeleteScope::default_for_type(delete_type));

        Self {
            message_ids: request.message_ids.clone(),
            conversation_id: request.conversation_id.clone(),
            delete_type,
            delete_scope,
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            notify_others: request.notify_others,
            target_user_id: None,
            hard_delete: request.delete_type == 2,
            operator_id: String::new(), // 从context填充
            tenant_id: String::new(),   // 从context填充
        }
    }
}

/// 应用层添加反应命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppAddReactionCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// 表情符号
    pub emoji: String,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::AddReactionRequest> for AppAddReactionCommand {
    fn from(request: &flare_grpc_proto::message::AddReactionRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            emoji: request.emoji.clone(),
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层移除反应命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRemoveReactionCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// 表情符号
    pub emoji: String,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::RemoveReactionRequest> for AppRemoveReactionCommand {
    fn from(request: &flare_grpc_proto::message::RemoveReactionRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            emoji: request.emoji.clone(),
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层置顶消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppPinMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 操作者ID
    pub operator_id: String,
    /// 置顶原因（可选）
    pub reason: Option<String>,
    /// 过期时间（可选）
    pub expire_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::PinMessageRequest> for AppPinMessageCommand {
    fn from(request: &flare_grpc_proto::message::PinMessageRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            operator_id: String::new(), // 由 handler 从 context 填充
            reason: if request.reason.is_empty() {
                None
            } else {
                Some(request.reason.clone())
            },
            expire_at: request.expire_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层取消置顶消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppUnpinMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 操作者ID
    pub operator_id: String,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::UnpinMessageRequest> for AppUnpinMessageCommand {
    fn from(request: &flare_grpc_proto::message::UnpinMessageRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            operator_id: String::new(),     // 由 handler 从 context 填充
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层标记消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMarkMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// 标记类型
    pub mark_type: i32,
    /// 标记颜色（可选）
    pub color: Option<String>,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::MarkMessageRequest> for AppMarkMessageCommand {
    fn from(request: &flare_grpc_proto::message::MarkMessageRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            mark_type: request.mark_type,
            color: if request.color.is_empty() {
                None
            } else {
                Some(request.color.clone())
            },
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层取消标记消息命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppUnmarkMessageCommand {
    /// 目标消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// 标记类型（-1表示取消所有标记）
    pub mark_type: i32,
    /// 租户ID
    pub tenant_id: String,
    /// 会话ID
    pub conversation_id: String,
}

impl From<&flare_grpc_proto::message::UnmarkMessageRequest> for AppUnmarkMessageCommand {
    fn from(request: &flare_grpc_proto::message::UnmarkMessageRequest) -> Self {
        Self {
            message_id: request.message_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            mark_type: request.mark_type,
            tenant_id: String::new(),       // 从context填充
            conversation_id: String::new(), // 从查询获取
        }
    }
}

/// 应用层批量标记已读命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppBatchMarkMessageReadCommand {
    /// 会话ID
    pub conversation_id: String,
    /// 用户ID
    pub user_id: String,
    /// 消息ID列表
    pub message_ids: Vec<String>,
    /// 已读时间
    pub read_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
}

impl From<&flare_grpc_proto::message::BatchMarkMessageReadRequest>
    for AppBatchMarkMessageReadCommand
{
    fn from(request: &flare_grpc_proto::message::BatchMarkMessageReadRequest) -> Self {
        Self {
            conversation_id: request.conversation_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            message_ids: request.message_ids.clone(),
            read_at: request.read_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            tenant_id: String::new(), // 从context填充
        }
    }
}

/// 应用层标记会话已读命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMarkConversationReadCommand {
    /// 会话ID
    pub conversation_id: String,
    /// 用户ID
    pub user_id: String,
    /// 已读时间
    pub read_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
}

impl From<&flare_grpc_proto::message::MarkConversationReadRequest>
    for AppMarkConversationReadCommand
{
    fn from(request: &flare_grpc_proto::message::MarkConversationReadRequest) -> Self {
        Self {
            conversation_id: request.conversation_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            read_at: request.read_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            tenant_id: String::new(), // 从context填充
        }
    }
}

/// 应用层标记全部会话已读命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMarkAllConversationsReadCommand {
    /// 用户ID
    pub user_id: String,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 会话类型筛选（可选）
    pub conversation_types: Vec<String>,
    /// 租户ID
    pub tenant_id: String,
}

impl From<&flare_grpc_proto::message::MarkAllConversationsReadRequest>
    for AppMarkAllConversationsReadCommand
{
    fn from(request: &flare_grpc_proto::message::MarkAllConversationsReadRequest) -> Self {
        Self {
            user_id: String::new(), // 由 handler 从 context 填充
            read_at: request.read_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            conversation_types: request.conversation_types.clone(),
            tenant_id: String::new(), // 从context填充
        }
    }
}

/// 应用层标记消息直到指定消息已读命令 - 完全与protobuf解耦
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMarkMessagesReadUntilCommand {
    /// 会话ID
    pub conversation_id: String,
    /// 用户ID
    pub user_id: String,
    /// 已读到的消息ID
    pub until_message_id: String,
    /// 已读时间戳（可选，默认当前时间）
    pub read_at: Option<DateTime<Utc>>,
    /// 租户ID
    pub tenant_id: String,
}

impl From<&flare_grpc_proto::message::MarkMessagesReadUntilRequest>
    for AppMarkMessagesReadUntilCommand
{
    fn from(request: &flare_grpc_proto::message::MarkMessagesReadUntilRequest) -> Self {
        Self {
            conversation_id: request.conversation_id.clone(),
            user_id: String::new(), // 由 handler 从 context 填充
            until_message_id: request.until_message_id.clone(),
            read_at: request.read_at.map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
            }),
            tenant_id: String::new(), // 从context填充
        }
    }
}
