use std::collections::HashMap;

use crate::domain::model::{
    ConflictResolutionPolicy, ConversationLifecycleState, ConversationParticipant,
    ConversationVisibility, DeviceState,
};

/// 批量确认命令
#[derive(Debug, Clone)]
pub struct BatchAcknowledgeCommand {
    pub cursors: Vec<(String, i64)>,
}

/// 创建会话命令
#[derive(Debug, Clone)]
pub struct CreateConversationCommand {
    pub conversation_type: String,
    pub business_type: String,
    pub participants: Vec<ConversationParticipant>,
    pub attributes: HashMap<String, String>,
    pub visibility: ConversationVisibility,
    /// 落库：单聊由领域强制清空；非单聊写入 conversations.channel_id
    pub channel_id: String,
}

/// 删除会话命令
#[derive(Debug, Clone)]
pub struct DeleteConversationCommand {
    pub conversation_id: String,
    pub hard_delete: bool,
}

/// 强制会话同步命令
#[derive(Debug, Clone)]
pub struct ForceConversationSyncCommand {
    pub conversation_ids: Vec<String>,
    pub reason: Option<String>,
}

/// 管理参与者命令
#[derive(Debug, Clone)]
pub struct ManageParticipantsCommand {
    pub conversation_id: String,
    pub to_add: Vec<ConversationParticipant>,
    pub to_remove: Vec<String>,
    pub role_updates: Vec<(String, Vec<String>)>,
}

/// 更新游标命令
#[derive(Debug, Clone)]
pub struct UpdateCursorCommand {
    pub conversation_id: String,
    pub message_ts: i64,
}

/// 标记会话已读命令（read_seq 为 0 时由服务端用 last_message_seq）
#[derive(Debug, Clone)]
pub struct MarkConversationAsReadCommand {
    pub conversation_id: String,
    pub read_seq: i64,
}

/// 更新设备状态命令
#[derive(Debug, Clone)]
pub struct UpdatePresenceCommand {
    pub device_id: String,
    pub device_platform: Option<String>,
    pub state: DeviceState,
    pub conflict_resolution: Option<ConflictResolutionPolicy>,
    pub notify_conflict: bool,
    pub conflict_reason: Option<String>,
}

/// 更新会话命令
#[derive(Debug, Clone)]
pub struct UpdateConversationCommand {
    pub conversation_id: String,
    pub display_name: Option<String>,
    pub attributes: Option<HashMap<String, String>>,
    pub visibility: Option<ConversationVisibility>,
    pub lifecycle_state: Option<ConversationLifecycleState>,
}
