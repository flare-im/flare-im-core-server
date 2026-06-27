use std::collections::HashMap;

use crate::domain::model::{
    ConflictResolutionPolicy, ConversationLifecycleState, ConversationParticipant,
    ConversationType, ConversationVisibility, DeviceState,
};

/// 创建会话命令
#[derive(Debug, Clone)]
pub struct CreateConversationCommand {
    pub conversation_type: ConversationType,
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
    pub sync_seq: i64,
}

/// 标记会话已读命令（read_seq 必须为明确的正数会话序列）
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

/// 更新当前用户在某会话上的偏好
#[derive(Debug, Clone)]
pub struct UpdateConversationUserSettingsCommand {
    pub conversation_id: String,
    pub is_pinned: Option<bool>,
    pub is_muted: Option<bool>,
    pub is_archived: Option<bool>,
    pub draft: Option<String>,
    pub base_settings_version: u64,
}
