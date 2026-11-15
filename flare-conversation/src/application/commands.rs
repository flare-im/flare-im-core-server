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

/// 清除会话早于某服务端时刻的历史命令
#[derive(Debug, Clone)]
pub struct PurgeConversationHistoryCommand {
    pub conversation_id: String,
    /// 服务端时间（Unix 毫秒）：只删除时间严格早于它的消息与事件；
    /// 有纪元围栏时只作用于没有纪元属性的消息，可以为 0
    pub before_ms: i64,
    /// 纪元属性名（消息 attributes 的键）；只与 `before_epoch > 0` 一起出现
    pub epoch_attribute: String,
    /// 纪元围栏：带纪元属性的消息只在纪元严格小于它时删除；0 = 不按纪元
    pub before_epoch: i64,
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
    pub is_mention_only: Option<bool>,
    pub is_pinned: Option<bool>,
    pub is_muted: Option<bool>,
    pub is_archived: Option<bool>,
    pub draft: Option<String>,
    pub base_settings_version: u64,
    pub clear_history_through_seq: Option<u64>,
    /// 大群里显式开启「全部消息通知」。
    pub notify_all: Option<bool>,
}

/// 部分更新调用者本人的用户级通知设置（原始请求字段，处理器校验后转成领域补丁）。
#[derive(Debug, Clone)]
pub struct UpdateUserNotificationSettingsCommand {
    /// 设置勿扰时段（整体替换）。
    pub quiet_hours: Option<crate::domain::model::QuietHours>,
    /// 取消勿扰时段；与 `quiet_hours` 互斥。
    pub clear_quiet_hours: bool,
    /// `NotificationPreview` 的线上取值。
    pub preview: Option<i32>,
    /// `MultiDeviceAlertPolicy` 的线上取值。
    pub multi_device: Option<i32>,
    /// 客户端已知版本；0 表示不校验。
    pub base_settings_version: u64,
}
