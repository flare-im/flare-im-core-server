use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;

use flare_proto::common::Message;
use flare_proto::common::{
    ConflictResolution as ProtoConflictResolution,
    ConversationLifecycleState as ProtoConversationLifecycleState,
    ConversationType as ProtoConversationType,
    ConversationVisibility as ProtoConversationVisibility, DeviceState as ProtoDeviceState,
};

/// 会话类型枚举（与 proto ConversationType、数据库 conversation_type INT 对齐）
///
/// 枚举值与 CID TypePrefix 一致：
/// - SINGLE = 1 (CID 前缀 1)
/// - GROUP = 2 (CID 前缀 2)
/// - AI = 3 (CID 前缀 3)
/// - SYSTEM = 4 (CID 前缀 4)
/// - CUSTOMER = 5 (CID 前缀 5)
/// - TEMP = 6 (CID 前缀 6)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationType {
    Unspecified = 0,
    Single = 1,
    Group = 2,
    Ai = 3,
    System = 4,
    Customer = 5,
    Temp = 6,
}

impl ConversationType {
    /// 从 `flare.common.v1.ConversationType`（见 `enums.proto`）整数值转换
    pub fn from_proto(value: i32) -> Self {
        match ProtoConversationType::try_from(value).ok() {
            Some(ProtoConversationType::Single) => Self::Single,
            Some(ProtoConversationType::Group) => Self::Group,
            Some(ProtoConversationType::Ai) => Self::Ai,
            Some(ProtoConversationType::System) => Self::System,
            Some(ProtoConversationType::Customer) => Self::Customer,
            Some(ProtoConversationType::Temp) => Self::Temp,
            _ => Self::Unspecified,
        }
    }

    /// 转换为 `flare.common.v1.ConversationType` 的整数值
    pub fn as_proto(&self) -> i32 {
        match self {
            ConversationType::Unspecified => ProtoConversationType::Unspecified as i32,
            ConversationType::Single => ProtoConversationType::Single as i32,
            ConversationType::Group => ProtoConversationType::Group as i32,
            ConversationType::Ai => ProtoConversationType::Ai as i32,
            ConversationType::System => ProtoConversationType::System as i32,
            ConversationType::Customer => ProtoConversationType::Customer as i32,
            ConversationType::Temp => ProtoConversationType::Temp as i32,
        }
    }

    /// 从持久化/缓存中的字符串解析（兼容数字字符串与英文别名）
    pub fn from_db_str(s: &str) -> Self {
        if let Ok(i) = s.trim().parse::<i32>() {
            return Self::from_int(i);
        }
        match s.trim().to_ascii_lowercase().as_str() {
            "single" => Self::Single,
            "group" => Self::Group,
            "ai" => Self::Ai,
            "system" => Self::System,
            "customer" => Self::Customer,
            "temp" => Self::Temp,
            _ => Self::Unspecified,
        }
    }

    /// 从可选的数据库/Redis 字段解析
    pub fn from_db_optional(s: Option<String>) -> Self {
        match s {
            Some(v) => Self::from_db_str(&v),
            None => Self::Unspecified,
        }
    }

    /// 从数据库 INT 值转换
    pub fn from_int(value: i32) -> Self {
        match value {
            1 => Self::Single,
            2 => Self::Group,
            3 => Self::Ai,
            4 => Self::System,
            5 => Self::Customer,
            6 => Self::Temp,
            _ => Self::Unspecified,
        }
    }

    /// 转换为数据库 INT 值
    pub fn as_int(&self) -> i32 {
        *self as i32
    }
}

#[derive(Clone, Debug)]
pub struct ConversationSummary {
    pub conversation_id: String,
    pub conversation_type: ConversationType,
    pub business_type: Option<String>,
    pub last_message_id: Option<String>,
    pub last_message_time: Option<DateTime<Utc>>,
    pub last_sender_id: Option<String>,
    pub last_message_type: Option<i32>,
    pub last_content_type: Option<String>,
    pub unread_count: i32,
    pub last_read_seq: i64,
    pub metadata: HashMap<String, String>,
    pub server_cursor_ts: Option<i64>,
    pub display_name: Option<String>,
    /// 会话当前最大消息 seq（与会话表 last_message_seq 对齐），供 Sync 快照拉消息与摘要
    pub last_message_seq: Option<i64>,
    /// 与库 `conversations.channel_id` 一致；单聊为空，Bootstrap 前组装修成对端 id
    pub channel_id: String,
    /// 非单聊成员版本，用于 SDK 判断是否需要独立同步成员。
    pub participant_version: u64,
    /// 非单聊成员轻量预览，严禁承载完整大群成员。
    pub member_preview: Vec<ConversationParticipant>,
}

#[derive(Clone, Debug)]
pub struct DevicePresence {
    pub device_id: String,
    pub device_platform: Option<String>,
    pub state: DeviceState,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum DeviceState {
    Unspecified,
    Online,
    Offline,
    Conflict,
}

impl DeviceState {
    pub fn from_str(state: &str) -> Self {
        match state {
            "online" => DeviceState::Online,
            "offline" => DeviceState::Offline,
            "conflict" => DeviceState::Conflict,
            _ => DeviceState::Unspecified,
        }
    }

    pub fn as_proto(&self) -> i32 {
        match self {
            DeviceState::Unspecified => ProtoDeviceState::Unspecified as i32,
            DeviceState::Online => ProtoDeviceState::Online as i32,
            DeviceState::Offline => ProtoDeviceState::Offline as i32,
            DeviceState::Conflict => ProtoDeviceState::Conflict as i32,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConversationBootstrapResult {
    pub summaries: Vec<ConversationSummary>,
    pub recent_messages: Vec<Message>,
    pub cursor_map: HashMap<String, i64>,
    pub policy: ConversationPolicy,
}

#[derive(Clone, Debug)]
pub struct ConversationParticipantsPage {
    pub conversation_id: String,
    pub participants: Vec<ConversationParticipant>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub participant_version: u64,
    pub member_count: i32,
}

#[derive(Clone, Debug)]
pub struct MessageSyncResult {
    pub messages: Vec<Message>,
    pub next_cursor: Option<String>,
    pub server_cursor_ts: Option<i64>,
    /// 基于 seq 的游标（可选，用于优化性能）
    pub server_cursor_seq: Option<i64>,
}

pub fn millis_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    Some(Utc.timestamp_millis_opt(ms).single()?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictResolutionPolicy {
    Unspecified,
    Exclusive,
    PlatformExclusive,
    Coexist,
    ForceLogout,
}

impl ConflictResolutionPolicy {
    pub fn as_proto(&self) -> i32 {
        match self {
            ConflictResolutionPolicy::Unspecified => ProtoConflictResolution::Unspecified as i32,
            ConflictResolutionPolicy::Exclusive => ProtoConflictResolution::Exclusive as i32,
            ConflictResolutionPolicy::PlatformExclusive => {
                ProtoConflictResolution::PlatformExclusive as i32
            }
            ConflictResolutionPolicy::Coexist => ProtoConflictResolution::Coexist as i32,
            ConflictResolutionPolicy::ForceLogout => ProtoConflictResolution::ForceLogout as i32,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConflictResolutionPolicy::Unspecified => "unspecified",
            ConflictResolutionPolicy::Exclusive => "exclusive",
            ConflictResolutionPolicy::PlatformExclusive => "platform_exclusive",
            ConflictResolutionPolicy::Coexist => "coexist",
            ConflictResolutionPolicy::ForceLogout => "force_logout",
        }
    }

    pub fn from_proto(value: i32) -> Self {
        match ProtoConflictResolution::try_from(value).ok() {
            Some(ProtoConflictResolution::Exclusive) => Self::Exclusive,
            Some(ProtoConflictResolution::PlatformExclusive) => Self::PlatformExclusive,
            Some(ProtoConflictResolution::Coexist) => Self::Coexist,
            Some(ProtoConflictResolution::ForceLogout) => Self::ForceLogout,
            _ => Self::Unspecified,
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "exclusive" => Some(Self::Exclusive),
            "platform-exclusive" | "platform_exclusive" => Some(Self::PlatformExclusive),
            "coexist" => Some(Self::Coexist),
            "force-logout" | "force_logout" => Some(Self::ForceLogout),
            "unspecified" => Some(Self::Unspecified),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConversationPolicy {
    pub conflict_resolution: ConflictResolutionPolicy,
    pub max_devices: i32,
    pub allow_anonymous: bool,
    pub allow_history_sync: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct Conversation {
    pub tenant_id: String,
    pub conversation_id: String,
    pub conversation_type: ConversationType,
    pub business_type: String,
    /// 单聊须空；群/频道等存消息 channel_id
    pub channel_id: String,
    pub display_name: Option<String>,
    pub attributes: HashMap<String, String>,
    pub participants: Vec<ConversationParticipant>,
    pub visibility: ConversationVisibility,
    pub lifecycle_state: ConversationLifecycleState,
    pub policy: Option<ConversationPolicy>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ConversationParticipant {
    pub user_id: String,
    pub roles: Vec<String>,
    pub muted: bool,
    pub pinned: bool,
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationVisibility {
    Unspecified,
    Private,
    Tenant,
    Public,
}

impl ConversationVisibility {
    pub fn from_proto(value: i32) -> Self {
        match ProtoConversationVisibility::try_from(value).ok() {
            Some(ProtoConversationVisibility::Private) => Self::Private,
            Some(ProtoConversationVisibility::Tenant) => Self::Tenant,
            Some(ProtoConversationVisibility::Public) => Self::Public,
            _ => Self::Unspecified,
        }
    }

    pub fn as_proto(&self) -> i32 {
        match self {
            ConversationVisibility::Unspecified => ProtoConversationVisibility::Unspecified as i32,
            ConversationVisibility::Private => ProtoConversationVisibility::Private as i32,
            ConversationVisibility::Tenant => ProtoConversationVisibility::Tenant as i32,
            ConversationVisibility::Public => ProtoConversationVisibility::Public as i32,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConversationVisibility::Unspecified => "unspecified",
            ConversationVisibility::Private => "private",
            ConversationVisibility::Tenant => "tenant",
            ConversationVisibility::Public => "public",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationLifecycleState {
    Unspecified,
    Active,
    Suspended,
    Archived,
    Deleted,
}

impl ConversationLifecycleState {
    pub fn from_proto(value: i32) -> Self {
        match ProtoConversationLifecycleState::try_from(value).ok() {
            Some(ProtoConversationLifecycleState::ConversationLifecycleActive) => Self::Active,
            Some(ProtoConversationLifecycleState::ConversationLifecycleSuspended) => {
                Self::Suspended
            }
            Some(ProtoConversationLifecycleState::ConversationLifecycleArchived) => Self::Archived,
            Some(ProtoConversationLifecycleState::ConversationLifecycleDeleted) => Self::Deleted,
            _ => Self::Unspecified,
        }
    }

    pub fn as_proto(&self) -> i32 {
        match self {
            ConversationLifecycleState::Unspecified => {
                ProtoConversationLifecycleState::Unspecified as i32
            }
            ConversationLifecycleState::Active => {
                ProtoConversationLifecycleState::ConversationLifecycleActive as i32
            }
            ConversationLifecycleState::Suspended => {
                ProtoConversationLifecycleState::ConversationLifecycleSuspended as i32
            }
            ConversationLifecycleState::Archived => {
                ProtoConversationLifecycleState::ConversationLifecycleArchived as i32
            }
            ConversationLifecycleState::Deleted => {
                ProtoConversationLifecycleState::ConversationLifecycleDeleted as i32
            }
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConversationLifecycleState::Unspecified => "unspecified",
            ConversationLifecycleState::Active => "active",
            ConversationLifecycleState::Suspended => "suspended",
            ConversationLifecycleState::Archived => "archived",
            ConversationLifecycleState::Deleted => "deleted",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConversationFilter {
    pub conversation_type: Option<ConversationType>,
    pub business_type: Option<String>,
    pub lifecycle_state: Option<ConversationLifecycleState>,
    pub visibility: Option<ConversationVisibility>,
    pub participant_user_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConversationSort {
    pub field: String,
    pub ascending: bool,
}

/// 话题（Thread）模型
#[derive(Clone, Debug)]
pub struct Thread {
    pub id: String,
    pub conversation_id: String,
    pub root_message_id: String,
    pub title: Option<String>,
    pub creator_id: String,
    pub reply_count: i32,
    pub last_reply_at: Option<DateTime<Utc>>,
    pub last_reply_id: Option<String>,
    pub last_reply_user_id: Option<String>,
    pub participant_count: i32,
    pub is_pinned: bool,
    pub is_locked: bool,
    pub is_archived: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub extra: HashMap<String, String>,
}

/// 话题排序方式
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadSortOrder {
    UpdatedDesc,    // 按更新时间降序（默认）
    UpdatedAsc,     // 按更新时间升序
    ReplyCountDesc, // 按回复数降序
}

/// 会话领域配置值对象（只包含领域相关的配置）
#[derive(Clone, Debug)]
pub struct ConversationDomainConfig {
    /// 最近消息限制（默认值）
    pub recent_message_limit: i32,
    /// Bootstrap 最大会话数（默认 100，避免响应过大）
    pub max_bootstrap_conversations: Option<usize>,
}

impl ConversationDomainConfig {
    pub fn new(recent_message_limit: i32) -> Self {
        Self {
            recent_message_limit,
            max_bootstrap_conversations: Some(100),
        }
    }

    pub fn default() -> Self {
        Self {
            recent_message_limit: 20,
            max_bootstrap_conversations: Some(100),
        }
    }
}
