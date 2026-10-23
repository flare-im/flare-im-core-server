use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;

mod admin;
pub use admin::{AdminConversation, AdminConversationPage};
pub mod read_receipt;
pub use read_receipt::{
    MemberReadPosition, MessageReadSummaries, MessageReadSummary, MessageReaderPage, ReadCounts,
    ReaderState, ReceiptMessage,
};

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
/// - CHANNEL = 7 (CID 前缀 7，订阅者模型)
/// - BROADCAST = 8 (CID 前缀 8，订阅者模型)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationType {
    Unspecified = 0,
    Single = 1,
    Group = 2,
    Ai = 3,
    System = 4,
    Customer = 5,
    Temp = 6,
    Channel = 7,
    Broadcast = 8,
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
            Some(ProtoConversationType::Channel) => Self::Channel,
            Some(ProtoConversationType::Broadcast) => Self::Broadcast,
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
            ConversationType::Channel => ProtoConversationType::Channel as i32,
            ConversationType::Broadcast => ProtoConversationType::Broadcast as i32,
        }
    }

    /// 从持久化/缓存中的字符串解析；只接受当前 canonical wire name。
    pub fn from_db_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "unspecified" => Self::Unspecified,
            "single" => Self::Single,
            "group" => Self::Group,
            "ai" => Self::Ai,
            "system" => Self::System,
            "customer" => Self::Customer,
            "temp" => Self::Temp,
            "channel" => Self::Channel,
            "broadcast" => Self::Broadcast,
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
            7 => Self::Channel,
            8 => Self::Broadcast,
            _ => Self::Unspecified,
        }
    }

    /// 转换为数据库 INT 值
    pub fn as_int(&self) -> i32 {
        *self as i32
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConversationType::Unspecified => "unspecified",
            ConversationType::Single => "single",
            ConversationType::Group => "group",
            ConversationType::Ai => "ai",
            ConversationType::System => "system",
            ConversationType::Customer => "customer",
            ConversationType::Temp => "temp",
            ConversationType::Channel => "channel",
            ConversationType::Broadcast => "broadcast",
        }
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
    pub last_message_preview: Option<String>,
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
    /// 当前用户：是否静音
    pub is_muted: bool,
    /// 当前用户：是否置顶
    pub is_pinned: bool,
    /// 当前用户：是否归档
    pub is_archived: bool,
    /// 用户偏好 LWW 版本
    pub settings_version: u64,
    /// 用户草稿
    pub draft: Option<String>,
    /// 当前用户的历史可见边界；seq <= visible_after_seq 的消息不可见。
    pub visible_after_seq: i64,
}

/// 用户在会话上的偏好设置（读写合一）
#[derive(Clone, Debug)]
pub struct ConversationUserSettings {
    pub is_pinned: bool,
    pub is_muted: bool,
    pub is_archived: bool,
    /// 只接收提到我的消息。与 `is_muted` 正交：muted 是「一条都别响」，
    /// 这个是「只有点名才响」。
    pub is_mention_only: bool,
    pub draft: Option<String>,
    pub settings_version: u64,
}

#[derive(Clone, Debug)]
pub struct UpdateConversationUserSettingsPatch {
    pub conversation_id: String,
    pub is_mention_only: Option<bool>,
    pub is_pinned: Option<bool>,
    pub is_muted: Option<bool>,
    pub is_archived: Option<bool>,
    pub draft: Option<String>,
    pub base_settings_version: u64,
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
    pub fn from_persistence_value(state: &str) -> Self {
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

/// 成员读按哪种口径回答。
///
/// - `InSeat`：在席——未隐藏会话、未退出。投递类读取（消息与事件扇出、实时订阅引导、离线推送）
///   用它：隐藏了会话的人不再收到推送，这是隐藏的列表语义。
/// - `Members`：成员——未退出即成员，隐藏了会话（从本人列表删除）的也算。成员名单、成员数、
///   成员核对（上行事件的操作者、插件资源桥）用它：隐藏只影响本人的列表展示，不改变成员关系。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ParticipantScope {
    #[default]
    InSeat,
    Members,
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
    Utc.timestamp_millis_opt(ms).single()
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

    pub fn parse_config_value(value: &str) -> Option<Self> {
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
    /// 该成员可见的历史下限：只返回 `conversation_seq > visible_from_seq` 的消息；0 = 不限。
    ///
    /// 核心只执行这条下限，不关心它为何被设置——「新成员是否可见入群前消息」是
    /// 业务规则，由业务层在加人时决定写不写当前 seq。
    pub visible_from_seq: i64,
    /// 只接收提到我的消息：其余消息照常投递，但不产生离线推送。与 `muted` 正交。
    pub mention_only: bool,
}

impl ConversationParticipant {
    /// 本人的列表设置（免打扰 `muted`、置顶 `pinned`、只接收提到我 `mention_only`）不是成员
    /// 身份，给别人看的成员行不带它们。
    fn clear_personal_settings(&mut self) {
        self.muted = false;
        self.pinned = false;
        self.mention_only = false;
    }
}

/// 面向成员的读取（成员名单、按 id 查成员、成员管理的应答、会话摘要的成员预览）只在 `viewer`
/// 本人那一行保留个人设置，其余行一律清空；`viewer` 为 `None`（服务读名单、员工视图的成员分页）
/// 时一行都不留。
/// 投递读取（在席口径的扇出、离线推送）是内部读取，要按每个人的免打扰判定推送，不经这里。
pub fn keep_personal_settings_of(
    participants: &mut [ConversationParticipant],
    viewer: Option<&str>,
) {
    for participant in participants {
        if viewer != Some(participant.user_id.as_str()) {
            participant.clear_personal_settings();
        }
    }
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

/// 按时间边界清除会话历史的结果。
///
/// 会话本身、成员和边界之后的消息都保留，只报告删掉了什么。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConversationHistoryPurge {
    /// 删除的消息条数
    pub purged_messages: u64,
    /// 被删消息里最大的会话 seq；没删任何消息时为 0
    pub max_purged_seq: u64,
}

/// 纪元围栏：消息 attributes 里 `attribute` 的值是非负整数时，只按它判定新旧。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEpochFence {
    /// 纪元属性名（消息 attributes 的键，持久化在 `messages.extra`）
    pub attribute: String,
    /// 纪元严格小于它的消息被删除
    pub before_epoch: i64,
}

/// 清除会话历史的边界。
///
/// 没有纪元围栏时就是原来的时间边界：`before` 必有，作用于全部消息与事件。
/// 有纪元围栏时，`before` 只作用于没有纪元属性的消息（`None` 表示这类消息一条不删），
/// 事件按被删消息与剩余消息的 seq 判定，不再看时间。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryPurgeBoundary {
    pub before: Option<DateTime<Utc>>,
    pub epoch_fence: Option<HistoryEpochFence>,
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
}

impl Default for ConversationDomainConfig {
    fn default() -> Self {
        Self {
            recent_message_limit: 20,
            max_bootstrap_conversations: Some(100),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConversationParticipant, ConversationType, keep_personal_settings_of};

    fn participant(user_id: &str) -> ConversationParticipant {
        ConversationParticipant {
            user_id: user_id.into(),
            roles: vec!["member".into()],
            muted: true,
            pinned: true,
            attributes: [("nickname".to_string(), user_id.to_string())].into(),
            visible_from_seq: 7,
            mention_only: true,
        }
    }

    /// Only the viewer's own row keeps the list settings; identity and public fields stay on every
    /// row, and a service viewer keeps none.
    #[test]
    fn personal_settings_stay_on_the_viewers_own_row_only() {
        let mut rows = vec![participant("alice"), participant("bob")];
        keep_personal_settings_of(&mut rows, Some("alice"));
        let settings = |p: &ConversationParticipant| (p.muted, p.pinned, p.mention_only);
        assert_eq!(settings(&rows[0]), (true, true, true));
        assert_eq!(settings(&rows[1]), (false, false, false));
        for row in &rows {
            assert_eq!(row.roles, ["member"]);
            assert_eq!(row.visible_from_seq, 7);
            assert_eq!(row.attributes["nickname"], row.user_id);
        }

        let mut rows = vec![participant("alice"), participant("bob")];
        keep_personal_settings_of(&mut rows, None);
        assert!(rows.iter().all(|p| settings(p) == (false, false, false)));
    }

    #[test]
    fn conversation_type_db_string_accepts_only_canonical_names() {
        assert_eq!(
            ConversationType::from_db_str("single"),
            ConversationType::Single
        );
        assert_eq!(
            ConversationType::from_db_str("channel"),
            ConversationType::Channel
        );
        assert_eq!(
            ConversationType::from_db_str("1"),
            ConversationType::Unspecified
        );
        assert_eq!(
            ConversationType::from_db_str("conversation_type_single"),
            ConversationType::Unspecified
        );
    }
}
