//! 防打扰闸门要用的存储与读取（Port）。
//!
//! - [`NotifyGateStore`]：槽位、设备预算、到期队列、延迟推送、展示索引与角标表。规则在应用层跑，
//!   这里只负责「一次读出、比较版本后一次写回」，不懂规则。
//! - [`DeviceSignalRepository`]：Online 写下的设备信号（回到前台、角标基线）。
//! - [`UserNotifySettingsRepository`]：用户级通知设置（勿扰时段、预览、多端策略）。
//!
//! 三者都是软状态或偏好：读不到时调用方放行，绝不因此少推一条。

use std::collections::HashMap;

use flare_im_contracts::Ctx;
use flare_notify_rules::UserPrefs;
use flare_server_core::error::FlareError;

use crate::domain::model::{
    BadgeBase, BadgeChange, BadgeValue, DelayedAlert, DeviceSignals, DueClaim, GateDevice,
    GateSlotKey, GateSnapshot,
};

/// 比较后写入里的一个值：存储里的版本字节等于 `expect` 才写；`value` 为 `None` 只比较不写。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedWrite {
    pub expect: [u8; 8],
    pub value: Option<Vec<u8>>,
}

/// 到期队列的改动。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DueChange {
    #[default]
    None,
    /// 登记（或改登记）这个槽位的尾部刷新。
    Schedule(i64),
    /// 取消这个槽位的尾部刷新。
    Cancel,
}

/// 角标表的改动。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadgeWrite {
    pub change: BadgeChange,
    /// 设备新报的基线；比存储采用过的新才采用。
    pub base: Option<BadgeBase>,
}

/// 一次比较后写入。全部比较通过才写，写入的各项一起生效。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateCommit {
    pub key: GateSlotKey,
    /// 槽位；`None` 不比较也不写。
    pub slot: Option<VersionedWrite>,
    /// 设备；`None` 不比较也不写。
    pub device: Option<VersionedWrite>,
    /// 槽位值保留多久（毫秒）。
    pub slot_ttl_ms: i64,
    /// 设备值保留多久（毫秒）。
    pub device_ttl_ms: i64,
    pub due: DueChange,
    /// 设备上开始展示这条消息：记进展示索引，撤回时据此找到要修正的设备。
    pub shown_message_id: Option<String>,
    pub badge: Option<BadgeWrite>,
}

impl GateCommit {
    pub fn new(key: GateSlotKey) -> Self {
        Self {
            key,
            slot: None,
            device: None,
            slot_ttl_ms: 0,
            device_ttl_ms: 0,
            due: DueChange::None,
            shown_message_id: None,
            badge: None,
        }
    }
}

/// 一次比较后写入的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// 写入了；带角标改动时是改动后的值。
    Applied { badge: Option<BadgeValue> },
    /// 版本对不上，什么都没写。调用方重读重算。
    Conflict,
}

#[async_trait::async_trait]
pub trait NotifyGateStore: Send + Sync {
    /// 一次读出一个槽位与它所在设备的状态。不存在的值是 `default()`。
    async fn load(&self, key: &GateSlotKey) -> Result<GateSnapshot, FlareError>;

    /// 比较后写入。存储不可用是错误；版本对不上是 [`CommitOutcome::Conflict`]。
    async fn commit(&self, commit: &GateCommit) -> Result<CommitOutcome, FlareError>;

    /// 记下一条推迟的提示：这个（设备，会话）还没有记录就新建并登记到期，已有就把这条累加进去
    /// （条数加上 `alert.count`、最新一条换成这条、档位取高的）。可以同时改角标表，返回改动后的角标。
    async fn defer_alert(
        &self,
        alert: &DelayedAlert,
        ttl_ms: i64,
        badge: Option<BadgeWrite>,
    ) -> Result<Option<BadgeValue>, FlareError>;

    /// 取走这个（设备，会话）的推迟提示；已被取消时为 `None`。
    async fn take_delayed(&self, key: &GateSlotKey) -> Result<Option<DelayedAlert>, FlareError>;

    /// 会话在别处被读到 `read_seq`：推迟的提示里最新那条不晚于它就取消（`read_seq` 为 0 表示
    /// 不知道读到哪，一律取消）。返回是否取消了。
    async fn cancel_delayed(&self, key: &GateSlotKey, read_seq: u64) -> Result<bool, FlareError>;

    /// 领取到期时刻不晚于 `now_ms` 的条目（槽位的尾部刷新与推迟提示、会话的波次），按到期时刻
    /// 至多 `limit` 条。领取即移除：同一条目只有一个实例处理。
    async fn claim_due(&self, now_ms: i64, limit: usize) -> Result<Vec<DueClaim>, FlareError>;

    /// 最近展示过这条消息的设备（可能已经换成别的消息，调用方要再核对槽位）。
    async fn shown_devices(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Vec<GateDevice>, FlareError>;

    /// 单独改一次角标表（不经过槽位，例如多端「只更新角标」）。
    async fn badge(
        &self,
        device: &GateDevice,
        conversation_id: &str,
        badge: BadgeWrite,
    ) -> Result<BadgeValue, FlareError>;
}

#[async_trait::async_trait]
pub trait DeviceSignalRepository: Send + Sync {
    /// 这些（用户，设备）的信号。没有信号的设备可以不出现在结果里。
    async fn device_signals(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        devices: &[(String, String)],
    ) -> Result<HashMap<(String, String), DeviceSignals>, FlareError>;
}

#[async_trait::async_trait]
pub trait UserNotifySettingsRepository: Send + Sync {
    /// 这批用户的用户级通知设置。没有设置过的用户可以不出现在结果里（调用方按默认值处理）。
    ///
    /// 调用方对错误 fail-open：用默认值，也就是没有勿扰时段、完整预览、多端延迟。
    async fn settings_for(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserPrefs>, FlareError>;
}
