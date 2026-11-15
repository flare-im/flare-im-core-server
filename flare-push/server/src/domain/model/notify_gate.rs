//! 防打扰闸门的持久化形状与值对象。
//!
//! 规则（决策、会话槽位、设备预算）只有一份实现，在 `flare-notify-rules` 里；这里只定义服务端
//! 怎么把规则的状态连同自己要记的几项一起存下来，以及到期队列、延迟推送、角标这些服务端独有的
//! 记录长什么样。
//!
//! 一个槽位存成一个值：规则的定长编码（58 字节，第 1..9 字节是版本号）后面接一段 JSON 尾巴，
//! 记着这个槽位上最新一条消息与设备上正在展示的那条。设备同理：规则的 169 字节后面接 JSON 尾巴，
//! 记着已经采用过的「回到前台」标记。比较后写入只看第 1..9 字节，尾巴不参与比较。

use std::collections::HashMap;

use flare_notify_rules::{
    ConversationKind, DEVICE_STATE_LEN, DeviceState, Gate, GateConfig, PreviewMode, SLOT_STATE_LEN,
    SlotState, Tier,
};
use serde::{Deserialize, Serialize};

use super::notify_wave::{DEFAULT_ALL_MENTION_INTERVAL_MS, WaveDue};

/// 版本号在编码里的位置：第 0 字节是布局号，第 1..9 字节是小端的 `version`。
const VERSION_RANGE: std::ops::Range<usize> = 1..9;

/// 存储里一个值的版本字节。不存在、过短的值按全零算，与写回脚本的口径一致：
/// 规则把解不出的值当作 `default()`，其版本号同样是 0。
pub fn raw_version(value: Option<&[u8]>) -> [u8; 8] {
    let mut out = [0u8; 8];
    if let Some(bytes) = value.filter(|bytes| bytes.len() >= VERSION_RANGE.end) {
        out.copy_from_slice(&bytes[VERSION_RANGE]);
    }
    out
}

/// 闸门状态所属的一台设备。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GateDevice {
    pub tenant_id: String,
    pub user_id: String,
    pub device_id: String,
}

impl GateDevice {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        device_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            device_id: device_id.into(),
        }
    }
}

/// 一个（设备，会话）槽位：系统通知栏里这个会话的那一条通知。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GateSlotKey {
    pub device: GateDevice,
    pub conversation_id: String,
}

impl GateSlotKey {
    pub fn new(device: GateDevice, conversation_id: impl Into<String>) -> Self {
        Self {
            device,
            conversation_id: conversation_id.into(),
        }
    }
}

/// 一条消息在槽位上留下的痕迹：会话里最新的那条，或设备上正在展示的那条。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotMessage {
    /// 服务端消息 ID；补提示读不到消息时可能为空。
    #[serde(rename = "m", default)]
    pub message_id: String,
    /// 会话 seq。
    #[serde(rename = "s", default)]
    pub seq: u64,
    /// 对这个接收者的提示等级（协议 `AlertClass` 的取值）。
    #[serde(rename = "a", default)]
    pub alert_class: i32,
}

/// 槽位值的尾巴：规则不管、服务端要记的几项。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotSide {
    /// 会话标识。到期领取时要靠它把最新一条读回来。
    #[serde(rename = "c", default)]
    pub conversation_id: String,
    /// 会话类型：0 单聊，1 群聊。延迟推送到期时进闸门要用。
    #[serde(rename = "k", default)]
    pub kind: u8,
    /// 进过闸门的最新一条：尾部刷新展示它。
    #[serde(rename = "l", default)]
    pub latest: SlotMessage,
    /// 设备上正在展示的那条：撤回它时要发撤回替换。为空表示没有可撤回的展示。
    #[serde(rename = "w", default)]
    pub shown: SlotMessage,
    /// 展示时设备的代号；设备回过前台（换代）后，那条通知已经被清掉，不再修正。
    #[serde(rename = "g", default)]
    pub shown_generation: u32,
    /// 展示的时刻（毫秒）。
    #[serde(rename = "t", default)]
    pub shown_at_ms: i64,
}

/// 一个槽位的完整记录。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotRecord {
    pub state: SlotState,
    pub side: SlotSide,
}

impl SlotRecord {
    /// 解码。规则的部分解不出就当作空槽位（与规则「解不出即不存在」一致），尾巴解不出就是空尾巴。
    pub fn decode(bytes: Option<&[u8]>) -> Self {
        let Some(bytes) = bytes.filter(|bytes| bytes.len() >= SLOT_STATE_LEN) else {
            return Self::default();
        };
        let state = SlotState::decode(&bytes[..SLOT_STATE_LEN]).unwrap_or_default();
        let side = serde_json::from_slice(&bytes[SLOT_STATE_LEN..]).unwrap_or_default();
        Self { state, side }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.state.encode().to_vec();
        // 尾巴是只含字符串与整数的结构体，序列化不会失败；万一失败就只存规则的部分。
        if let Ok(side) = serde_json::to_vec(&self.side) {
            out.extend_from_slice(&side);
        }
        out
    }
}

/// 设备值的尾巴。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct DeviceSide {
    /// 已经采用过的「回到前台」标记（Online 写下的毫秒时刻）。
    #[serde(rename = "a", default)]
    active_applied_ms: i64,
}

/// 一台设备的完整记录。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeviceRecord {
    pub state: DeviceState,
    /// 已经采用过的「回到前台」标记；Online 写的标记比它新，就要先对设备做一次 `on_active`。
    pub active_applied_ms: i64,
}

impl DeviceRecord {
    pub fn decode(bytes: Option<&[u8]>) -> Self {
        let Some(bytes) = bytes.filter(|bytes| bytes.len() >= DEVICE_STATE_LEN) else {
            return Self::default();
        };
        let state = DeviceState::decode(&bytes[..DEVICE_STATE_LEN]).unwrap_or_default();
        let side: DeviceSide =
            serde_json::from_slice(&bytes[DEVICE_STATE_LEN..]).unwrap_or_default();
        Self {
            state,
            active_applied_ms: side.active_applied_ms,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.state.encode().to_vec();
        let side = DeviceSide {
            active_applied_ms: self.active_applied_ms,
        };
        if let Ok(side) = serde_json::to_vec(&side) {
            out.extend_from_slice(&side);
        }
        out
    }
}

/// 读出的一个槽位与它所在设备的状态，连同比较后写入要用的原始版本字节。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GateSnapshot {
    pub slot: SlotRecord,
    pub slot_version: [u8; 8],
    pub device: DeviceRecord,
    pub device_version: [u8; 8],
}

/// Online 写下、闸门要看的设备信号。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeviceSignals {
    /// 设备最近一次以前台状态登录或上报的时刻（毫秒），没有为 0。
    pub active_at_ms: i64,
    /// 设备最近一次上报非前台状态时带的角标基线。
    pub badge_base: Option<BadgeBase>,
}

/// 角标基线：设备转入后台时自己报的未读总数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadgeBase {
    pub count: u32,
    /// 上报的时刻（毫秒）。比闸门采用过的新才采用。
    pub at_ms: i64,
}

/// 一次写入对角标表做什么。角标表 = 基线 + 各会话的贡献，绝对值 = 两者之和。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadgeChange {
    /// 这个会话多了这么多条计入未读的消息（平时是一条，波次与推迟到期的一批是这一批的条数）。
    Add(u32),
    /// 这个会话被读了：去掉它的全部贡献。
    Remove,
    /// 这个会话里一条计入未读的消息被撤回：贡献减一。
    Decrement,
    /// 不改，只读出当前值。
    Get,
}

/// 角标表改动后的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadgeValue {
    /// 角标绝对值。
    pub count: u32,
    /// 这次改动是否真的改了值（去掉、减掉的贡献不为零，或加了一条）。
    pub changed: bool,
}

/// 到期队列里的一项。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DueKind {
    /// 尾部刷新：吞并的消息到间隔结束补发一次，内容在槽位的尾巴里。
    Tail,
    /// 延迟推送：别的设备在看，这台设备的提示推迟，内容在延迟记录里。
    Delay,
}

impl DueKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Tail => "tail",
            Self::Delay => "delay",
        }
    }
}

/// 到期队列里领取出来的一项。字段顺序决定排序：同一时刻、同一台设备的项按会话标识的字节序处理，
/// 与规则向量的回放规则一致。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DueEntry {
    pub due_ms: i64,
    pub key: GateSlotKey,
    pub kind: DueKind,
    /// 这一项登记在哪个状态存储里，到期时回到同一个存储处理。
    pub origin: GateStoreOrigin,
}

/// 到期队列里领取出来的一项：某个槽位的尾部刷新或推迟提示，或某个会话的波次。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DueClaim {
    Slot(DueEntry),
    Wave(WaveDue),
}

impl DueClaim {
    pub fn due_ms(&self) -> i64 {
        match self {
            Self::Slot(entry) => entry.due_ms,
            Self::Wave(wave) => wave.due_ms,
        }
    }

    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Slot(entry) => &entry.key.device.tenant_id,
            Self::Wave(wave) => &wave.key.tenant_id,
        }
    }
}

impl PartialOrd for DueClaim {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 先按到期时刻；同一时刻槽位在前、波次在后，各自再按自己的字段。
impl Ord for DueClaim {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        self.due_ms()
            .cmp(&other.due_ms())
            .then_with(|| match (self, other) {
                (Self::Slot(left), Self::Slot(right)) => left.cmp(right),
                (Self::Wave(left), Self::Wave(right)) => left.cmp(right),
                (Self::Slot(_), Self::Wave(_)) => Ordering::Less,
                (Self::Wave(_), Self::Slot(_)) => Ordering::Greater,
            })
    }
}

/// 闸门状态存储：推送专用的 Redis，或它不可用时的进程内近似。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum GateStoreOrigin {
    #[default]
    Primary,
    Fallback,
}

/// 一条推迟发出的提示（多端「延迟」策略）。同一个（设备，会话）只有一条，期间来的消息累加进来。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayedAlert {
    pub key: GateSlotKey,
    pub kind: ConversationKind,
    /// 到期后进闸门的档位；期间来过的最高档位。
    pub tier: Tier,
    /// 期间最新的一条。
    pub latest: SlotMessage,
    /// 期间来了几条。
    pub count: u32,
    /// 第一条登记的时刻（毫秒）。设备在这之后回过前台，就不再推送。
    pub created_ms: i64,
    /// 到期时刻（毫秒）。
    pub due_ms: i64,
}

/// 档位的存储取值。
pub fn tier_code(tier: Tier) -> u8 {
    match tier {
        Tier::Quiet => 0,
        Tier::Normal => 1,
        Tier::Priority => 2,
    }
}

pub fn tier_of_code(code: u8) -> Tier {
    match code {
        0 => Tier::Quiet,
        2 => Tier::Priority,
        _ => Tier::Normal,
    }
}

/// 会话类型的存储取值。
pub fn kind_code(kind: ConversationKind) -> u8 {
    match kind {
        ConversationKind::Direct => 0,
        ConversationKind::Group => 1,
    }
}

pub fn kind_of_code(code: u8) -> ConversationKind {
    match code {
        0 => ConversationKind::Direct,
        _ => ConversationKind::Group,
    }
}

/// 一台有推送绑定的设备里，提示要用到的登记信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceProfile {
    pub device_id: String,
    /// 绑定登记的 IANA 时区，可能为空。
    pub time_zone: String,
    /// 是不是 iOS 设备：只有它的推送带角标绝对值（`aps.badge`），角标表也只为它维护。
    pub ios: bool,
}

impl DeviceProfile {
    /// 只知道设备标识时的登记：时区未知，不按 iOS 处理。
    pub fn unknown(device_id: impl Into<String>) -> Self {
        Self {
            device_id: device_id.into(),
            time_zone: String::new(),
            ios: false,
        }
    }
}

/// 一个租户的提示策略：闸门参数、是否强制隐藏预览、大群 @所有人 的配额间隔。
#[derive(Debug, Clone, Copy)]
pub struct TenantNotifyPolicy {
    pub gate: Gate,
    pub force_hidden_preview: bool,
    /// 超过成员上限的群里，同一个会话两次全员 @所有人 波次之间至少隔多久（毫秒）。
    pub all_mention_interval_ms: i64,
}

impl Default for TenantNotifyPolicy {
    fn default() -> Self {
        Self {
            gate: Gate::default(),
            force_hidden_preview: false,
            all_mention_interval_ms: DEFAULT_ALL_MENTION_INTERVAL_MS,
        }
    }
}

/// 全部租户的提示策略。没有单独配置的租户用默认值。
#[derive(Debug, Clone, Default)]
pub struct NotifyPolicy {
    default: TenantNotifyPolicy,
    tenants: HashMap<String, TenantNotifyPolicy>,
}

impl NotifyPolicy {
    pub fn new(tenants: HashMap<String, TenantNotifyPolicy>) -> Self {
        Self {
            default: TenantNotifyPolicy::default(),
            tenants,
        }
    }

    pub fn of(&self, tenant_id: &str) -> &TenantNotifyPolicy {
        self.tenants.get(tenant_id).unwrap_or(&self.default)
    }

    /// 这个租户的闸门参数。
    pub fn gate_config(&self, tenant_id: &str) -> GateConfig {
        *self.of(tenant_id).gate.config()
    }

    /// 这个租户在超过成员上限的群里，同一个会话两次全员 @所有人 波次之间的最短间隔（毫秒）。
    pub fn all_mention_interval_ms(&self, tenant_id: &str) -> i64 {
        self.of(tenant_id).all_mention_interval_ms
    }

    /// 接收者实际的预览：用户设置与租户强制隐藏取更严的一个。
    pub fn preview(&self, tenant_id: &str, user: PreviewMode) -> PreviewMode {
        if self.of(tenant_id).force_hidden_preview {
            user.strictest(PreviewMode::Hidden)
        } else {
            user
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_notify_rules::{ConversationTag, GateInput, SlotAction};

    #[test]
    fn records_round_trip_with_their_side_data() {
        let gate = Gate::default();
        let mut slot = SlotRecord::default();
        let mut device = DeviceRecord::default();
        let input = GateInput {
            kind: ConversationKind::Group,
            tier: Tier::Normal,
            tag: ConversationTag::of(b"conv-1"),
        };
        let admitted = gate.admit(1_000, input, &mut slot.state, &mut device.state);
        assert_eq!(admitted.slot, SlotAction::Loud { count: 1 });
        slot.side = SlotSide {
            conversation_id: "conv-1".into(),
            kind: 1,
            latest: SlotMessage {
                message_id: "m-1".into(),
                seq: 7,
                alert_class: 3,
            },
            shown: SlotMessage {
                message_id: "m-1".into(),
                seq: 7,
                alert_class: 3,
            },
            shown_generation: 0,
            shown_at_ms: 1_000,
        };
        device.active_applied_ms = 42;

        let encoded = slot.encode();
        assert_eq!(raw_version(Some(encoded.as_slice())), 1u64.to_le_bytes());
        assert_eq!(SlotRecord::decode(Some(encoded.as_slice())), slot);
        let encoded = device.encode();
        assert_eq!(raw_version(Some(encoded.as_slice())), 1u64.to_le_bytes());
        assert_eq!(DeviceRecord::decode(Some(encoded.as_slice())), device);
        // 尾巴之外的部分小，槽位值远低于 200 字节。
        assert!(slot.encode().len() < 200, "{}", slot.encode().len());
    }

    /// 坏值按不存在处理：规则从 `default()` 开始，版本按原始字节比较，坏值也能被盖掉。
    #[test]
    fn unreadable_values_are_absent_but_keep_their_raw_version() {
        assert_eq!(SlotRecord::decode(None), SlotRecord::default());
        assert_eq!(
            SlotRecord::decode(Some(&b"short"[..])),
            SlotRecord::default()
        );
        assert_eq!(raw_version(None), [0u8; 8]);
        assert_eq!(raw_version(Some(&b"short"[..])), [0u8; 8]);
        let mut garbage = vec![9u8; SLOT_STATE_LEN + 3];
        garbage[1..9].copy_from_slice(&5u64.to_le_bytes());
        assert_eq!(
            SlotRecord::decode(Some(garbage.as_slice())).state,
            SlotState::default()
        );
        assert_eq!(raw_version(Some(garbage.as_slice())), 5u64.to_le_bytes());
        assert_eq!(
            DeviceRecord::decode(Some(garbage.as_slice())),
            DeviceRecord::default()
        );
    }

    #[test]
    fn a_tenant_can_force_hidden_previews() {
        let mut tenants = HashMap::new();
        tenants.insert(
            "strict".to_string(),
            TenantNotifyPolicy {
                force_hidden_preview: true,
                ..TenantNotifyPolicy::default()
            },
        );
        let policy = NotifyPolicy::new(tenants);
        assert_eq!(
            policy.preview("strict", PreviewMode::Full),
            PreviewMode::Hidden
        );
        assert_eq!(
            policy.preview("other", PreviewMode::SenderOnly),
            PreviewMode::SenderOnly
        );
        assert_eq!(policy.gate_config("other"), GateConfig::default());
        assert_eq!(
            policy.all_mention_interval_ms("other"),
            DEFAULT_ALL_MENTION_INTERVAL_MS
        );
    }

    /// 到期队列按到期时刻领取，槽位与波次混在一起时也一样。
    #[test]
    fn due_claims_order_by_time_first() {
        use super::super::notify_wave::WaveKey;
        let slot = |due_ms| {
            DueClaim::Slot(DueEntry {
                due_ms,
                key: GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "c1"),
                kind: DueKind::Tail,
                origin: GateStoreOrigin::Primary,
            })
        };
        let wave = |due_ms| {
            DueClaim::Wave(WaveDue {
                due_ms,
                key: WaveKey::new("t2", "g1"),
                origin: GateStoreOrigin::Primary,
            })
        };
        let mut claims = [slot(30), wave(20), slot(10), wave(30)];
        claims.sort();
        assert_eq!(
            claims.iter().map(DueClaim::due_ms).collect::<Vec<_>>(),
            vec![10, 20, 30, 30]
        );
        assert!(matches!(claims[2], DueClaim::Slot(_)));
        assert_eq!(claims[1].tenant_id(), "t2");
    }

    #[test]
    fn stored_codes_round_trip() {
        for tier in [Tier::Quiet, Tier::Normal, Tier::Priority] {
            assert_eq!(tier_of_code(tier_code(tier)), tier);
        }
        for kind in [ConversationKind::Direct, ConversationKind::Group] {
            assert_eq!(kind_of_code(kind_code(kind)), kind);
        }
    }
}
