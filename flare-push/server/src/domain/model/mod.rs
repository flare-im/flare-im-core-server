//! 领域模型（Entity, Value Object）

mod device_reachability;
mod notify_gate;
mod notify_wave;
mod offline_lane;

pub use device_reachability::{LinkState, UserReachability};
pub use notify_gate::{
    BadgeBase, BadgeChange, BadgeValue, DelayedAlert, DeviceProfile, DeviceRecord, DeviceSignals,
    DueClaim, DueEntry, DueKind, GateDevice, GateSlotKey, GateSnapshot, GateStoreOrigin,
    NotifyPolicy, SlotMessage, SlotRecord, SlotSide, TenantNotifyPolicy, kind_code, kind_of_code,
    raw_version, tier_code, tier_of_code,
};
pub use notify_wave::{
    Arrived, DEFAULT_ALL_MENTION_INTERVAL_MS, MentionAll, WAVE_LADDER_MS, WaveArrival, WaveBatch,
    WaveDue, WaveDueOutcome, WaveKey, WaveMessage, WaveState, WaveTrigger, wave_interval_ms,
    wave_state_ttl_ms,
};
pub use offline_lane::OfflineLane;
