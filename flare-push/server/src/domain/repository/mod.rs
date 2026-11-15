//! 领域仓储接口（Port）

mod device_binding_repository;
mod handoff_source_repository;
mod notify_gate_repository;
mod notify_policy_repository;
mod notify_wave_repository;

pub use device_binding_repository::{BoundDevices, DeviceBindingRepository};
pub use handoff_source_repository::{
    MessageLookupRepository, RecentConversation, RecentConversationRepository,
};
pub use notify_gate_repository::{
    BadgeWrite, CommitOutcome, DeviceSignalRepository, DueChange, GateCommit, NotifyGateStore,
    UserNotifySettingsRepository, VersionedWrite,
};
pub use notify_policy_repository::{NotifyPolicyRepository, NotifyPreference, ParticipantPage};
pub use notify_wave_repository::{WaveCommit, WaveMessageWrite, WaveSnapshot, WaveStore};
