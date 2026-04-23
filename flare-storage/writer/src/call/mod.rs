//! 通话与 RTC 实例相关写侧仓储（增量）。

pub mod call_room_binding_repository;
pub mod call_session_repository;
pub mod capability_instance_repository;

pub use call_room_binding_repository::{
    CallRoomBindingRecord, CallRoomBindingRepository, PostgresCallRoomBindingRepository,
};
pub use call_session_repository::{
    CallSessionRecord, CallSessionRepository, PostgresCallSessionRepository,
};
pub use capability_instance_repository::{
    CapabilityInstanceRecord, CapabilityInstanceRepository, PostgresCapabilityInstanceRepository,
};
