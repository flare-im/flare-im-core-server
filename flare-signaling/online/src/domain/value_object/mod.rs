//! 值对象（Value Object）
//!
//! 不可变对象，通过值相等判断，包含验证逻辑

mod connection_id;
mod connection_quality;
mod device_id;
mod device_priority;
mod token_version;
mod user_id;

pub use connection_id::ConnectionId;
pub use connection_quality::{ConnectionQuality, NetworkType, QualityLevel};
pub use device_id::DeviceId;
pub use device_priority::DevicePriority;
pub use token_version::TokenVersion;
pub use user_id::UserId;
