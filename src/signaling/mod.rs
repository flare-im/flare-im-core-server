//! 信令层共享类型与工具（原 flare-signaling/common 迁入）
//!
//! 被 gateway、online、route 等信令相关服务共同使用。

pub mod error;
pub mod models;
pub mod utils;

pub use error::{SignalingError, SignalingResult};
pub use models::*;
pub use utils::*;
