//! 领域枚举类型
//!
//! 提供领域层通用的枚举类型定义

pub mod message_fsm_state;
pub mod persistence_mode;

pub use message_fsm_state::MessageFsmState;
pub use persistence_mode::PersistenceMode;
