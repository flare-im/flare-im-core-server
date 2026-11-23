//! 领域枚举类型
//!
//! 提供领域层通用的枚举类型定义

pub mod message_category;
pub mod persistence_mode;

pub use message_category::MessageCategory;
pub use persistence_mode::PersistenceMode;
