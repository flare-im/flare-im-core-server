//! 仓储接口定义（Port）

pub mod message_storage;
pub mod visibility_storage;

pub use message_storage::MessageStorage;
pub use visibility_storage::VisibilityStorage;