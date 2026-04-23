//! CQRS Handler（编排层）

pub mod action_handler;
mod event_handler;
pub mod hook;
pub mod plugin;
mod storage_handler;

pub use action_handler::MessageActionHandler;
pub use event_handler::EventHandler;
pub use hook::MessageHandler;
pub use plugin::CallCapabilityBridge;
pub use storage_handler::StorageHandler;
