//! CQRS Handler（编排层）

pub mod action_handler;
pub mod burn_worker;
mod event_handler;
pub mod hook;
pub mod plugin;
mod storage_handler;
mod wal_replay_handler;

pub use action_handler::MessageActionHandler;
pub use burn_worker::{
    BurnDueMessage, BurnDueMessageRepository, BurnEventSink, BurnWorkerBatchResult,
    MessageBurnWorker,
};
pub use event_handler::EventHandler;
pub use hook::MessageHandler;
pub use plugin::CallCapabilityBridge;
pub use storage_handler::StorageHandler;
pub use wal_replay_handler::{WalReplayHandler, WalReplayReport};
