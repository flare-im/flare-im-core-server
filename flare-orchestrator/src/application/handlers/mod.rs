//! CQRS Handler（编排层）

pub mod action_handler;
pub mod burn_worker;
mod event_handler;
mod storage_handler;

pub use action_handler::MessageActionHandler;
pub use burn_worker::{
    BurnDueMessage, BurnDueMessageRepository, BurnEventSink, BurnWorkerBatchResult,
    MessageBurnWorker,
};
pub use event_handler::EventHandler;
pub use storage_handler::StorageHandler;
