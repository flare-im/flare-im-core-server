//! CQRS Handler（编排层）

pub mod command_handler;
pub mod core_trait_adapter;
pub mod operation_handler;
pub mod query_handler;

pub use command_handler::MessageCommandHandler;
pub use core_trait_adapter::CoreMessageCommandHandlerAdapter;
pub use operation_handler::MessageOperationHandler;
pub use query_handler::MessageQueryHandler;