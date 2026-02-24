//! CQRS Handler（编排层）

pub mod command_handler;
pub mod query_handler;
pub mod operation_handler;

pub use command_handler::MessageCommandHandler;
pub use query_handler::MessageQueryHandler;
pub use operation_handler::MessageOperationHandler;