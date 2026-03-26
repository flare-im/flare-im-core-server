//! 查询侧 Handler（Reader 仅提供 Query，不提供 Command）

pub mod query_handler;

pub use query_handler::{MessageStorageQueryHandler, QueryMessagesResult};
