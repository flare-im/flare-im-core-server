//! Hook 应用入口：命令与查询处理器。

pub mod command_handler;
pub mod query_handler;

pub use command_handler::HookCommandHandler;
pub use query_handler::HookQueryHandler;
