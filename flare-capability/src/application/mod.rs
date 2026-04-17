//! 应用层：Hook 命令/查询处理器 + 能力扩展（目录、分发、示例用例）。

pub mod capability;
pub mod handlers;

pub use handlers::{HookCommandHandler, HookQueryHandler};
