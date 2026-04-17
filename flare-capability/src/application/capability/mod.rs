//! 能力扩展应用层：静态目录查询、分发用例、可选编排示例。

mod catalog;
mod dispatch;
mod examples;

pub use catalog::list_registered_capabilities;
pub use dispatch::dispatch_capability_command;
pub use examples::{SendMessageUseCaseExample, StartCallUseCaseExample};
