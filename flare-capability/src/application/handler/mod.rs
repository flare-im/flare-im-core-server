//! 应用层 **Handler**：用例编排（委托领域服务 / 读端口），不包含具体 RTC、Hook 执行细节。

mod capability_dispatch;
mod hook_command;
mod hook_query;

pub use capability_dispatch::dispatch_capability_command;
pub use hook_command::HookCommandHandler;
pub use hook_query::HookQueryHandler;
