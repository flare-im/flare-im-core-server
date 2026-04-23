//! 应用层 **Command**（写路径）：跨聚合的用例入口，不含传输细节。

mod hook_materialize;

pub use hook_materialize::materialize_hook_execution_plan;
