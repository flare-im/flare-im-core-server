//! 服务模块 - 应用启动、依赖注入、服务注册
//!
//! 与 flare-storage 一致：wire 构建 ApplicationContext，bootstrap 提供 ApplicationBootstrap::run()。

pub mod bootstrap;
mod wire;

pub use bootstrap::ApplicationBootstrap;
pub use wire::ApplicationContext;
