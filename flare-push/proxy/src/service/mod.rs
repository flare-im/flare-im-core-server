//! 应用启动与依赖装配

mod bootstrap;
mod wire;

pub use bootstrap::ApplicationBootstrap;
pub use wire::ApplicationContext;
