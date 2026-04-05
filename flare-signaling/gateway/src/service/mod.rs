//! 服务模块 - 包含服务启动、注册和管理相关功能

pub mod bootstrap;
pub mod builder;
pub mod display;
pub mod startup;
mod wire;

pub use bootstrap::ApplicationBootstrap;
pub use display::{GrpcServiceInfo, StartupInfo};
pub use wire::{ApplicationContext, GrpcServices};
