//! # Access Gateway配置模块
//!
//! 提供Access Gateway的配置加载和解析

pub mod port_config;
pub mod settings;

pub use port_config::PortConfig;
pub use settings::AccessGatewayConfig;
