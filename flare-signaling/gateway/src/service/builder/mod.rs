//! 服务构建器模块
//!
//! 提供服务器、处理器和配置的构建逻辑

pub mod config;
pub mod handler;
pub mod server;

pub use config::{parse_compression_algorithm, setup_encryption_config, EncryptionConfig};
pub use handler::{build_authenticator, build_long_connection_handler};
pub use server::{build_flare_server, build_long_connection_server};
