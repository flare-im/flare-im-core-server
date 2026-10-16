//! Hook 引擎与能力扩展的基础设施：配置加载、传输适配器、Hook 执行证据（缓冲、批量写库、保留期、
//! 指标）、PostgreSQL 持久化。

pub mod adapters;
pub mod capability;
pub mod config;
pub mod hook_execution;
pub mod persistence;
