//! Hook 引擎与能力扩展的基础设施：配置加载、传输适配器、监控、PostgreSQL 持久化。

pub mod adapters;
pub mod capability;
pub mod config;
pub mod monitoring;
pub mod persistence;
