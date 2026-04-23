//! Hook 引擎与能力扩展的基础设施：配置加载、传输适配器、监控、PostgreSQL 持久化。

pub mod adapters;
pub mod capability;
pub mod config;
pub mod monitoring;
pub mod persistence;
/// RTC 插件编排（进程级）：与 [`crate::domain::capability::ports::RtcCapability`] 协同的元数据与选路。
pub mod rtc;
