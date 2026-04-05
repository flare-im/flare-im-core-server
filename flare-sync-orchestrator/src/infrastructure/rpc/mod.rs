//! RPC 客户端实现层
//!
//! 本模块提供基于 tonic 的 RPC 客户端实现，
//! 实现 application 层定义的 Port trait。

mod sync_adapters;

pub use sync_adapters::GrpcSyncAdapters;
