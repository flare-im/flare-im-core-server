//! 同步编排领域层：同步意图、关键事件分级、游标单调性、序号水位与不丢同步约束。
//!
//! 不包含 gRPC 或下游 HTTP/RPC 细节；对外依赖通过 `application` 层端口注入。

pub mod error;
pub mod model;
pub mod service;

pub use error::SyncDomainError;
