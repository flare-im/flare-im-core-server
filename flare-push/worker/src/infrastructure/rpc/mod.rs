//! RPC 客户端实现层
//!
//! 本模块提供基于 tonic 的 RPC 客户端实现。

mod online_client;

pub use online_client::OnlineServiceClient;
