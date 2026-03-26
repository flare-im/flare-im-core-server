//! 领域端口的基础设施实现（与 `domain/ports` **文件名一一对应**）
//!
//! 每个 `domain/ports/*.rs` 中的 trait，在本目录有同名模块承载实现（Router / 仓储等）。

mod connection_port;
mod connection_query;
mod push_port;
mod message_port;
mod event_prot;
mod data_port;
mod ack_port;
mod context_resolver;
mod route_grpc_pool;
mod storage_sync_grpc_pool;
mod storage_sync_port;

pub use connection_port::ConnectionRepository;
pub use connection_query::ManagerConnectionQuery;
pub use push_port::PushRepository;
pub use message_port::RouterMessageCommandPort;
pub use event_prot::RouterEventCommandPort;
pub use data_port::RouterDataCommandPort;
pub use ack_port::RouterAckReportPort;
pub use storage_sync_port::StorageSyncPort;
pub use context_resolver::{build_gateway_ctx_from_info, ConnectionContextResolver};
pub use route_grpc_pool::SignalingRouteGrpcPool;
pub use storage_sync_grpc_pool::StorageSyncGrpcPool;
