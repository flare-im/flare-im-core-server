//! 领域端口的基础设施实现（与 `domain/ports` **文件名一一对应**）
//!
//! 每个 `domain/ports/*.rs` 中的 trait，在本目录有同名模块承载实现（Router / 仓储等）。

mod ack_port;
mod connection_port;
mod connection_query;
mod context_resolver;
mod conversation_read_grpc_pool;
mod data_port;
mod device_handoff_port;
mod event_prot;
mod link_probe_port;
mod message_port;
mod push_port;
mod push_proxy_grpc_pool;
mod realtime_broadcast_port;
mod route_grpc_pool;
mod static_read_endpoints;
mod storage_sync_grpc_pool;
mod storage_sync_port;

pub use ack_port::RouterAckReportPort;
pub use connection_port::ConnectionRepository;
pub use connection_query::ManagerConnectionQuery;
pub use context_resolver::{ConnectionContextResolver, build_gateway_ctx_from_info};
pub use conversation_read_grpc_pool::ConversationReadGrpcPool;
pub use data_port::RouterDataCommandPort;
pub use device_handoff_port::PushProxyHandoffReporter;
pub use event_prot::RouterEventCommandPort;
pub use link_probe_port::ServerHandleLinkProbe;
pub use message_port::RouterMessageCommandPort;
pub use push_port::PushRepository;
pub use push_proxy_grpc_pool::PushProxyGrpcPool;
pub use realtime_broadcast_port::GatewayRealtimeBroadcast;
pub use route_grpc_pool::SignalingRouteGrpcPool;
pub(crate) use static_read_endpoints::StaticReadEndpoints;
pub use storage_sync_grpc_pool::StorageSyncGrpcPool;
pub use storage_sync_port::StorageSyncPort;
