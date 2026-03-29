//! 领域仓储接口（Port）
//!
//! 定义各种路由的抽象接口，遵循 DDD 的 Port 模式

pub mod ack_routing_port;
pub mod data_routing_port;
pub mod event_routing_port;
pub mod message_routing_port;
pub mod route_repository;

pub use ack_routing_port::AckRoutingPort;
pub use data_routing_port::DataRoutingPort;
pub use event_routing_port::EventRoutingPort;
pub use message_routing_port::{Ctx, MessageRoutingPort};
pub use route_repository::{DefaultRouteRepository, NoopRouteRepository, RouteRepository};
