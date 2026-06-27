//! 领域层：网关推送目标与路由合并（无 I/O）。

pub mod model;
pub mod offline_delivery;
pub mod push_routing;

pub use model::GatewayPushTarget;
pub use offline_delivery::DeviceTokenRepository;
