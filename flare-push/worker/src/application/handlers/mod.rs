//! 应用层编排：Online 设备解析 + 按网关分组直推 Access Gateway。

mod online_gateway_delivery;

pub use online_gateway_delivery::GatewayPushExecutor;
