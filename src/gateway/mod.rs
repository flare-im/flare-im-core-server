//! Gateway Router 模块
//!
//! 跨地区网关路由组件，根据 gateway_id 路由到对应的 Access Gateway。
//! 支持单地区/多地区自适应部署。

mod config;
pub mod router;

pub use config::{
    GatewayEnvScope, GatewayGrpcConfig, GatewaySettings, RateLimitConfig, ServerConfig,
    TracingConfig, require_secure_token_secret,
};
pub use router::{GatewayRouter, GatewayRouterConfig, GatewayRouterTrait};
