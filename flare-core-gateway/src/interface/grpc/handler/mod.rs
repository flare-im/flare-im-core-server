//! gRPC 网关：透明代理到 Media / Hook / Message(Orchestrator) / Online。

pub mod simple_gateway;

pub use simple_gateway::SimpleGatewayHandler;
