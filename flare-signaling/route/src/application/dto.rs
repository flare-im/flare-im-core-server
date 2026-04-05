//! 应用层数据传输对象（DTO）
//!
//! 用于封装应用层到接口层的数据传输
//!
//! 错误处理：使用 flare_server_core::error::Result 和 FlareError

use flare_grpc_proto::signaling::router::RouteMetadata;
use std::collections::HashMap;

/// 消息路由结果（只包含业务数据）
#[derive(Debug, Clone)]
pub struct MessageRouteResult {
    pub response_data: Vec<u8>,
    pub routed_endpoint: String,
    pub metadata: RouteMetadata,
}

/// 操作事件路由结果（只包含业务数据，response_data 为 protobuf 编码的响应）
#[derive(Debug, Clone)]
pub struct EventRouteResult {
    pub response_data: Vec<u8>,
    pub routed_endpoint: String,
    pub metadata: RouteMetadata,
}

/// 工具函数：构建路由元数据（与 proto RouteMetadata 一致，无 trace 字段）
pub fn build_route_metadata(
    route_duration_ms: i64,
    business_duration_ms: i64,
    decision_duration_ms: i64,
    svid: &str,
    load_balance_strategy: i32,
) -> RouteMetadata {
    RouteMetadata {
        route_duration_ms,
        business_duration_ms,
        decision_duration_ms,
        from_cache: false,
        decision_details: {
            let mut details = HashMap::new();
            details.insert("svid".to_string(), svid.to_string());
            details.insert(
                "load_balance_strategy".to_string(),
                format!("{}", load_balance_strategy),
            );
            details
        },
    }
}
