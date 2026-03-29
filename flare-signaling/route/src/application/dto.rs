//! 应用层数据传输对象（DTO）
//!
//! 用于封装应用层到接口层的数据传输

use flare_proto::signaling::router::RouteMetadata;
use std::collections::HashMap;

/// 消息路由结果
#[derive(Debug, Clone)]
pub struct MessageRouteResult {
    pub response_data: Vec<u8>,
    pub routed_endpoint: String,
    pub metadata: RouteMetadata,
    pub error_code: Option<u32>,
    pub error_message: Option<String>,
}

/// 操作事件路由结果（response_data 为 protobuf 编码的 OperationResponse）
#[derive(Debug, Clone)]
pub struct EventRouteResult {
    pub response_data: Vec<u8>,
    pub routed_endpoint: String,
    pub metadata: RouteMetadata,
    pub error_code: Option<u32>,
    pub error_message: Option<String>,
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
