//! 路由上下文辅助（消息与事件路由共用）
//!
//! 追踪等通过 Context 的 trace_id/request_id 在 metadata 中传递，此处仅保留与 RouteOptions 的兼容占位。

use flare_grpc_proto::signaling::router::RouteOptions;
use flare_server_core::context::Context;

/// 是否启用追踪（与 RouteOptions 一致；实际 trace 通过 Context.trace_id() 在 metadata 传递）
#[inline]
#[allow(dead_code)]
pub fn enable_tracing(_ctx: &Context, route_options: &RouteOptions) -> bool {
    route_options.enable_tracing
}
