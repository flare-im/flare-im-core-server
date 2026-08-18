//! 将媒体控制面后端登记到 [`PluginRouteBook`]，供 `CapabilityService` 客户端发现。

use std::collections::HashMap;

use crate::infrastructure::capability::PluginRouteBook;
use flare_grpc_proto::capability::RegisteredPluginInstance;

/// 登记当前已连接的媒体控制面 gRPC 入口。
pub fn register_plugin_route(
    plugin_routes: &PluginRouteBook,
    tenant_id: &str,
    plugin_id: &str,
    capability_id: &str,
    grpc_endpoint: &str,
) {
    let mut labels = HashMap::new();
    labels.insert("backend_class".into(), "media_control".into());

    let instance = RegisteredPluginInstance {
        plugin_id: plugin_id.to_string(),
        capability_id: capability_id.to_string(),
        grpc_authority: grpc_endpoint.to_string(),
        labels,
        // 由内部装配登记的端点同样没有清单：如实标 unverified。
        // 「内部的所以可信」是错觉——可信与否要看有没有可校验的声明，
        // 而不是看谁登记的。
        plugin_version: String::new(),
        api_version: String::new(),
        manifest_sha256: String::new(),
        declared_operations: Vec::new(),
        unverified: true,
    };
    plugin_routes.upsert(tenant_id, instance);
    tracing::info!(
        tenant_id = %tenant_id,
        plugin_id = %plugin_id,
        capability_id = %capability_id,
        endpoint = %grpc_endpoint,
        "registered media control backend in PluginRouteBook"
    );
}
