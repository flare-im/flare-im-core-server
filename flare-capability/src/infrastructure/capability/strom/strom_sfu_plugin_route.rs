//! 将 **flare-strom-sfu** 登记到 [`PluginRouteBook`]，供 `CapabilityService` / `ExtensionPlugin` 发现。

use flare_grpc_proto::capability::RegisteredPluginInstance;
use std::collections::HashMap;

use crate::infrastructure::capability::routing::PluginRouteBook;

pub const STROM_PLUGIN_ID: &str = "flare-strom-sfu";
pub const STROM_CAPABILITY_CONTROL: &str = "media.strom_sfu.control";

/// 在 capability 进程内登记当前已连接的 strom-sfu gRPC 入口（与 `FLARE_STROM_SFU_GRPC_ENDPOINT` 一致）。
pub fn register_strom_sfu_plugin_route(plugin_routes: &PluginRouteBook, grpc_endpoint: &str) {
    let tenant_id = std::env::var("FLARE_STROM_SFU_ROUTE_TENANT_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "0".to_string());

    let mut labels = HashMap::new();
    labels.insert("rtc_backend".into(), "strom".into());
    labels.insert("sfu_control_package".into(), "flare.sfu.control.v1".into());

    let instance = RegisteredPluginInstance {
        plugin_id: STROM_PLUGIN_ID.to_string(),
        capability_id: STROM_CAPABILITY_CONTROL.to_string(),
        grpc_authority: grpc_endpoint.to_string(),
        labels,
    };
    plugin_routes.upsert(&tenant_id, instance);
    tracing::info!(
        tenant_id = %tenant_id,
        plugin_id = %STROM_PLUGIN_ID,
        capability_id = %STROM_CAPABILITY_CONTROL,
        endpoint = %grpc_endpoint,
        "registered flare-strom-sfu in PluginRouteBook"
    );
}
