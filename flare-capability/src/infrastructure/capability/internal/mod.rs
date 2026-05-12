//! 内部运行时适配装配（不对外暴露具体后端实现类型）。
//!
//! 说明：
//! - 对外仍只暴露 `RtcCapability` / `ExtensionOperationHandler` 协议；
//! - 具体后端实现（远端控制面）仅在本模块内接线；
//! - 外部调用方只需通过 `wire_runtime_adapters(..)` 触发装配。

use std::sync::Arc;

use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};

#[cfg(feature = "backend-remote")]
mod remote_extension_ops;
#[cfg(feature = "backend-remote")]
mod remote_route_book;
#[cfg(feature = "backend-remote")]
mod remote_rtc_adapter;

#[cfg(feature = "backend-remote")]
pub(crate) async fn wire_media_control_backend(
    registry: &CapabilityExtensionRegistry,
    plugin_routes: &Arc<PluginRouteBook>,
    tenant_id: &str,
    plugin_id: &str,
    capability_id: &str,
    grpc_endpoint: &str,
) -> anyhow::Result<()> {
    use crate::domain::capability::ExtensionOperationHandler;
    use remote_extension_ops::MediaControlExtensionOperations;
    use remote_route_book::register_plugin_route;
    use remote_rtc_adapter::MediaControlGrpcRtcCapability;

    let rtc = Arc::new(MediaControlGrpcRtcCapability::connect(grpc_endpoint.to_string()).await?);
    registry
        .set_rtc_backend_for_tenant(tenant_id, Some(rtc.clone()))
        .await;
    if tenant_id == "default" {
        registry.set_rtc_backend(Some(rtc.clone())).await;
    }

    let handler: Arc<dyn ExtensionOperationHandler> = Arc::new(
        MediaControlExtensionOperations::new(Some(rtc), plugin_routes.clone()),
    );
    registry.register_extension_operations(handler).await;
    register_plugin_route(
        plugin_routes.as_ref(),
        tenant_id,
        plugin_id,
        capability_id,
        grpc_endpoint,
    );
    tracing::info!(
        endpoint = %grpc_endpoint,
        plugin_id = %plugin_id,
        capability_id = %capability_id,
        "wired remote media control backend"
    );
    Ok(())
}

#[cfg(not(feature = "backend-remote"))]
pub(crate) async fn wire_media_control_backend(
    _registry: &CapabilityExtensionRegistry,
    _plugin_routes: &Arc<PluginRouteBook>,
    _tenant_id: &str,
    _plugin_id: &str,
    _capability_id: &str,
    _grpc_endpoint: &str,
) -> anyhow::Result<()> {
    Ok(())
}
