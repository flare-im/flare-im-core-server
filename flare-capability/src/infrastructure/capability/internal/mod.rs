//! 内部运行时适配装配（不对外暴露具体后端实现类型）。
//!
//! 远端媒体插件（如 `flare-strom-sfu`）统一走 **lazy + 服务发现**：
//! 启动阶段仅登记 RTC 后端与路由，首次 `Dispatch` 时再解析 Consul 实例。

use std::sync::Arc;

use flare_im_contracts::utils::normalize_tenant_id;

use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::config::capability_runtime::discovery_route_authority;

#[cfg(feature = "backend-remote")]
mod remote_extension_ops;
#[cfg(feature = "backend-remote")]
mod remote_route_book;
#[cfg(feature = "backend-remote")]
mod remote_rtc_adapter;

#[cfg(feature = "backend-remote")]
#[allow(clippy::too_many_arguments)]
async fn attach_media_control_backend(
    registry: &CapabilityExtensionRegistry,
    plugin_routes: &Arc<PluginRouteBook>,
    tenant_id: &str,
    plugin_id: &str,
    capability_id: &str,
    rtc: Arc<remote_rtc_adapter::MediaControlGrpcRtcCapability>,
    route_authority: &str,
    source: &str,
) -> flare_server_core::error::Result<()> {
    use crate::domain::capability::ExtensionOperationHandler;
    use remote_extension_ops::MediaControlExtensionOperations;
    use remote_route_book::register_plugin_route;

    let tenant_id = normalize_tenant_id(tenant_id);

    registry
        .set_rtc_backend_for_tenant(&tenant_id, Some(rtc.clone()))
        .await;
    if tenant_id == "0" {
        registry.set_rtc_backend(Some(rtc.clone())).await;
    }

    let handler: Arc<dyn ExtensionOperationHandler> = Arc::new(
        MediaControlExtensionOperations::new(Some(rtc), plugin_routes.clone()),
    );
    registry.register_extension_operations(handler).await;
    register_plugin_route(
        plugin_routes.as_ref(),
        &tenant_id,
        plugin_id,
        capability_id,
        route_authority,
    );
    tracing::info!(
        route_authority = %route_authority,
        source = %source,
        plugin_id = %plugin_id,
        capability_id = %capability_id,
        tenant_id = %tenant_id,
        "registered media control backend"
    );
    Ok(())
}

/// 按 `capability_runtime.plugin_discovery_endpoints` 登记 lazy RTC 后端（无启动期拨号）。
pub(crate) async fn register_discovered_media_plugins(
    registry: &CapabilityExtensionRegistry,
    plugin_routes: &Arc<PluginRouteBook>,
    runtime: &CapabilityRuntimeConfig,
) -> flare_server_core::error::Result<()> {
    #[cfg(feature = "backend-remote")]
    {
        let mut registered = 0usize;
        for ep in runtime.media_control_endpoints() {
            let rtc = Arc::new(
                remote_rtc_adapter::MediaControlGrpcRtcCapability::from_service_name(
                    ep.service_name.as_str(),
                )
                .await?,
            );
            attach_media_control_backend(
                registry,
                plugin_routes,
                ep.tenant_id.as_str(),
                ep.plugin_id.as_str(),
                ep.capability_id.as_str(),
                rtc,
                discovery_route_authority(ep.service_name.as_str()).as_str(),
                "discovery",
            )
            .await?;
            registered += 1;
        }

        if registered == 0
            && let Ok(endpoint) = std::env::var("FLARE_MEDIA_CONTROL_GRPC_ENDPOINT")
        {
            let endpoint = endpoint.trim();
            if !endpoint.is_empty() {
                let rtc = Arc::new(
                    remote_rtc_adapter::MediaControlGrpcRtcCapability::from_static_lazy(
                        endpoint.to_string(),
                    )?,
                );
                attach_media_control_backend(
                    registry,
                    plugin_routes,
                    "0",
                    "media-control",
                    "rtc.media.control",
                    rtc,
                    endpoint,
                    "static-env",
                )
                .await?;
                registered += 1;
            }
        }

        if registered == 0 {
            tracing::info!(
                "no media plugin configured; rtc.* Dispatch returns NotRegistered until plugin registers"
            );
        }
    }
    Ok(())
}
