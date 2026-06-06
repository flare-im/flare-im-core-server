//! RTC 信令桥：把实时通话控制视图与能力实例路由串起来。

use std::sync::Arc;

use super::event::CallSignalRouteView;
use super::router::{CallSignalRouter, CapabilityRouteHint};

/// 网关侧桥接入口：保持 **薄** —— 业务 enrich 仍在 `flare-orchestrator::CallCapabilityBridge`。
pub struct CallSignalBridge {
    router: Arc<CallSignalRouter>,
}

impl CallSignalBridge {
    pub fn new(router: Arc<CallSignalRouter>) -> Self {
        Self { router }
    }

    pub async fn on_uplink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        self.router.route_uplink(tenant_id, signal).await
    }

    pub async fn on_downlink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        self.router.route_downlink(tenant_id, signal).await
    }
}
