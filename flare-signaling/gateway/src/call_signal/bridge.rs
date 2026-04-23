//! RTC 信令桥：把 `EVENT_CALL_SIGNAL` 与能力实例路由串起来（不替代 orchestrator enrich）。

use std::sync::Arc;

use flare_proto::common::CallSignalEvent;

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
        cs: &CallSignalEvent,
    ) -> anyhow::Result<Option<CapabilityRouteHint>> {
        self.router.route_uplink(tenant_id, cs).await
    }

    pub async fn on_downlink(
        &self,
        tenant_id: &str,
        cs: &CallSignalEvent,
    ) -> anyhow::Result<Option<CapabilityRouteHint>> {
        self.router.route_downlink(tenant_id, cs).await
    }
}
