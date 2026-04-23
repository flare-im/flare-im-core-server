//! 应用层桥接器：封装 `EVENT_CALL_SIGNAL` enrich 用例入口（业务规则在 domain service）。

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::Event;

use crate::domain::repository::CapabilityDispatchGateway;
use crate::domain::service::CallSignalEnrichmentService;
use crate::error::Result;

/// 应用层薄封装：负责依赖注入与调用时序，领域规则由 [`CallSignalEnrichmentService`] 承担。
pub struct CallCapabilityBridge {
    service: Arc<CallSignalEnrichmentService>,
}

impl CallCapabilityBridge {
    pub fn new(gateway: Arc<dyn CapabilityDispatchGateway>) -> Self {
        Self {
            service: Arc::new(CallSignalEnrichmentService::new(gateway)),
        }
    }

    pub async fn enrich_call_signal_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: &mut Event,
    ) -> Result<()> {
        self.service
            .enrich_call_signal_event(ctx, tenant_id, event)
            .await
    }
}
