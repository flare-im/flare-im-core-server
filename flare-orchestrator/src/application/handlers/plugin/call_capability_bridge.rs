//! 音视频通话 RTC 能力桥：编排层入口，领域规则在 [`crate::domain::service::CallSignalEnrichmentService`]。
//!
//! # 职责
//! - 持有 `Arc<CallSignalEnrichmentService>`，内部使用 [`crate::domain::repository::CapabilityDispatchGateway`]
//!   向 `flare-capability` 发起 `Dispatch`（`rtc.call.*`、`rtc.media.*`）。
//! - 对外仅暴露 [`CallCapabilityBridge::enrich_call_signal_event`]，供扩展编排器在**事件持久化之前**调用，
//!   以便将媒体后端返回的 `room_id` / `peer_id` / `signaling_ws_base` 等写回 [`flare_proto::common::CallSignalEvent`]。
//!
//! # 调用链（上行）
//! 1. 客户端/SDK 构造 `Event`：`r#type = EVENT_CALL_SIGNAL`，`payload = CallSignal`（invite、accept、ice_candidate 等）。
//! 2. gRPC [`crate::interface::grpc::MessageSendGrpcHandler::execute_event`] 或等价入口 →
//!    [`crate::application::handlers::EventHandler::handle_general_event`]。
//! 3. 校验通过后 [`crate::application::extension::ExtensionOrchestrator::enrich_event_before_persist`]（若 wire 注入本桥且路由允许）→ **本模块**。
//! 4. 领域服务 enrich 成功后 → [`crate::domain::service::EventDomainService::allocate_seq`] → `push_event`。
//!
//! # 开关与路由
//! - `MessageOrchestratorConfig::capability_rtc_bridge_enabled == false` 时 **不创建** 本桥，通话事件仅走 IM 扇出，不联动 RTC。
//! - `extension_plugin_event_type_allowlist` 非空时，必须包含 `EventCallSignal` 对应整型，否则 `ExtensionRouting` 会跳过 enrich。
//!
//! 失败策略（fail-open / fail-closed）与 `ext` 降级键由 [`crate::application::extension::ExtensionOrchestrator`] 与
//! [`crate::domain::extension::ExtensionPolicy`] 统一处理。

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::Event;

use crate::domain::repository::CapabilityDispatchGateway;
use crate::domain::service::CallSignalEnrichmentService;
use crate::error::Result;

/// 应用层薄封装：依赖注入与时序收口；**不**内含业务分支（均在 `CallSignalEnrichmentService`）。
#[derive(Clone)]
pub struct CallCapabilityBridge {
    service: Arc<CallSignalEnrichmentService>,
}

impl CallCapabilityBridge {
    /// `gateway` 通常为 gRPC `CapabilityDispatchClient` 适配实现，由 `wire` 在 RTC 开关开启时注入。
    pub fn new(gateway: Arc<dyn CapabilityDispatchGateway>) -> Self {
        Self {
            service: Arc::new(CallSignalEnrichmentService::new(gateway)),
        }
    }

    /// 对 `EVENT_CALL_SIGNAL` 在入库前做 RTC enrich；非通话事件由调用方过滤，领域内也会二次校验。
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
