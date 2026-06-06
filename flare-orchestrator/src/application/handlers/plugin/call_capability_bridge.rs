//! 音视频通话 RTC 能力桥。
//!
//! # 职责
//! - 保留 RTC capability gateway 的注入点。
//! - 通话/媒体信令不再作为 `common.Event` 的 durable payload 进入 IM Core。
//! - 后续 RTC 插件应通过 `RealtimeControlPacket` / capability packet 路由，而不是恢复 `EVENT_CALL_SIGNAL`。

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::Event;

use crate::domain::repository::CapabilityDispatchGateway;
use flare_server_core::error::Result;

/// 应用层薄封装：依赖注入与时序收口。
#[derive(Clone)]
pub struct CallCapabilityBridge {
    _gateway: Arc<dyn CapabilityDispatchGateway>,
}

impl CallCapabilityBridge {
    /// `gateway` 通常为 gRPC `CapabilityDispatchClient` 适配实现，由 `wire` 在 RTC 开关开启时注入。
    pub fn new(gateway: Arc<dyn CapabilityDispatchGateway>) -> Self {
        Self { _gateway: gateway }
    }

    /// Durable `Event` no longer carries RTC call signals. This hook is a
    /// no-op until the RTC plugin path is rebuilt on realtime/capability packets.
    pub async fn enrich_call_signal_event(
        &self,
        _ctx: &Ctx,
        _tenant_id: &str,
        _event: &mut Event,
    ) -> Result<()> {
        Ok(())
    }
}
