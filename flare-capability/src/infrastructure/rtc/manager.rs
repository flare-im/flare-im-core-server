//! 能力编排门面：注册、心跳、draining、禁用、选路（应用服务骨架；后续注入真实 Registry/Selector/Health）。

use flare_core_base::context::Ctx;

use super::capability::{CapabilityKind, RtcBackendDescriptor};
use super::health::DynCapabilityHealthChecker;
use super::plugin::{CapabilityPlugin, DynCapabilityPlugin};
use super::registry::DynCapabilityRegistry;
use super::selector::DynCapabilitySelector;
use crate::domain::capability::Result as CapResult;

/// RTC 插件编排器（FSM 在 conversation 域；此处只做 **能力面** 协调）。
#[derive(Clone)]
pub struct CapabilityManager {
    registry: DynCapabilityRegistry,
    selector: DynCapabilitySelector,
    health: DynCapabilityHealthChecker,
}

impl CapabilityManager {
    pub fn new(
        registry: DynCapabilityRegistry,
        selector: DynCapabilitySelector,
        health: DynCapabilityHealthChecker,
    ) -> Self {
        Self {
            registry,
            selector,
            health,
        }
    }

    pub async fn register(&self, ctx: &Ctx, plugin: DynCapabilityPlugin) -> CapResult<()> {
        self.registry.register(ctx, plugin).await
    }

    /// 对某实例做一次健康探测（不等价于插件内 `CapabilityPlugin::heartbeat`，后者由插件主动上报周期另接）。
    pub async fn heartbeat(&self, ctx: &Ctx, instance_id: &str) -> CapResult<bool> {
        self.health.probe_instance(ctx, instance_id).await
    }

    pub async fn plugin_heartbeat(&self, ctx: &Ctx, plugin: DynCapabilityPlugin) -> CapResult<()> {
        CapabilityPlugin::heartbeat(plugin.as_ref(), ctx).await
    }

    pub async fn mark_draining(&self, ctx: &Ctx, plugin: DynCapabilityPlugin) -> CapResult<()> {
        plugin.mark_draining(ctx).await
    }

    pub async fn disable(&self, ctx: &Ctx, plugin: DynCapabilityPlugin) -> CapResult<()> {
        plugin.disable(ctx).await
    }

    pub async fn select_for_new_call(
        &self,
        ctx: &Ctx,
        kind: CapabilityKind,
        tenant_id: &str,
    ) -> CapResult<RtcBackendDescriptor> {
        self.selector.select_for_new_call(ctx, kind, tenant_id).await
    }

    pub async fn resolve_for_existing_room(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        room_id: &str,
        call_id: Option<&str>,
    ) -> CapResult<Option<RtcBackendDescriptor>> {
        self.selector
            .resolve_for_existing_room(ctx, tenant_id, room_id, call_id)
            .await
    }
}
