//! 通话信令路由骨架：按 `call_id` / `sfu_room_id` 解析 capability 实例（读绑定表由下游注入）。
//!
//! 第一版仅保留接口形状；实际查询应通过注入的 `RouteLookup`（后续接 storage / 本地缓存）。

use async_trait::async_trait;
use std::sync::Arc;

use super::event::CallSignalRouteView;

/// 解析结果：告诉编排器/网关下游应投递的实例键（opaque string）。
#[derive(Debug, Clone)]
pub struct CapabilityRouteHint {
    pub capability_instance_id: String,
    pub sfu_room_id: Option<String>,
    pub call_id: Option<String>,
}

/// 由基础设施实现的查找端口（避免网关直接依赖 DB）。
#[async_trait]
pub trait CallBindingLookup: Send + Sync {
    async fn resolve_by_call_id(
        &self,
        tenant_id: &str,
        call_id: &str,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>>;

    async fn resolve_by_room_id(
        &self,
        tenant_id: &str,
        sfu_room_id: &str,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>>;
}

/// 路由表：决定「后续 enrich / 下行推送」使用的实例提示。
pub struct CallSignalRouter {
    lookup: Arc<dyn CallBindingLookup>,
}

impl CallSignalRouter {
    pub fn new(lookup: Arc<dyn CallBindingLookup>) -> Self {
        Self { lookup }
    }

    /// 上行：客户端 → 网关 →（经本路由）orchestrator / capability。
    pub async fn route_uplink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        if let Some(call_id) = signal.call_id.as_deref() {
            return self.lookup.resolve_by_call_id(tenant_id, call_id).await;
        }
        if let Some(rid) = signal.sfu_room_id.as_deref() {
            return self.lookup.resolve_by_room_id(tenant_id, rid).await;
        }
        Ok(None)
    }

    /// 下行：系统 → 网关 → 客户端（骨架：按绑定回填目标连接键，具体 fanout 在既有 Push 路径实现）。
    pub async fn route_downlink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        self.route_uplink(tenant_id, signal).await
    }
}
