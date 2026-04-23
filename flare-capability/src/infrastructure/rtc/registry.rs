//! 插件注册表端口：维护可用实例视图（CQRS 读模型侧接口；写路径由 Manager 编排）。

use async_trait::async_trait;
use std::sync::Arc;

use flare_core_base::context::Ctx;

use super::capability::{CapabilityKind, RtcBackendDescriptor};
use super::plugin::DynCapabilityPlugin;
use crate::domain::capability::Result as CapResult;

/// 注册表：实例级插件登记（与 `CapabilityExtensionRegistry` 正交，专注 RTC 插件进程）。
#[async_trait]
pub trait CapabilityRegistry: Send + Sync {
    async fn register(&self, ctx: &Ctx, plugin: DynCapabilityPlugin) -> CapResult<()>;

    async fn unregister(&self, ctx: &Ctx, plugin_id: &str) -> CapResult<()>;

    async fn list_by_kind(&self, ctx: &Ctx, kind: CapabilityKind) -> CapResult<Vec<RtcBackendDescriptor>>;

    async fn get_descriptor(&self, ctx: &Ctx, instance_id: &str) -> CapResult<Option<RtcBackendDescriptor>>;
}

pub type DynCapabilityRegistry = Arc<dyn CapabilityRegistry>;
