//! 健康检查端口：聚合插件心跳，供编排器 / 运维决策。

use std::sync::Arc;

use async_trait::async_trait;

use flare_core_base::context::Ctx;

use super::capability::RtcBackendDescriptor;
use crate::domain::capability::Result as CapResult;

#[async_trait]
pub trait CapabilityHealthChecker: Send + Sync {
    async fn probe_instance(&self, ctx: &Ctx, instance_id: &str) -> CapResult<bool>;

    async fn list_healthy(&self, ctx: &Ctx) -> CapResult<Vec<RtcBackendDescriptor>>;
}

pub type DynCapabilityHealthChecker = Arc<dyn CapabilityHealthChecker>;
