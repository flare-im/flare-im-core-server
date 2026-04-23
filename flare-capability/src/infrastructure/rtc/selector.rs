//! 新呼叫选路与老房间解析（第一版：新房间绑新实例；老房间自然结束，不做 PC 热迁移）。

use std::sync::Arc;

use async_trait::async_trait;

use flare_core_base::context::Ctx;

use super::capability::{CapabilityKind, RtcBackendDescriptor};
use crate::domain::capability::Result as CapResult;

/// 选路策略：与 draining / disabled 标记协同（具体算法后续接 storage 投影）。
#[async_trait]
pub trait CapabilitySelector: Send + Sync {
    /// 新 invite：选择当前可接入实例（跳过 draining/disabled）。
    async fn select_for_new_call(
        &self,
        ctx: &Ctx,
        kind: CapabilityKind,
        tenant_id: &str,
    ) -> CapResult<RtcBackendDescriptor>;

    /// 已存在房间：按 `room_id` / `call_id` 绑定解析实例（只读）。
    async fn resolve_for_existing_room(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        room_id: &str,
        call_id: Option<&str>,
    ) -> CapResult<Option<RtcBackendDescriptor>>;
}

pub type DynCapabilitySelector = Arc<dyn CapabilitySelector>;
