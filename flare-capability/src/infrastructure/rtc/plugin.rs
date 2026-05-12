//! 能力插件端口：描述「一类 RTC 后端实现」（进程内或独立进程控制面等，由部署装配）。

use async_trait::async_trait;
use std::sync::Arc;

use flare_core_base::context::Ctx;

use super::capability::{CapabilityKind, RtcBackendDescriptor};
use crate::domain::capability::Result as CapResult;

/// 插件生命周期与元数据（不负责具体 SDP；媒体信令仍走 IM + 客户端既有通路）。
#[async_trait]
pub trait CapabilityPlugin: Send + Sync {
    /// 稳定插件 id（部署自定义，例如 `rtc-backend-a`）。
    fn plugin_id(&self) -> &str;

    fn kind(&self) -> CapabilityKind;

    /// 控制面可达描述（gRPC endpoint、可选 mTLS 配置键等）。
    fn descriptor(&self) -> RtcBackendDescriptor;

    /// 进程启动后注册到编排器（上报实例 id、版本）。
    async fn on_register(&self, ctx: &Ctx) -> CapResult<()>;

    /// 心跳：用于 `CapabilityHealthChecker` 聚合。
    async fn heartbeat(&self, ctx: &Ctx) -> CapResult<()>;

    /// 进入 draining：拒绝新会话（实现由插件进程执行）。
    async fn mark_draining(&self, ctx: &Ctx) -> CapResult<()>;

    /// 彻底禁用（运维切流后）。
    async fn disable(&self, ctx: &Ctx) -> CapResult<()>;
}

pub type DynCapabilityPlugin = Arc<dyn CapabilityPlugin>;
