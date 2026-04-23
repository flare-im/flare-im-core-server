//! Hook **出站集成** 在领域层的传输分类（与 [`crate::domain::model::HookTransportConfig`] 一一对应）。

use crate::domain::model::HookTransportConfig;

/// 传输面语义（供查询/审计/文档化；不承载网络细节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookTransportSurface {
    /// 进程外 gRPC（直连 endpoint 或经注册中心解析 `service_name`）。
    GrpcHook,
    /// HTTP Webhook（同步请求/响应或幂等投递，由适配器实现细节决定）。
    WebhookHook,
    /// 进程内扩展（Local 插件 target）。
    LocalExtension,
}

#[must_use]
pub fn classify_transport(config: &HookTransportConfig) -> HookTransportSurface {
    match config {
        HookTransportConfig::Grpc { .. } => HookTransportSurface::GrpcHook,
        HookTransportConfig::Webhook { .. } => HookTransportSurface::WebhookHook,
        HookTransportConfig::Local { .. } => HookTransportSurface::LocalExtension,
    }
}
