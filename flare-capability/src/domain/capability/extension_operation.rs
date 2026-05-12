//! **ExtensionPlugin 通用扩展点**：按 `operation` 前缀路由到不同的 handler。
//!
//! 核心仅提供"端口 + 路由"；具体 operation（如 `flare.media.v1.*`）由外部插件 crate
//! 在 `wire(..)` 时实现 [`ExtensionOperationHandler`] 并通过
//! [`crate::infrastructure::capability::CapabilityExtensionRegistry::register_extension_operations`]
//! 挂入。核心不识别任何具体 operation 的语义。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use prost_types::Any;
use tonic::Status;

/// 单个 operation 处理器：声明自己接管的 operation 前缀，实现 `call`。
///
/// - `operation_prefixes` 返回至少一个命名空间前缀（如 `"flare.media.v1."`）；
///   路由器按**最长前缀匹配**决定分发目标。
/// - `call` 接收原始 `operation` 名与 `Any` 载荷，返回 `Any` 结果（由路由器再包装为 `GenericResponse`）。
#[async_trait]
pub trait ExtensionOperationHandler: Send + Sync {
    /// 稳定的 handler 标识（日志 / 指标）。
    fn id(&self) -> &str;

    /// 该 handler 认领的 operation 前缀集合（例如 `["flare.media.v1."]`）。
    fn operation_prefixes(&self) -> &[&'static str];

    /// 处理一次 `ExtensionPlugin.Call`。返回的 `Any` 会被核心路由器以 `ok=true` 回包；
    /// 错误用 [`tonic::Status`] 表达（会原样外传）。
    async fn call(&self, ctx: &Ctx, operation: &str, payload: Option<Any>) -> Result<Any, Status>;
}

/// 方便插件装配层传递 `Arc<dyn ...>`。
pub type DynExtensionOperationHandler = Arc<dyn ExtensionOperationHandler>;
