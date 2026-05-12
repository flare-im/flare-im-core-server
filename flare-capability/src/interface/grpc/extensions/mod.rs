//! `ExtensionPlugin` gRPC 传输层：核心只提供**通用路由器** [`ExtensionPluginRouter`]，
//! 按 `operation` 前缀把请求转给插件注册的 [`crate::domain::capability::ExtensionOperationHandler`]。
//!
//! 具体后端（媒体控制协议实现 / LiveKit / Janus / …）的 operation 语义由插件 crate 实现并在
//! `wire(..)` 时注入，核心对它们一无所知。

mod router;

pub use router::ExtensionPluginRouter;
