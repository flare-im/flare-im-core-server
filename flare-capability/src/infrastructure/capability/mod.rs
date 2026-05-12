//! 能力扩展基础设施（按职责分包）。
//!
//! - [`registration`]：扩展注册表与授权内存模型
//! - [`dispatch`]：PreSend / Recipient 运行时与 Dispatch 限流
//! - [`routing`]：插件路由簿与 RTC 路由器
//! - [`adapters`] / [`builtin`]：可选适配器与内置 Guard / Resolver
//!
//! 具体后端实现位于私有 `internal` 子模块，仅用于运行时装配；
//! crate 对外只暴露协议与通用注册/路由能力。

pub mod adapters;
pub mod builtin;
pub mod dispatch;
pub mod registration;
pub mod routing;

mod internal;
pub mod use_case_samples;

pub use dispatch::{DispatchRateLimiter, PreSendGuardRuntime, RecipientResolverRuntime};
pub use registration::{CapabilityExtensionRegistry, InMemoryCapabilityGrants, RegistryInner};
pub use routing::{PluginRouteBook, RtcCapabilityRouter};
pub use use_case_samples::{SendMessageUseCaseExample, StartCallUseCaseExample};

pub(crate) use internal::wire_media_control_backend;
