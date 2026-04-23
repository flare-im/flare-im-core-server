//! 能力扩展基础设施（按职责分包）。
//!
//! - [`registration`]：扩展注册表与授权内存模型
//! - [`dispatch`]：PreSend / Recipient 运行时与 Dispatch 限流
//! - [`routing`]：插件路由簿与 RTC 路由器
//! - [`strom`]：flare-strom-sfu gRPC 控制面与路由登记
//! - [`adapters`] / [`builtin`]：可选适配器与内置 Guard/Resolver

pub mod adapters;
pub mod builtin;
pub mod dispatch;
pub mod registration;
pub mod routing;
pub mod strom;

mod sfu_rtc_capability;
pub mod use_case_samples;

pub use dispatch::{DispatchRateLimiter, PreSendGuardRuntime, RecipientResolverRuntime};
pub use registration::{CapabilityExtensionRegistry, InMemoryCapabilityGrants, RegistryInner};
pub use routing::{PluginRouteBook, RtcCapabilityRouter};
pub use sfu_rtc_capability::SfuRtcCapability;
pub use strom::{
    register_strom_sfu_plugin_route, StromSfuGrpcRtcCapability, STROM_CAPABILITY_CONTROL,
    STROM_PLUGIN_ID,
};
pub use use_case_samples::{SendMessageUseCaseExample, StartCallUseCaseExample};
