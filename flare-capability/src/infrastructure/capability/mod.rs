//! 能力扩展基础设施：注册表、运行时、SFU 适配、内外部适配器。

pub mod adapters;
pub mod builtin;
pub mod dispatch_rate_limit;
pub mod grants;
pub mod pre_send_runtime;
pub mod recipient_runtime;
pub mod registry;
pub mod rtc_router;
pub mod sfu_rtc_capability;

pub use dispatch_rate_limit::DispatchRateLimiter;
pub use grants::InMemoryCapabilityGrants;
pub use pre_send_runtime::PreSendGuardRuntime;
pub use recipient_runtime::RecipientResolverRuntime;
pub use registry::{CapabilityExtensionRegistry, RegistryInner};
pub use rtc_router::RtcCapabilityRouter;
pub use sfu_rtc_capability::SfuRtcCapability;
