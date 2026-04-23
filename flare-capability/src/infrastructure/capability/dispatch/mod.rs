//! 消息分发链路上的 Guard / Resolver 运行时与限流。

mod dispatch_rate_limit;
mod pre_send_runtime;
mod recipient_runtime;

pub use dispatch_rate_limit::DispatchRateLimiter;
pub use pre_send_runtime::PreSendGuardRuntime;
pub use recipient_runtime::RecipientResolverRuntime;
