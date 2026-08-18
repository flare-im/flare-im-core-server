//! 插件发现与 RTC 后端路由。

mod plugin_route_book;
mod rtc_dispatch_route;
mod rtc_router;

pub use plugin_route_book::PluginRouteBook;
pub use rtc_dispatch_route::{DEFAULT_RTC_CAPABILITY_PREFIX, RtcDispatchRoute};
pub use rtc_router::RtcCapabilityRouter;
