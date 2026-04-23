//! **flare-strom-sfu**：独立进程 SFU 控制面客户端与 `PluginRouteBook` 登记。

mod strom_sfu_grpc_rtc_capability;
mod strom_sfu_plugin_route;

pub use strom_sfu_grpc_rtc_capability::StromSfuGrpcRtcCapability;
pub use strom_sfu_plugin_route::{
    register_strom_sfu_plugin_route, STROM_CAPABILITY_CONTROL, STROM_PLUGIN_ID,
};
