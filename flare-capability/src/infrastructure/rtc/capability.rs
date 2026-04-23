//! RTC 能力类型与 **领域端口再导出**。
//!
//! **说明**：`RtcCapability` 的权威定义在 [`crate::domain::capability::ports`]；
//! 此处再导出以便 `crate::rtc::*` 单入口阅读；编排实现见 [`super::manager::CapabilityManager`]。

pub use crate::domain::capability::ports::RtcCapability;

use serde::{Deserialize, Serialize};

/// 能力大类（编排 / 选路用，不等同于 capability_id 字符串）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    /// 进程内 `flare-sfu` 适配（现有路径）。
    FlareSfuInProc,
    /// 独立插件进程 `flare-strom-sfu`（gRPC `sfu_control.proto`）。
    StromSfuPlugin,
    /// 预留：其他 SFU 实现。
    CustomSfu,
}

/// 控制面定位信息（第一版仅数据结构；后续接 tonic 与配置中心）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RtcBackendDescriptor {
    pub instance_id: String,
    pub grpc_endpoint: Option<String>,
    pub version: Option<String>,
    pub draining: bool,
    pub disabled: bool,
}
