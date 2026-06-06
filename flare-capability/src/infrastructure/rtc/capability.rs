//! RTC 能力类型与 **领域端口再导出**。
//!
//! **说明**：`RtcCapability` 的权威定义在 [`crate::domain::capability::ports`]；
//! 此处再导出以便 `crate::rtc::*` 单入口阅读；编排实现见 [`super::manager::CapabilityManager`]。

pub use crate::domain::capability::ports::RtcCapability;

use serde::{Deserialize, Serialize};
use std::fmt;

/// 能力大类（编排 / 选路用，不等同于 capability_id 字符串）。
///
/// 使用**开放字符串**标识，避免在核心枚举里硬编码具体实现名；部署方可自定义取值
/// （例如 `in_proc_sfu`、`remote_grpc`、`vendor_x`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[derive(Default)]
pub struct CapabilityKind(String);

impl CapabilityKind {
    pub fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// 进程内媒体后端（由部署装配的具体 `SfuPlugin` / 等价实现决定）。
    pub fn in_proc_sfu() -> Self {
        Self("in_proc_sfu".to_string())
    }

    /// 独立进程控制面（由部署装配的 gRPC 控制面服务决定）。
    pub fn remote_control_plane() -> Self {
        Self("remote_control_plane".to_string())
    }

    /// 预留：任意自定义实现 id。
    pub fn custom(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for CapabilityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
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
