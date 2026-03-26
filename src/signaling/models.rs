//! 信令服务共享数据模型（原 flare-signaling/common）
//!
//! 路由、在线状态、连接等信令层通用类型，与 proto 解耦。

use serde::{Deserialize, Serialize};

/// 路由信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteInfo {
    /// 服务ID (SVID)
    pub svid: String,
    /// 服务端点
    pub endpoint: String,
    /// 服务实例ID
    pub instance_id: Option<String>,
    /// 健康状态
    pub healthy: bool,
    /// 权重 (用于负载均衡)
    pub weight: i32,
}

/// 在线状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineStatusInfo {
    /// 用户ID
    pub user_id: String,
    /// 是否在线
    pub online: bool,
    /// 网关ID
    pub gateway_id: Option<String>,
    /// 服务器ID
    pub server_id: Option<String>,
    /// 设备ID
    pub device_id: Option<String>,
    /// 最后活跃时间 (毫秒时间戳)
    pub last_seen: i64,
}

/// 连接信息（信令层：user/device/gateway 等）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// 连接ID
    pub connection_id: String,
    /// 用户ID
    pub user_id: String,
    /// 设备ID
    pub device_id: String,
    /// 网关ID
    pub gateway_id: String,
    /// 连接时间 (毫秒时间戳)
    pub connected_at: i64,
    /// 最后活跃时间 (毫秒时间戳)
    pub last_active_at: i64,
}
