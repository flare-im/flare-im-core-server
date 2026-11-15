//! 设备交接上报端口
//!
//! 网关失联后，清扫器移除它名下的会话；这些设备的新消息提示从链路转交给推送通道，推送侧据此
//! 补一次提示。Online 只上报「网关失联」这一种交接：正常断开、登出、踢线与客户端主动托管都由
//! 持有连接的网关自己上报。

use flare_server_core::error::Result;

/// 一台设备因所在网关失联而交接给推送通道。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayLostHandoff {
    pub tenant_id: String,
    pub user_id: String,
    pub device_id: String,
    /// 被清扫的会话 ID（登录时返回给网关的那个）。
    pub session_id: String,
    /// 失联的网关（真实网关 ID；重启遗留的孤儿集合记在原网关名下）。
    pub gateway_id: String,
    /// 这条链路最早可能从何时起收不到投递（Unix 毫秒）：推送侧从这里起找有新消息的会话。
    pub since_ms: i64,
    /// 清扫器移除会话的时刻（Unix 毫秒）。
    pub occurred_at_ms: i64,
}

/// 把交接送到推送通道的入口。
pub trait DeviceHandoffReporter: Send + Sync {
    /// 上报一次交接。推送侧明确收下才算成功；不可达、被拒绝都返回错误，由调用方计数。
    async fn report(&self, handoff: &GatewayLostHandoff) -> Result<()>;
}
