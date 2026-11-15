//! 链路探活端口
//!
//! 送达确认超时后，网关要立刻验证这条连接是不是还活着：发一帧 PING，等不到回应就关掉。
//! 基础设施实现见 [`crate::infrastructure::ports::ServerHandleLinkProbe`]。

use async_trait::async_trait;
use flare_server_core::error::Result;

#[async_trait]
pub trait ILinkProbePort: Send + Sync {
    /// 向连接写一帧 PING。
    async fn ping(&self, connection_id: &str) -> Result<()>;

    /// 关闭连接：移出连接表并关掉底层传输，随后的断开事件照常走注销路径
    /// （清订阅、注销 Online 会话、结束送达确认跟踪）。
    async fn close(&self, connection_id: &str) -> Result<()>;
}
