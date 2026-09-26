//! 会话成员名单来源（读侧）：统一读扩散投递时，按会话解析在册成员以订阅其在线连接。
//!
//! 生产实现是会话服务的 gRPC 读池；抽成端口是为了让投递路径的订阅对账能在单测里被驱动。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

#[async_trait]
pub trait ConversationParticipantSource: Send + Sync {
    /// 会话当前在册成员的 user_id（不含已移出的成员）。
    async fn list_participants(&self, tx: &Ctx, conversation_id: &str) -> Result<Vec<String>>;
}
