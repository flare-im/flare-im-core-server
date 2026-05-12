//! 消息上行端口（Command 侧）：网关 → 消息路由 / 写入链路

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_im_core::error::Result;
use flare_proto::common::{Message, SendAck};

#[async_trait]
pub trait IMessageCommandPort: Send + Sync {
    async fn send_message(&self, tx: &Ctx, message: Message) -> Result<SendAck>;
}
