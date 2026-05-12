//! 客户端 ACK 上报端口（Command 侧）

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_im_core::error::Result;
use flare_proto::common::{AckBatch, ConversationAck, PushAck};

#[async_trait]
pub trait IAckReportPort: Send + Sync {
    async fn report_push_ack(&self, tx: &Ctx, ack: PushAck) -> Result<()>;

    async fn report_conversation_ack(&self, tx: &Ctx, ack: ConversationAck) -> Result<()>;

    async fn report_ack_batch(&self, tx: &Ctx, ack: AckBatch) -> Result<()>;
}
