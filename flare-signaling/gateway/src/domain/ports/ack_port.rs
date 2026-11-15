//! 客户端 ACK 上报端口（Command 侧）
//!
//! 送达确认（`ConversationAck`）不在这里：它只在网关内消化，见
//! [`crate::domain::service::SendAckDomainService::absorb_delivery_acks`]。

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_proto::common::{AckBatch, PushAck, ReadAck};
use flare_server_core::error::Result;

#[async_trait]
pub trait IAckReportPort: Send + Sync {
    async fn report_push_ack(&self, tx: &Ctx, ack: PushAck) -> Result<()>;

    async fn report_read_ack(&self, tx: &Ctx, ack: ReadAck) -> Result<()>;

    async fn report_ack_batch(&self, tx: &Ctx, ack: AckBatch) -> Result<()>;
}
