//! The stored message an operation names (recall, edit, delete, reaction, pin, mark), read before
//! the operation event is sequenced so it can be authorized against the message itself.
use flare_im_contracts::Ctx;
use flare_proto::common::Message;
use flare_server_core::error::Result;

#[async_trait::async_trait]
pub trait MessageTargetRepository: Send + Sync {
    /// The message whose server id is `server_msg_id` in `tenant_id`, as stored; `None` when there
    /// is none (yet: the writer stores a message shortly after it is delivered). An error is an
    /// outage and is answered retryable.
    async fn message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> Result<Option<Message>>;
}
