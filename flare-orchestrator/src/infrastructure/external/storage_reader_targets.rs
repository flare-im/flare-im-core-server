//! [`MessageTargetRepository`] over Storage Reader `GetMessage`, read as the Orchestrator itself: a
//! Cell-signed `Service` context for the event's tenant, which Reader admits for this lookup only
//! (`MESSAGE_OPERATION_READERS`). Nothing the caller sent names the identity of the read.
use std::time::Duration;

use flare_grpc_proto::storage::GetMessageRequest;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_im_contracts::Ctx;
use flare_proto::common::Message;
use flare_server_core::SignedContextSigner;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use tonic::transport::Channel;

use crate::domain::repository::MessageTargetRepository;

/// Bound of one lookup.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

pub struct StorageReaderMessageTargets {
    channel: Channel,
    /// The Cell signer (`FLARE_INTERNAL_CTX_PRIVATE_KEY`); without it every lookup fails, so
    /// operations that need one are refused (retryable), never allowed unchecked.
    signer: Option<SignedContextSigner>,
}

impl StorageReaderMessageTargets {
    pub fn new(channel: Channel, signer: Option<SignedContextSigner>) -> Self {
        Self { channel, signer }
    }
}

fn unavailable(details: &str) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::ServiceUnavailable,
        "message operation target unavailable",
    )
    .details(details)
    .build_error()
}

#[async_trait::async_trait]
impl MessageTargetRepository for StorageReaderMessageTargets {
    async fn message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> Result<Option<Message>> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| unavailable("the orchestrator has no Cell signing key"))?;
        let token = flare_im_service_kit::signed_read::sign_service_read(
            signer,
            flare_im_contracts::service_names::ORCHESTRATOR,
            tenant_id,
            ctx.request_id(),
        )?;
        let mut request = tonic::Request::new(GetMessageRequest {
            message_id: server_msg_id.to_owned(),
        });
        request.set_timeout(READ_TIMEOUT);
        flare_server_core::set_signed_context_token(&mut request, &token)
            .map_err(|_| unavailable("the read context could not be encoded"))?;
        let mut client = StorageReaderServiceClient::new(self.channel.clone());
        match tokio::time::timeout(READ_TIMEOUT, client.get_message(request)).await {
            Ok(Ok(response)) => Ok(response.into_inner().message),
            Ok(Err(status)) if status.code() == tonic::Code::NotFound => Ok(None),
            Ok(Err(status)) => {
                tracing::warn!(code = ?status.code(), "message operation target read failed");
                Err(unavailable("storage reader refused or failed the lookup"))
            }
            Err(_) => Err(unavailable("storage reader did not answer in time")),
        }
    }
}
