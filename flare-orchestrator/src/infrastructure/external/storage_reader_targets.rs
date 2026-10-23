//! [`MessageTargetRepository`] over Storage Reader, read as the Orchestrator itself: a Cell-signed
//! `Service` context for the event's tenant, which Reader admits for these lookups only
//! (`MESSAGE_OPERATION_READERS`). One message is `GetMessage`; a batch whose conversations are all
//! that is needed (a read receipt's messages) is one `StorageMessageLookupService.LocateMessages`.
//! Nothing the caller sent names the identity of the read.
use std::collections::HashMap;
use std::time::Duration;

use flare_grpc_proto::storage::storage_message_lookup_service_client::StorageMessageLookupServiceClient;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{GetMessageRequest, LocateMessagesRequest};
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

impl StorageReaderMessageTargets {
    /// `payload` as a request signed as the Orchestrator for `tenant_id`.
    fn signed_request<T>(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        payload: T,
    ) -> Result<tonic::Request<T>> {
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
        let mut request = tonic::Request::new(payload);
        request.set_timeout(READ_TIMEOUT);
        flare_server_core::set_signed_context_token(&mut request, &token)
            .map_err(|_| unavailable("the read context could not be encoded"))?;
        Ok(request)
    }
}

#[async_trait::async_trait]
impl MessageTargetRepository for StorageReaderMessageTargets {
    async fn message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> Result<Option<Message>> {
        let request = self.signed_request(
            ctx,
            tenant_id,
            GetMessageRequest {
                message_id: server_msg_id.to_owned(),
            },
        )?;
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

    async fn conversations_of(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        server_msg_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        if server_msg_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let request = self.signed_request(
            ctx,
            tenant_id,
            LocateMessagesRequest {
                server_msg_ids: server_msg_ids.to_vec(),
            },
        )?;
        let mut client = StorageMessageLookupServiceClient::new(self.channel.clone());
        match tokio::time::timeout(READ_TIMEOUT, client.locate_messages(request)).await {
            Ok(Ok(response)) => Ok(response
                .into_inner()
                .messages
                .into_iter()
                .map(|located| (located.server_msg_id, located.conversation_id))
                .collect()),
            Ok(Err(status)) => {
                tracing::warn!(code = ?status.code(), "message location lookup failed");
                Err(unavailable("storage reader refused or failed the lookup"))
            }
            Err(_) => Err(unavailable("storage reader did not answer in time")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use flare_grpc_proto::storage::storage_message_lookup_service_server::{
        StorageMessageLookupService, StorageMessageLookupServiceServer,
    };
    use flare_grpc_proto::storage::{LocateMessagesResponse, LocatedMessage};

    use super::*;

    /// Who asked (actor type, actor, tenant) for which ids.
    type Asked = (
        flare_server_core::SignedActorType,
        String,
        String,
        Vec<String>,
    );

    /// A lookup service that records who asked for which ids and locates `m-here` and `m-there`.
    #[derive(Clone, Default)]
    struct FakeLookup {
        seen: Arc<Mutex<Vec<Asked>>>,
    }

    #[tonic::async_trait]
    impl StorageMessageLookupService for FakeLookup {
        async fn locate_messages(
            &self,
            request: tonic::Request<LocateMessagesRequest>,
        ) -> std::result::Result<tonic::Response<LocateMessagesResponse>, tonic::Status> {
            let claims = request
                .extensions()
                .get::<Ctx>()
                .and_then(|ctx| ctx.get_data::<flare_server_core::VerifiedContext>())
                .and_then(flare_server_core::VerifiedContext::claims)
                .expect("the boundary only admits verified calls")
                .clone();
            let ids = request.into_inner().server_msg_ids;
            self.seen.lock().unwrap().push((
                claims.actor_type,
                claims.actor_id,
                claims.tenant_id,
                ids.clone(),
            ));
            if ids.iter().any(|id| id == "broken") {
                return Err(tonic::Status::unavailable("down"));
            }
            Ok(tonic::Response::new(LocateMessagesResponse {
                messages: ids
                    .into_iter()
                    .filter_map(|id| {
                        let conversation = match id.as_str() {
                            "m-here" => "2-here",
                            "m-there" => "2-there",
                            _ => return None,
                        };
                        Some(LocatedMessage {
                            server_msg_id: id,
                            conversation_id: conversation.into(),
                        })
                    })
                    .collect(),
            }))
        }
    }

    /// A receipt's messages are located in one call signed as the Orchestrator for the event's
    /// tenant; ids not stored are absent; an outage is retryable; nothing is asked for no ids.
    #[tokio::test]
    async fn a_batch_is_located_in_one_signed_call() {
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        let key = ed25519_dalek::SigningKey::from_bytes(&[43; 32]);
        let signer =
            SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
                .unwrap();
        let verifier = flare_server_core::SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let fake = FakeLookup::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = flare_server_core::SignedFirstContextLayer::new(Some(verifier))
            .require_signed(true)
            .layer(StorageMessageLookupServiceServer::new(fake.clone()));
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let targets = StorageReaderMessageTargets::new(channel, Some(signer));
        let caller: Ctx = Arc::new(
            flare_server_core::Context::with_request_id("req")
                .with_tenant_id("tenant-a")
                .with_user_id("mallory"),
        );
        let ids = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();

        let located = targets
            .conversations_of(&caller, "tenant-a", &ids(&["m-here", "m-there", "ghost"]))
            .await
            .unwrap();
        assert_eq!(
            located,
            HashMap::from([
                ("m-here".to_string(), "2-here".to_string()),
                ("m-there".to_string(), "2-there".to_string()),
            ])
        );
        let error = targets
            .conversations_of(&caller, "tenant-a", &ids(&["broken"]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert!(
            targets
                .conversations_of(&caller, "tenant-a", &[])
                .await
                .unwrap()
                .is_empty()
        );

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "one call per batch, none for no ids");
        assert_eq!(seen[0].3, ids(&["m-here", "m-there", "ghost"]));
        for (actor_type, actor, tenant, _) in &seen {
            assert_eq!(*actor_type, flare_server_core::SignedActorType::Service);
            assert_eq!(actor, flare_im_contracts::service_names::ORCHESTRATOR);
            assert_eq!(tenant, "tenant-a");
        }

        let unsigned = StorageReaderMessageTargets::new(
            Channel::from_static("http://127.0.0.1:9").connect_lazy(),
            None,
        );
        let error = unsigned
            .conversations_of(&caller, "tenant-a", &ids(&["m-here"]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    }
}
