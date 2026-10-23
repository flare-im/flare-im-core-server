//! The internal event entry for plugin shared state: Orchestrator `MessageEventService.ExecuteEvent`
//! (the same `handle_event` recalls and reactions use). Orchestrator allocates the conversation seq,
//! fans the event out (single chats per member, groups by conversation broadcast) and hands it to
//! Storage Writer.
//!
//! The call carries Capability's own signed context and nothing else: a `System` actor named
//! [`STATE_PUBLISHER_ID`] with the single role [`STATE_PUBLISH_ROLE`], signed with Capability's key
//! (`kid=capability`, never resolved by a legacy Cell key). Orchestrator accepts shared state from
//! that context only, never from unsigned `x-actor-type` metadata.
use crate::domain::repository::plugin_state::PluginStateEventSink;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_grpc_proto::message::ExecuteEventRequest;
use flare_grpc_proto::message::message_event_service_client::MessageEventServiceClient;
use flare_im_service_kit::plugin_platform::{STATE_PUBLISH_ROLE, STATE_PUBLISHER_ID};
use flare_proto::common::{Event, EventType, PluginStateEvent, event::Payload};
use flare_server_core::error::{FlareError, Result};
use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
use std::sync::Arc;
use std::time::Duration;

/// Actor id of the system identity on these events.
pub const PLUGIN_STATE_ACTOR: &str = STATE_PUBLISHER_ID;
/// Bound of one `ExecuteEvent`.
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub struct GrpcPluginStateEvents {
    /// `http(s)://` or `discovery://` authority of Orchestrator.
    pub authority: String,
    /// Capability's own context key (`FLARE_CAPABILITY_CTX_PRIVATE_KEY`, `kid=capability`).
    pub signer: Arc<SignedContextSigner>,
}

/// The event for one bound card. `event_id` is fresh per attempt; the seq Orchestrator assigns is
/// the identity downstream.
pub fn plugin_state_event(conversation_id: &str, state: PluginStateEvent) -> Event {
    Event {
        conversation_id: conversation_id.to_owned(),
        conversation_seq: 0,
        r#type: EventType::EventMessagePluginState as i32,
        created_at: chrono::Utc::now().timestamp_millis(),
        event_id: uuid::Uuid::new_v4().to_string(),
        request_id: None,
        payload: Some(Payload::PluginState(state)),
    }
}

/// The signed identity of one publish: the tenant, a fresh request id and the publisher.
pub fn publisher_identity(tenant_id: &str) -> SignedContextIdentity {
    SignedContextIdentity::new(
        SignedActorType::System,
        PLUGIN_STATE_ACTOR,
        tenant_id,
        format!("plugin-state-{}", uuid::Uuid::new_v4()),
    )
    .with_roles(vec![STATE_PUBLISH_ROLE.into()])
}

#[async_trait::async_trait]
impl PluginStateEventSink for GrpcPluginStateEvents {
    async fn publish(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        event: PluginStateEvent,
    ) -> Result<()> {
        let token = self
            .signer
            .sign(&publisher_identity(tenant_id))
            .map_err(|_| FlareError::system("plugin state context could not be signed"))?;
        let channel = resolve_plugin_channel(&self.authority)
            .await
            .map_err(|error| FlareError::system(format!("orchestrator unavailable: {error}")))?;
        let mut request = tonic::Request::new(ExecuteEventRequest {
            svid: String::new(),
            event: Some(plugin_state_event(conversation_id, event)),
        });
        request.set_timeout(CALL_TIMEOUT);
        flare_server_core::set_signed_context_token(&mut request, &token)
            .map_err(|_| FlareError::system("plugin state context could not be encoded"))?;
        MessageEventServiceClient::new(channel)
            .execute_event(request)
            .await
            .map_err(|status| {
                FlareError::system(format!("orchestrator refused plugin state: {status}"))
            })?;
        Ok(())
    }
}

/// The context Orchestrator builds from a publish signed with a throwaway Capability key: for
/// tests that drive Orchestrator's event handler in process.
#[cfg(test)]
pub(crate) fn verified_publisher_context(tenant_id: &str) -> flare_server_core::context::Ctx {
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::plugin_platform::CARD_POST_KID;
    let key = ed25519_dalek::SigningKey::from_bytes(&[37; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap()
            .with_kid(CARD_POST_KID)
            .unwrap();
    let mut keys = flare_server_core::auth::KeySet::empty();
    keys.insert(
        CARD_POST_KID,
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap();
    let token = signer.sign(&publisher_identity(tenant_id)).unwrap();
    let mut carrier = tonic::Request::new(());
    carrier
        .metadata_mut()
        .insert("x-flare-ctx", token.parse().unwrap());
    flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
        carrier.metadata(),
        &flare_server_core::SignedContextVerifier::from_key_set(keys),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_event_is_a_plugin_state_event_of_the_system() {
        let event = plugin_state_event(
            "2-group",
            PluginStateEvent {
                server_msg_id: "m-1".into(),
                plugin_id: "example.poll".into(),
                entity: "poll-1".into(),
                version: 2,
                values_json: "{}".into(),
            },
        );
        assert_eq!(event.r#type, EventType::EventMessagePluginState as i32);
        assert_eq!(event.conversation_id, "2-group");
        assert_eq!(event.conversation_seq, 0, "Orchestrator assigns the seq");
        assert!(!event.event_id.is_empty());
        assert!(matches!(event.payload, Some(Payload::PluginState(ref s)) if s.version == 2));
        let identity = publisher_identity("app-a");
        assert_eq!(identity.tenant_id, "app-a");
        assert_eq!(identity.actor_id, PLUGIN_STATE_ACTOR);
        assert_eq!(identity.actor_type, SignedActorType::System);
        assert_eq!(identity.roles, vec![STATE_PUBLISH_ROLE.to_string()]);
    }
}
