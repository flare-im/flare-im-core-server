//! The platform card path to Message Ingest (plugin platform v2 §1).
//!
//! The route of the ticket's conversation is read from Conversation as the ticket's user (an
//! active member, or the read is refused): the conversation type comes from the CID prefix and is
//! checked against the detail, the channel is the group of a group chat or the peer of a single
//! chat. The card is then sent with `MessageSendService.SendMessage` under Capability's own context
//! key (`kid=capability`): the user's identity, the single role `plugin.card.post` and the request
//! id that binds the context to this exact post ([`plugin_platform::card_post_request_id`]).
use crate::domain::repository::plugin_card::{
    CardPostError, CardRoute, PluginCardDelivery, PostedCard,
};
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_grpc_proto::conversation::{
    GetConversationDetailRequest, ListConversationParticipantsRequest,
    conversation_read_service_client::ConversationReadServiceClient,
};
use flare_grpc_proto::message::SendMessageRequest;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_im_service_kit::plugin_platform::{CARD_POST_ROLE, card_post_request_id};
use flare_proto::common::{ConversationType, Message};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use std::sync::Arc;
use std::time::Duration;

/// Bound of one Conversation read or Ingest send.
const CALL_TIMEOUT: Duration = Duration::from_secs(2);

pub struct GrpcPluginCardDelivery {
    /// Message Ingest `MessageSendService` (`http(s)://` or `discovery://`).
    pub ingest_authority: String,
    /// Conversation `ConversationReadService`.
    pub conversation_authority: String,
    /// Capability's card post key (`kid=capability`).
    pub post_signer: Arc<SignedContextSigner>,
    /// The Cell signer of the user-scoped Conversation read (the resource bridge's).
    pub read_signer: Arc<SignedContextSigner>,
}

/// The CID prefix kinds a plugin card can be posted to.
pub fn conversation_kind(conversation_id: &str) -> Option<ConversationType> {
    match conversation_id.as_bytes().first() {
        Some(b'1') => Some(ConversationType::Single),
        Some(b'2') => Some(ConversationType::Group),
        _ => None,
    }
}

/// Whether a failed call may be retried: only availability and deadlines are; a refusal
/// (permission, membership, invalid input, PreSend policy) is final.
fn classify(status: &tonic::Status) -> CardPostError {
    use tonic::Code;
    let reason = flare_server_core::error::grpc::decode_error_detail(status)
        .map(|detail| detail.reason)
        .filter(|reason| !reason.is_empty())
        .unwrap_or_else(|| status.message().chars().take(200).collect());
    match status.code() {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::Aborted
        | Code::ResourceExhausted
        | Code::Unknown
        | Code::Internal
        | Code::Cancelled => CardPostError::Unavailable(reason),
        _ => CardPostError::Refused(reason),
    }
}

fn unavailable(error: impl std::fmt::Display) -> CardPostError {
    CardPostError::Unavailable(error.to_string().chars().take(200).collect())
}

impl GrpcPluginCardDelivery {
    fn read<T>(
        &self,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
        body: T,
    ) -> Result<tonic::Request<T>, CardPostError> {
        let token = self
            .read_signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                user_id,
                tenant_id,
                request_id,
            ))
            .map_err(unavailable)?;
        let mut request = tonic::Request::new(body);
        request.set_timeout(CALL_TIMEOUT);
        set_signed_context_token(&mut request, &token).map_err(unavailable)?;
        Ok(request)
    }
}

#[async_trait::async_trait]
impl PluginCardDelivery for GrpcPluginCardDelivery {
    async fn route(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        request_id: &str,
    ) -> Result<CardRoute, CardPostError> {
        let kind = conversation_kind(conversation_id).ok_or_else(|| {
            CardPostError::Refused("plugin cards go to single and group chats only".into())
        })?;
        let channel = resolve_plugin_channel(&self.conversation_authority)
            .await
            .map_err(unavailable)?;
        let mut conversations = ConversationReadServiceClient::new(channel);
        let detail = conversations
            .get_conversation_detail(self.read(
                tenant_id,
                user_id,
                request_id,
                GetConversationDetailRequest {
                    conversation_id: conversation_id.into(),
                },
            )?)
            .await
            .map_err(|status| classify(&status))?
            .into_inner()
            .detail
            .ok_or_else(|| CardPostError::Refused("conversation not found".into()))?;
        let expected = match kind {
            ConversationType::Single => "single",
            _ => "group",
        };
        if detail.conversation_id != conversation_id || detail.conversation_type != expected {
            return Err(CardPostError::Refused(
                "conversation routing mismatch".into(),
            ));
        }
        let channel_id = match kind {
            ConversationType::Group => detail.channel_id,
            _ => {
                let page = conversations
                    .list_conversation_participants(self.read(
                        tenant_id,
                        user_id,
                        request_id,
                        ListConversationParticipantsRequest {
                            conversation_id: conversation_id.into(),
                            cursor: String::new(),
                            limit: 8,
                            include_removed: false,
                            ext: [("skip_meta".to_string(), "1".to_string())].into(),
                        },
                    )?)
                    .await
                    .map_err(|status| classify(&status))?
                    .into_inner();
                let peers: Vec<String> = page
                    .participants
                    .into_iter()
                    .map(|p| p.user_id)
                    .filter(|id| id != user_id)
                    .collect();
                match peers.as_slice() {
                    [peer] => peer.clone(),
                    _ => {
                        return Err(CardPostError::Refused(
                            "single chat peer not resolved".into(),
                        ));
                    }
                }
            }
        };
        if channel_id.is_empty() || channel_id.trim() != channel_id {
            return Err(CardPostError::Refused(
                "conversation channel missing".into(),
            ));
        }
        Ok(CardRoute {
            conversation_type: kind as i32,
            channel_id,
        })
    }

    async fn post(&self, tenant_id: &str, message: Message) -> Result<PostedCard, CardPostError> {
        let request_id = card_post_request_id(tenant_id, &message)
            .ok_or_else(|| CardPostError::Refused("not a plugin card".into()))?;
        let token = self
            .post_signer
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::User,
                    &message.sender_id,
                    tenant_id,
                    &request_id,
                )
                .with_roles(vec![CARD_POST_ROLE.into()]),
            )
            .map_err(unavailable)?;
        let channel = resolve_plugin_channel(&self.ingest_authority)
            .await
            .map_err(unavailable)?;
        let conversation_id = message.conversation_id.clone();
        let mut request = tonic::Request::new(SendMessageRequest {
            conversation_id,
            message: Some(message),
            sync: false,
            svid: String::new(),
        });
        request.set_timeout(CALL_TIMEOUT);
        set_signed_context_token(&mut request, &token).map_err(unavailable)?;
        let response = MessageSendServiceClient::new(channel)
            .send_message(request)
            .await
            .map_err(|status| classify(&status))?
            .into_inner();
        if !response.success || response.server_msg_id.is_empty() {
            return Err(CardPostError::Unavailable(
                "ingest answered without a message id".into(),
            ));
        }
        Ok(PostedCard {
            server_msg_id: response.server_msg_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_cards_route_to_single_and_group_chats_only() {
        assert_eq!(
            conversation_kind("1ADW0CVC4ESXQ0046R"),
            Some(ConversationType::Single)
        );
        assert_eq!(
            conversation_kind("2AW1QQ2SKVWFEPJRXN"),
            Some(ConversationType::Group)
        );
        for other in ["", "3AAI", "4ASYS", "7CHANNEL", "sync:alice"] {
            assert_eq!(conversation_kind(other), None, "{other}");
        }
    }

    #[test]
    fn only_availability_and_deadlines_are_retried() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Internal,
        ] {
            assert!(matches!(
                classify(&tonic::Status::new(code, "x")),
                CardPostError::Unavailable(_)
            ));
        }
        for code in [
            tonic::Code::PermissionDenied,
            tonic::Code::FailedPrecondition,
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::Unauthenticated,
        ] {
            assert!(matches!(
                classify(&tonic::Status::new(code, "x")),
                CardPostError::Refused(_)
            ));
        }
    }
}
