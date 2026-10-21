//! Public IM APIs plus an internal Social adapter. Core links no Social crate.
use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::resource_access::{ResourceAccessBackend, ResourceActor};
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_grpc_proto::capability::{
    GenericRequest, plugin_resource_backend_client::PluginResourceBackendClient,
};
use flare_grpc_proto::conversation::{
    GetConversationDetailRequest, ListConversationParticipantsRequest,
    conversation_read_service_client::ConversationReadServiceClient,
};
use flare_im_capability_core::plugin::resource::*;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use serde_json::Value;
use std::sync::Arc;

#[cfg(test)]
#[path = "resource_access_tests.rs"]
mod tests;

pub struct GrpcResourceAccess {
    pub conversation_authority: String,
    pub social_group_authority: Option<String>,
    pub signer: Arc<SignedContextSigner>,
}
fn denied<T>(_: T) -> CapabilityError {
    CapabilityError::PolicyDenied("resource unavailable or access denied".into())
}
impl GrpcResourceAccess {
    fn request<T>(&self, actor: &ResourceActor, body: T) -> Result<tonic::Request<T>> {
        let token = self
            .signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                &actor.user_id,
                &actor.tenant_id,
                &actor.request_id,
            ))
            .map_err(denied)?;
        let mut request = tonic::Request::new(body);
        set_signed_context_token(&mut request, &token).map_err(denied)?;
        Ok(request)
    }
}
#[async_trait::async_trait]
impl ResourceAccessBackend for GrpcResourceAccess {
    async fn read(&self, actor: &ResourceActor, request: &ResourceRequest) -> Result<Value> {
        let channel = resolve_plugin_channel(&self.conversation_authority)
            .await
            .map_err(denied)?;
        let mut im = ConversationReadServiceClient::new(channel)
            .max_decoding_message_size(MAX_RESOURCE_BYTES * 2);
        // Existing public query checks active membership and history bounds.
        let detail = im
            .get_conversation_detail(self.request(
                actor,
                GetConversationDetailRequest {
                    conversation_id: actor.conversation_id.clone(),
                },
            )?)
            .await
            .map_err(denied)?
            .into_inner()
            .detail
            .ok_or_else(|| denied(()))?;
        if detail.conversation_id != actor.conversation_id || detail.member_count < 0 {
            return Err(denied(()));
        }
        match request {
            ResourceRequest::ConversationRead {} => serde_json::to_value(ConversationView {
                conversation_id: detail.conversation_id,
                conversation_type: detail.conversation_type,
                display_name: detail.display_name,
                member_count: detail.member_count as u32,
            })
            .map_err(denied),
            ResourceRequest::ConversationParticipantsRead { cursor, limit } => {
                let page = im
                    .list_conversation_participants(self.request(
                        actor,
                        ListConversationParticipantsRequest {
                            conversation_id: actor.conversation_id.clone(),
                            cursor: cursor.clone(),
                            limit: *limit as i32,
                            include_removed: false,
                            ext: Default::default(),
                        },
                    )?)
                    .await
                    .map_err(denied)?
                    .into_inner();
                if page.conversation_id != actor.conversation_id
                    || page.participants.len() > *limit as usize
                {
                    return Err(denied(()));
                }
                serde_json::to_value(ParticipantPage {
                    conversation_id: page.conversation_id,
                    participants: page
                        .participants
                        .into_iter()
                        .map(|p| ParticipantView {
                            user_id: p.user_id,
                            roles: p.roles,
                        })
                        .collect(),
                    next_cursor: page.next_cursor,
                    has_more: page.has_more,
                })
                .map_err(denied)
            }
            ResourceRequest::SocialGroupMemberRead { user_id } => {
                if detail.conversation_type != "group" || !valid_resource_id(&detail.channel_id) {
                    return Err(denied(()));
                }
                let authority = self
                    .social_group_authority
                    .as_ref()
                    .ok_or_else(|| denied(()))?;
                let channel = resolve_plugin_channel(authority).await.map_err(denied)?;
                let payload = serde_json::to_vec(&GroupMemberLookup {
                    group_id: detail.channel_id.clone(),
                    user_id: user_id.clone(),
                })
                .map_err(denied)?;
                let response = PluginResourceBackendClient::new(channel)
                    .max_decoding_message_size(MAX_RESOURCE_BYTES)
                    .call(self.request(
                        actor,
                        GenericRequest {
                            operation: HostCapability::SocialGroupMemberRead.id().into(),
                            request_id: actor.request_id.clone(),
                            payload: Some(prost_types::Any {
                                type_url:
                                    "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                                value: payload,
                            }),
                            ..Default::default()
                        },
                    )?)
                    .await
                    .map_err(denied)?
                    .into_inner();
                if !response.ok || response.request_id != actor.request_id {
                    return Err(denied(()));
                }
                let member: GroupMemberView =
                    serde_json::from_slice(&response.payload.ok_or_else(|| denied(()))?.value)
                        .map_err(denied)?;
                if member.group_id != detail.channel_id || member.user_id != *user_id {
                    return Err(denied(()));
                }
                serde_json::to_value(member).map_err(denied)
            }
        }
    }
}
