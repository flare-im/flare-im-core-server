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

/// Decoding bound of an IM read. Participants arrive with attributes the plugin
/// views drop, so a page of [`MAX_PARTICIPANTS_PAGE`] needs more than the
/// plugin-facing [`MAX_RESOURCE_BYTES`].
const IM_RESPONSE_BYTES: usize = 8 * MAX_RESOURCE_BYTES;
/// `ListConversationParticipantsRequest.ext` key of Conversation's member lookup by id
/// (`flare-conversation` `PARTICIPANT_MEMBERSHIP_EXT_KEY`): comma-separated user ids,
/// answered with the members among them in one page. A member is a participant who has
/// not left; one who hid the conversation from their own list still is, so an operation a
/// plugin gates on membership does not turn them away.
const PARTICIPANT_MEMBERSHIP_EXT_KEY: &str = "member_ids";
/// `ListConversationParticipantsRequest.ext` key asking Conversation's participant pages
/// for the same member scope (`flare-conversation` `PARTICIPANT_MEMBERS_SCOPE_EXT_KEY`);
/// without it Conversation pages only members who have not hidden the conversation.
const PARTICIPANT_MEMBERS_SCOPE_EXT_KEY: &str = "members";
/// Pages a members check may walk when Conversation ignores the lookup key and
/// pages every participant instead (an older Conversation during a rollout):
/// 200 000 members at [`MAX_PARTICIPANTS_PAGE`]. Past it the check is denied
/// rather than answered from a partial list.
const MAX_MEMBERS_CHECK_PAGES: usize = 400;

pub struct GrpcResourceAccess {
    pub conversation_authority: String,
    pub social_group_authority: Option<String>,
    pub signer: Arc<SignedContextSigner>,
}
fn denied<T>(_: T) -> CapabilityError {
    CapabilityError::PolicyDenied("resource unavailable or access denied".into())
}
/// Actor id of the reads Capability makes as itself (no user), e.g. for a plugin runtime credential.
pub const CAPABILITY_SERVICE_ACTOR: &str = "flare-capability";

impl GrpcResourceAccess {
    fn request<T>(&self, actor: &ResourceActor, body: T) -> Result<tonic::Request<T>> {
        self.signed(
            &SignedContextIdentity::new(
                SignedActorType::User,
                &actor.user_id,
                &actor.tenant_id,
                &actor.request_id,
            ),
            body,
        )
    }
    fn signed<T>(&self, identity: &SignedContextIdentity, body: T) -> Result<tonic::Request<T>> {
        let token = self.signer.sign(identity).map_err(denied)?;
        let mut request = tonic::Request::new(body);
        set_signed_context_token(&mut request, &token).map_err(denied)?;
        Ok(request)
    }
}
/// The offset a participants cursor names: empty is the first page.
fn participant_offset(cursor: &str) -> Result<u64> {
    if cursor.is_empty() {
        return Ok(0);
    }
    // Digits only (validated); 128 of them can still overflow.
    cursor.parse::<u64>().map_err(denied)
}
impl GrpcResourceAccess {
    /// Which of `user_ids` are members of the ticket's conversation (not left; hiding the
    /// conversation does not matter).
    /// Conversation answers the lookup key in one page; an older Conversation
    /// pages every participant, which is walked until each id is found or the
    /// list ends, so the answer is the same either way.
    async fn active_members(
        &self,
        im: &mut ConversationReadServiceClient<tonic::transport::Channel>,
        identity: &SignedContextIdentity,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>> {
        let wanted: std::collections::BTreeSet<&str> =
            user_ids.iter().map(String::as_str).collect();
        let mut found = std::collections::BTreeSet::new();
        let mut cursor = String::new();
        for _ in 0..MAX_MEMBERS_CHECK_PAGES {
            let page = im
                .list_conversation_participants(
                    self.signed(
                        identity,
                        ListConversationParticipantsRequest {
                            conversation_id: conversation_id.to_owned(),
                            cursor: cursor.clone(),
                            limit: MAX_PARTICIPANTS_PAGE as i32,
                            include_removed: false,
                            ext: [
                                ("skip_meta".to_string(), "1".to_string()),
                                (
                                    PARTICIPANT_MEMBERS_SCOPE_EXT_KEY.to_string(),
                                    "1".to_string(),
                                ),
                                (
                                    PARTICIPANT_MEMBERSHIP_EXT_KEY.to_string(),
                                    user_ids.join(","),
                                ),
                            ]
                            .into(),
                        },
                    )?,
                )
                .await
                .map_err(denied)?
                .into_inner();
            if page.conversation_id != conversation_id
                || page.participants.len() > MAX_PARTICIPANTS_PAGE as usize
            {
                return Err(denied(()));
            }
            found.extend(
                page.participants
                    .into_iter()
                    .map(|p| p.user_id)
                    .filter(|id| wanted.contains(id.as_str())),
            );
            if !page.has_more || found.len() == wanted.len() {
                return Ok(found);
            }
            if page.next_cursor.is_empty() || page.next_cursor == cursor {
                return Err(denied(()));
            }
            cursor = page.next_cursor;
        }
        Err(denied(()))
    }
}
#[async_trait::async_trait]
impl ResourceAccessBackend for GrpcResourceAccess {
    async fn read(&self, actor: &ResourceActor, request: &ResourceRequest) -> Result<Value> {
        let channel = resolve_plugin_channel(&self.conversation_authority)
            .await
            .map_err(denied)?;
        let mut im = ConversationReadServiceClient::new(channel)
            .max_decoding_message_size(IM_RESPONSE_BYTES);
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
                // The plugin cursor is the decimal offset of the next participant
                // (the protocol allows digits only); Conversation reads a plain
                // number as an offset in its (joined_at, user_id) order, while its
                // own keyset cursor never reaches the plugin.
                let offset = participant_offset(cursor)?;
                let page = im
                    .list_conversation_participants(
                        self.request(
                            actor,
                            ListConversationParticipantsRequest {
                                conversation_id: actor.conversation_id.clone(),
                                cursor: if offset == 0 {
                                    String::new()
                                } else {
                                    offset.to_string()
                                },
                                limit: *limit as i32,
                                include_removed: false,
                                ext: [
                                    ("skip_meta".to_string(), "1".to_string()),
                                    (
                                        PARTICIPANT_MEMBERS_SCOPE_EXT_KEY.to_string(),
                                        "1".to_string(),
                                    ),
                                ]
                                .into(),
                            },
                        )?,
                    )
                    .await
                    .map_err(denied)?
                    .into_inner();
                if page.conversation_id != actor.conversation_id
                    || page.participants.len() > *limit as usize
                    || (page.has_more && page.participants.is_empty())
                {
                    return Err(denied(()));
                }
                let next_cursor = if page.has_more {
                    offset
                        .checked_add(page.participants.len() as u64)
                        .ok_or_else(|| denied(()))?
                        .to_string()
                } else {
                    String::new()
                };
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
                    next_cursor,
                    has_more: page.has_more,
                })
                .map_err(denied)
            }
            ResourceRequest::ConversationMembersCheck { user_ids } => {
                let identity = SignedContextIdentity::new(
                    SignedActorType::User,
                    &actor.user_id,
                    &actor.tenant_id,
                    &actor.request_id,
                );
                let members = self
                    .active_members(&mut im, &identity, &actor.conversation_id, user_ids)
                    .await?;
                serde_json::to_value(MembersCheckView::partition(user_ids, |id| {
                    members.contains(id)
                }))
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

    async fn tenant_members(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        request_id: &str,
        user_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>> {
        let channel = resolve_plugin_channel(&self.conversation_authority)
            .await
            .map_err(denied)?;
        let mut im = ConversationReadServiceClient::new(channel)
            .max_decoding_message_size(IM_RESPONSE_BYTES);
        let identity = SignedContextIdentity::new(
            SignedActorType::Service,
            CAPABILITY_SERVICE_ACTOR,
            tenant_id,
            request_id,
        );
        self.active_members(&mut im, &identity, conversation_id, user_ids)
            .await
    }
}
