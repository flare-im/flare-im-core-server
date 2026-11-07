use flare_core::common::conversation::{
    generate_group_conversation_id, is_group_chat_conversation,
};
use flare_im_service_kit::roster_policy::{check_channels, trusted_internal_reader};
use flare_server_core::{context::Context, error::grpc::IntoGrpc};
use tonic::Status;

use super::ConversationGrpcHandler;
use crate::domain::model::{
    ConversationSummary, ConversationType, EXTERNAL_GROUP_ROSTER_BUSINESS_TYPE,
};

// Legacy rows can carry a missing or mismatched route. A provider answer
// about another group's channel does not authorize this conversation.
fn canonical_group_route(conversation_id: &str, channel_id: &str) -> bool {
    !channel_id.is_empty() && generate_group_conversation_id(channel_id) == conversation_id
}

impl ConversationGrpcHandler {
    pub(super) async fn can_disclose_roster(
        &self,
        ctx: &Context,
        conversation_id: &str,
    ) -> Result<bool, Status> {
        if trusted_internal_reader(ctx) || !is_group_chat_conversation(conversation_id) {
            return Ok(true);
        }
        let Some((channel_id, explicitly_external)) = self
            .query_handler
            .external_roster_channel(ctx, conversation_id, self.roster_policy_endpoint.is_some())
            .await
            .into_grpc()?
        else {
            return Ok(true);
        };
        if !canonical_group_route(conversation_id, &channel_id) {
            return Ok(false);
        }
        let visibility = check_channels(
            self.roster_policy_endpoint.as_deref(),
            ctx,
            std::slice::from_ref(&channel_id),
        )
        .await
        .unwrap_or_default();
        Ok(visibility.allows_full_roster(&channel_id, explicitly_external))
    }

    pub(super) async fn protect_member_previews(
        &self,
        ctx: &Context,
        rows: &mut [ConversationSummary],
    ) {
        if trusted_internal_reader(ctx) {
            return;
        }
        let explicitly_external = |row: &ConversationSummary| {
            row.business_type.as_deref() == Some(EXTERNAL_GROUP_ROSTER_BUSINESS_TYPE)
        };
        let needs_policy_check = |row: &ConversationSummary| {
            explicitly_external(row)
                || (self.roster_policy_endpoint.is_some()
                    && row.conversation_type == ConversationType::Group)
        };
        let system_message = |row: &ConversationSummary| {
            row.last_message_type == Some(flare_proto::common::MessageType::System as i32)
        };
        let mut ids: Vec<_> = rows
            .iter()
            .filter(|row| {
                needs_policy_check(row)
                    && canonical_group_route(&row.conversation_id, &row.channel_id)
                    && (!row.member_preview.is_empty() || system_message(row))
            })
            .map(|row| row.channel_id.clone())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        // Summaries remain available when the policy provider is unreachable.
        // Only an explicit unmanaged answer permits local roster disclosure.
        let visibility = check_channels(self.roster_policy_endpoint.as_deref(), ctx, &ids)
            .await
            .unwrap_or_default();
        for row in rows.iter_mut().filter(|row| {
            needs_policy_check(row) && (!row.member_preview.is_empty() || system_message(row))
        }) {
            if !canonical_group_route(&row.conversation_id, &row.channel_id)
                || !visibility.allows_full_roster(&row.channel_id, explicitly_external(row))
            {
                row.member_preview.clear();
                if system_message(row) {
                    row.last_message_preview = None;
                    row.last_sender_id = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_or_mismatched_route_cannot_use_the_provider() {
        let conversation_id = generate_group_conversation_id("route");
        assert!(canonical_group_route(&conversation_id, "route"));
        assert!(!canonical_group_route(&conversation_id, "other"));
        assert!(!canonical_group_route(&conversation_id, ""));
    }
}
