//! Live authorization for identity-bearing views of an externally managed group.

use std::{collections::HashSet, time::Duration};

use flare_grpc_proto::conversation::{
    CheckRosterVisibilityRequest,
    conversation_roster_policy_service_client::ConversationRosterPolicyServiceClient,
};
use flare_server_core::{
    SignedActorType, VerifiedContext, context::Context, set_signed_context_token,
};
use tonic::{Request, Status};

use crate::discovery::channel_resolve::resolve_grpc_channel;

pub const ENDPOINT_ENV: &str = "CONVERSATION_ROSTER_POLICY_GRPC_URL";

pub fn endpoint_from_env() -> Option<String> {
    std::env::var(ENDPOINT_ENV)
        .ok()
        .map(|endpoint| endpoint.trim().to_owned())
        .filter(|endpoint| !endpoint.is_empty())
}

pub fn trusted_internal_reader(ctx: &Context) -> bool {
    ctx.get_data::<VerifiedContext>()
        .and_then(VerifiedContext::claims)
        .is_some_and(|claims| {
            matches!(
                claims.actor_type,
                SignedActorType::Service | SignedActorType::System
            )
        })
}

#[derive(Default)]
pub struct RosterVisibility {
    full_roster: HashSet<String>,
    unmanaged: HashSet<String>,
}

impl RosterVisibility {
    pub fn allows_full_roster(&self, channel_id: &str, explicitly_external: bool) -> bool {
        self.full_roster.contains(channel_id)
            || (!explicitly_external && self.unmanaged.contains(channel_id))
    }
}

pub async fn check_channels(
    endpoint: Option<&str>,
    ctx: &Context,
    channel_ids: &[String],
) -> Result<RosterVisibility, Status> {
    if channel_ids.is_empty() {
        return Ok(RosterVisibility::default());
    }
    let token = ctx
        .get_data::<VerifiedContext>()
        .and_then(VerifiedContext::token)
        .ok_or_else(|| Status::permission_denied("verified roster viewer required"))?;
    let endpoint =
        endpoint.ok_or_else(|| Status::unavailable("conversation roster policy unavailable"))?;
    let fetch = async {
        let channel = resolve_grpc_channel(endpoint)
            .await
            .map_err(|_| Status::unavailable("conversation roster policy unavailable"))?;
        let mut client = ConversationRosterPolicyServiceClient::new(channel);
        let mut visibility = RosterVisibility::default();
        for ids in channel_ids.chunks(500) {
            let mut req = Request::new(CheckRosterVisibilityRequest {
                channel_ids: ids.to_vec(),
            });
            set_signed_context_token(&mut req, token)
                .map_err(|_| Status::permission_denied("invalid roster viewer"))?;
            let response = client.check_roster_visibility(req).await?.into_inner();
            visibility.full_roster.extend(
                response
                    .full_roster_channel_ids
                    .into_iter()
                    .filter(|id| ids.contains(id)),
            );
            visibility.unmanaged.extend(
                response
                    .unmanaged_channel_ids
                    .into_iter()
                    .filter(|id| ids.contains(id)),
            );
        }
        Ok(visibility)
    };
    tokio::time::timeout(Duration::from_millis(750), fetch)
        .await
        .map_err(|_| Status::unavailable("conversation roster policy timed out"))?
}

#[cfg(test)]
mod tests {
    use super::RosterVisibility;

    #[test]
    fn explicit_unmanaged_does_not_override_external_ownership() {
        let visibility = RosterVisibility {
            full_roster: ["allowed".into()].into(),
            unmanaged: ["generic".into()].into(),
        };
        assert!(visibility.allows_full_roster("allowed", true));
        assert!(visibility.allows_full_roster("allowed", false));
        assert!(visibility.allows_full_roster("generic", false));
        assert!(!visibility.allows_full_roster("generic", true));
        assert!(!visibility.allows_full_roster("omitted", false));
        assert!(!RosterVisibility::default().allows_full_roster("generic", false));
    }
}
