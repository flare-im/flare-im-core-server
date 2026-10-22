//! The internal event entry for plugin shared state: Orchestrator `MessageEventService.ExecuteEvent`
//! (the same `handle_event` recalls and reactions use). Orchestrator allocates the conversation seq,
//! fans the event out (single chats per member, groups by conversation broadcast) and hands it to
//! Storage Writer. The call carries a system actor, never a user identity.
use crate::domain::repository::plugin_state::PluginStateEventSink;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_grpc_proto::message::ExecuteEventRequest;
use flare_im_service_kit::clients::MessageEventServiceClientWrapper;
use flare_proto::common::{Event, EventType, PluginStateEvent, event::Payload};
use flare_server_core::context::{ActorContext, ActorType, Context, Ctx};
use flare_server_core::error::{FlareError, Result};
use std::sync::Arc;

/// Actor id of the system identity on these events.
pub const PLUGIN_STATE_ACTOR: &str = "flare-capability";

pub struct GrpcPluginStateEvents {
    /// `http(s)://` or `discovery://` authority of Orchestrator.
    pub authority: String,
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

/// The system context of one publish: the tenant, a fresh request id and the system actor.
pub fn system_context(tenant_id: &str) -> Ctx {
    let request_id = format!("plugin-state-{}", uuid::Uuid::new_v4());
    Arc::new(
        Context::with_request_id(&request_id)
            .with_tenant_id(tenant_id)
            .with_actor(ActorContext::new(PLUGIN_STATE_ACTOR).with_type(ActorType::System)),
    )
}

#[async_trait::async_trait]
impl PluginStateEventSink for GrpcPluginStateEvents {
    async fn publish(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        event: PluginStateEvent,
    ) -> Result<()> {
        let channel = resolve_plugin_channel(&self.authority)
            .await
            .map_err(|error| FlareError::system(format!("orchestrator unavailable: {error}")))?;
        let ctx = system_context(tenant_id);
        MessageEventServiceClientWrapper::from_channel(channel)
            .execute_event_with_ctx(
                &ctx,
                ExecuteEventRequest {
                    svid: String::new(),
                    event: Some(plugin_state_event(conversation_id, event)),
                },
            )
            .await
    }
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
        let ctx = system_context("app-a");
        assert_eq!(ctx.tenant_id(), Some("app-a"));
        assert_eq!(ctx.user_id(), None);
        let actor = ctx.actor().unwrap();
        assert_eq!(actor.actor_id(), PLUGIN_STATE_ACTOR);
        assert_eq!(actor.actor_type, ActorType::System);
    }
}
