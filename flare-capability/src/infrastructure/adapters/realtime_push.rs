//! `im.realtime.deliver` to Push Proxy `PushService.PushCapability`: the packet reaches every
//! online connection of the recipients as `DataPacket.capability` (online only, never stored,
//! never pushed offline). The call carries a Cell-signed service context of the delivery's tenant
//! (actor [`CAPABILITY_SERVICE_ACTOR`]); Push keeps it to that tenant end to end.
use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::realtime_delivery::{RealtimePacket, RealtimePacketDelivery};
use crate::infrastructure::adapters::resource_access::CAPABILITY_SERVICE_ACTOR;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use flare_grpc_proto::push::PushCapabilityRequest;
use flare_grpc_proto::push::push_service_client::PushServiceClient;
use flare_proto::common::CapabilityPacket;
use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Bound of one `PushCapability`: a realtime packet that waits longer is no longer realtime.
const CALL_TIMEOUT: Duration = Duration::from_secs(1);
/// `CapabilityPacket.version` of plugin realtime packets.
pub const REALTIME_PACKET_VERSION: &str = "1";

pub struct GrpcRealtimePush {
    /// `http(s)://` or `discovery://` authority of Push Proxy `PushService`.
    pub authority: String,
    /// The Cell signer (`FLARE_INTERNAL_CTX_PRIVATE_KEY`).
    pub signer: Arc<SignedContextSigner>,
}

/// The downlink packet: `capability_id` is the plugin id, `correlation_id` the delivery id, and
/// the attributes name the conversation, plugin, delivery and expiry.
pub fn capability_packet(packet: &RealtimePacket) -> CapabilityPacket {
    CapabilityPacket {
        capability_id: packet.plugin_id.clone(),
        packet_type: packet.packet_type.clone(),
        version: REALTIME_PACKET_VERSION.into(),
        payload: packet.payload_json.clone(),
        attributes: HashMap::from([
            ("conversation_id".into(), packet.conversation_id.clone()),
            ("plugin_id".into(), packet.plugin_id.clone()),
            ("delivery_id".into(), packet.delivery_id.clone()),
            ("expires_at_ms".into(), packet.expires_at_ms.to_string()),
        ]),
        correlation_id: Some(packet.delivery_id.clone()),
    }
}

fn unavailable(error: impl std::fmt::Display) -> CapabilityError {
    CapabilityError::System(format!("realtime delivery unavailable: {error}"))
}

#[async_trait::async_trait]
impl RealtimePacketDelivery for GrpcRealtimePush {
    async fn deliver(&self, packet: &RealtimePacket) -> Result<()> {
        let token = self
            .signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                CAPABILITY_SERVICE_ACTOR,
                &packet.tenant_id,
                &packet.request_id,
            ))
            .map_err(unavailable)?;
        let channel = resolve_plugin_channel(&self.authority)
            .await
            .map_err(unavailable)?;
        let mut request = tonic::Request::new(PushCapabilityRequest {
            user_ids: packet.user_ids.clone(),
            packet: Some(capability_packet(packet)),
            expire_at_ms: packet.expires_at_ms,
        });
        request.set_timeout(CALL_TIMEOUT);
        flare_server_core::set_signed_context_token(&mut request, &token).map_err(unavailable)?;
        PushServiceClient::new(channel)
            .push_capability(request)
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_packet_follows_the_wire_contract() {
        let packet = capability_packet(&RealtimePacket {
            tenant_id: "app-a".into(),
            request_id: "req-1".into(),
            plugin_id: "flare.call".into(),
            conversation_id: "1-ab".into(),
            user_ids: vec!["u1".into(), "u2".into()],
            packet_type: "call.snapshot".into(),
            payload_json: br#"{"callId":"c1"}"#.to_vec(),
            delivery_id: "d-1".into(),
            expires_at_ms: 1_730_000_060_000,
        });
        assert_eq!(packet.capability_id, "flare.call");
        assert_eq!(packet.packet_type, "call.snapshot");
        assert_eq!(packet.version, "1");
        assert_eq!(packet.payload, br#"{"callId":"c1"}"#);
        assert_eq!(packet.correlation_id.as_deref(), Some("d-1"));
        assert_eq!(
            packet.attributes,
            HashMap::from([
                ("conversation_id".to_string(), "1-ab".to_string()),
                ("plugin_id".to_string(), "flare.call".to_string()),
                ("delivery_id".to_string(), "d-1".to_string()),
                ("expires_at_ms".to_string(), "1730000060000".to_string()),
            ])
        );
    }
}
