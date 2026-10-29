//! Port of the host capability `im.realtime.deliver`: a plugin's non-durable packet to the online
//! connections of members of one conversation. Core checks who may send and to whom; this port
//! only hands the packet to the push path (online only, never stored, never pushed offline).
use crate::domain::capability::Result;

/// One accepted delivery, after its credential, conversation and recipients were checked.
#[derive(Debug, Clone, PartialEq)]
pub struct RealtimePacket {
    pub tenant_id: String,
    /// The bridge request id: the delivery is traced under it.
    pub request_id: String,
    pub plugin_id: String,
    pub conversation_id: String,
    /// Members of the conversation, in request order.
    pub user_ids: Vec<String>,
    pub packet_type: String,
    /// The UTF-8 JSON bytes of the plugin's payload object.
    pub payload_json: Vec<u8>,
    pub delivery_id: String,
    /// Core's receive time plus the requested TTL (Unix epoch milliseconds).
    pub expires_at_ms: i64,
}

#[async_trait::async_trait]
pub trait RealtimePacketDelivery: Send + Sync {
    /// Hand the packet to the online push path. At most once: a failure is reported, never retried.
    async fn deliver(&self, packet: &RealtimePacket) -> Result<()>;
}
