//! Ports of the platform card path (plugin platform v2 §1): Capability posts the card a plugin
//! operation returned as the ticket's user into the ticket's conversation, through Message Ingest.
use flare_proto::common::Message;

/// How Ingest routes a message of a conversation: the proto conversation type and the channel
/// (the peer of a single chat, the group of a group chat).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardRoute {
    pub conversation_type: i32,
    pub channel_id: String,
}

/// Why one post did not go out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardPostError {
    /// The conversation or Ingest refused the post (not a member, muted, invalid): another attempt
    /// cannot succeed. Carries the refusing service's reason.
    Refused(String),
    /// Transport, availability or deadline: the post may be retried with the same client message
    /// id, which Ingest deduplicates.
    Unavailable(String),
}

impl std::fmt::Display for CardPostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {reason}"),
            Self::Unavailable(reason) => write!(f, "unavailable: {reason}"),
        }
    }
}

/// A card Ingest accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedCard {
    pub server_msg_id: String,
}

#[async_trait::async_trait]
pub trait PluginCardDelivery: Send + Sync {
    /// The route of `conversation_id` read as `user_id` (an active member, or the read is refused).
    async fn route(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        request_id: &str,
    ) -> std::result::Result<CardRoute, CardPostError>;

    /// Send `message` (a plugin card whose sender is the ticket's user) through Ingest with
    /// Capability's card post context.
    async fn post(
        &self,
        tenant_id: &str,
        message: Message,
    ) -> std::result::Result<PostedCard, CardPostError>;
}
