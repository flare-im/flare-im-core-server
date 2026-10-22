//! Ports of plugin entity shared state (`EVENT_MESSAGE_PLUGIN_STATE`): the bindings Storage Writer
//! registers when it stores a plugin card, and the internal event entry that sequences and fans an
//! event out (Orchestrator `ExecuteEvent`).
use flare_proto::common::PluginStateEvent;

/// One stored plugin card showing an entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityBinding {
    pub conversation_id: String,
    pub server_msg_id: String,
    /// The highest version fanned out to this message (0: none yet).
    pub published_version: u64,
    /// The card was recalled: it takes no more state, and its actions are refused.
    pub recalled: bool,
}

/// A binding whose `published_version` [`PluginEntityBindingStore::claim_version`] raised, with the
/// version it held before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedBinding {
    pub conversation_id: String,
    pub server_msg_id: String,
    pub prior_version: u64,
}

#[async_trait::async_trait]
pub trait PluginEntityBindingStore: Send + Sync {
    /// Every binding of `(tenant, plugin, entity)`, recalled ones included.
    async fn bindings(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> flare_server_core::error::Result<Vec<EntityBinding>>;

    /// Raise `published_version` to `version` on every live binding below it, atomically per row,
    /// and return those rows. A binding already at or above `version` (another instance published
    /// it first) is not returned, so a version reaches each message at most once.
    async fn claim_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        version: u64,
    ) -> flare_server_core::error::Result<Vec<ClaimedBinding>>;

    /// Hand a claimed version back after its event could not be sent, when the row still holds it.
    async fn release_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        binding: &ClaimedBinding,
        version: u64,
    ) -> flare_server_core::error::Result<()>;
}

/// Sequences one event in a conversation and fans it out (single chats per member, groups by
/// conversation broadcast), as the system, never as a user.
#[async_trait::async_trait]
pub trait PluginStateEventSink: Send + Sync {
    async fn publish(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        event: PluginStateEvent,
    ) -> flare_server_core::error::Result<()>;
}
