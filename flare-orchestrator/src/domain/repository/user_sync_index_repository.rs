use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationChange {
    pub conversation_id: String,
    pub version: u64,
    pub max_conversation_seq: u64,
    pub occurred_at_ms: i64,
}

/// User-level sync index updated whenever a durable change becomes visible to a user.
///
/// The index is the Phase-1 read-diffusion entry point: clients can compare their
/// user-level version first, then fetch the changed conversation ids before
/// falling back to per-conversation sync.
#[async_trait]
pub trait UserSyncIndexRepository: Send + Sync {
    async fn record_conversation_change(
        &self,
        ctx: &Ctx,
        user_ids: &[String],
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
    ) -> Result<()>;

    async fn record_conversation_version_bump(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
    ) -> Result<u64>;

    async fn diff_changed_conversations(
        &self,
        ctx: &Ctx,
        known: &[(String, u64)],
    ) -> Result<Vec<ConversationChange>>;
}
