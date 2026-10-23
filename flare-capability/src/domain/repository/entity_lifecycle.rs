//! Port of the plugin entity lifecycle outbox (`plugin_entity_lifecycle_outbox`, plugin platform
//! v2 §3). A row asks Core to tell a plugin that the card of one of its entities went away: Storage
//! Writer adds `recalled` in the statement that marks a bound card recalled, Capability adds
//! `post_failed` when it could not post the card a plugin returned. Capability's worker delivers
//! each row at least once to the operation the contribution declares as `entityLifecycle.unbound`,
//! with a platform system ticket.
use std::time::Duration;

/// One pending notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleTask {
    pub id: i64,
    pub tenant_id: String,
    pub plugin_id: String,
    pub contribution_id: String,
    pub entity: String,
    /// `recalled` or `post_failed`.
    pub reason: String,
    pub server_msg_id: Option<String>,
    /// Claims so far, this one included.
    pub attempts: i32,
    /// A `post_failed` added while the post's outcome was unknown
    /// ([`EntityLifecycleOutbox::enqueue_unconfirmed`]).
    pub unconfirmed: bool,
    /// When the row was added (ms since the epoch).
    pub created_at_ms: i64,
}

/// How a row ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOutcome {
    /// The plugin answered (accepted, or rejected with its own code: it saw the notification).
    Delivered,
    /// Nothing to deliver: the contribution declares no `unbound` operation.
    Skipped,
    /// Given up after the retention window (the plugin stayed disabled or unreachable).
    Abandoned,
}

impl LifecycleOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Skipped => "skipped",
            Self::Abandoned => "abandoned",
        }
    }
}

#[async_trait::async_trait]
pub trait EntityLifecycleOutbox: Send + Sync {
    /// Add a notification; the same (tenant, plugin, entity, reason) is added once. A confirmed
    /// `post_failed` replaces an unconfirmed one ([`Self::enqueue_unconfirmed`]): it becomes due
    /// at once and counts for [`Self::post_failed`].
    async fn enqueue(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        reason: &str,
        server_msg_id: Option<&str>,
    ) -> flare_server_core::error::Result<()>;

    /// A `post_failed` whose cause is not known yet: Ingest did not answer whether it stored the
    /// card (unavailable, timed out, a first send still in flight). The row is due after
    /// `verify_after`; the worker then delivers it only if the entity still has no binding
    /// (Storage Writer binds the card when it stores it). It does not count for
    /// [`Self::post_failed`], so the caller's retry with the same command may still post (Ingest
    /// replays a stored card). Added once; an existing row of the entity is left as it is.
    async fn enqueue_unconfirmed(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        verify_after: Duration,
    ) -> flare_server_core::error::Result<()>;

    /// Whether the platform already gave up posting the card of this entity (a confirmed
    /// `post_failed` row exists): a replayed result must not post a card for an entity the plugin
    /// rolled back. Unconfirmed rows do not count.
    async fn post_failed(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> flare_server_core::error::Result<bool>;

    /// Claim up to `limit` due rows for `lease`: another worker sees them again only after it.
    async fn claim_due(
        &self,
        limit: u32,
        lease: Duration,
    ) -> flare_server_core::error::Result<Vec<LifecycleTask>>;

    /// End a row.
    async fn complete(
        &self,
        id: i64,
        outcome: LifecycleOutcome,
        note: Option<&str>,
    ) -> flare_server_core::error::Result<()>;

    /// Try a row again after `delay`, recording why this attempt failed.
    async fn retry_after(
        &self,
        id: i64,
        delay: Duration,
        error: &str,
    ) -> flare_server_core::error::Result<()>;
}
