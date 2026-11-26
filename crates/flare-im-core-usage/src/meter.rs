//! Meter catalog (design §15.4). Dimension values are `&'static str` from closed sets, so a
//! counter key never allocates and its cardinality is bounded by the code, not by callers.

/// Messages the broker accepted at ingest, by `category` (`normal`, `notification`,
/// `operation`, `temporary`).
pub const MESSAGES_ACCEPTED: &str = "messages.accepted";
/// New (not deduplicated) messages committed by the storage writer.
pub const MESSAGES_PERSISTED: &str = "messages.persisted";
/// Highest number of concurrent online sessions of the tenant on this instance in the window.
pub const CONNECTIONS_PEAK: &str = "connections.peak";
/// Connected time of the tenant's online sessions on this instance, in whole minutes.
pub const CONNECTION_MINUTES: &str = "connection.minutes";
/// Distinct users that came online in the UTC day, as hashes of `(tenant, user_id)` carried in
/// `active_users`; the consumer upserts `(tenant, day, user_hash)`.
pub const USERS_ACTIVE_DAILY: &str = "users.active_daily";
/// Offline push deliveries handed to a provider, by `provider` and `result`.
pub const PUSH_ATTEMPTS: &str = "push.attempts";
/// Object bytes, by `op`: `added` when an upload completes, `removed` when the object is deleted.
pub const MEDIA_BYTES_STORED: &str = "media.bytes_stored";
/// Object bytes handed out by a signed download URL or served by Media itself.
pub const MEDIA_BYTES_EGRESS: &str = "media.bytes_egress";
/// Authenticated public API requests, by `route_group`.
pub const API_CALLS: &str = "api.calls";

/// Result of one offline push delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushOutcome {
    /// Backend task completed (may have no eligible device); not physical device delivery.
    Completed,
    /// Failed and will be redelivered.
    Retryable,
    /// Failed for good (dead-lettered).
    Failed,
}

impl PushOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Retryable => "retryable",
            Self::Failed => "failed",
        }
    }
}

/// One counter series inside a tenant: meter plus its dimension values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Counter {
    MessagesAccepted {
        category: &'static str,
    },
    MessagesPersisted,
    PushAttempts {
        provider: &'static str,
        result: &'static str,
    },
    MediaBytesStored {
        op: &'static str,
    },
    MediaBytesEgress,
    ApiCalls {
        route_group: &'static str,
    },
}

impl Counter {
    pub(crate) const fn meter(self) -> &'static str {
        match self {
            Self::MessagesAccepted { .. } => MESSAGES_ACCEPTED,
            Self::MessagesPersisted => MESSAGES_PERSISTED,
            Self::PushAttempts { .. } => PUSH_ATTEMPTS,
            Self::MediaBytesStored { .. } => MEDIA_BYTES_STORED,
            Self::MediaBytesEgress => MEDIA_BYTES_EGRESS,
            Self::ApiCalls { .. } => API_CALLS,
        }
    }

    /// Dimension name/value pairs, in a fixed order.
    pub(crate) fn dims(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Self::MessagesAccepted { category } => vec![("category", category)],
            Self::MessagesPersisted | Self::MediaBytesEgress => Vec::new(),
            Self::PushAttempts { provider, result } => {
                vec![("provider", provider), ("result", result)]
            }
            Self::MediaBytesStored { op } => vec![("op", op)],
            Self::ApiCalls { route_group } => vec![("route_group", route_group)],
        }
    }
}
