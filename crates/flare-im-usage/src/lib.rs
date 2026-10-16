//! Usage pre-aggregation (v3.1 C11, v4 R7-5).
//!
//! Core services count usage in process, per `(tenant, meter, dimensions)` and per wall-clock
//! window (60 seconds by default), and publish one compact `flare.usage.window.v1` event per
//! `(instance, tenant, window)` to `flare.usage.v1.<tenant>` on the JetStream stream
//! [`STREAM`]. No message adds a broker write of its own: the cost is one event per instance,
//! tenant and window. Core-window metering consumption/aggregation is still pending; the
//! current Platform consumer handles only Social snapshots. Do not treat this as billing.
//!
//! Counting points (one per service):
//!
//! | Service | Meter |
//! |---|---|
//! | message-ingest, after the broker accepted the send | `messages.accepted` by `category` |
//! | storage-writer, after a new message was persisted | `messages.persisted` |
//! | signaling-gateway, online session registered/closed | `connections.peak`, `connection.minutes`, `users.active_daily` |
//! | push-worker, offline delivery attempt | `push.attempts` by `provider`, `result` |
//! | media, object stored/removed, object-store download signed or local file served | `media.bytes_stored` by `op`, `media.bytes_egress` |
//! | api-gateway, authenticated request | `api.calls` by `route_group` |
//!
//! This is bounded best-effort telemetry, with at-least-once retries while retained. Every event carries the message id
//! `{instance}:{tenant}:{window_start_ms}:{part}`; JetStream drops a retried publish inside the
//! stream's duplicate window and the consumer upserts by the same key after it. The instance is
//! unique per process run, so a restart inside a window never reuses a key. An event that fails
//! to publish stays in a bounded backlog and is retried on the next window; the backlog, the
//! number of counter keys and the active-user sets are bounded, and what does not fit is
//! dropped and counted (`usage_records_dropped_total`, `usage_events_dropped_total`). Shutdown
//! flushes the current partial window. A crash also loses any unacknowledged in-memory backlog;
//! overload/shutdown can drop events. Neither metering completeness nor durable billing is promised.
//!
//! [`NATS_URL_ENV`] unset disables the reporter with one warning (local development, Community
//! deployments without a platform); set but unreachable at startup is a startup error, the same
//! rule as the Social usage snapshots that share the stream. The event contract is documented in
//! `docs/usage-events.md`.

mod event;
mod meter;
mod metrics;
mod recorder;
mod reporter;

pub use event::{UsageEvent, WINDOW_EVENT, user_hash};
pub use meter::{
    API_CALLS, CONNECTION_MINUTES, CONNECTIONS_PEAK, MEDIA_BYTES_EGRESS, MEDIA_BYTES_STORED,
    MESSAGES_ACCEPTED, MESSAGES_PERSISTED, PUSH_ATTEMPTS, PushOutcome, USERS_ACTIVE_DAILY,
};
pub use recorder::{LiveReading, UsageRecorder};
pub use reporter::{UsageLimits, UsageReporter};

/// JetStream stream shared with the Social usage snapshots (`flare.usage.snapshot.v1`).
pub const STREAM: &str = "flare_usage";
/// Subject family; each event goes to `flare.usage.v1.<tenant>`.
pub const SUBJECT_PREFIX: &str = "flare.usage.v1";
/// NATS endpoint of the usage stream; unset disables the reporter.
pub const NATS_URL_ENV: &str = "FLARE_USAGE_NATS_URL";
/// Window length in seconds (10..=3600, default 60).
pub const WINDOW_SECS_ENV: &str = "FLARE_USAGE_WINDOW_SECS";
