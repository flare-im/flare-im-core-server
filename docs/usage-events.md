# Usage events (`flare.usage.window.v1`)

Core counts usage in process and publishes pre-aggregated windows for the platform's metering
(design v3.1 C11 and §15.4). No message causes a broker write of its own: each producing
instance publishes one event per tenant and window. The library is `crates/flare-im-usage`.

## Stream and configuration

| Item | Value |
|---|---|
| JetStream stream | `flare_usage`, subjects `flare.usage.v1.>`, 90 days retention, 10 minute duplicate window (shared with the Social `flare.usage.snapshot.v1` snapshots; every producer declares the same spec) |
| Subject | `flare.usage.v1.<app>` (`app` is the IM tenant id) |
| Message id | `{instance}:{app}:{window_start_ms}:{part}`, sent as `Nats-Msg-Id` |
| `FLARE_USAGE_NATS_URL` | NATS of the usage stream. Unset: the service reports no usage and logs one warning. Set but unreachable at startup: the service does not start. |
| `FLARE_USAGE_WINDOW_SECS` | Window length, 10..=3600 seconds, default 60. Windows are aligned to the wall clock. Read only when `FLARE_USAGE_NATS_URL` is set; an invalid value then stops startup. |

The supervisor manifest hands both variables to the six producing services only.

## Counting points

| Service (`producer`) | Meter | Dimensions | Counted when |
|---|---|---|---|
| `flare-message-ingest` | `messages.accepted` | `category`: `normal`, `notification`, `operation`, `temporary` | the broker acknowledged the send. A send that answered `WalAccepted` is not counted here; WAL replay persists it and the writer counts it. Idempotent replays are not counted. |
| `flare-storage-writer` | `messages.persisted` | — | a new message was committed (redelivered duplicates are not counted) |
| `flare-signaling-gateway` | `connections.peak` | — | highest number of concurrently registered online sessions of the tenant on this instance during the window |
| `flare-signaling-gateway` | `connection.minutes` | — | connected time of the tenant's online sessions on this instance, whole minutes; the remainder carries into the next window and is rounded when the tenant has no session left |
| `flare-signaling-gateway` | `users.active_daily` | — | a user's online session registered; once per tenant, user and UTC day per instance, carried as hashes in `active_users` |
| `flare-push-worker` | `push.attempts` | `provider`: `apns`, `fcm`, `getui`, `outbox`; `result`: `completed`, `retryable`, `failed` | a backend attempt finished (including no eligible device); not physical device delivery |
| `flare-media` | `media.bytes_stored` | `op`: `added`, `removed` | a new object was registered (upload, multipart or direct completion; deduplicated uploads add nothing) / an object was deleted (delete or orphan cleanup) |
| `flare-media` | `media.bytes_egress` | — | a download URL of an object in object storage was signed (object size; the bytes leave from the object store or CDN), or Media read out a locally stored file (bytes read; signing such a file counts nothing) |
| `flare-api-gateway` | `api.calls` | `route_group`: `media`, `messages`, `conversations`, `presence`, `devices`, `other` | a request passed authentication and the tenant gate |

## Event

```json
{
  "event": "flare.usage.window.v1",
  "producer": "flare-signaling-gateway",
  "instance": "flare-signaling-gateway-01JA2B3C4D5E6F7G8H9J0K1M2N",
  "app": "acme",
  "window_start_ms": 1760140800000,
  "window_secs": 60,
  "part": 0,
  "meters": [
    {"meter": "connections.peak", "dims": {}, "value": 42},
    {"meter": "connection.minutes", "dims": {}, "value": 37}
  ],
  "active_users": {"day": 20251011, "user_hashes": ["5f1c0a9e3b7d2c41"]}
}
```

- `instance` is unique per process start, so a restart inside a window never reuses an id.
- `part` 0 carries `meters` and the first batch of active users. More than 1,000 hashes, or
  hashes of two UTC days in one window, continue in parts 1, 2, … with empty `meters`.
- `active_users` is omitted when there are none. A user hash is the first eight bytes of
  `SHA-256(app || 0x1f || user_id)` as 16 lowercase hex digits; the same user hashes alike on
  every instance.
- Events without any reading are not sent.

## Delivery

Bounded best-effort telemetry, not durable billing. While retained, retries are at least once.
JetStream drops a repeated message id inside the 10 minute duplicate window;
after it the consumer must deduplicate by the keys below. An event that is not acknowledged
stays in a bounded in-memory backlog (20,000 events or 32 MiB; when full, the oldest event is
dropped) and is retried with the same id after the next window; the first failure in a round
stops that round, so an outage costs one publish timeout per window. Windows close when the
wall clock passes their end; after the clock steps back, the next window lasts until it is past
its end again, so labels never repeat. Shutdown abandons a publish round still waiting on the
stream and flushes the current partial window plus the backlog under the same ids, giving it at
most three seconds. A crash loses both the window in progress and unacknowledged in-memory
backlog; overload/shutdown may also discard readings. These losses must not be presented as
complete billing/accounting data.

Memory is bounded: at most 50,000 counter series per window, 500,000 remembered active-user
hashes per day and 200,000 hashes waiting for the next event. Records beyond the limits are
dropped and counted, never queued without limit.

## Consumer requirements

Not yet implemented for Core windows: the current Platform consumer handles Social snapshots
only. The following is the pending integration contract, not a claim that window rows already exist.

The platform metering consumer (Cell Agent or `platform-commerce` usage) must:

1. Accept `event == "flare.usage.window.v1"` next to the existing `flare.usage.snapshot.v1`
   on the same durable consumer, and ignore unknown events.
2. Upsert one window row per meter reading, keyed by
   `(instance, meter, dims, app, window_start_ms)`, writing the value (not adding to it), so a
   redelivered event changes nothing.
3. Upsert active users keyed by `(app, day, user_hash)`.
4. Aggregate across instances per window by summing. For `connections.peak` the sum is an
   upper bound of the Cell's concurrent sessions in that window (each instance reports its own
   peak, and the peaks need not coincide); a period's peak is the maximum over its windows.
   Stored bytes are `added − removed`.

## Metrics

All labelled by `producer` (and `reason` for the drops); per-tenant data stays in the events:
`usage_events_published_total`, `usage_publish_failures_total`,
`usage_records_dropped_total{reason=invalid_tenant|counter_limit|active_user_limit}`,
`usage_events_dropped_total{reason=backlog_full|shutdown|encode}` and `usage_backlog_events`.
A service with reporting enabled exposes every series at zero from startup.
