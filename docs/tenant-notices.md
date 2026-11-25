# Tenant notices

A tenant notice tells every online client of one tenant that a piece of tenant-wide
configuration changed, so that clients re-read it now instead of on their next reconnect or
cold start. Core carries the notice; it never carries the configuration and never knows what
the configuration means. The first users are the plugin catalog (Core's own) and Social's
feature snapshot, which drives the composer "+" panel layout.

## Model

`TenantNotice { tenant_id, topic, version }` (`crates/flare-im-service-kit/src/tenant_notice.rs`).

- `topic` names what changed: at most 64 bytes of `[a-z0-9._-]`. Topics under `flare.` are
  Core's (`flare.plugin.catalog`, `flare.access`, `flare.release`); every other topic belongs to
  the namespace its publisher registered (`social.features`).
- `version` is the publisher's monotonic version of that configuration. A notice that is not
  newer than the last one published for the same `(tenant, topic)` is dropped.
- `tenant_id` `*` addresses every tenant (a platform default changed).

## Publishing

Business services publish through `flare.capability.v1.TenantNoticeService.Publish`
(`flare-im-protocol-grpc/proto/capability_service.proto`) with `{topic, version}`; the tenant is the
signed service context's (`x-flare-ctx`). The answer is `{accepted, current_version}`.

- Only a registered publisher may publish, and only under its namespaces:
  `FLARE_TENANT_NOTICE_PUBLISHERS` on flare-capability, `service=prefix[,prefix|*]` entries
  separated by `;`, for example `flare-social-bridge=social.,*` (namespace `social.`, may
  address `*`). Empty accepts no publisher; Core's own catalog notices are unaffected.
- Publishing is one Redis Lua script: it compares the version with the last one kept at
  `<channel>:last:<tenant>:<topic>` and, only when newer, stores it and publishes to the notice
  channel. The check and the fan-out are atomic, so concurrent publishers never deliver an
  older version after a newer one. The last version is kept for 30 days.
- A publisher emits the notice after its own state commits (Social writes it to its outbox in
  the transaction that accepts the feature projection, and the bridge worker publishes it).
- `flare.access` (version: the access manifest serial) and `flare.release` (version: the
  application's release generation) come from `TenantProjection.UpsertClientPolicy`. The
  projection adds them to `tenant_notice_outbox` in its transaction when the watermark moves;
  Capability's relay publishes them within about a second and deletes the row.

## Delivery

The signaling gateway subscribes to the notice channel and sends each online connection of the
tenant `DataPacket.tenant_notice` (`flare-im-protocol/proto/data.proto`):

```text
TenantNotice { topic, version, spread_ms, tenant_wide }
```

- `spread_ms` is how long clients spread their re-reads over:
  `clamp(ceil(connections × 1000 / R), 3000, 60000)` ms for the node's connections of that
  tenant, with `R = FLARE_NOTICE_REFRESH_RPS_PER_NODE` (default 500 re-reads per second per
  gateway node). A tenant with 10 000 online clients on one node spreads over 20 s.
- `tenant_wide` is set for a `*` notice, which the gateway sends to every tenant's connections.
- A client never sends `tenant_notice` upstream; the gateway rejects it.

### Build floor eviction

The version gate runs at CONNECT. When an application's `client_policy` raises a floor later
(a higher `min_build`, a `min_build` that reaches its effective time, or a new blocked range),
the gateway also closes the online connections that CONNECT would now refuse:

- On a `flare.access` or `flare.release` notice of the application, the gateway drops its cached
  policy and sweeps that application's connections on the node. A periodic sweep of every
  application (`FLARE_BUILD_FLOOR_SWEEP_SECS`, default 60, `0` turns it off) covers floors that
  become effective by time and policy changes that move no watermark.
- Each selected connection first receives the same typed refusal CONNECT sends,
  `ERROR(UPGRADE_REQUIRED)` with `min_build`, then is closed. Its reconnect is refused at CONNECT
  with the same frame, so a client that does not act on the first one still ends in the forced
  update.
- The closes of one sweep are spread over `FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS` (default 30)
  and kept at or below `FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE` per node (default 200), so a
  raised floor does not send every old client back to CONNECT at once. Each connection is judged
  again right before its close; a floor lowered inside the window spares it.
- `connections_evicted_total{reason}` counts the closes (`below_min_build`, `blocked_build`).

## Client SDK

The core SDK merges notices per topic within the spread window: each client waits a random
delay inside `spread_ms` and keeps only the highest version it saw.

- `flare.plugin.catalog` is handled inside the SDK: the plugin catalog is invalidated and
  re-read, then `PluginEvent::CatalogChanged` is published.
- Every other topic becomes `SdkEvent::Extension { source: "flare.tenant_notice",
  event_type: <topic>, payload: {"version": "<decimal>", "tenantWide": bool} }`, which the
  product SDK that owns the namespace consumes (Social: `social.features` re-reads the feature
  snapshot with `x-flare-features-after`, then emits `social.entries.changed`).

A missed notice is not an error: clients re-read on reconnect and on cold start, so a notice
only shortens the time an online client shows stale configuration.

## Deployment

| Service | Setting |
| --- | --- |
| flare-capability | `FLARE_TENANT_NOTICE_PUBLISHERS` (empty: no external publisher) |
| flare-signaling-gateway | `FLARE_NOTICE_REFRESH_RPS_PER_NODE` (optional, default 500) |
| flare-signaling-gateway | `FLARE_BUILD_FLOOR_SWEEP_SECS` (default 60), `FLARE_BUILD_FLOOR_EVICTION_WINDOW_SECS` (default 30), `FLARE_BUILD_FLOOR_EVICTION_RPS_PER_NODE` (default 200) |
| publisher | resolves `flare-capability` (Consul, or an explicit gRPC address) |
