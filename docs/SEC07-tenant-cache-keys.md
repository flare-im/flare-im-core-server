# SEC-07 tenant-scoped ephemeral state (direct cutover)

This change intentionally does **not** read the old tenantless Redis keys. Roll out
Online, Signaling Gateway, API Gateway, Push, ACK and Storage Writer together, with
old long connections drained. Existing ephemeral sessions, subscriptions, ACKs and
idempotency reservations expire by TTL; they are not copied into the new keyspace.
Do not perform a mixed-version rolling deployment against the same Redis namespace.

| State | New key / channel | Identity source |
| --- | --- | --- |
| Online session hash | `session:{tenant}:{user}` | verified short-lived internal User context from the authenticated signaling connection |
| Tenant online index | `tenant:sessions:{tenant}`; members `v2:{user-byte-length}:{user}:{device}` | same verified tenant |
| Presence | `presence:{tenant}:{user}`, `presence_events:{tenant}`, `user_presence_events:{tenant}` | verified User or explicitly allowed query Service context |
| Subscription | `subscription:{tenant}:{user}`, `topic:subscribers:{tenant}:{topic}` | verified Online context |
| Kick pub/sub | configured token-store channel; JSON `{tenant_id,user_id,device_id?,connection_id?}` | Console control token, authenticated user self-kick, or tenant-bound token revocation |
| ACK status / in-memory ACK cache | `ack:v2:{tenant}:{message-byte-length}:{message}:{user}` | required canonical tenant in ACK API, no default tenant |
| Storage Writer idempotency | `storage:idempotency:{tenant}:{message-id}` | required canonical tenant in writer context, no default tenant |
| Push Proxy task status | `{prefix}:task:v2:{tenant}:{task-id}` | verified signed Service request tenant; old tenantless task status keys are not read |
| Gateway conversation subscribers | in-memory `(tenant_id, conversation_id) -> connection_ids` with atomic reverse index | authenticated connection context for eager joins; delivery context for member bootstrap |
| Gateway resolved participants / large-group sweep / typing aggregate | in-memory tuple `(tenant_id, conversation_id)` | required canonical tenant; no fallback to tenant `0` |

Online Login requires a signed User context with `credential_id=signaling-gateway`.
Its signed `request_id` is the physical Gateway connection ID and must equal the
Login body's `metadata.connection_id`. The Gateway obtains that value from its
authenticated connection manager, not client-supplied metadata. Login performs
conflict-policy eviction and the per-user device cap in one Redis Lua operation.
The cap counts distinct device IDs. If a cap is exceeded, the oldest device is
evicted; same-device reconnect replaces the old physical connection. Kick signals
target the old `connection_id` so a delayed signal cannot close its replacement.
`SelfKickDevice` is a separate signed User RPC; Console `KickDevice` remains
control-token-only. The API Gateway user-facing device-kick endpoint calls the
self-kick RPC.

Online reads `postgres.tenant_runtime` from the resolved app config, with
`SIGNALING_ONLINE_TENANT_RUNTIME_POSTGRES_URL` as an explicit override.
`SIGNALING_ONLINE_TENANT_POLICY=strict` requires the projection database and
rejects unknown/unavailable/non-active tenants. A projected nonzero
`max_online_devices_per_user` takes precedence over
`SIGNALING_ONLINE_MAX_DEVICES_PER_USER` (default `0`, unlimited). An unavailable
configured projection rejects Online Login even under lenient policy, preventing
a database outage from silently removing the device cap.

The Redis session mutation is atomic, but kick pub/sub can use a separate Redis
instance and is not durable. A publish failure after admission is logged and does
not turn the committed login into a false failure. It can leave an evicted socket
alive until a later successful kick/disconnect; closing this gap requires a durable
kick stream or Gateway-side periodic authoritative-session revalidation. Redis
publisher ACLs should restrict the kick channel. This limitation is a release risk
for deployments requiring a hard, instantaneous active-socket device limit.

The Gateway's realtime local relay and typing aggregation are also
tenant/conversation scoped. The originating Gateway may broadcast to peers only
from an `AuthenticatedConnectionOrigin` created by its connection manager after
token authentication; it signs a short-lived User internal context with
`credential_id=signaling-gateway`. A receiving Gateway verifies that token and
checks the actor/credential plus a domain-separated SHA-256 digest of the exact
conversation ID and encoded packet (in the signed request ID) before consulting
its tenant-scoped local subscription registry. Missing signer or verifier drops
cross-node ephemeral relay instead of accepting raw metadata. Client-supplied
typing aggregates are dropped; the Gateway alone produces them. A deployment
must put the Cell public key on each receiving Gateway and the private key only
on originating Gateways that authenticate connections. The digest prevents a
captured token from being modified to target another conversation or payload;
an identical replay remains possible within the token TTL. The receiver only
fans out ephemeral realtime/typing frames; it does not mutate authoritative
message, membership or session state. This path therefore promises best-effort
delivery, **not at-most-once delivery**. Rate limiting and bounded replay-ID
tracking are required before the relay is used for state-changing controls.

Push Server and Push Worker now use tenant-scoped Online keys/cache. Push
Server's direct Redis read also requires an already-verified context, and their
Online gRPC queries forward only its original token. Current
broker producer identity and topic ACL provenance are not proven, so a Push event
with only an envelope tenant is **not** re-signed as a Service identity. Such
queries fail closed until broker provenance plus a scoped Service signer is in
place; this can reduce Push device routing and offline device push. The current
message-to-Push-Worker Kafka/JetStream producer chain and ACLs must be audited:
bind a producer workload identity to a tenant-aware signed event at publication,
verify the signature, audience, payload digest and replay ID at consumption, and
only then mint a narrowly scoped online-query Service context. Do not promote an
envelope tenant or broker header to Service identity. Until that provenance is
implemented and tested end-to-end, offline push is **not accepted as a completed
SEC-07 path**. The Storage Writer idempotency key is
scoped by its context tenant, but separate ingress authentication must establish
that context's provenance before it is considered an authorization boundary.

Push Proxy gRPC now requires `FLARE_INTERNAL_CTX_PUBLIC_KEY` at startup and a
valid signed Cell context on every RPC. Push creation/status methods accept
only signed `Service`, while device-token registration/unregistration accept
only signed `User` whose ID equals the body user ID. The signed tenant must be
canonical; any tenant claim in push request/option/payload metadata must match
it. Bare `x-tenant-id`/`x-user-id` no longer establishes identity. Push task
status is tenant-keyed; old status keys intentionally become unavailable.
Push Server routing no longer substitutes its deployment default when the
incoming context lacks a canonical tenant, and a `PushEnvelope.tenant_id`
must equal that context tenant. **This does not authenticate MQ headers**:
these are defensive rejection checks, not permission to sign the resulting
task. Existing SDKs or Core callers using unsigned PushService gRPC must first
migrate to a trusted signed transport bridge, or calls will fail closed.

The current provenance chain, before a signed broker envelope exists:

| Stage | Topic / schema | Current trust gap |
| --- | --- | --- |
| MessagePipeline / PushProxy → Push Server | `flare.im.push.messages` and `.events` (`MqEnvelope`), `.envelope` (`PushEnvelope`) | MessagePipeline emits via generic `Producer::send`; PushProxy previously admitted unsigned gRPC. MQ headers are plain Ctx propagation, not identity. |
| Push Server → Push Worker | `flare.im.push.online` / `.offline` (`PushTaskEnvelope`) | `PushServerMqPublisher` sends raw protobuf with no signed headers. `PushRouterHandler` derives its task tenant from the incoming Ctx; missing tenant now rejects, but header authenticity is still unproven. |
| Push Worker → Online / Gateway | gRPC with short-lived Cell context | Worker has no verified broker-event origin and therefore must not mint a Service token. Gateway rejects unsigned delivery and Online queries reject unsigned identity. |

`PushProxyMqPublisher::publish_push_task` can also write worker topics directly,
although there are no current in-repository callers. It must be removed or
separately authorized before broker ACL rollout. The checked-in Kafka profile
contains no SASL/ACL settings and the topic creation script only creates
topics; NATS currently connects from a URL without code-enforced per-subject
permissions. Neither a `source` string nor raw broker headers prove a producer.

AccessGateway's delivery and all direct push gRPC endpoints now require a
verified short-lived Service context with `actor_id=flare-push-worker` and a
canonical tenant. `DeliverToConversation` additionally requires every nested
message/event to target its outer conversation. The Gateway Router forwards
only the original `VerifiedContext` token; a naked `Ctx` from a broker envelope
does not become an outbound credential. This deliberately interrupts the
current Push Worker → Gateway conversation delivery and Notification/ACK/Custom
routes, since the worker's MQ input lacks proven producer identity; granting the
worker a private key would merely launder the untrusted envelope. These routes
must remain fail-closed until the signed event/ACL provenance bridge above is
implemented. Direct Push Server device routing has the same provenance gap.
This is a breaking deployment boundary: the current worker treats a
`PermissionDenied` route failure as non-retryable, publishes the task to its
DLQ, and ACKs the broker record. Do not start that consumer on the new code
until provenance is in place. Push Server's ingress handlers can likewise
DLQ+ACK a normal event whose unsigned broker context now fails the canonical
tenant check. `PUSH_SERVER_EVENT_PROVENANCE_MODE` and
`PUSH_WORKER_EVENT_PROVENANCE_MODE` both default to `paused`; each service now
fails at bootstrap **before constructing a consumer or DLQ publisher**, leaving
broker records untouched. Explicit `paused` behaves the same. `signed-v1` is
reserved and also fails startup until source signatures and consumer verifiers
are integrated; unknown/boolean values fail as invalid configuration. There
is no unsafe override. Pause both consumers to retain broker backlogs within
their configured retention windows, then validate replay before resuming. No
mixed old/new Push Server/Worker/Gateway rollout is safe for this path. Missing
peer relay signatures return `UNAUTHENTICATED`, an
unconfigured verifier returns `UNAVAILABLE`, and an unauthorized actor or
packet/body mismatch returns `PERMISSION_DENIED`.
Push Proxy's signed Service `PushMessage`, `PushNotification`, and `PushCustom`
entry points also default to `PUSH_PROXY_EVENT_PROVENANCE_MODE=paused` and
return `UNAVAILABLE` before enqueuing or creating a misleading pending task.
The reserved `signed-v1` value does not enable them until the complete wire
contract and both consumers exist; device-token registration and status query
remain separate authenticated operations.
Backlog retention is bounded by the **effective live broker configuration and
byte capacity**, not merely a checked-in value: `config/base.toml` intends
`max_age_seconds=86400` for the Push JetStream stream and the Kafka creation
script intends `retention.ms=86400000` for new Push topics. The Core topology
adapter now carries the four typed stream limits into all seven NATS callers
and the checked-in config explicitly declares a separate 7-day framework DLQ
stream; older deployed binaries may still have created a 7-day Push stream.
Existing Kafka topics are not updated by `--if-not-exists`, and JetStream's
1 GiB `DiscardOld` can evict records sooner. Operators must inspect the live
stream/topic, halt or shed upstream
publication, and budget capacity before a prolonged pause. Startup fail-fast
alone cannot guarantee even a 24-hour recoverable backlog. **Release blocker:** do not
deploy the new Push Server/Worker/Gateway combination until signed provenance,
consumer replay for both ingress and worker streams, broker retention/capacity
budget and a backlog migration rehearsal are all verified. Paused consumers
are safety brakes, not an available Push service.

To remove this release gate: (1) authorize distinct producer workload
identities for each exact push topic/stream using Kafka/NATS publish/consume
ACLs; (2) after source authentication, emit a versioned Ed25519-signed immutable
event envelope binding `producer_id`, topic, tenant, payload SHA-256, schema,
event ID and produced-at, with key IDs and verification-key rotation; event
validity/key retention must cover broker retention and replay windows, not the
60-second gRPC token TTL; (3) Push Server must verify every inbound signature
*before* routing, reject missing/altered/foreign-topic events without ACKing
normal messages, and sign its derived task with a distinct workload key that
also binds task payload digest, tenant and source event ID; (4) Worker verifies
that derived signature, nested tenant/target consistency and durable replay-ID
deduplication before constructing a `VerifiedPushEvent`; only then may it issue
a short-lived, audience-scoped `Service(actor_id=flare-push-worker)` token for
Online/Gateway; (5) test forged/altered/cross-topic/cross-tenant/replayed
events, key rotation, backlog after restart, broker outage and DLQ replay;
(6) activate `signed-v1` only after those checks and an operator-reviewed
backlog rollout. No step may infer identity from bare broker headers or
`PushTaskEnvelope.tenant_id`.

Implementation slices for the broker provenance contract (not implemented by
this SEC-07 patch):

1. Add `SignedPushRecordV1` to `flare-proto/proto/topic_envelope.proto`: version,
   key ID, producer ID, exact topic, partition key, tenant ID, event ID,
   produced-at, source event ID (for derived worker tasks), payload schema,
   payload SHA-256, raw payload and Ed25519 signature. Sign a documented
   domain-separated, length-prefixed byte frame of these fields; do not rely
   on protobuf map order or unsigned broker headers. Keep this keypair
   separate from the Cell gRPC context key.
2. Change the authenticated emitters in
   `crates/flare-im-message-pipeline/src/messaging/push_repository.rs` and
   `flare-push/proxy/src/infrastructure/mq_publisher.rs` to publish that record
   only after their ingress has established a verified tenant. Remove or lock
   down PushProxy's unused direct `publish_push_task` API. Inventory any WAL
   replay publisher separately: a reconstructed durable row is not by itself
   a producer identity.
3. In `flare-push/server/src/interface/messaging/{message_consumer,event_consumer,push_consumer}.rs`,
   verify the record with a producer/topic allowlist **before** decoding or
   routing; compare signed tenant with nested `MqEnvelope`/`PushEnvelope` and
   Context. In `flare-push/server/src/infrastructure/mq/publisher.rs`, issue
   a newly signed derived record for `.online`/`.offline` with a dedicated
   Push Server key and source event ID.
4. In `flare-push/worker/src/interface/messaging/{online_consumer,offline_consumer}.rs`,
   verify the Push Server signature and nested `PushTaskEnvelope` tenant,
   target and conversation; store dedup state by `(producer, tenant, event_id)`
   after successful processing. Broker redelivery after a transient error must
   remain retryable; do not claim exactly-once APNs/FCM delivery. Only the
   verified task may enter `online_gateway_delivery.rs` and obtain a
   method-scoped Gateway/Online workload token. Prefer a dedicated Worker
   signing key/verifier over distributing the all-purpose Cell private key.
5. Add broker identity and topic/subject publish-consume ACL provisioning to
   the deployment manifests (including production `flare-deploy`), plus key
   rotation and alerting. Start with `signed-v1` disabled; rehearse replay of
   the old unsigned backlog while the legacy consumer is still isolated.
   Old unsigned records cannot be made trustworthy by a migration reader that
   merely copies their `tenant_id`. After verified cutover, invalid signatures
   go to a protected quarantine stream with alerting and ACK only after that
   write succeeds; ordinary unavailable dependencies NACK without DLQ.

Focused verification:

```bash
cargo check -p flare-signaling-online -p flare-signaling-gateway -p flare-api-gateway -p flare-push-worker -p flare-push-server --all-targets
cargo test -p flare-signaling-online --lib
# With an isolated Redis instance at FLARE_TEST_REDIS_URL:
cargo test -p flare-signaling-online --lib admission_is_atomic_tenant_scoped_and_kicks_only_old_physical_connection -- --ignored
cargo check -p flare-im-ack -p flare-storage-writer --all-targets
cargo test -p flare-signaling-gateway --lib same_conversation_id
cargo test -p flare-signaling-gateway --lib relay_rejects
cargo test -p flare-im-service-kit --lib conversation_delivery_rejects_unsigned_tenant_metadata
cargo test -p flare-push-worker --lib provenance_gate_tests
cargo check -p flare-push-proxy -p flare-push-server --all-targets
cargo test -p flare-push-proxy --lib identity_boundary_tests
cargo test -p flare-push-proxy --lib identity_startup_tests
cargo test -p flare-push-proxy --lib identical_task_ids_in_two_tenants
cargo test -p flare-push-server --lib missing_or_noncanonical_tenant
cargo test -p flare-push-server --lib push_envelope_tenant_must_equal_routing_context
```

The Gateway library suite passed 76 tests in the restricted workspace. Two
QUIC port-probe tests require a host socket bind and fail there with
`PermissionDenied`; they were excluded in the restricted rerun and are not
claimed as passing. Run them on an unrestricted CI host before release.
