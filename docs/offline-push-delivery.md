# Offline mobile push — local development closure

The worker supports regular alert notifications through FCM HTTP v1, native APNs and Getui.
These adapters are not VoIP/Live Activity/Critical Alert implementations. Vendor acceptance
does not mean a phone received or displayed a notification, and offline push is not message sync.

## Normal entry and explicit configuration

The authenticated end-user route is `PUT /api/v1/devices/{device_id}/push-token`, with
`platform`, `provider` (`fcm`, `apns`, `getui`), `token`, `bindingId`,
`expectedVersion` and `expiresAtMs` (camelCase). GET on that route takes provider and optional
bindingId for read-back. Unconditional DELETE has been removed.
The gateway and Push Proxy keep the verified User/app identity; body fields cannot choose
another user/app. The internal proto has the same three typed providers. Registration accepts
at most 128 device/provider entries per user; size/control-character and APNs hex checks run
before Redis. A device-bound JWT is mandatory at the public route. Registration uses a
client-generated binding fence and Redis's millisecond timestamp.

`PUSH_WORKER_OFFLINE_DELIVERY_BACKEND` defaults to `disabled` (DLQ/explicit parking,
not silent phone delivery). Choose `fcm`, `apns` or `getui` for one provider, or `mobile` plus
an explicit unique `PUSH_WORKER_OFFLINE_PROVIDERS=fcm,apns` list. The latter runs at most
three provider futures concurrently, so an unavailable vendor cannot starve another.
`outbox` remains an explicitly selected development backend and is forbidden in production.

Mobile backends require `PUSH_WORKER_MOBILE_APPLICATIONS_FILE`, a private regular single-link
0600 JSON mount (at most 4 MiB/1,024 applications), owned operationally by push-worker.
Schema `flare.mobile-push-applications/v1` contains an `applications` array; each entry has
an exact `app_id` plus optional `fcm`, `apns`, and `getui` credentials. Unknown fields, duplicate
applications, empty providers and providers outside the explicit backend allowlist reject startup.
There is no default application/project/topic or global environment credential fallback; old
global vendor credential variables explicitly reject. Normal attested app context must match
the task before an executor is selected. An unconfigured application with a registered mobile
device goes to the existing terminal failure/DLQ path instead of borrowing another application's
credentials; a web-only user with no OS registration needs no vendor and completes without a send.
For a configured application, an existing registration on an unconfigured provider is also an
explicit terminal configuration failure, not a silent all-devices success. Configured channels
still run first; existing acceptance receipts apply if the operator later replays the DLQ.

Each `fcm` record is the service account object; its OAuth destination is fixed to
`https://oauth2.googleapis.com/token`. Each `apns` record has `team_id`, `key_id`,
`private_key_pem`, `topic` and optional `sandbox`; `getui` has vendor `app_id`, `app_key`,
`master_secret`. Its credential destination is fixed to the official
`https://restapi.getui.com/v2/<app_id>`; an old custom `...BASE_URL` cannot redirect secrets.
Credentials are validated at startup and never printed. Refresh is single-flight per application/vendor executor.

This is a **private per-application operational configuration**, not a tenant self-service
credential UI, encrypted Platform credential database or hot-rotation service. Different
applications may now share a delivery Cell without sharing a project/topic. Operators validate
bundle/project/environment mappings before supplying a new file and restarting the future runtime;
this task does not replace any existing runtime secret. Platform self-service and SDK/OS token
lifecycle integration remain explicit work; no phone/vendor delivery is implied.

The conditional lifecycle is now implemented in Rust and generated bindings; see
`../../flare-im-core-sdk/docs/native-push-lifecycle.md`. Token/permission callbacks and
resume/network recovery have a thin Flutter bridge. No Firebase/APNs plugin or device is
automatically initialized, and OS channels/background display/badges still need host/device
integration. Tenant self-service credentials and final runtime installation remain separate work.

## Binding authority and unknown outcomes

The singleton Redis/Dragonfly authority owns one slot per app/provider/device, across users.
A CAS replacement removes the old user's token index and installs the new one in one script.
The index keys are length-delimited hashes; app/user/device delimiters cannot alias entries.
This authority is **not Redis Cluster compatible** and must not be rolled back underneath live
clients. Loss/restore of binding fences is a security-relevant runtime reset, not a safe retry.

Before PUT, the SDK saves a random 256-bit hex revocation capability; bindingId is its SHA256.
`POST /api/v1/push/bindings/revoke` accepts only that capability, requires no expired login,
and cannot register/read a token or choose an app/user. Gateway uses its signed Service identity
for this exact operation. An unknown capability creates a tombstone, so cancellation can win
before a delayed PUT. Anonymous unbound tombstones are capped at 4,096 per authority with
bounded expiry pruning; capacity exhaustion returns unavailable without pretending revocation
succeeded. Existing known bindings can still be revoked. Tombstones and retired slots last 24 hours; commands have at most ten
minutes of validity. Active fences do not expire. Repeating an immutable command is idempotent;
changing its content or stale expectedVersion conflicts. Read-back never returns an OS token
or another account's identity. Public ingress rate limiting still applies to revocation.

Workers recheck the actual current slot after credential acquisition and before sending to
FCM/APNs (before Getui's client call). Vendor invalidation clears only the observed binding
and leaves its revocation fence. This is not an atomic transaction with an external vendor:
a request already in flight can still arrive. Offline logout retains pending cleanup and
does not claim server revocation until a later successful reconcile.

## Delivery and failure semantics

- HTTP connect budget 3 seconds, request/body budget 10 seconds, task budget 30 seconds;
  no redirects, environment proxy or hidden HTTP retry. Vendor bodies are bounded to 16 KiB
  and never copied into diagnostics. Known status/code classifications only are used.
- FCM deletes only a typed `FcmError/UNREGISTERED` with HTTP404. Generic 404 or
  `INVALID_ARGUMENT` may indicate routing/payload problems and must not delete devices.
- APNs deletes only `410 Unregistered` with a valid invalidation timestamp not older than
  the observed registration. Wrong topic/environment, unknown 410 and malformed bodies
  cannot erase a valid new registration. Deletion atomically compares the stored generation
  **and** token, protecting same-token re-registration as well as token rotation.
- 429, 5xx, transport/response failure and unresolved acceptance are retryable. Terminal
  provider/config/payload rejection goes to the existing DLQ path. Cleanup/receipt failures
  remain unresolved, not swallowed as success. One device success never hides another failure.
- Per-registration, canonical task acceptance receipts in Redis live seven days. Broker retry
  skips already-accepted devices; header map ordering cannot change the receipt identity.
  A vendor ACK followed by process/Redis failure can still duplicate a notification: this
  is at-least-once, not exactly-once. Receipt expiry/eviction also permits another send.
- Envelope expiry is milliseconds throughout Core. APNs headers use seconds; FCM Android
  TTL and Getui TTL are capped by remaining task lifetime. A task at its expiry is not sent.
- `push.attempts{result=completed}` includes no eligible device/backend completion. It is
  not a delivered-device metric. E2EE/redacted messages retain the existing generic preview.
- Pure state/control events and empty, non-display Notification message carriers never create
  OS alerts. The shared business-neutral typed policy is applied before offline fanout and
  again by vendor executors. Required inline client events are still attempted even when
  the online projection is stale or the user muted mobile alerts. No Social key/metadata hack
  is used. Mixed batches retain actual display-eligible messages.
- Notification content supplies its actual title/body and sound/badge preferences when no
  explicit `offline_push_info` overrides it. APNs and Getui iOS omit default sound for a quiet
  notice; Getui does not increment the badge when disabled. FCM/APNs carry display hints for
  the app. **Android channel behavior is still owned by the OS/app adapter and not verified**;
  APNs carries a badge hint, not a fabricated absolute unread count. Final native channels,
  background handling and unread badges remain SDK/device work.

The worker receives only attested broker records; this change does not bypass provenance,
authorization, message ordering, or the existing bounded broker retry/DLQ policy.

Classification references: [FCM error codes](https://firebase.google.com/docs/cloud-messaging/error-codes),
[APNs responses](https://developer.apple.com/documentation/usernotifications/handling-notification-responses-from-apns),
and [Getui REST v2 codes](https://docs.getui.com/getui/server/rest_v2/code/).

## Related local verification, not final R6

Focused module tests cover provider classification/TTL, explicit configuration, actual loopback
HTTP send/partial-retry/unknown acceptance/response bounds, signed registration boundaries,
and a separately opted-in **private Redis Unix-socket process** for generation CAS and receipts.
The HTTP peer is controlled test infrastructure, not Firebase/Apple or a real device. The Redis
test creates no shared database, stops its exact child and removes only its own random directory.

The per-application file/factory/routing suite is part of the final worker result: 58 passed,
2 explicitly ignored; strict worker/service-kit all-targets Clippy passed. This includes the
no-default app boundary, private file/size/schema checks and missing registered provider case.
Earlier 57/2 and the preceding worker results overlap; they are not additional coverage.

Core's independent R7 lane (`8e120ee`, `922ffaa`, `caaec1c`, `c8d8673`, `9629a8d`) was
adopted rather than rewritten: tracing, explicit Hook-file
startup failure, sampled execution evidence/per-tenant Hook circuits, unused implementation
removal, and six usage producers. Existing durable audit lifecycle tasks were retained in Media
and Writer. Hook evidence is bounded lossy diagnostic telemetry, not transactional audit;
`0015_20261016_hook_execution_evidence.sql` is source DDL, not an executed upgrade. The old
lane's migration snapshots/compatibility tests were intentionally not adopted. Usage-window
consumption/aggregation and the actual Social/IAM ID lease entry points were subsequently
implemented in the owning Social/Platform modules; see `docs/usage-events.md`. They are not
already installed in the old runtime and do not complete the native mobile lifecycle.

An earlier candidate's six related Core libraries passed 354 tests (3 ignored separately);
the push registry's explicit Redis method passed independently. API push-token tests passed
17 methods using a controlled tonic receiver. Three Hook feature methods passed with an owned
PG18 cluster, minimal current Hook/audit tables, actual production repositories/dispatch and
signed local RPC peers; no existing database or historical upgrade was used. A private actual
JetStream publisher method passed separately. These counts overlap the earlier four-library
151/3 and worker 51/2 runs and must not be added together. Capability/Media default libraries
also passed during integration; their ignored PG/S3/other methods are not implied to have run.
The subsequent typed event/display boundary passed Contracts/Push Server/Worker 119 methods
(2 ignored), followed by affected Worker 54/2 after sound/content projection; these sets also
overlap. Architectural env/tracing/tenant guards passed 9 and supervisor tests passed 16.

Initial failures were retained as failures: the adopted Hook fixture constructed an obsolete
administrator, the new transport module/import needed crate visibility, old tests still expected
`delivered`/APNs rejection, and strict Clippy flagged two nested branches. They were corrected
against the current contract; no production authorization or assertion was weakened to pass.

Computer checks found the old nine-container stack stopped and
`http://tenant.localhost:5188/login` refused connection. No old containers were started,
no user tabs reloaded, and no browser business assertion is counted as passed. Final UI,
Flutter, real vendor credentials/devices and the new eight-resident system remain R6 work
after the remaining functionality is complete.
