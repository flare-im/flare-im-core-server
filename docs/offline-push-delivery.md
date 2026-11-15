# Offline mobile push — local development closure

The worker supports regular alert notifications through FCM HTTP v1, native APNs and Getui.
These adapters are not VoIP/Live Activity/Critical Alert implementations. Vendor acceptance
does not mean a phone received or displayed a notification, and offline push is not message sync.

## Normal entry and explicit configuration

The authenticated end-user route is `PUT /api/v1/devices/{device_id}/push-token`, with
`platform`, `provider` (`fcm`, `apns`, `getui`), `token`, `bindingId`,
`expectedVersion` and `expiresAtMs` (camelCase), plus the optional binding attributes
`appBundle`, `environment`, `locale`, `timeZone`, `appVersion` and `capabilities` (absent and
`null` both mean unset). GET on that route takes provider and optional
bindingId for read-back. Unconditional DELETE has been removed.
The gateway and Push Proxy keep the verified User/app identity; body fields cannot choose
another user/app. The internal proto has the same three typed providers. Registration accepts
at most 128 device/provider entries per user; size/control-character and APNs hex checks run
before Redis. A device-bound JWT is mandatory at the public route. Registration uses a
client-generated binding fence and Redis's millisecond timestamp.

The gateway only trims the attributes; Push Proxy validates them before Redis:

| Attribute | Rule |
|---|---|
| `appBundle` | iOS bundle id or Android applicationId, at most 255 bytes of ASCII letters, digits, `.`, `-`, `_`. Required on every provider: the APNs topic, the FCM app, and the package whose open activity a tap on a Getui Android vendor notification starts |
| `environment` | `sandbox` or `production`, and only on an `apns` binding; empty means production |
| `locale` | BCP 47 tag such as `zh-CN`, at most 35 bytes of visible ASCII |
| `timeZone` | IANA name such as `Asia/Shanghai`, at most 64 bytes of visible ASCII |
| `appVersion` | At most 64 bytes of visible ASCII |
| `capabilities` | Unique values out of `dataMessages` (Android receives FCM data messages) and `serviceExtension` (iOS notification service extension); stored sorted |

The whole record, attributes included, is part of the digest that identifies a registration
attempt: repeating it is idempotent, and changing any attribute under the same `bindingId`
conflicts, so a client whose bundle, environment, locale or capabilities changed registers a new
binding with `expectedVersion` set to the current one.

`PUSH_WORKER_OFFLINE_DELIVERY_BACKEND` defaults to `disabled` (DLQ/explicit parking,
not silent phone delivery). Choose `fcm`, `apns` or `getui` for one provider, or `mobile` plus
an explicit unique `PUSH_WORKER_OFFLINE_PROVIDERS=fcm,apns` list. The latter runs at most
three provider futures concurrently, so an unavailable vendor cannot starve another.
`outbox` remains an explicitly selected development backend and is forbidden in production.
With `FLARE_ENV=production` the variable must also be set: an unset backend would fall back to
`disabled` without anyone having chosen it, so the worker refuses to start before connecting
to MQ. An explicit `disabled` is a deliberate choice (a deployment without mobile clients still
needs the worker for online push); it starts and logs one warning that offline push is off.

Mobile backends require `PUSH_WORKER_MOBILE_APPLICATIONS_FILE`, a private regular single-link
0600 JSON mount (at most 4 MiB/1,024 applications), owned operationally by push-worker.
Schema `flare.mobile-push-applications/v2` contains an `applications` array; each entry has
an exact `app_id` plus optional `fcm`, `apns`, and `getui` credentials. Unknown fields, duplicate
applications, empty providers and providers outside the explicit backend allowlist reject startup.
A `flare.mobile-push-applications/v1` document is refused with an error that names v2: its APNs
record had a single `topic` and a global `sandbox` switch, which v2 replaces with the allowed
`topics` and the per-binding environment.
There is no default application/project/topic or global environment credential fallback; old
global vendor credential variables explicitly reject. Normal attested app context must match
the task before an executor is selected. An unconfigured application whose target device has a
push binding goes to the existing terminal failure/DLQ path instead of borrowing another
application's credentials; a target device with no binding needs no vendor and completes without
a send. For a configured application, a binding of the target device on an unconfigured provider
is also an explicit terminal configuration failure, not a silent success. Configured channels
still run first; existing acceptance receipts apply if the operator later replays the DLQ.
Only the target device's bindings are consulted; another device of the same user never decides
the outcome of this task.

Each `fcm` record is the service account object; its OAuth destination is fixed to
`https://oauth2.googleapis.com/token`. Each `apns` record has `team_id`, `key_id`,
`private_key_pem` and `topics`, the bundle ids this key may push to (at least one, unique, at
most 64). `getui` has vendor `app_id`, `app_key`, `master_secret` and an optional
`android_vendor` object with the optional `huawei_category`, `honor_importance`,
`xiaomi_channel_id`, `xiaomi_template_id`, `xiaomi_group_channel_id`,
`xiaomi_group_template_id`, `oppo_channel_id`, `oppo_private_template_id`, `oppo_notify_level`
and `vivo_category` (short visible-ASCII identifiers; `oppo_notify_level` is 1, 2 or 16). A
Xiaomi channel and its template come together, the group pair needs the direct pair, and an
OPPO notify level needs the OPPO template; anything else rejects startup (a snapshot record
that breaks a rule is left out by the publisher). Its credential destination is fixed to the official
`https://restapi.getui.com/v2/<app_id>`; an old custom `...BASE_URL` cannot redirect secrets.
Credentials are validated at startup and never printed. Refresh is single-flight per application/vendor executor.

```json
{
  "schema": "flare.mobile-push-applications/v2",
  "applications": [{
    "app_id": "0",
    "apns": { "team_id": "ABCDE12345", "key_id": "FGHIJ67890", "private_key_pem": "...",
              "topics": ["com.example.app", "com.example.app.beta"] },
    "fcm": { "project_id": "...", "client_email": "...", "private_key": "..." },
    "getui": { "app_id": "...", "app_key": "...", "master_secret": "...",
               "android_vendor": { "huawei_category": "IM", "xiaomi_channel_id": "100001",
                                   "xiaomi_template_id": "xm-direct" } }
  }]
}
```

Static mode is a **private per-application operational configuration**, not a tenant self-service
credential UI, encrypted Platform credential database or hot-rotation service. Different
applications may now share a delivery Cell without sharing a project/topic. Operators validate
bundle/project/environment mappings before supplying a new file and restarting the future runtime;
this path does not replace any existing runtime secret. No phone/vendor delivery is implied.

For a live configuration authority, choose `PUSH_WORKER_MOBILE_SNAPSHOT_FILE` instead of
the static file. It uses the same closed document plus `valid_until_ms` (at most 30 seconds
in the future) and optional per-application `disabled_providers`. Core rechecks within one
second of a delivery, preserves executors/OAuth caches for unchanged credentials, and
rejects missing/invalid/expired snapshots without stale/static/cross-app fallback. Refresh
has a two-second file-read budget and try-lock admission. Explicitly disabled providers
intentionally do not send; a registered provider absent from configuration remains an error.
The file remains private and read-only to Core; any trusted producer can implement this
contract. There is **no dependency on Platform, staff roles, organizations or its SQL/API**.

Core's default service Hook profiles likewise remain empty/business-neutral. Product
deployments must supply their own policy profile explicitly. End-to-end product acceptance
must verify the policy is installed and actually denies unauthorized sends.

The conditional lifecycle is now implemented in Rust and generated bindings; see
`../../flare-core-sdk-engine/docs/native-push-lifecycle.md`. Token/permission callbacks and
resume/network recovery have a thin Flutter bridge. No Firebase/APNs plugin or device is
automatically initialized, and OS channels/background display/badges still need host/device
integration. Platform's encrypted self-service source and leased publisher are implemented
separately in Social; their actual installation and mounted full-stack validation remain R6 work.

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

Server-initiated revocation is a separate entry to the same authority.
`PushService.RevokeDeviceBindings` is called by Online when a device is kicked, replaced by
another device, removed by a tenant kick, or its identity session is revoked; reasons are
`kicked`, `device_replaced`, `tenant_kicked` and `identity_revoked`, used for the log line only.
Push Proxy accepts it from the signed `flare-signaling-online` Service identity alone (not the
gateway, never a user), and the body tenant must equal the signed tenant. It names a user, an
optional device (empty means every device of that user in the tenant) and a mandatory
`registered_before`: only bindings whose Redis registration time is at or before it are
revoked, on every provider. Revocation arrives asynchronously, so a binding the device
registered after logging in again is newer than the revoking event and survives it. Each
binding is removed by the same script step as the client revocation, atomically per call:
the slot keeps its version but owns nothing, the registry row is deleted and the binding id
becomes a 24-hour tombstone. Read-back therefore reports `revoked`, the same binding id cannot
be registered again, and a CAS on the kept version can. The response is the number of bindings
revoked; zero is success and a repeat returns zero. One info line per call records tenant,
user, device (or `all`), reason and count, never a token. A revoked device does not get its
binding back by reconnecting: it has to register a new one.

### Activity and lazy expiry

Each binding record carries `last_seen_ms`, 0 until the device first inspects it. The client
inspects its binding (GET on the push-token route) every time it returns to the foreground;
when the caller owns the slot and the inspected binding is the current one (`state` is
`active`), the inspect script stamps the row with Redis's millisecond time in the same step.
Another user's or a stale binding id's inspection changes nothing.

A binding whose last activity (`last_seen_ms`, or `registered_at_ms` when never seen) is older
than 60 days is absent. Nothing sweeps the registry; expiry is applied where bindings are read:

- The router does not count it as a bound device, so no offline task is planned for it. The
  router only reads; a device bound through two providers stays bound while either is active.
- The worker skips it when it looks up the task's device and removes it with the same
  conditional removal it uses for a vendor-reported dead token (same registration id and token,
  the slot keeps its version, the binding id becomes a 24-hour tombstone), and in addition only
  while the row is still inactive, so an inspection that lands meanwhile keeps the binding. A
  failed removal is logged and retried by the next lookup; the binding is skipped either way.

`PUSH_SERVER_BINDING_INACTIVE_DAYS` and `PUSH_WORKER_BINDING_INACTIVE_DAYS` (default 60; zero or
unparsable means the default) must agree. The device registers a new binding after expiry like
after any other revocation.

Workers recheck the actual current slot after credential acquisition and before sending to
FCM/APNs (before Getui's client call). Vendor invalidation clears only the observed binding
and leaves its revocation fence. This is not an atomic transaction with an external vendor:
a request already in flight can still arrive. Offline logout retains pending cleanup and
does not claim server revocation until a later successful reconcile.

## Router: per-device reachability, handoff and catch-up

Push Server decides per device, not per user, whether the link or the push channel owns the
alert for a new message. For every recipient it reads two things: the Online session records
(Redis hash `session:{tenant}:{user}`, one JSON value per connection; with the gRPC Online
backend, `BatchGetUserPresence`) and the binding registry that Push Proxy writes.

- A device is live when it has a session whose `app_state` is `active` or `standby`. A record
  without `app_state` counts as `active`. A device whose only sessions are `parking` or
  `deferred` is not live: its connection is still up, but the push channel owns its alerts.
  An unknown value is not live either. The gRPC backend applies the same rule to
  `DeviceInfo.app_state` (`ACTIVE`, `STANDBY` and unspecified are live). A session record that
  cannot be parsed, or has no `device_id`, still counts as a session but makes no device live.
- The online task goes to every recipient that has any session, whatever its app state. Mute,
  mention-only and device state never narrow it.
- An offline task is emitted at routing time for each device that has a push binding and is
  not live. A `deferred` phone therefore receives the message over its link and a push for the
  alert, with no ack timeout and no handoff per message; a live desktop does not suppress the
  push to the same user's parked phone. The sender's own devices are skipped. Mute and
  mention-only are applied here and only here, and a failed preference lookup lets the push
  through.
- Bindings are read through a 30-second in-process cache that also remembers empty answers.
  When the registry cannot be read the record's offline tasks are skipped and online delivery
  continues. A binding inactive for `PUSH_SERVER_BINDING_INACTIVE_DAYS` (default 60) does not
  count; a record whose times cannot be read still does, and the worker judges it.
- With no upstream expiry a `loud` or `digest` task expires after 24 hours and every other
  disposition after 10 minutes.

`PUSH_SERVER_DEVICE_TOKEN_REDIS_URL` (default: the `push` Redis profile, then
`redis://127.0.0.1:6379`) and `PUSH_SERVER_DEVICE_TOKEN_KEY_PREFIX` (default
`flare:im:push:device_tokens`) must name the same registry as the `PUSH_PROXY_DEVICE_TOKEN_*`
and `PUSH_WORKER_DEVICE_TOKEN_*` settings; otherwise the router sees no binding and emits no
offline task. `PUSH_SERVER_CONVERSATION_READ_ENDPOINT` and
`PUSH_SERVER_EVENT_PING_PARTICIPANT_PAGE_SIZE` are no longer read.

A device whose alerts move from the link to the push channel after frames were already
written to it is reported with `PushService.ReportDeviceHandoff`. Push Proxy accepts it from
the signed gateway and Online Service identities only, checks that it belongs to the signed
tenant, names a user, a device and a cause, and carries at most 128 pending items, then
publishes it to `flare.im.push.handoff` keyed by user. Push Server consumes that topic and
emits catch-up alerts for that one device:

- `parked`, `ack_timeout` and `link_lost` carry the unacknowledged items. They are collapsed
  to the newest seq per conversation, most recently written first, at most 16 conversations.
- `gateway_lost` carries none. Push Server asks Conversation
  (`ConversationInternalReadService.ListRecentUnreadConversations`, signed Service identity
  required) for the user's conversations with new unread messages since `since`, takes up to
  20 candidates and emits at most 5 alerts. A handoff without `since` alerts nothing.
- A device without a binding gets nothing; the handoff reads the registry directly, not
  through the cache. Conversations the user has left or muted, the user's own messages and
  messages that never alert are skipped, and mention-only is applied to the message that is
  read back. When the message cannot be read back the task carries a
  placeholder and the worker falls back to its generic text.
- With the anti-disturbance gate enabled, every caught-up conversation goes through `decide` and
  the gate like a normal alert, on the bulk lane: the device just lost its link, but a burst of
  catch-ups still rings only as the gate allows, a message the gate already handled for the device
  only updates its notification quietly, and with another attentive device the catch-up is
  delayed and cancelled by a read. A device reachability lookup that fails only means no other
  device counts as attentive. With the gate disabled each caught-up conversation is one task.
- A catch-up task has the idempotency key normal routing computes for the same message, device
  and disposition, and that key is the broker message id, so a redelivered handoff or a race with
  normal routing is dropped inside the broker's duplicate window instead of ringing twice.
- A downstream that is temporarily unavailable makes the broker redeliver the whole handoff;
  a malformed or foreign-tenant handoff, or a read the Conversation service refuses, goes to
  the DLQ. Catch-up never publishes an online task.

Router counters: `push_server_offline_tasks_total{disposition}`,
`push_server_offline_lane_tasks_total{lane}` (`direct`, `group`, `bulk`; the same tasks by
priority lane), `push_server_device_handoffs_total{cause}` (a redelivered handoff counts again),
`push_server_catch_up_alerts_total` (also counted as offline tasks) and
`push_server_binding_lookup_failures_total`. The gate's and the waves' counters are listed at the
end of their sections.

## Anti-disturbance gate

After the filtering above, every candidate device goes through the notification rules of
`flare-protocol/flare-notify-rules`, the single implementation the client engine also runs. Push
Server does not re-implement them: it reads the persisted state, runs the crate in process and
writes the result back with a compare-and-set. Catch-up alerts from a handoff and the batches of
a group conversation's waves (next section) go through the same decision and gate.

### Decision inputs

Per device, `decide` gets:

- the recipient's alert class (`mention` when the message names the recipient or `@all`) and
  whether it names them (`unknown` when the content cannot be parsed, which only matters for
  mention-only);
- the conversation preferences already read for mute and mention-only;
- the user's notification settings (quiet hours, preview, multi-device policy) from
  `ConversationInternalReadService.BatchGetUserNotificationSettings`, one batch per routing pass
  for the users that have candidate devices, cached in process for 60 seconds per (tenant, user),
  defaults included; a failed read uses the defaults (no quiet hours, full preview, delay);
- the device's local minute of week, from the binding's IANA `time_zone` at the current instant
  (daylight saving included); a binding without a zone, or with one that is not recognised, is
  computed in UTC;
- whether another device of the user is attentive: a live session whose `app_state` is `active`
  (or absent) and whose `attention` is `attentive` (gRPC backend: `DeviceInfo.app_state`
  `ACTIVE`/unspecified and `attention` `ATTENTIVE`).

The outcome: `drop` emits nothing; `badge_only` updates the badge and, for an iOS device, emits a
`badge_only` task (no collapse key, so it never replaces the notification); `delay` registers a
delayed alert (below); `quiet` and `candidate` go into the gate. A push that belongs to no
conversation (system notice, custom push) has no slot: it is decided but never coalesced, delayed
or counted in the badge. Bindings carry `time_zone` and `platform`: a device is iOS when one of
its bindings is `apns` or registered as `ios`; the profiles are read from the same registry as
the bound devices, through their own 30-second cache.

### Gate store

The gate state lives in push Redis: `PUSH_SERVER_GATE_REDIS_URL`, by default the `[redis.push]`
profile (the binding registry's instance), then `redis://127.0.0.1:6379`. Every key segment that
holds an id is length-framed (`{bytes}:{value}`), so ids containing colons cannot collide; a
conversation appears only as the 32-hex digest of its collapse key. `{device}` below is the
framed tenant, user and device.

| Key | Value | Expiry |
|---|---|---|
| `push:gate:dev:{device}` | Device budget: the crate's 169-byte state followed by a JSON tail (the last applied "back to active" marker) | 24 h after the last write |
| `push:gate:slot:{device}:{conversation}` | Conversation slot: the crate's 58-byte state followed by a JSON tail (conversation id and kind, newest message id/seq/alert class, the message currently shown with the device generation and time it was shown), about 150 bytes | slot idle expiry (30 min by default) + 10 min |
| `push:gate:delay:{device}:{conversation}` | Delayed alert (hash: conversation, kind, tier, newest message id/seq/alert class, count, created, due) | delay + 10 min |
| `push:gate:badge:{device}` | Badge ledger (hash: `b` base, `s` sum of contributions, `m` time of the adopted base, `c:{conversation}` per-conversation contribution) | 7 days after the last write |
| `push:gate:shown:{tenant}:{conversation}:{message digest}` | Set of framed (user, device) that were shown this message | as the slot |
| `push:gate:wave:{tenant}:{conversation}` | A group conversation's wave (hash: `v` version, `s` state JSON, `m` body of the newest held message, at most 16 KiB) | the longer of the @all interval and the top wave interval, + 10 min |
| `push:gate:due:{shard}` | Due queue (sorted set, score = due time in ms; member `t:` or `d:` followed by the framed tenant, user, device and conversation, or `w:` followed by the framed tenant and conversation) | none |

The slot TTL is longer than the crate's idle expiry, so expiring keys and the crate's lazy expiry
agree; a value that does not decode is treated as absent, like the crate does.

One admission is two round trips: `MGET` of the slot and the device, then one Lua script. The
script compares bytes 1..9 of each stored value (the crate's version field; an absent or short
value counts as eight zero bytes) with the bytes read, and only when both match writes the slot
and the device, adds or removes the conversation's due entry, adds the device to the shown index
and applies the badge change, all at once. On a mismatch nothing is written and Push Server
reads, recomputes and tries again, three attempts in all; after that the message is let through
as a conversation's first (`fail_open`: loud for normal and priority tiers, quiet for the quiet
tier) without touching state. When push Redis is unavailable (error or an 800 ms timeout) the
same operation runs against an in-process gate with the same rules and state per instance,
bounded to 100 000 entries per kind; every new conversation still alerts at least once and the
noise bound holds approximately. Device signals and settings that cannot be read are treated as
absent.

### Outcomes and tasks

- `loud` and `quiet` emit one task for the device with that disposition and the slot's count
  (`[N条]` on the title). The task's `preview` is the user's preview made stricter by the
  tenant's forced hidden preview; tasks to an iOS device carry the absolute `badge`.
- A batch (a delayed alert that is due, a wave) enters the gate as its number of messages at once,
  capped at 1 000: one task at most, showing the newest message with the full count, and the badge
  grows by the batch's count.
- `defer` registers the slot's tail refresh in the due queue at the crate's due time; `count_only`
  only counts (the tail is already registered). Neither emits a task.
- A record the broker redelivers (the message is already the slot's newest) changes nothing in the
  gate and emits one `quiet` update with the current count, so a retry neither rings twice nor
  inflates the count or the badge.
- In digest mode a ringing digest emits an extra `digest` task with the device-wide collapse key
  `d:` + 32 hex (`device_digest_collapse_key`), `count` = messages and `digest_conversations` =
  conversations of the period, an empty `conversation_id` (it opens the conversation list) and
  `conversation_seq` 0; the conversations' own notifications keep updating quietly.

### Due queue: tail refresh and delayed delivery

The due queue is sharded by device (`due_shards`, default 16); every instance polls every shard
every `due_poll_interval_ms` (default 250 ms) and claims at most `due_batch` (default 256) entries
per shard with a script that reads and removes them at once, so an entry is handled by one
instance. A claimed entry that fails is dropped: the notification shows older content or count
until the next message or until the app opens. Each entry is handled with a context signed as
Push Server for the entry's tenant (the Cell private key), because there is no broker record to
re-attest.

- **Tail refresh** (`t:`). The router reads the device's signals, loads the slot and the device,
  runs `on_due` and writes back. `Tail` loads the newest message recorded in the slot through the
  same Storage Reader lookup handoff uses (a placeholder when it cannot be read, so the worker uses
  generic text), and emits a `quiet` task with the slot's count and, for iOS, the current badge.
  `Defer` re-registers the entry (vendor cap); `Nothing` (read, expired, or already shown) emits
  nothing.
- **Delayed delivery** (`d:`). A `delay` decision (another device attentive, policy delay, 15 s)
  writes `push:gate:delay:...` and registers its due time; later messages of the same
  conversation for the same device are added to that record (count + 1, newest message, highest
  tier) and keep the first due time. A read of the conversation on any device up to the newest
  message cancels the record and its entry. When due, the record is taken; if the device came
  back to active after the record was created, or the device is live now, nothing is sent;
  otherwise the batch enters the gate as `count` messages at once and emits what the gate says
  (one `loud`/`quiet` task with the full count showing the newest message, plus a digest when one
  rings). When the delay cannot be registered the message enters the gate immediately.
- **Wave** (`w:`). A group conversation's held messages are enumerated once (next section).

### Corrections

Only devices that were actually pushed are corrected, so the cost is bounded.

- **Read elsewhere.** For every read receipt event the router sees (whether it is routed to
  users or broadcast), the reader's bound devices are looked up: the delayed alert of that
  conversation is cancelled when the read covers its newest message, the slot is closed
  (`on_read`), a pending tail is removed, and the conversation's badge contribution is removed. A
  device whose slot was live and sent gets a `clear` task (the conversation's collapse key; the
  new badge for iOS). Otherwise an iOS device whose badge changed gets a `badge_only` task.
  Repeated receipts change nothing.
- **Recall.** For a recall event the shown index lists the devices that were shown the recalled
  message; for each, when its open slot still shows that message, was shown in the device's
  current generation and is not idle, the shown record is cleared (the slot version still moves),
  the badge contribution drops by one, and a `recall_replace` task with the count minus one is
  emitted, with the message read back for the title when possible.
- **Back to active.** Online writes `push:dev:active:{tenant}:{user}:{device}` (Redis time in ms,
  kept 2 days) in its session Redis whenever a device logs in or reports itself `active`. The
  gate reads that marker with the device's other signals and, when it is newer than the marker the
  device state last applied, runs `on_active` first: every slot of the device becomes stale and
  digest mode ends. The marker that was applied is stored with the device state.

### Badge

The badge is an absolute value per device, kept only for iOS devices: base plus the
contributions of at most 500 conversations (contributions of further conversations go into the
base and cannot be removed by a read). When a device reports a non-active state Online writes
`push:dev:badge:{tenant}:{user}:{device}` = `{unread total}:{Redis time in ms}` (kept 2 days); the
gate adopts it when it is newer than the base it adopted, dropping all contributions. Every
admitted or delayed message that counts toward unread (any class but `none`) adds one to its
conversation, a read removes the conversation, a recall of the shown message removes one. Without
a base (a device that disconnected without reporting) the count starts from zero. The base can be
stale until the app opens: messages read on another device before the base was written stay in
it, and the client corrects the badge when it runs.

### Tenant overrides

`[services.push_server.notify]` in `config/services/push-server.toml` holds `enabled`,
`due_shards`, `due_poll_interval_ms`, `due_batch`, and per tenant
`[services.push_server.notify.tenants."<tenant>"]` with `force_hidden_preview`,
`all_mention_interval_ms` (the @all quota of groups above the member cap, default 600 000; zero
or negative is logged and uses the default) and a `gate` table whose field names are those of
`GateConfig` (ladders as arrays of milliseconds). An override is validated with
`GateConfig::validate`; an invalid one is logged and the tenant uses the defaults (its forced
preview still applies). The shared typed configuration does not know this table, so Push Server
reads it from the same files the loader merges (`base.toml`, `shared/*.toml`, `services/*.toml`,
`overrides/*.toml` under `FLARE_CONFIG_PATH`, later files winning), without environment
placeholders. With `enabled = false` every message emits one task per device as before the gate,
and there are no waves either (see below).

### Gate counters

`push_server_notify_decisions_total{decision}` (`drop`, `quiet`, `candidate`, `priority`,
`delay`, `badge_only`), `push_server_notify_gate_outcomes_total{outcome}` (`loud`, `quiet`,
`defer`, `count_only`, `digest`, `tail`, `due_defer`, `due_nothing`, `fail_open`),
`push_server_notify_gate_cas_conflicts_total`,
`push_server_notify_gate_fail_open_total{reason}` (`conflict`, `store`),
`push_server_notify_gate_fallbacks_total`,
`push_server_notify_due_entries_total{kind,result}` (`tail`/`delay`; `emitted`, `cancelled`,
`rescheduled`, `nothing`) and `push_server_notify_corrections_total{kind}` (`clear`,
`recall_replace`, `badge`). Gated tasks are also counted in `push_server_offline_tasks_total`.

## Group conversations: waves, large groups and @all

The orchestrator does not materialize the recipients of a conversation above
`MESSAGE_ORCHESTRATOR_LARGE_CONVERSATION_PUSH_THRESHOLD` (default 500): the event arrives with an
empty recipient list, Push Server broadcasts the online task per conversation and has to enumerate
the members for the offline push. Doing that for every message makes the cost of an active group
grow with its message rate, so the enumeration is rate limited per conversation. Direct chats and
smaller groups, whose events carry their recipients, are unchanged: one routing pass per message
(mentions on the direct lane, other group alerts on the group lane).

### Waves

- The first message of an idle conversation enumerates its recipients at once and starts an
  interval. Messages that arrive while the interval runs are only accumulated in push Redis
  (`push:gate:wave:*`): their number, the highest alert class, the newest message (id, seq,
  sender and its body, up to 16 KiB) and, per user, how many of them were already pushed to that
  user directly (mentions, the user's own messages; at most 256 users are tracked, others may
  have their count inflated by those few). The first accumulated message registers one `w:` entry
  in the due queue at the end of the interval; later ones only re-register the same member.
- When the entry is due, the batch is enumerated once and every recipient device enters the gate
  with the batch's count less what that user already got, showing the newest message (a
  placeholder with only its ids when the body was not kept). A message that arrives after the
  interval ended but before the due loop claimed it enumerates the held messages together with
  itself.
- Intervals step through 3 s, 10 s, 30 s and 120 s: up one level when messages arrived during an
  interval, down one level for every full interval without messages.
- Mentioned members never wait. The mentioned user ids come from the message content, so they are
  first checked with one membership lookup by id (`preferences_for`); members are pushed at once
  through the normal per-user routing (preferences, rules, gate, mention class, direct lane), and
  the wave does not push that message to them again. When the lookup fails nobody is pushed
  directly; members who are recipients of the wave still get it there.
- An urgent message ends the running interval and enumerates at once.
- Recipients (members and their preferences, or the subscribers below) are cached in process per
  conversation for 15 seconds, bounded to 500 000 recipients in all, so one wave queries once.
- The wave state is soft. Compare-and-set conflicts retry three times; when push Redis is
  unavailable the wave runs on the same in-process store as the gate, whose due entries the same
  due loop claims; when neither works the message is enumerated at once and pushed alone. A
  claimed wave that cannot be processed is dropped: those members see the messages when they open
  the app.
- With the gate disabled there are no waves: every group message enumerates at once and is pushed
  alone.
- A wave holds conversation-level state only, not per-member read positions. A member who reads
  the conversation on another device while the wave runs can still get an alert for those messages
  when the wave fires, unless that device is still attentive at that moment (then the
  multi-device delay applies and the read cancels it). The window is at most one interval.

### Large groups

`PUSH_SERVER_OFFLINE_FANOUT_MEMBER_CAP` (default 1000) is the member cap. The waves of a group
above it enumerate only the members who explicitly enabled "all messages" for it,
read from `ConversationInternalReadService.ListNotificationSubscribers` (signed Service identity;
members in the conversation without mute or mention-only, ordered by user id, at most 5 000). The
size check is one bounded page of `cap + 1` participants. Mentioned members are pushed directly as
above, whether or not they subscribed.

### @all

- In a group within the cap, @all is a mention for every member: it ends the running interval and
  enumerates at once, every member is alerted with the mention class (mention-only members
  included, muted ones not), on the bulk lane.
- In a group above the cap, @all is one full-membership wave: it enumerates every member page by
  page (500 per page through `ListConversationParticipants`, at most 1 000 pages), pushing each
  page before reading the next, so nothing grows with the group. It runs in the background of the
  instance that received the message, so the record is acknowledged without waiting for the whole
  membership; members not reached when that process stops see the message when they open the
  app. Subscribers get the held messages with it; everyone else gets this one message. It is
  allowed once per conversation per
  `all_mention_interval_ms` (default 10 minutes, per tenant); beyond the quota the message counts
  as a normal message of the conversation (subscribers only, normal class), and explicitly
  mentioned members are still pushed directly. When the wave state cannot be used, @all above the
  cap is never widened to the whole membership. Who may use @all is a group permission of the
  product layer; Core only enforces the rate and the lane.

Wave counters: `push_server_notify_waves_total{event}` (`immediate`, `started`, `held`, `fired`,
`fail_open`), `push_server_wave_recipients_enumerated_total{audience}` (`members`,
`subscribers`, `everyone`), `push_server_wave_direct_mentions_total` and
`push_server_all_mentions_total{result}` (`members`, `wave`, `throttled`).

## Priority lanes

Offline tasks are published on three subjects inside the `FLARE_PUSH` stream (`flare.im.push.*`,
the last token has no dot), each consumed by its own worker consumer, so a bulk backlog never holds
up direct chats:

| Subject | Push Server setting | Carries | Worker consumer group, concurrency |
|---|---|---|---|
| `flare.im.push.offline` | `PUSH_SERVER_PUSH_OFFLINE_TOPIC` | direct chats and pushes outside conversations, urgent, mentions, corrections (`clear`, `badge_only` after a read, `recall_replace`), delayed alerts of those | `{group}-offline`, `PUSH_WORKER_OFFLINE_CONCURRENCY` (64) |
| `flare.im.push.offline_group` | `PUSH_SERVER_PUSH_OFFLINE_GROUP_TOPIC` | normal group alerts, waves of groups within the cap, tail refreshes | `{group}-offline-group`, `PUSH_WORKER_OFFLINE_GROUP_CONCURRENCY` (32) |
| `flare.im.push.offline_bulk` | `PUSH_SERVER_PUSH_OFFLINE_BULK_TOPIC` | waves of groups above the cap, @all waves, digests, catch-up | `{group}-offline-bulk`, `PUSH_WORKER_OFFLINE_BULK_CONCURRENCY` (16) |

The worker reads the same subjects from `PUSH_WORKER_PUSH_OFFLINE_TOPIC`,
`PUSH_WORKER_PUSH_OFFLINE_GROUP_TOPIC` and `PUSH_WORKER_PUSH_OFFLINE_BULK_TOPIC`. Online tasks keep
their own consumer in `{group}` (`PUSH_WORKER_CONSUMER_GROUP`, default `push-worker`) with
`PUSH_WORKER_ONLINE_CONCURRENCY` (128). Each consumer has its own JetStream durable (or Kafka
group); a consumer only prunes stale durables of its own group, so the four never remove each
other's. The online consumer's durable starts at new messages when it is created, so narrowing
its subscription never replays stale online tasks; the offline consumers start at the beginning
of the stream (receipts and expiry keep a replay from ringing twice). All four share the retry
budget and dead-letter to `flare.im.dlq.push-delivery`.

## Per-device tasks and disposition

An offline task targets one device, not a user. The router plans per device and fills the
per-device fields of `PushTaskEnvelope`; the worker reads only that device's registry fields
(`provider:device_id`, at most one binding per provider) and never lists the user's other
devices. A task without `device_id`, or with an unspecified/unknown `disposition`, is
malformed: it goes to the DLQ and is never fanned out to all devices. An expired task is
acknowledged first, whatever its shape. A `collapse_key` longer than 64 bytes or outside
visible ASCII is a terminal rejection, because it becomes an HTTP header.

| Field | Meaning for the worker |
|---|---|
| `device_id` | Target device; required |
| `collapse_key` | The same key replaces the earlier notification (one per conversation); empty means no replacement |
| `thread_key` | Grouping key in the OS notification centre |
| `disposition` | What the task does on the device; required, see the table below |
| `alert_class` | Only `urgent` changes delivery (time-sensitive on a `loud` task); always forwarded to the client |
| `count` | Unread messages the notification stands for; above 1 the title is prefixed `[N条] `, capped at `[99+条] ` |
| `badge` | Absolute badge; absent leaves the badge alone |
| `preview` | The recipient's preview (user setting made stricter by the tenant); unspecified means full |
| `digest_conversations` | Conversations a `digest` task stands for (its `count` is the messages) |
| `conversation_seq` | Forwarded so client code can drop an older update |
| `idempotency_key` | Task identity for acceptance receipts |

Replacement on the device uses each vendor's own control: the `apns-collapse-id` header, on
FCM to Android the payload's `collapse_id` (the device's platform module replaces by it; no
`android.collapse_key` is sent), and
Getui `notify_id` (first four bytes of the key's SHA-256, sign bit cleared, 0 mapped to 1) on
its Android vendor notification with `apns-collapse-id` on its iOS channel.

### Keys for client code

Every vendor carries the same snake_case keys. APNs keeps them as top-level custom keys next
to `aps`, numbers as numbers; FCM `data` and the Getui transmission carry string values.

| Key | Value |
|---|---|
| `uid`, `tenant_id` | Recipient user and tenant; the client drops a push for another account |
| `conversation_id`, `message_id`, `seq` | Newest message represented; `message_id` may be empty for a catch-up without a loaded message |
| `collapse_id` | The task's `collapse_key` (`c:` + 32 hex), one notification per conversation. Not named `collapse_key`: that is a reserved FCM data key the Android SDK removes from the data an app can read |
| `thread_id` | The task's `thread_key`, the grouping key |
| `disposition`, `alert_class` | Lower-case names, e.g. `recall_replace`, `mention` |
| `count` | Unread messages the notification stands for, at least 1 |
| `badge` | Absolute badge, only when the task carries one |
| `title`, `body` | Server-rendered text, in FCM `data` to an Android device and in the Getui transmission only. On APNs, and FCM to an Apple device, they are in `aps.alert` (FCM: the `notification` block); the iOS platform module copies them into the map |
| `sound` | `1` when the notification may ring, else `0` |

`show_badge`, `play_sound`, `collapse_key` and the Getui camelCase keys (`collapseId`,
`alertClass`, `playSound`, `showBadge`, `userId`, `deviceId`, `payloadKind`, `provider`) are no
longer sent.

### Per vendor

- **APNs.** `apns-topic` is the binding's `app_bundle`, which must be listed in the application's
  `apns.topics`; the host is the sandbox when the binding's `environment` is `sandbox`, production
  otherwise. A binding without an app bundle, or with one not in `topics`, is a terminal error
  with no vendor call, counted by `push_worker_apns_binding_rejected_total{reason}`
  (`no_app_bundle`, `topic_not_allowed`).
- **FCM to an Android binding** is a data-only message: no `notification` block and no
  `android.notification`. `data` holds every key above; `android.priority` is `high` for `loud`
  and `digest` and `normal` otherwise; there is no `android.collapse_key` (a collapsible message
  counts against FCM's per-device burst of 20 with one refill every 3 minutes; the platform module
  replaces its notification by the payload's `collapse_id` and the gate bounds the volume);
  `android.ttl` is capped by the remaining task lifetime. The Android platform module renders it
  on its channels `flare.messages`, `flare.quiet`, `flare.mentions` and `flare.urgent`.
- **FCM to an Apple binding** keeps the `notification` block (for alerts) and the `apns` block,
  whose `aps` and headers are those of direct APNs; `data` holds the keys without `title` and
  `body`; there is no `android` block.
- **Getui.** `push_message.transmission` is the JSON of the keys (string values, with `title` and
  `body`); the app's code receives it while the Getui channel is online. `push_channel.ios`
  carries the APNs alert with the same JSON as its `payload`. For `loud` and `digest` only,
  `push_channel.android.ups` adds the vendor notification the phone maker's system shows when
  the Getui channel is offline: title, body, `notify_id`, and `click_type: intent` with
  `intent://open#Intent;scheme=flarepush;launchFlags=0x14000000;component=<app_bundle>/com.flare.im.push.FlarePushOpenActivity;S.flare_push=<keys>;end`,
  which opens the Android platform module's open activity (also `flarepush://open`) with every
  key except `title` and `body` as one URI-encoded JSON object in the string extra
  `flare_push`; the text stays out because routing does not need it and makers bound the intent
  length. A Getui binding without an app bundle is a terminal error with no vendor call. Plus
  `options` from `getui.android_vendor`; unset ones are omitted:
  - every push: Huawei `HW` `/message/android/category`, Honor `HO`
    `/android/notification/importance`, vivo `VV` `/category`;
  - direct and group chats only (payload `kind`): Xiaomi `XM` `/extra.channel_id`,
    `/extra.template_id` (group values fall back to direct ones) and `/extra.template_param`, a
    JSON string `{"title","content"}`; OPPO `OP` `/channel_id` (old private channel) and, with a
    private template, the new classification `/category` `IM`, `/notify_level` (default 2),
    `/private_msg_template_id`, `/private_title_parameters` `{"title"}` and
    `/private_content_parameters` `{"content"}` (JSON objects), plus `/app_message_id` (the
    request id). Notices carry no `XM` or `OP` options: those channels and templates are for
    chat, and filing a notification there is what both makers penalise. Getui marks Xiaomi's
    `/extra.channel_id` as required; whether a push without any `XM` options is refused was not
    verified.
  - Text limits: the shared `ups.notification` title and body are cut to 40 and 100 characters
    (the smallest of Getui's per-maker limits: title Xiaomi 50, OPPO 50, vivo 40; body Huawei
    256, Xiaomi 128, OPPO 200, vivo 100); the template variables to Xiaomi 50 and 128 and OPPO
    50 and 200. Counting is in UTF-16 units, cuts fall between grapheme clusters, and a cut ends
    with `…` inside the limit. Xiaomi's values also lose emoji and other `\p{So}` symbols and
    unassigned characters, falling back to the wording below when nothing is left.
  - When the alert shows neither sender nor content (preview hidden, content withheld, nothing
    showable), Xiaomi's template gets the chat kind and the count (`私聊消息` / `群聊消息`,
    `N 条新消息，打开应用查看`; English `Direct message` / `Group message`,
    `N new messages, open the app to read them`) instead of the generic text, which Xiaomi lists
    as a violation for chat. The other makers keep the generic text.
  Quiet updates and recall replacements reach Android through the transmission only, because
  most makers cannot replace a notification silently. The field names, limits and business
  codes were checked against Getui's REST v2 pages ("公共参数", "多厂商参数", "业务返回码说明")
  on 2026-11-12; no sandbox request has been sent with them yet. They all live in
  `android_vendor_channel`, `vendor_options` and the constants of `getui_push.rs`.

| Disposition | APNs, and the `apns` block of FCM | FCM on Android | Getui |
|---|---|---|---|
| `loud`, `digest` | priority 10, `interruption-level` `active` (`time-sensitive` for an urgent `loud`), sound when the content asks for it | high priority data message | transmission, iOS alert with sound when the content asks for it, Android vendor notification |
| `quiet` | priority 5, `passive`, no sound | normal priority data message | transmission, iOS alert without sound; no Android vendor notification |
| `recall_replace` | as `quiet`, body replaced by the recalled-message text | as `quiet` | as `quiet` |
| `badge_only` | `aps` holds only `badge`: no alert, no sound | normal priority data message | not sent |
| `clear` | the badge-only payload when a badge is given, otherwise no vendor call | normal priority data message | not sent |

An APNs alert (`loud`, `digest`, `recall_replace`) to a binding that declares `serviceExtension`
carries `mutable-content: 1`; every alert carries `thread-id` when `thread_key` is set;
`aps.badge` is present whenever the task carries a badge. A `badge_only` task without a badge
has no effect and is acknowledged on every channel. An already displayed APNs banner is not
removed remotely. Without an absolute badge Getui keeps `auto_badge: "+1"` for content that
counts toward the badge; with one it sends the absolute value. The recalled-message text and
the count prefix are fixed Chinese strings like the other fallback texts, not localized.

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
- 5xx, transport/response failure and unresolved acceptance are retryable; throttling (429,
  quota) is handled by the vendor governance below. Terminal provider/config/payload rejection
  goes to the existing DLQ path. Cleanup/receipt failures remain unresolved, not swallowed as
  success. A task covers one device and goes out through one of its bindings (below).
- Per-registration task acceptance receipts in Redis live seven days. The task identity is
  the router's `idempotency_key`, stable across redelivery and republish; a task without one
  is identified by its canonical content, where header map ordering cannot change the identity.
  Broker retry skips an already-accepted binding.
  A vendor ACK followed by process/Redis failure can still duplicate a notification: this
  is at-least-once, not exactly-once. Receipt expiry/eviction also permits another send.
- Envelope expiry is milliseconds throughout Core. APNs headers use seconds; FCM Android
  TTL and Getui TTL are capped by remaining task lifetime. A task at its expiry is not sent.
- `push.attempts{result=completed}` includes no eligible device/backend completion. It is
  not a delivered-device metric. E2EE/redacted messages retain the existing generic preview.
- Pure state/control events and empty, non-display Notification message carriers never create
  OS alerts. The shared business-neutral typed policy is applied before offline fanout and
  again by vendor executors for `loud`, `quiet` and `digest` tasks; `recall_replace`,
  `badge_only` and `clear` do not need a displayable payload. Required inline client events
  are still attempted even when the online projection is stale or the user muted mobile
  alerts. No Social key/metadata hack is used. Mixed batches retain actual display-eligible
  messages.
- Notification content supplies its actual title/body and sound/badge preferences when no
  explicit `offline_push_info` overrides it. The disposition can only tighten this: sound is
  sent for `loud` and `digest` only, and only when the content asks for it. APNs and Getui iOS
  omit default sound for a quiet notice; Getui does not increment the badge when disabled.
  The `sound` key tells client code whether it may ring. **Rendering FCM data messages and the
  Getui transmission on Android is owned by the platform module and not verified on devices**;
  APNs carries an absolute `aps.badge` only when the task supplies one, never a fabricated
  unread count.
- APNs bindings whose app bundle is missing or not an allowed topic are terminal before any
  vendor call (see above). An inactive binding the worker finds is not a failure: it is skipped
  and removed, and a device without other bindings completes without a send.

The worker receives only attested broker records; this change does not bypass provenance,
authorization, message ordering, or the existing bounded broker retry/DLQ policy.

Classification references: [FCM error codes](https://firebase.google.com/docs/cloud-messaging/error-codes),
[APNs responses](https://developer.apple.com/documentation/usernotifications/handling-notification-responses-from-apns),
and [Getui REST v2 codes](https://docs.getui.com/getui/server/rest_v2/code/).

## Vendor governance: one push per task

A device bound through several vendors (an Android phone through Getui and FCM) gets each task
through **one** binding. The worker lists the device's bindings once and tries them in
`[services.push_worker] binding_order` (default `apns`, `getui`, `fcm`; vendors left out follow in
that order). The first binding whose vendor takes the task ends it. The next binding is tried
only when the current one cannot take it: the vendor's breaker is open, the vendor or the local
rate limit throttles, the attempt failed in a way a retry may fix, the token is dead (it is
removed), or the vendor has nothing to carry for this task on this device. A terminal failure
ends the task (DLQ); a registered provider without credentials is a terminal configuration
failure unless the application disabled it.

| Control | Scope | Behaviour |
|---|---|---|
| Concurrency limit | vendor, per worker | `max_concurrency` slots; no slot within 2 s counts as saturated and switches |
| Circuit breaker | vendor | rolling window of `window` calls (minimum `min_calls`), opens at `failure_ratio`, stays open `open_ms`, then `half_open_probes` probes. Only server errors and missing answers count; a refused credential or payload belongs to one application and does not move it |
| Token bucket | vendor and application credential | `rate_per_sec` and `burst`; a token more than 50 ms away marks the application throttled until it |
| Throttle mark | vendor and application credential, or one binding | from the vendor's answer (Retry-After honoured within 1 to 300 s, 2 s without one). APNs answers 429 per device token, so its mark holds that binding only; FCM `QUOTA_EXCEEDED` and Getui's call-frequency codes 10003 and 10005 and daily quota 30009 hold the application |
| Digest coalescer | device | below |

Limits come from `[services.push_worker.vendors.<apns|fcm|getui>]` (defaults: APNs 256 and
2000/s, FCM 256 and 1000/s, Getui 64 and 300/s, breaker 0.5 over 20 calls, minimum 10, open
30 s, 3 probes). The state lives as long as the worker; a credential lease rotation rebuilds
the vendor adapters, not their governance.

**Throttling degrades to a digest.** When every binding that could take the task is throttled,
the task is not retried into the storm: the worker holds it in memory per device (the newest
task per collapse key, at most 64 keys and 10 000 devices), acknowledges the broker message,
and once the vendor is available again hands ONE task per device back to delivery: a `digest`
("M new messages in N chats", device digest collapse key) when more than one conversation is
pending, otherwise the newest task of the one conversation with its count (a quiet refresh
whose loud predecessor never reached the device rings once). Held tasks past their expiry are
dropped. The trade-off is deliberate: the hold is in memory, so a worker that stops loses at
most these late notifications, never messages (the app syncs on open and corrects the badge);
persisting it would add a write to the path of a notification that is already late. A full
hold lets the broker redeliver instead. Flushes that fail for other reasons back off (5, 10,
20, 40 s) and give up after five.

**Switching can notify twice.** A switch after a transport failure or a deadline (acceptance
unknown) may reach the device through both vendors if the first one did accept. The payload
carries `collapse_id` and `seq`, and the device's engine keeps one notification per collapse id
and ignores an alert at an equal or lower seq, so the second arrival replaces or is dropped
rather than ringing again. A switch after an explicit refusal (breaker open, throttled,
nothing to carry) cannot duplicate.

Metrics: `flare_push_vendor_sent_total{vendor, app_version, receipt_capable}`,
`flare_push_vendor_latency_seconds{vendor}`, `flare_push_vendor_failures_total{vendor, reason}`,
`flare_push_vendor_throttled_total{vendor}`, `flare_push_vendor_breaker_state{vendor}` (0
closed, 1 half-open, 2 open), `flare_push_vendor_switch_total{from, to}`,
`flare_push_digest_coalesced_total{vendor}`, `flare_push_digest_flush_total{result}`. The
`app_version` label is client-reported: each process keeps the first 64 well-formed values and
labels the rest `other` (the same helper bounds `flare_push_receipts_total` in Push Proxy).
Every attempt, switch, hold and expiry is also a `vendor` entry of the decision trace.

## Decision trace and arrival receipts

Every push stage writes what it decided for a message and a device into the push Redis (the
instance of the gate and the binding registry), kept 24 hours, so an operator can answer "why did
this message not alert" (Admin Gateway `GET /api/v1/admin/users/{user_id}/push-traces`):

| Stage | Writer | Fields |
|---|---|---|
| `route` | Push Server | `state` (`active`, `standby`, `deferred`, `parked`, `dark`, `unknown`), `path` (`online`, `direct`, `handoff`, `no_binding`, `catchup`, `wave`, `subscriber`) |
| `handoff` | Push Server | `cause`, `pending` |
| `decide` | Push Server | `class`, `decision`, `reason`, `delay_ms` |
| `gate` | Push Server | `result` (`loud`, `quiet`, `defer`, `count_only`, `digest`, `fail_open`), `due_ms`, `count` |
| `task` | Push Server | `disposition`, `collapse`, `lane` (`offline`, `group`, `bulk`), `badge` |
| `vendor` | Push Worker | `provider`, `outcome`, `latency_ms`, `app_version`, `code` |
| `receipt` | Push Proxy | `provider`, `path`, `received_at_ms` (device clock), `app_version`, `disposition` |
| `correction` | Push Server | `kind` (`clear`, `recall_replace`, `badge_only`) |

Every entry also carries `conv`. A trace is keyed by the `message_id` the offline task carries to
the device: the alerting message's server id (a placeholder such as `handoff:{conversation}:{seq}`
when it could not be read back, the task id for pushes outside a conversation), or
`conversation#seq` when a receipt names no message. Offline task envelopes carry that id rather
than the inline event's id, so router, worker and receipt entries land on one trace.
`push:trace:{tenant}:{user}:{key}` holds the newest 128 entries and `push:trace:idx:{tenant}:{user}`
the newest 200 keys. Entries hold labels and ids only,
never message content or tokens. All three processes write through one recorder
(`flare_im_service_kit::push_trace`): a bounded queue drained by a background pipelined writer,
configured in `[services.<service>.trace]`; a full queue or an unreachable Redis drops entries and
counts them in `flare_push_trace_dropped_total{service}` instead of delaying a push.

Devices report arrival receipts through API Gateway `POST /api/v1/push/receipts` (at most 100
per call, same identity rules as the push-token routes); Push Proxy writes a `receipt` entry for
each and counts `flare_push_receipts_total{vendor, app_version, path}`. Divided by the worker's
`flare_push_vendor_sent_total{receipt_capable="true"}` this is the arrival rate per vendor and app
version.

## Deployment settings

Per process, what a deployment has to provide for per-device routing, handoff and revocation:

| Process | Setting | Notes |
|---|---|---|
| Online | (none for the gate) | Writes `push:dev:active:*` and `push:dev:badge:*` in its session Redis on login and on every device state report; Push Server reads them through `PUSH_SERVER_REDIS_URL`, which must name the same instance (both default to `conversation_store`) |
| Online | `SIGNALING_ONLINE_PUSH_PROXY_URL`, `FLARE_INTERNAL_CTX_PRIVATE_KEY` | Push Proxy address (`http(s)://` or `discovery://flare-push-proxy`), no default. Unset or empty: no gateway-lost handoff and no binding revocation. Set without the Cell private key: Online refuses to start. `release/supervise-core.toml` passes the key and, in `FLARE_REGISTRY_MODE=static`, defaults the URL to the container's Push Proxy `http://127.0.0.1:50091` |
| Signaling gateway | `ACCESS_GATEWAY_DELIVERY_ACK_ACTIVE_DEADLINE_MS` (3000), `ACCESS_GATEWAY_DELIVERY_ACK_STANDBY_DEADLINE_MS` (1500), `ACCESS_GATEWAY_DELIVERY_PROBE_TIMEOUT_MS` (5000) | Optional; zero or unparsable means the default. Handoffs go to Push Proxy by discovery (`flare-push-proxy`, falling back to `http://127.0.0.1:50091`), signed with the Cell private key the gateway already holds |
| Push Proxy | `PUSH_PROXY_DEVICE_TOKEN_REDIS_URL`, `PUSH_PROXY_DEVICE_TOKEN_KEY_PREFIX` | Same defaults as Push Server and Push Worker: the `push` Redis profile, then `redis://127.0.0.1:6379`, and `flare:im:push:device_tokens` |
| Push Server | `PUSH_SERVER_GATE_REDIS_URL`, `[services.push_server.notify]` | Gate state (default: the `push` Redis profile). Startup fails when it cannot be reached; at run time an unreachable instance falls back to the in-process gate. The due loop signs its reads with the Cell private key below |
| Push Server | `PUSH_SERVER_DEVICE_TOKEN_REDIS_URL`, `PUSH_SERVER_DEVICE_TOKEN_KEY_PREFIX`, `PUSH_SERVER_BINDING_INACTIVE_DAYS`, `FLARE_INTERNAL_CTX_PRIVATE_KEY` | The registry must be reachable from Push Server. Each consumed record is attested as Push Server with the Cell private key, and that signed context goes with the reads of Conversation's `ConversationInternalReadService` and the message read-back; the internal read service refuses an unsigned read, so without the key `gateway_lost` handoffs go to the DLQ |
| Push Server | `PUSH_SERVER_OFFLINE_FANOUT_MEMBER_CAP`, `PUSH_SERVER_PUSH_OFFLINE_GROUP_TOPIC`, `PUSH_SERVER_PUSH_OFFLINE_BULK_TOPIC` | Optional: member cap above which waves enumerate subscribers (1000), and the group and bulk lanes' subjects (must match the worker's) |
| Push Worker | `PUSH_WORKER_OFFLINE_DELIVERY_BACKEND`, `PUSH_WORKER_MOBILE_APPLICATIONS_FILE` or `PUSH_WORKER_MOBILE_SNAPSHOT_FILE`, `PUSH_WORKER_DEVICE_TOKEN_*`, `PUSH_WORKER_BINDING_INACTIVE_DAYS` | The backend is mandatory with `FLARE_ENV=production`; credentials use schema v2 |
| Push Worker | `PUSH_WORKER_PUSH_OFFLINE_GROUP_TOPIC`, `PUSH_WORKER_PUSH_OFFLINE_BULK_TOPIC`, `PUSH_WORKER_ONLINE_CONCURRENCY`, `PUSH_WORKER_OFFLINE_CONCURRENCY`, `PUSH_WORKER_OFFLINE_GROUP_CONCURRENCY`, `PUSH_WORKER_OFFLINE_BULK_CONCURRENCY` | Optional: lane subjects and per-consumer concurrency (128, 64, 32, 16) |

The three `*_DEVICE_TOKEN_REDIS_URL` and `*_DEVICE_TOKEN_KEY_PREFIX` pairs name one registry, and
the two `*_BINDING_INACTIVE_DAYS` agree. `flare.im.push.handoff`, `flare.im.push.offline_group` and
`flare.im.push.offline_bulk` belong to the `FLARE_PUSH` JetStream stream (`flare.im.push.*`); a
broker ACL or Kafka topic list that names push subjects one by one has to include them
(`deploy/kafka-topics.sh` does). Conversation mounts `ConversationInternalReadService` on its
existing listener and verifies the caller with the Cell public key it already requires.

Before enabling a vendor backend, operators also need:

- **APNs topics.** Each application's `apns` record lists in `topics` every bundle id its
  clients register (an app and its beta build are two topics). In the platform console that is
  the topics list of the application's APNs credentials; the leased snapshot carries it.
  A binding whose bundle is missing from the list is rejected before any vendor call.
- **Android maker categories.** Huawei, Honor, Xiaomi, OPPO and vivo throttle or fold
  notifications that are not in an approved instant messaging category or channel. Apply for the
  IM category in each maker's developer console (approval times are outside our control), then
  put the granted values in the application's `getui.android_vendor` object
  (`huawei_category`, `honor_importance`, `xiaomi_channel_id`, `oppo_channel_id`,
  `vivo_category`).

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
