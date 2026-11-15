# flare-push-worker

English · [中文](README.zh-CN.md)

Offline push delivery. When a user is offline, messages pass through this worker to a vendor push channel, and finally land in the device's notification tray.

**This is a bottom-line capability for mobile products** — if messages can't be received while offline, the IM can't go to production.

## Choosing the delivery backend

Set `FLARE_ENV=production` in production. The worker then checks the backend before
connecting to MQ. The variable must be set: when it is missing the worker would fall back
to `disabled` and send every offline task to the DLQ without failing loudly, so it refuses
to start. `outbox` is refused as well, because it persists notifications without delivering
them. An explicit `disabled` is accepted — a deployment without mobile clients still needs
the worker for online push — and logs one warning at startup that offline push is off.
Every other backend logs one line at startup, `offline push <backend> backend enabled`
(with the provider list), which the deployment smoke check reads.
This check does not replace sandbox device-delivery acceptance tests.

```bash
PUSH_WORKER_OFFLINE_DELIVERY_BACKEND=mobile   # outbox | getui | fcm | apns | mobile | disabled
PUSH_WORKER_OFFLINE_PROVIDERS=apns,getui,fcm  # mobile only: the vendors this worker sends through
```

| Backend | Coverage | When to use |
|---|---|---|
| `apns` | Native iOS | When you need VoIP calls, Live Activity, or Critical Alert — these are **only supported by native APNs** |
| `fcm` | Android + iOS forwarded via FCM | Products targeting overseas mainly; one integration covers both platforms |
| `getui` | The full lineup of domestic Android | Domestic products: vendor channels such as Xiaomi/Huawei/OPPO/vivo are aggregated by Getui |
| `mobile` | Every vendor listed in `PUSH_WORKER_OFFLINE_PROVIDERS` | Products with several channels: one worker sends each task through one binding of the device (see below) |
| `outbox` | — | Only persist, do not deliver; consumed by an external system. Rejected when `FLARE_ENV=production` |
| `disabled` | — | Turn off offline push. It is the default outside production; with `FLARE_ENV=production` it must be written explicitly |

> To cover several channels, use `mobile` and list them in `PUSH_WORKER_OFFLINE_PROVIDERS`: the worker picks one binding of the device per task. `apns`, `fcm` and `getui` are the same delivery restricted to one vendor.

## Configuration

Vendor credentials are per application, in a credentials document of schema `flare.mobile-push-applications/v2` (one entry per application, looked up by the task's tenant), from exactly one source:

| Source | Variable | Notes |
|---|---|---|
| Static file | `PUSH_WORKER_MOBILE_APPLICATIONS_FILE` | A regular file, not a symbolic link, one hard link, no group or other permission bits (for example `0600`), at most 4 MiB and 1024 applications. A mistake is a startup error |
| Platform snapshot | `PUSH_WORKER_MOBILE_SNAPSHOT_FILE` | Written by the platform every 5 seconds with `valid_until_ms` at most 30 seconds ahead; an expired or unreadable lease makes delivery retry, never fall back |

Setting both is refused. Global credential variables are refused at startup as well, whatever the backend: `PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON`, `PUSH_WORKER_APNS_TEAM_ID`, `PUSH_WORKER_APNS_KEY_ID`, `PUSH_WORKER_APNS_PRIVATE_KEY`, `PUSH_WORKER_APNS_TOPIC`, `PUSH_WORKER_APNS_SANDBOX`, `PUSH_WORKER_GETUI_APP_ID`, `PUSH_WORKER_GETUI_APP_KEY`, `PUSH_WORKER_GETUI_MASTER_SECRET`, `PUSH_WORKER_GETUI_BASE_URL`. The four Getui names are read only by the ignored sandbox test `getui_sandbox_push`.

### APNs

```json
"apns": {
  "team_id": "XXXXXXXXXX",
  "key_id": "YYYYYYYYYY",
  "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...",
  "topics": ["com.example.app", "com.example.app.beta"]
}
```

The `apns-topic` is the app bundle the device binding registered (`appBundle`), and it must be listed in `topics`; otherwise the task goes to the DLQ and counts in `push_worker_apns_binding_rejected_total`. Sandbox or production also follows the binding's `environment`: a device token registered by a development build is only valid in the sandbox and returns `BadDeviceToken` in production. The credentials no longer carry a global topic or sandbox switch.

### FCM

```json
"fcm": {
  "project_id": "example-project",
  "client_email": "push@example-project.iam.gserviceaccount.com",
  "private_key": "-----BEGIN PRIVATE KEY-----\n..."
}
```

### Getui

```json
"getui": {
  "app_id": "ExampleGetuiAppId",
  "app_key": "...",
  "master_secret": "...",
  "android_vendor": {
    "huawei_category": "IM",
    "honor_importance": "NORMAL",
    "xiaomi_channel_id": "100001",
    "xiaomi_template_id": "xm-direct",
    "xiaomi_group_channel_id": "100002",
    "xiaomi_group_template_id": "xm-group",
    "oppo_private_template_id": "op-im",
    "oppo_notify_level": 2,
    "vivo_category": "IM"
  }
}
```

`PUSH_WORKER_GETUI_DEFAULT_TTL_MS` (default 7200000, never above Getui's three days) and `PUSH_WORKER_GETUI_REQUEST_TIMEOUT_MS` (default 5000) tune the Getui calls.

`android_vendor` is optional; every value is what the phone maker approved for the app, and an unset value is not sent. A value is at most 64 visible ASCII characters. Huawei (`huawei_category`), Honor (`honor_importance`) and vivo (`vivo_category`) go with every push. Xiaomi and OPPO file chats as private messages through a channel and a template registered with them, used for direct and group chats only (`kind` `direct` or `group`); notices carry no Xiaomi or OPPO options:

- Xiaomi: `xiaomi_channel_id` and `xiaomi_template_id` (both or neither) for direct chats; `xiaomi_group_channel_id` and `xiaomi_group_template_id` for group chats, each falling back to the direct one (they need the direct pair).
- OPPO: `oppo_private_template_id` sends chats under the new message classification as category `IM` with `oppo_notify_level` (1, 2 or 16, default 2; only with the template); `oppo_channel_id` is the old private channel.
- The templates have two variables, `title` and `content`, filled from the rendered title and body, cut to Xiaomi 50 and 128 and OPPO 50 and 200 characters. Xiaomi's values lose emoji and other `\p{So}` symbols. The shared vendor notification (`ups.notification`) is cut to 40 and 100 characters, the smallest limits Getui lists. Counting is in UTF-16 units, cuts fall between grapheme clusters, and a cut ends with `…` inside the limit.
- When the notification shows neither sender nor content (preview hidden, content withheld), Xiaomi gets the chat kind and the count instead of the generic text, which Xiaomi lists as a violation: `私聊消息` / `群聊消息` and `N 条新消息，打开应用查看` (`Direct message` / `Group message`, `N new messages, open the app to read them`).

> **Private keys and secrets must always be injected via a secret-management service into the credentials source; do not commit them to version control.**

## Invalid-token cleanup

When a vendor explicitly informs that "this token is permanently invalid", the worker removes it from the registry, to avoid infinitely retrying a device that is permanently unreachable.

The determination is deliberately **conservative**:

| Vendor | Determined as invalid | Not determined (will retry) |
|---|---|---|
| APNs | `410 Unregistered` with a timestamp not before the binding's registration | Rate limiting, 5xx, `400 BadDeviceToken`, `400 DeviceTokenNotForTopic`, `ExpiredProviderToken`, `PayloadTooLarge` |
| FCM | `404` with the FCM error code `UNREGISTERED` | Rate limiting, 5xx, `INVALID_ARGUMENT`, `PERMISSION_DENIED` |
| Getui | `20001` `target user is invalid` | Everything else |

**Misjudging a temporary failure as invalid will delete a normal device's token, after which the user will never receive pushes again, and cannot self-heal without reinstalling the App.** Better to retry a few more times than to delete by mistake — a unit test specifically pins down this boundary.

## One task, one device

An offline task targets exactly one device (`device_id`) and is delivered only to that device's push bindings; the user's other devices each get a task of their own. A task without a target device or without a disposition is malformed and goes to the DLQ — it is never fanned out to every device of the user. A failing device therefore retries alone and cannot cause duplicate notifications elsewhere, and a binding the vendor already accepted is skipped when the task is redelivered (the router's `idempotency_key` identifies the task).

## One push per device, governed per vendor

A device bound through several vendors gets the task through **one** binding: the device's bindings are listed once and tried in `[services.push_worker] binding_order` (default `apns`, `getui`, `fcm`). The next binding is tried only when the current one cannot take the task: its vendor's breaker is open, the vendor or the local rate limit throttles the application (or, for APNs, this one device token), the attempt failed in a way a retry may fix, the token is dead (it is removed), or the vendor has nothing to carry for this task. A terminal failure sends the task to the DLQ.

Each vendor has a concurrency limit and a rolling-window circuit breaker (only server errors and missing answers count against it); each vendor and application credential has a token bucket. Limits are set in `[services.push_worker.vendors.<apns|fcm|getui>]` (`max_concurrency`, `rate_per_sec`, `burst`, `failure_ratio`, `window`, `min_calls`, `open_ms`, `half_open_probes`).

Throttling degrades to a digest: when a vendor answers 429 / quota exhausted (Retry-After is honoured; Getui's call-frequency codes 10003 and 10005 and its daily quota 30009 count too) or the local bucket is empty, and no other binding can take the task, the worker holds it in memory per device (newest task per collapse key), acknowledges the message, and once the vendor is available again sends one push per device: a `digest` when more than one conversation is pending, otherwise the newest task with its count. The hold is bounded; when it is full the message is redelivered instead. A worker that stops loses only what it holds; the messages themselves sync when the app opens.

APNs answers 429 about one device token, so only that binding waits and is held; FCM and Getui throttling holds the application's devices on that vendor.

Metrics: `flare_push_vendor_sent_total{vendor, app_version, receipt_capable}` (`app_version` bounded to the first 64 values per process, the rest `other`, like the proxy's receipts counter), `flare_push_vendor_latency_seconds{vendor}`, `flare_push_vendor_failures_total{vendor, reason}`, `flare_push_vendor_throttled_total{vendor}`, `flare_push_vendor_breaker_state{vendor}`, `flare_push_vendor_switch_total{from, to}`, `flare_push_digest_coalesced_total{vendor}`, `flare_push_digest_flush_total{result}`. Every attempt is also written to the push decision trace (stage `vendor`).

## Priority lanes

The router publishes offline tasks on three subjects of the `FLARE_PUSH` stream, and the worker runs one consumer per subject, each in its own consumer group with its own concurrency, so a backlog of bulk tasks never holds up direct chats. Online tasks have a fourth consumer.

| Subject (setting) | Carries | Consumer group | Concurrency (setting, default) |
|---|---|---|---|
| `flare.im.push.online` (`push_online_topic`) | online delivery | `{group}` | `PUSH_WORKER_ONLINE_CONCURRENCY`, 128 |
| `flare.im.push.offline` (`push_offline_topic`) | direct chats, urgent, mentions, corrections | `{group}-offline` | `PUSH_WORKER_OFFLINE_CONCURRENCY`, 64 |
| `flare.im.push.offline_group` (`PUSH_WORKER_PUSH_OFFLINE_GROUP_TOPIC`) | normal group alerts, tail refreshes | `{group}-offline-group` | `PUSH_WORKER_OFFLINE_GROUP_CONCURRENCY`, 32 |
| `flare.im.push.offline_bulk` (`PUSH_WORKER_PUSH_OFFLINE_BULK_TOPIC`) | large-group waves, @all waves, digests, catch-up | `{group}-offline-bulk` | `PUSH_WORKER_OFFLINE_BULK_CONCURRENCY`, 16 |

`{group}` is `PUSH_WORKER_CONSUMER_GROUP` (default `push-worker`). The three offline consumers share one handler; the subjects must match the push server's `PUSH_SERVER_PUSH_OFFLINE_*_TOPIC`. A consumer prunes stale JetStream durables of its own group only. The online consumer's durable starts at new messages when it is first created, so it never replays stale online tasks.

## Disposition

The router decides what each task does on the device; the vendor adapters only translate it.

| Disposition | Effect |
|---|---|
| `loud` / `digest` | Shown and may ring: high priority, sound when the message asks for it |
| `quiet` | Replaces the notification with the same `collapse_key` without ringing again |
| `recall_replace` | Same as `quiet`, with the body replaced by the recalled-message text |
| `badge_only` | Updates the badge only; no banner, no sound |
| `clear` | Android (FCM): a data message the app acts on. APNs: a badge update when a badge is given, otherwise acknowledged without a vendor call |

Notifications of one conversation share a `collapse_key` (`apns-collapse-id`, Getui `notify_id`, and on Android FCM the payload's `collapse_id`, by which the platform module replaces its own notification), so a busy conversation stays a single notification. FCM to Android is a data-only message without `android.collapse_key`: a collapsible message counts against FCM's per-device burst of 20 with one refill every 3 minutes. Every vendor carries the same snake_case keys for client code: `uid`, `tenant_id`, `conversation_id`, `message_id`, `seq`, `collapse_id`, `thread_id`, `disposition`, `alert_class`, `count`, `badge` (when known), `title`/`body` (FCM data and Getui transmission), `sound`, and `sender_id`, `sender_name` (the sender name exactly as it appears in the body, empty when it does not) and `kind` (`direct`, `group`, `notice`) so client code can substitute local display names. APNs alerts (`loud`, `digest`, `recall_replace`) to a binding that declares `serviceExtension` carry `mutable-content: 1`. The key is `collapse_id` because `collapse_key` is a reserved FCM data key. Expired tasks are acknowledged without delivery. A binding inactive for 60 days (`PUSH_WORKER_BINDING_INACTIVE_DAYS`) is skipped and removed.

## Notification text

The server renders a title and a body for every notification it shows. They are a fallback: client code that runs on arrival (Android data messages, an iOS notification service extension) may render again with local conversation names and the device language.
The strings and the language rule come from the `text` module of `flare-notify-rules` (`flare-protocol/flare-notify-rules`), which the client engine uses for its own notifications too.

- **Language**: the target binding's `locale`. A tag whose primary subtag is `zh` (`zh`, `zh-CN`, `zh-Hant-TW`, `zh_CN`) renders Chinese, any other tag English, a binding without one Chinese. Text the sender wrote is never translated; only the templates change.
- **Sanitizing**: the sender's `offline_push_info` and any text taken from the message (text, rich-text plain text, system text, notification title and body) lose control characters, bidirectional override and isolate characters, and invisible format characters (zero-width space, BOM, Hangul fillers); whitespace runs collapse to one space. Titles are cut to 64 and bodies to 180 grapheme clusters, and to 256 and 1024 UTF-8 bytes, with a trailing `…` counted inside the limit; a single cluster longer than 64 bytes keeps only its first character. Text that is empty after cleaning counts as absent. The limits hold for the final text, count prefix and mention marker included.
- **Fallbacks**: without a body, a placeholder by content type (`[图片]` / `[Image]`, video, voice, file, location, contact card, link, sticker, emoji, rich text, `[消息]` / `[Message]` for the rest); without a title, `新消息` / `New message`; without any content, `你收到一条新消息` / `You have a new message` (`你收到一条通知` / `You have a new notification` for notifications).
- **Mention**: alert class `mention` puts `[有人@你]` / `[Mentioned you]` before the body, unless the text already says so.
- **Preview setting** (`preview` of the task): full (also when unset) shows title and body; sender only keeps the title and replaces the body with `发来一条消息` / `sent you a message`, mention marker kept; hidden uses the generic title and body. An unknown value counts as hidden. Hidden, redacted or purged content and E2EE placeholders always get the generic text, whatever the setting.
- **Count and digest**: a notification standing for several unread messages is titled `[N条] ` / `[N messages] ` (capped at `99+`). A digest reads `N 个会话有 M 条新消息` / `M new messages in N chats` from `count` and `digest_conversations`, and replaces the notification under the collapse key the router sent.

## Adding a new channel

1. Implement `BindingDelivery` under `infrastructure/` (one attempt through one binding; switching, retries and throttling are the binding selection's)
2. For "envelope → title/body", use `push_display::push_plan` and render the alert with `PushAlert::text(Lang::from_locale(&binding.locale))`, **do not decode it yourself** —
   each channel's judgment of "what to display when a message is recalled / content is not visible / the recipient hides previews" must be consistent; that is the place most prone to leaking content
3. Add a `Vendor`, an enum branch in `OfflineDeliveryBackend`, the credentials in `mobile_credentials.rs` and the wiring in `wire.rs`
4. Write the invalidity determination as a standalone pure function and cover it with tests, especially the cases that **should not be determined as invalid**

`fcm_push.rs` and `apns_push.rs` are both templates you can copy directly.
