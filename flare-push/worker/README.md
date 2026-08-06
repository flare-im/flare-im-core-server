# flare-push-worker

English · [中文](README.zh-CN.md)

Offline push delivery. When a user is offline, messages pass through this worker to a vendor push channel, and finally land in the device's notification tray.

**This is a bottom-line capability for mobile products** — if messages can't be received while offline, the IM can't go to production.

## Choosing the delivery backend

```bash
PUSH_WORKER_OFFLINE_DELIVERY_BACKEND=fcm   # outbox | getui | fcm | apns | disabled
```

| Backend | Coverage | When to use |
|---|---|---|
| `apns` | Native iOS | When you need VoIP calls, Live Activity, or Critical Alert — these are **only supported by native APNs** |
| `fcm` | Android + iOS forwarded via FCM | Products targeting overseas mainly; one integration covers both platforms |
| `getui` | The full lineup of domestic Android | Domestic products: vendor channels such as Xiaomi/Huawei/OPPO/vivo are aggregated by Getui |
| `outbox` | — | Only persist, do not deliver; consumed by an external system |
| `disabled` | — | Turn off offline push |

> A single worker instance runs only one backend. To cover multiple channels at once, start multiple instances, each configured with its own backend —
> the device-token registry carries a `provider` field, so each instance only handles the batch of tokens that belong to it, without interfering with the others.

## Configuration

### APNs

```bash
PUSH_WORKER_APNS_TEAM_ID=XXXXXXXXXX          # Apple Developer Team ID
PUSH_WORKER_APNS_KEY_ID=YYYYYYYYYY           # Key ID of the p8 private key
PUSH_WORKER_APNS_PRIVATE_KEY="$(cat AuthKey_YYYYYYYYYY.p8)"
PUSH_WORKER_APNS_TOPIC=com.example.app       # bundle id
PUSH_WORKER_APNS_SANDBOX=false               # must be set to true for device tokens from development builds
```

> ⚠️ **Setting `SANDBOX` wrong is the most common pitfall**: a device token registered by a development build is only valid in the sandbox,
> and using it in production will uniformly return `BadDeviceToken`, which looks as if all tokens have become invalid.

### FCM

```bash
PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON="$(cat service-account.json)"
```

### Getui

```bash
PUSH_WORKER_GETUI_APP_ID=...
PUSH_WORKER_GETUI_APP_KEY=...
PUSH_WORKER_GETUI_MASTER_SECRET=...
```

> **Private keys and secrets must always be injected via environment variables or a secret-management service; do not write them into a config file and commit it to version control.**

## Invalid-token cleanup

When a vendor explicitly informs that "this token is permanently invalid", the worker removes it from the registry, to avoid infinitely retrying a device that is permanently unreachable.

The determination is deliberately **conservative**:

| Vendor | Determined as invalid | Not determined (will retry) |
|---|---|---|
| APNs | `410 Gone`, `400 BadDeviceToken`, `400 DeviceTokenNotForTopic` | Rate limiting, 5xx, `ExpiredProviderToken`, `PayloadTooLarge` |
| FCM | `404`, `UNREGISTERED`, `INVALID_ARGUMENT` | Rate limiting, 5xx, `PERMISSION_DENIED` |

**Misjudging a temporary failure as invalid will delete a normal device's token, after which the user will never receive pushes again, and cannot self-heal without reinstalling the App.** Better to retry a few more times than to delete by mistake — a unit test specifically pins down this boundary.

## Multi-device semantics

A user may have multiple devices. **As long as one device is delivered to successfully, it counts as overall success** — otherwise, an old device that keeps failing would trigger a retry of the entire batch, causing duplicate notifications for the other normal devices.

## Adding a new channel

1. Implement `OfflinePushExecutor` under `infrastructure/`
2. For "envelope → title/body", use `push_display::notification_display` directly, **do not decode it yourself** —
   each channel's judgment of "what to display when a message is recalled / content is not visible" must be consistent; that is the place most prone to leaking content
3. Add an enum branch in `OfflineDeliveryBackend` and the wiring in `wire.rs`
4. Write the invalidity determination as a standalone pure function and cover it with tests, especially the cases that **should not be determined as invalid**

`fcm_push.rs` and `apns_push.rs` are both templates you can copy directly.
