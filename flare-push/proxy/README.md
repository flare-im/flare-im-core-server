# Flare Push Proxy

English · [中文](README.zh-CN.md)

Exposes the **PushService** gRPC API (`push_service.proto`), writing the push requests submitted by callers into MQ, which the [Push Server](../server) consumes to perform online/offline routing and the actual push.

## Responsibilities

- **PushMessage**: receives a `PushMessageRequest`, wraps its `Message` in a protobuf `MqEnvelope` (`payload_kind = MESSAGE`, `push_only = true`, keyed by conversation ID), and writes it to the message-push inbound topic (default `flare.im.push.messages`). This is the same wire format as the message pipeline's push-only records and is decoded by the Push Server's message consumer; `MqEnvelope` has no push-options field, so the request's `options` / `metadata` are not carried in the MQ record.
- **PushNotification**: receives a `PushNotificationRequest`, constructs a bare `PushEnvelope`, and writes it to the unified envelope topic (default `flare.im.push.envelope`).
- **PushCustom**: receives a `PushCustomRequest`, constructs a bare `PushEnvelope`, and writes it to the unified envelope topic (default `flare.im.push.envelope`).
- **ReportDeviceHandoff**: receives a `DeviceHandoff` (a device whose new-message alerts move from its live link to the push channel) and writes it as-is to `flare.im.push.handoff`, keyed by user ID. It is not on any user path: only the verified service identities of the signaling gateway and the signaling online service are admitted, the handoff's tenant must be the caller's signed tenant, and a handoff carries at most 128 pending items.
- **RevokeDeviceBindings**: server-initiated revocation of device push bindings, called by the signaling online service when a device is kicked, replaced by another device, removed by a tenant kick, or its identity session is revoked. It writes nothing to MQ; it removes the bindings from the device token registry. Only the verified `flare-signaling-online` service identity is admitted, the request tenant must be the caller's signed tenant, `registered_before` (Unix ms) is mandatory, and `reason` is one of `kicked`, `identity_revoked`, `tenant_kicked`, `device_replaced`. An empty `device_id` means every device of the user. Only bindings registered at or before `registered_before` are revoked, across all providers, and each is left in the same state as a client revocation (`RevokeDevicePushBinding`): the slot keeps its version, the binding ID is a 24-hour tombstone. The response carries the number of bindings revoked; zero is success and repeats are safe.
- **ReportPushReceipts**: arrival receipts a device reports after a push reached it (the iOS notification extension, or the app process), at most 100 per call. Only the user identity the API gateway signs is admitted; tenant and user must be the signed ones. Each receipt whose provider (`apns`, `fcm`, `getui`) and path (`extension`, `app`) are known is written as a `receipt` entry of the push decision trace and counted in `flare_push_receipts_total{vendor, app_version, path}` (app versions outside 32 characters of `[0-9A-Za-z._+-]` count as `other`); malformed receipts are skipped. Receipts never affect delivery.
- **PushAdminService.AdminQueryPushTrace**: a user's push decision trace (24 hours in the push Redis, written by Push Server, Push Worker and this service), for one message or the newest `limit` (default 20, at most 50). Only the Staff context Admin Gateway forwards is admitted, verified like Online's `AdminListUserDevices` (`core:users:read`, tenant from the context). The trace writer is configured in `[services.push_proxy.trace]`.
- RPCs such as template push, scheduled push, and status query currently return `Unimplemented` and can be extended as needed.

## Configuration

| Environment variable | Description | Default |
|----------|------|------|
| `PUSH_PROXY_LISTEN` | gRPC listen address | `0.0.0.0:50091` |
| `PUSH_PROXY_JETSTREAM_URL` | JetStream address | Same as the push config or `nats://127.0.0.1:24222` |
| `PUSH_PROXY_PUSH_REQUEST_TOPIC` | Message-push inbound topic | `flare.im.push.messages` |
| `PUSH_PROXY_PUSH_ENVELOPE_TOPIC` | Notification/custom unified envelope topic | `flare.im.push.envelope` |
| `PUSH_PROXY_PUSH_ONLINE_TOPIC` | Online push task topic | `flare.im.push.online` |
| `PUSH_PROXY_PUSH_OFFLINE_TOPIC` | Offline push task topic | `flare.im.push.offline` |
| `PUSH_PROXY_JETSTREAM_TIMEOUT_MS` | JetStream send timeout (milliseconds) | `5000` |

It can also be indirectly influenced at bootstrap via the JetStream config reference (`push_server.jetstream`) in application configs such as `config/services/push-server.toml`.

## Running

```bash
# Use the default configuration
cargo run -p flare-push-proxy

# Specify the listen port and JetStream
PUSH_PROXY_LISTEN=0.0.0.0:50091 \
PUSH_PROXY_JETSTREAM_URL=nats://127.0.0.1:24222 \
cargo run -p flare-push-proxy
```

## Relationship with the Push Server

- **Proxy**: a stateless gRPC entry point, responsible only for authentication (optional), parameter validation, and writing to MQ.
- **Server**: consumes `flare.im.push.messages`, `flare.im.push.events`, `flare.im.push.envelope`, and `flare.im.push.handoff`, checks per-device reachability, generates `PushTaskEnvelope`s (one online task per recipient batch, one offline task per bound device that is not live), and then writes them to the online/offline worker topics. A handoff produces catch-up offline tasks for the handed-off device only.

`PUSH_PROXY_PUSH_ENVELOPE_TOPIC` must match the Push Server's `push_envelope_topic`, otherwise notifications and custom pushes will not be received by the unified-envelope consumer.
