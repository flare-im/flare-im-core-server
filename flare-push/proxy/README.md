# Flare Push Proxy

English · [中文](README.zh-CN.md)

Exposes the **PushService** gRPC API (`push_service.proto`), writing the push requests submitted by callers into MQ, which the [Push Server](../server) consumes to perform online/offline routing and the actual push.

## Responsibilities

- **PushMessage**: receives a `PushMessageRequest`, wraps its `Message` in a protobuf `MqEnvelope` (`payload_kind = MESSAGE`, `push_only = true`, keyed by conversation ID), and writes it to the message-push inbound topic (default `flare.im.push.messages`). This is the same wire format as the message pipeline's push-only records and is decoded by the Push Server's message consumer; `MqEnvelope` has no push-options field, so the request's `options` / `metadata` are not carried in the MQ record.
- **PushNotification**: receives a `PushNotificationRequest`, constructs a bare `PushEnvelope`, and writes it to the unified envelope topic (default `flare.im.push.envelope`).
- **PushCustom**: receives a `PushCustomRequest`, constructs a bare `PushEnvelope`, and writes it to the unified envelope topic (default `flare.im.push.envelope`).
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
- **Server**: consumes `flare.im.push.messages`, `flare.im.push.events`, and `flare.im.push.envelope`, checks presence, generates a `PushTaskEnvelope`, and then writes it to the online/offline worker topic.

`PUSH_PROXY_PUSH_ENVELOPE_TOPIC` must match the Push Server's `push_envelope_topic`, otherwise notifications and custom pushes will not be received by the unified-envelope consumer.
