# Flare Push Proxy

[English](README.md) · 中文

暴露 **PushService** gRPC API（`push_service.proto`），将调用方提交的推送请求写入 MQ，由 [Push Server](../server) 消费并执行在线/离线分流与实际推送。

## 职责

- **PushMessage**：接收 `PushMessageRequest`，写入消息推送入站 topic（默认 `flare.im.push.messages`）。
- **PushNotification**：接收 `PushNotificationRequest`，构造成裸 `PushEnvelope`，写入统一信封 topic（默认 `flare.im.push.envelope`）。
- **PushCustom**：接收 `PushCustomRequest`，构造成裸 `PushEnvelope`，写入统一信封 topic（默认 `flare.im.push.envelope`）。
- 模板、定时推送、查询状态等 RPC 当前返回 `Unimplemented`，可按需扩展。

## 配置

| 环境变量 | 说明 | 默认 |
|----------|------|------|
| `PUSH_PROXY_LISTEN` | gRPC 监听地址 | `0.0.0.0:50090` |
| `PUSH_PROXY_JETSTREAM_URL` | JetStream 地址 | 同 push 配置或 `nats://127.0.0.1:24222` |
| `PUSH_PROXY_PUSH_REQUEST_TOPIC` | 消息推送入站 Topic | `flare.im.push.messages` |
| `PUSH_PROXY_PUSH_ENVELOPE_TOPIC` | 通知/自定义统一信封 Topic | `flare.im.push.envelope` |
| `PUSH_PROXY_PUSH_ONLINE_TOPIC` | 在线推送任务 Topic | `flare.im.push.online` |
| `PUSH_PROXY_PUSH_OFFLINE_TOPIC` | 离线推送任务 Topic | `flare.im.push.offline` |
| `PUSH_PROXY_JETSTREAM_TIMEOUT_MS` | JetStream 发送超时（毫秒） | `5000` |

也可通过 `config/services/push-server.toml` 等应用配置中的 JetStream 配置引用（`push_server.jetstream`）间接影响 bootstrap。

## 运行

```bash
# 使用默认配置
cargo run -p flare-push-proxy

# 指定监听端口与 JetStream
PUSH_PROXY_LISTEN=0.0.0.0:50090 \
PUSH_PROXY_JETSTREAM_URL=nats://127.0.0.1:24222 \
cargo run -p flare-push-proxy
```

## 与 Push Server 的关系

- **Proxy**：无状态 gRPC 入口，只负责鉴权（可选）、参数校验与写入 MQ。
- **Server**：消费 `flare.im.push.messages`、`flare.im.push.events`、`flare.im.push.envelope`，查在线、生成 `PushTaskEnvelope`，再写入 online/offline worker topic。

`PUSH_PROXY_PUSH_ENVELOPE_TOPIC` 需与 Push Server 的 `push_envelope_topic` 一致，否则通知和自定义推送不会被统一信封消费者接收。
