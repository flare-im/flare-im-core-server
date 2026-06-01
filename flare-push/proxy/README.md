# Flare Push Proxy

暴露 **PushService** gRPC API（`push.proto`），将调用方提交的推送消息、推送通知、ACK 写入 JetStream，由 [Push Server](../server) 消费并执行实际推送与 ACK 处理。

## 职责

- **PushMessage**：接收 `PushMessageRequest`，按 `user_ids` 展开为多条 `PushTaskEnvelope`，写入 `task_topic`（默认 `flare.im.push.tasks`）。
- **PushNotification**：接收 `PushNotificationRequest`，按 `user_ids` 展开为信封，写入 `notification_topic`（默认 `flare.im.push.notifications`）。
- 模板、定时推送、查询状态等 RPC 当前返回 `Unimplemented`，可按需扩展。

## 配置

| 环境变量 | 说明 | 默认 |
|----------|------|------|
| `PUSH_PROXY_LISTEN` | gRPC 监听地址 | `0.0.0.0:50090` |
| `PUSH_PROXY_JETSTREAM_BOOTSTRAP` | JetStream 地址 | 同 push 配置或 `127.0.0.1:29092` |
| `PUSH_PROXY_TASK_TOPIC` | 推送任务 Topic | `flare.im.push.tasks` |
| `PUSH_PROXY_NOTIFICATION_TOPIC` | 推送通知 Topic | `flare.im.push.notifications` |
| `PUSH_PROXY_ACK_TOPIC` | ACK Topic | `flare.im.push.acks` |
| `PUSH_PROXY_JETSTREAM_TIMEOUT_MS` | JetStream 发送超时（毫秒） | `5000` |
| `PUSH_PROXY_DEFAULT_TENANT_ID` | 默认租户 ID（信封填充） | `0` |

也可通过 `config/services/push_server.toml` 等应用配置中的 JetStream 配置引用（`push_server.jetstream`）间接影响 bootstrap 与 topic 默认值。

## 运行

```bash
# 使用默认配置
cargo run -p flare-push-proxy

# 指定监听端口与 JetStream
PUSH_PROXY_LISTEN=0.0.0.0:50090 \
PUSH_PROXY_JETSTREAM_BOOTSTRAP=127.0.0.1:29092 \
cargo run -p flare-push-proxy
```

## 与 Push Server 的关系

- **Proxy**：无状态 gRPC 入口，只负责鉴权（可选）、参数校验与写入 JetStream。
- **Server**：消费 `task_topic` / `ack_topic`，查在线、选网关、下发推送并处理 ACK。

Topic 名称需与 Push Server 的配置一致（如 `task_topic`、`ack_topic`），否则 Server 收不到任务或 ACK。
