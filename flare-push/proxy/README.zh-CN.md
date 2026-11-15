# Flare Push Proxy

[English](README.md) · 中文

暴露 **PushService** gRPC API（`push_service.proto`），将调用方提交的推送请求写入 MQ，由 [Push Server](../server) 消费并执行在线/离线分流与实际推送。

## 职责

- **PushMessage**：接收 `PushMessageRequest`，把其中的 `Message` 封装为 protobuf `MqEnvelope`（`payload_kind = MESSAGE`、`push_only = true`，分区键为会话 ID），写入消息推送入站 topic（默认 `flare.im.push.messages`）。与 message pipeline 的 push-only 记录同一线格式，由 Push Server 的消息消费端解码；`MqEnvelope` 没有推送选项字段，请求的 `options` / `metadata` 不进入 MQ 记录。
- **PushNotification**：接收 `PushNotificationRequest`，构造成裸 `PushEnvelope`，写入统一信封 topic（默认 `flare.im.push.envelope`）。
- **PushCustom**：接收 `PushCustomRequest`，构造成裸 `PushEnvelope`，写入统一信封 topic（默认 `flare.im.push.envelope`）。
- **ReportDeviceHandoff**：接收 `DeviceHandoff`（一台设备的新消息提示从链路转交给推送通道），原样写入 `flare.im.push.handoff`，分区键为用户 ID。它不在任何用户路径上：只接受接入网关与 Online 的已验证服务身份，交接的租户必须是调用方签名里的租户，一次交接最多带 128 条待补提示的条目。
- **RevokeDeviceBindings**：服务端吊销设备推送绑定。设备被踢下线、被另一台设备顶替、被租户踢出，或它的身份会话被吊销时由 Online 调用。它不写 MQ，直接从设备令牌登记表里移除绑定。只接受 `flare-signaling-online` 的已验证服务身份，请求里的租户必须是调用方签名里的租户，`registered_before`（Unix 毫秒）必填，`reason` 取 `kicked`、`identity_revoked`、`tenant_kicked`、`device_replaced` 之一。`device_id` 为空表示该用户的全部设备。只吊销登记时间不晚于 `registered_before` 的绑定，覆盖所有通道；每个绑定留下的状态与客户端吊销（`RevokeDevicePushBinding`）相同：槽位保留版本，绑定 ID 留 24 小时墓碑。返回实际吊销的绑定数，0 也是成功，可以重复调用。
- **ReportPushReceipts**：设备收到推送后（iOS 通知扩展或应用进程）上报的到达回执，一次至多 100 条。只接受 API 网关代签的用户身份，租户与用户必须等于签名。通道（`apns`、`fcm`、`getui`）与来路（`extension`、`app`）都认得的回执写成推送决策链里的一条 `receipt`，并计入 `flare_push_receipts_total{vendor, app_version, path}`（应用版本超出 32 个 `[0-9A-Za-z._+-]` 字符的记为 `other`）；认不出的跳过。回执不影响投递。
- **PushAdminService.AdminQueryPushTrace**：管理员查一个用户的推送决策链（在推送 Redis 里留 24 小时，由 Push Server、Push Worker 与本服务写），按一条消息查，或列最近 `limit` 条（缺省 20，至多 50）。只认 Admin Gateway 转发的 Staff 上下文，校验方式与 Online 的 `AdminListUserDevices` 相同（`core:users:read`，租户取自上下文）。写入端的配置在 `[services.push_proxy.trace]`。
- 模板、定时推送、查询状态等 RPC 当前返回 `Unimplemented`，可按需扩展。

## 配置

| 环境变量 | 说明 | 默认 |
|----------|------|------|
| `PUSH_PROXY_LISTEN` | gRPC 监听地址 | `0.0.0.0:50091` |
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
PUSH_PROXY_LISTEN=0.0.0.0:50091 \
PUSH_PROXY_JETSTREAM_URL=nats://127.0.0.1:24222 \
cargo run -p flare-push-proxy
```

## 与 Push Server 的关系

- **Proxy**：无状态 gRPC 入口，只负责鉴权（可选）、参数校验与写入 MQ。
- **Server**：消费 `flare.im.push.messages`、`flare.im.push.events`、`flare.im.push.envelope`、`flare.im.push.handoff`，按设备查可达状态、生成 `PushTaskEnvelope`（在线任务按收件人分批，离线任务给每台有推送绑定且不活跃的设备各一条），再写入 online/offline worker topic。交接只为被交接的那台设备产出补提示的离线任务。

`PUSH_PROXY_PUSH_ENVELOPE_TOPIC` 需与 Push Server 的 `push_envelope_topic` 一致，否则通知和自定义推送不会被统一信封消费者接收。
