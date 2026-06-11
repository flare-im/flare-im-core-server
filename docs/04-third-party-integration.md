# 第三方接入与使用说明

第三方接入 Flare IM Core 时，先区分接入方是谁：终端 SDK、业务服务端、管理后台、内部服务还是扩展插件。不同入口的可靠性、权限和协议稳定性不同。

**推荐策略**：生产环境业务系统接入优先使用 gRPC PreSend/PostSend Hook 和内部 typed gRPC 调用。HTTP/OpenAPI 主要用于外部三方、管理后台、低频后台任务和临时适配；不要把高 QPS 主链业务调用长期压在 HTTP facade 上。

## 接入模式

| 接入方 | 推荐入口 | 用途 | 说明 |
|--------|----------|------|------|
| 终端客户端 | `flare-signaling/gateway` + IM SDK | 实时收发、重连、ACK、sync | 推荐给 App/Web/Desktop。 |
| 业务服务端 | typed gRPC 优先，`flare-api-gateway` HTTP API 作为 facade | 服务端发消息、查会话、媒体、presence | 高频可信内网调用用 typed gRPC；外部、后台和低频任务用 HTTP/OpenAPI。 |
| 管理后台 | `flare-admin-gateway` Admin API | 审计、查询、导出、强制操作 | 需要 Admin token/scope 和审计原因。 |
| 内部可信服务 | 内部 typed gRPC | 高性能低开销调用 | 只在内网、mTLS/service token、版本受控时使用。 |
| 业务规则扩展 | gRPC Hook / Capability | 风控、权限、审核、RTC、机器人 | 主链 Hook 推荐 gRPC，避免 HTTP 序列化和连接管理放大尾延迟。 |
| 业务系统 | typed gRPC Bridge + gRPC Hook | 好友、群、成员、禁言、黑名单 | Core 消费校验结果和会话/成员同步结果。 |

## 推荐调用策略

| 场景 | 推荐协议 | 推荐服务/入口 | 说明 |
|------|----------|---------------|------|
| 高频服务端发消息、系统消息、业务事件 | typed gRPC | `MessageSendService` / `MessageEventService` / `MessageActionService` | 可信内网低开销调用，metadata 明确透传租户、用户和 trace。 |
| 好友、群、成员变更同步到 Core | typed gRPC | `ConversationManageService` / `MessageEventService.ExecuteEvent` | 业务系统 Bridge 推荐直接调用 typed gRPC。 |
| 发信前权限、风控、黑名单、禁言校验 | gRPC Hook | `PreSend` Hook | 主链门禁必须短超时、fail-fast，并通过服务发现或固定 endpoint 管理。 |
| 发送后审计、BI、送达分析 | gRPC Hook 或 MQ consumer | `PostSend` / `Delivery` Hook | 旁路能力可 ignore/retry，避免拖慢主链 ACK。 |
| 外部三方、开放平台、业务后台低频操作 | HTTP/OpenAPI | `flare-api-gateway` | 稳定公开合同、易鉴权、易文档化。 |
| 管理审计、导出、强制操作 | HTTP Admin facade | `flare-admin-gateway` | 需要 Admin scope、审计原因、分页和限流。 |

## 身份与上下文

所有写请求都必须有可信身份来源。

HTTP/Gateway 推荐 header：

```http
Authorization: Bearer <access_token>
x-tenant-id: tenant-1
x-request-id: req-or-idempotency-key
x-trace-id: trace-id
x-device-id: device-1
x-app-id: app-1
```

规则：

- Gateway 只消费认证 provider 返回的 principal。
- 请求体中的 `sender_id`、`tenant_id` 不能覆盖认证身份。
- 下游 gRPC metadata 必须携带 `x-tenant-id`、`x-user-id`、`x-request-id`、`x-trace-id`。
- Admin 写操作必须携带 `x-actor-id` 和 `x-audit-reason`。

## 幂等要求

| 字段 | 用途 |
|------|------|
| `client_msg_id` | 消息级幂等，客户端或业务服务端必须稳定生成。 |
| `x-request-id` | 请求级幂等和 trace 关联。 |
| `Idempotency-Key` | HTTP 写请求幂等键，推荐和 `client_msg_id` 关联。 |

重试时不要生成新的 `client_msg_id`，否则 Core 会认为是新业务消息。

## HTTP API

`flare-api-gateway` 当前暴露公共 API 前缀 `/api/v1`。

HTTP API 是公开 facade，不是生产主链高频调用的首选。业务系统和 Core 在同一可信网络内时，推荐直接使用 typed gRPC；HTTP 更适合开放平台、后台、机器人、管理面、低频任务和跨语言快速接入。

| API | 用途 |
|-----|------|
| `POST /api/v1/messages/send` | 服务端/机器人/业务后台发送消息。 |
| `POST /api/v1/messages/events/custom` | 服务端/业务后台提交自定义业务事件，转为 `MessageEventService.ExecuteEvent(EVENT_CUSTOM)`。 |
| `POST /api/v1/messages/recall` | 撤回消息。 |
| `POST /api/v1/messages/read` | 标记消息已读。 |
| `GET /api/v1/conversations` | 会话列表。 |
| `GET /api/v1/conversations/participants` | 会话成员分页。 |
| `POST /api/v1/conversations/participants/manage` | 管理会话参与者，需强权限。 |
| `GET /api/v1/presence/users/{user_id}` | 查询在线状态。 |
| `POST /api/v1/presence/users/batch` | 批量查询在线状态。 |
| `POST /api/v1/presence/logout` | 登出/踢下线入口。 |
| `/api/v1/medias/*` | 上传、预签名、文件信息、引用、处理。 |
| `/api-doc/openapi.json` | OpenAPI。 |

### HTTP 发送消息

当前 HTTP gateway 会把 `content` 透明包装为 `CustomContent(type = "http.json")`，适合业务后台、机器人、三方 JSON 消息。强类型文本、图片、卡片等更推荐通过 SDK 或内部 typed gRPC 发送。

```bash
curl -X POST "http://127.0.0.1:50050/api/v1/messages/send" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-tenant-id: tenant-1" \
  -H "x-request-id: req-10001" \
  -H "Content-Type: application/json" \
  -d '{
    "conversation_id": "single:alice:bob",
    "client_msg_id": "biz-10001",
    "conversation_type": 1,
    "channel_id": "bob",
    "message_type": 19,
    "content": {
      "type": "order.paid.v1",
      "order_id": "order-9",
      "text": "订单已支付"
    },
    "sync": false,
    "svid": "mall-service"
  }'
```

响应：

```json
{
  "code": 0,
  "data": {
    "server_msg_id": "server-id",
    "seq": 42,
    "success": true
  },
  "reason": null,
  "message": null,
  "track": "trace-id"
}
```

### HTTP 自定义事件

HTTP facade 只开放 `EVENT_CUSTOM` 的薄封装，适合低频业务状态变更；撤回、已读、reaction 等稳定操作优先使用对应 typed gRPC 或专门 HTTP action API。

```bash
curl -X POST "http://127.0.0.1:50050/api/v1/messages/events/custom" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-tenant-id: tenant-1" \
  -H "x-request-id: req-10002" \
  -H "Content-Type: application/json" \
  -d '{
    "conversation_id": "group:g100",
    "namespace": "mall",
    "name": "order_paid",
    "version": "v1",
    "payload": {
      "order_id": "order-9",
      "amount": 19900
    },
    "attributes": {
      "source": "order-service"
    },
    "svid": "mall-service"
  }'
```

### 查询会话

```bash
curl "http://127.0.0.1:50050/api/v1/conversations?limit=20" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-tenant-id: tenant-1"
```

### 查询成员

```bash
curl "http://127.0.0.1:50050/api/v1/conversations/participants?conversation_id=group:g1&limit=100" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-tenant-id: tenant-1"
```

### 查询在线状态

```bash
curl "http://127.0.0.1:50050/api/v1/presence/users/alice" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-tenant-id: tenant-1"
```

## 内部 gRPC

内部服务和业务系统 Bridge 在可信内网中推荐直接调用 typed gRPC，例如：

| 服务 | 用途 |
|------|------|
| `MessageSendService` | `SendMessage`、`BatchSendMessage`、`SendSystemMessage`、`SendAck`、`SendCustomData`。 |
| `MessageEventService` | `ExecuteEvent`。 |
| `MessageActionService` | 撤回、编辑、删除、已读、reaction、pin、mark。 |
| `ConversationReadService` | 会话列表、详情、成员分页。 |
| `ConversationManageService` | 创建/更新/删除会话、成员管理、强制同步。 |
| `StorageReaderService` | 历史消息、事件、管理查询。 |
| `OnlineService` | presence 和设备状态。 |
| `MediaService` | 上传、文件、引用、对象存储。 |
| `CapabilityService` | 能力发现、授权、插件分发。 |

内部 gRPC 要求：

- 使用内网或 mTLS/service token。
- metadata 透传 `x-tenant-id`、`x-user-id`、`x-request-id`、`x-trace-id`。
- 外部三方不建议直接暴露内部 gRPC，优先使用 HTTP API 或专门 gRPC facade。
- 高频写路径优先使用 typed gRPC，避免 HTTP JSON facade 的额外转换和合同漂移。

## Hook 接入

Hook 用于把业务规则接入主链，但不污染 Core。业务系统主链 Hook 推荐使用 gRPC transport；Webhook/HTTP Hook 只建议用于低频、临时适配或非主链旁路场景。

当前支持的 Hook 类别包括：

- `PreSend`
- `PostSend`
- `Delivery`
- `Recall`
- `MessageRead`
- `MessageReaction`
- `ConversationLifecycle`
- `ConversationMember`
- Push 相关 Hook
- Presence/User online/offline Hook
- Custom Hook

### PreSend Hook

适合：

- 好友关系校验。
- 群成员/禁言/黑名单校验。
- 风控、敏感词、审核。
- 消息补充展示字段。

示例配置：

```toml
[[pre_send]]
name = "business-policy-pre-send"
description = "发信前业务系统权限校验"
enabled = true
priority = 110
timeout_ms = 500
require_success = true
error_policy = "fail_fast"

[pre_send.selector]
tenants = ["0"]
conversation_types = ["single", "group"]
message_types = ["text", "image", "custom"]

[pre_send.transport]
type = "grpc"
endpoint = "discovery://business-policy-hook"

[pre_send.metadata]
hook_operation = "flare.hook.v1.pre_send"
policy = "business_message_permission"
```

### PostSend / Delivery Hook

适合：

- 审计。
- BI 统计。
- 送达率分析。
- 异步业务联动。

建议 `require_success = false`，`error_policy = "ignore"` 或 `retry`，避免旁路系统拖慢主链。

## Capability 接入

Capability 用于注册和调度可选能力：

- RTC/SFU。
- 机器人。
- 审核引擎。
- 外部业务插件。
- 风控/反垃圾。

原则：

- Capability 通过能力发现和授权判断是否可用。
- Core 可以调用能力 enrich 或 dispatch，但能力后端失败策略必须明确。
- SFU/RTC 等能力不能成为普通消息链路的强制依赖。

## 业务系统接入

业务系统负责：

- 用户资料。
- 好友关系。
- 黑名单。
- 群资料。
- 群成员、角色、禁言。
- 好友/群业务事件。

Core 需要业务系统提供：

- PreSend Hook：发信前权限校验，生产推荐 gRPC Hook。
- Conversation bridge：创建/更新会话和成员投影，生产推荐 typed gRPC。
- System message/event：把业务变更转为 Core 时间线或事件，生产推荐 typed gRPC。
- Recipient resolver：群成员和单聊接收者解析。

## 媒体接入

文件/图片/视频建议流程：

1. 调用 `/api/v1/medias/upload-url` 或 direct upload API 获取上传凭证。
2. 客户端或业务服务上传对象。
3. 调用 create reference 建立业务引用。
4. 发送 `IMAGE`、`VIDEO`、`FILE` 或 `CUSTOM` 消息，引用 media file id。
5. 消息删除、撤回或业务清理时更新引用或执行孤儿清理。

## 接入检查清单

- 是否明确接入入口：SDK、HTTP、Admin、内部 gRPC、Hook、Capability。
- 主链业务 Hook 是否使用 gRPC transport。
- 高频业务服务到 Core 的调用是否使用 typed gRPC。
- 是否有可信身份和租户上下文。
- 是否所有写请求都有稳定 `client_msg_id` 或 idempotency key。
- 是否区分持久消息和临时通知。
- 是否把好友/群/权限放在业务系统，而不是 Core。
- 是否给 Hook 设置短超时、重试/失败策略和选择器。
- 是否给业务消息使用 typed content 或清晰的 `CustomContent.type`。
- 是否接入 observability：trace id、request id、ledger、MQ lag、DLQ。
