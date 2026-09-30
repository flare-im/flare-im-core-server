# Flare Admin Gateway

[English](README.md) · 中文

`flare-admin-gateway` 是 Flare IM Core 的内网管理 API 入口。它只提供安全 API 边界、typed HTTP facade、审计上下文校验和运维只读快照，不提供管理后台页面、业务管理员、角色审批或菜单系统。

## 边界

| 服务 | 暴露面 | 主要职责 |
|------|--------|----------|
| `flare-api-gateway` | public/third-party HTTP API | 消息、会话、在线、媒体等业务接入 API |
| `flare-admin-gateway` | internal Admin HTTP API | 管理查询、导出任务、网关运维快照、能力发现 |
| 业务管理后台 | 业务侧 | UI、角色、审批、菜单、管理员生命周期 |

生产部署建议把 `flare-admin-gateway` 放在内网、VPN、mTLS 或 service mesh policy 后面，不与 public gateway 共用公网暴露面。

## 默认入口

```http
GET  /api/v1/admin/auth/check
GET  /api/v1/admin/capabilities
GET  /api/v1/admin/gateway/health
GET  /api/v1/admin/gateway/upstreams
GET  /api/v1/admin/gateway/routes
GET  /api/v1/admin/gateway/config
POST /api/v1/admin/messages/query
GET  /api/v1/admin/messages/{message_id}
GET  /api/v1/admin/messages/{message_id}/events
POST /api/v1/admin/messages/write-ledger/query
POST /api/v1/admin/messages/export
GET  /api/v1/admin/conversations
GET  /api/v1/admin/conversations/{conversation_id}
GET  /api/v1/admin/conversations/{conversation_id}/participants
GET  /api/v1/admin/users/{user_id}/devices
GET  /api/v1/admin/users/{user_id}/presence
POST /api/v1/admin/users/{user_id}/kick
POST /api/v1/admin/users/{user_id}/devices/{device_id}/kick
POST /api/v1/admin/tenant/kick-all
GET  /api/v1/admin/tenant/runtime
GET  /api/v1/admin/capabilities/tenant
GET  /api/v1/admin/plugins
```

`medias/*` 路由见 OpenAPI 文档。会话、在线、踢线、租户运行时、能力开关与插件路由（ADM-01）的说明见 [docs/admin-api.md](../docs/admin-api.md)（英文）。

本地默认监听 `0.0.0.0:50051`，配置文件为 `config/services/admin-gateway.toml`。

## API 文档

`GET /swagger-ui` 提供 `GET /api-doc/openapi.json` 的 Swagger UI 页面。Swagger UI 的样式与脚本内嵌在二进制中，经 `/swagger-ui/swagger-ui.css` 与 `/swagger-ui/swagger-ui-bundle.js` 提供，页面不加载任何外部资源，隔离网络中也能打开。这两个文件是未经修改的 Swagger UI 5.3.1 `dist/` 产物，许可为 Apache-2.0，许可证与 NOTICE 保存在 `assets/swagger-ui/`。

## 认证

Admin Gateway 不签发、不刷新、不存储 token。它只通过 `FLARE_ADMIN_GATEWAY_AUTH_MODE=core_jwt` 或 `FLARE_ADMIN_GATEWAY_AUTH_MODE=http_hook` 恢复标准 principal。

示例：

```bash
FLARE_ADMIN_GATEWAY_SERVER_PORT=50051
FLARE_ADMIN_GATEWAY_AUTH_MODE=http_hook
FLARE_ADMIN_GATEWAY_AUTH_HOOK_URL=http://127.0.0.1:8088/internal/admin/auth/validate
FLARE_ADMIN_GATEWAY_AUTH_HOOK_TIMEOUT_MS=800
FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET=change-me
FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET_HEADER=x-flare-admin-auth-hook-secret
```

推荐业务认证系统返回以下 scope：

```text
admin_gateway:admin
admin_gateway:admin:*
```

Admin 写操作必须携带：

```http
x-tenant-id: tenant-1
x-actor-id: admin-1
x-audit-reason: compliance-investigation
x-request-id: request-1
```

也可以用 `idempotency-key` 替代 `x-request-id` 做幂等和审计关联。
两个受限的只读 POST：`/messages/query` 与 `/messages/write-ledger/query`，仍要求已签名的员工令牌和明确租户，但不要求写操作审计头；`/messages/export` 仍按受保护的写类操作处理。

## 消息写链路账本查询

`POST /api/v1/admin/messages/write-ledger/query` 用于排查消息从 broker accepted 到 archive/storage/WAL/ACK 的写入状态。该接口是只读内部运维 API，必须带 `x-tenant-id`，并且请求体至少包含 `server_id`、`conversation_id`、`write_state`、`failed_only=true` 或 `updated_after/updated_before` 之一，避免误触发大表扫描。

示例：

```json
{
  "conversation_id": "conv-1",
  "failed_only": true,
  "updated_after": 1700000000,
  "limit": 100
}
```

典型 `write_state` 包括 `broker_accepted`、`archive_persisted`、`storage_persisted`、`wal_cleaned`、`ack_published` 以及 `wal_cleanup_failed`、`ack_publish_failed` 等失败状态。
