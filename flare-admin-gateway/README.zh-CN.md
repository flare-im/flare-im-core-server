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
GET  /api/v1/admin/capabilities/catalog
GET  /api/v1/admin/capabilities/tenant
PUT  /api/v1/admin/capabilities/tenant/{capability_id}
GET  /api/v1/admin/plugins
POST /api/v1/admin/plugins/register
POST /api/v1/admin/plugins/deregister
GET  /api/v1/admin/hooks
POST /api/v1/admin/hooks
GET  /api/v1/admin/hooks/{hook_id}
PUT  /api/v1/admin/hooks/{hook_id}
DELETE /api/v1/admin/hooks/{hook_id}
PUT  /api/v1/admin/hooks/{hook_id}/status
GET  /api/v1/admin/hooks/{hook_id}/statistics
GET  /api/v1/admin/hooks/{hook_id}/executions
GET  /api/v1/admin/medias/bucket
GET  /api/v1/admin/medias/objects
POST /api/v1/admin/medias/cleanup-orphaned-assets
POST /api/v1/admin/medias/object-acl
```

会话、在线、踢线、租户运行时、能力目录与开关、插件、Hook 治理以及媒资路由的说明见 [docs/admin-api.md](../docs/admin-api.md)（英文）。路由集合就是共享的 Core 管理路由表 `core_admin::ROUTES`；本清单与 OpenAPI 文档及该表的一致性由测试保证。

本地默认监听 `0.0.0.0:50051`，配置文件为 `config/services/admin-gateway.toml`。

## API 文档

`GET /swagger-ui` 提供 `GET /api-doc/openapi.json` 的 Swagger UI 页面。Swagger UI 的样式与脚本内嵌在二进制中，经 `/swagger-ui/swagger-ui.css` 与 `/swagger-ui/swagger-ui-bundle.js` 提供，页面不加载任何外部资源，隔离网络中也能打开。这两个文件是未经修改的 Swagger UI 5.3.1 `dist/` 产物，许可为 Apache-2.0，许可证与 NOTICE 保存在 `assets/swagger-ui/`。

## 认证

Admin Gateway 不签发、不刷新、不存储 token，也不持有平台密钥。每个 `/api/v1/admin/**` 请求都带且只带一个网关 admin 分区签的 Staff 上下文（网关 admin 分区是本面的唯一调用方）：

```http
x-flare-ctx: <Staff 上下文>
```

- Cell 内部签名上下文（EdDSA，有效期不超过 60 秒），`actor_type=staff`，用登记为 `kid=gateway` 的 Cell 钥签；
- `tenant_id` 是被管理的应用，只能是一个具体租户（空与 `*` 被拒绝）；
- `permission` 是共享路由表 `core_admin::ROUTES` 里该路由的权限码（如 `core:users:kick`）；平台员工没有 `org_id`，两条 `plugins/*` 写路由只允许平台员工。

网关用 Cell 公钥集（`FLARE_INTERNAL_CTX_PUBLIC_KEYS` 叠加在 `FLARE_INTERNAL_CTX_PUBLIC_KEY` 之上，Staff 钉死 `kid=gateway`）验签，启动时缺失即失败；再用与 admin 分区相同的路由表解析路由，核对签名里的权限码；验过后删掉请求里所有身份头。上下文缺失、重复、格式错误、过期、由其他密钥或 hook 钥签发时回 401；权限码不符回 403 `ADMIN_PERMISSION_DENIED`；组织员工调平台专属路由回 403 `ADMIN_SCOPE_DENIED`。`x-tenant-id` 与 `x-actor-id` 既不要求也不读。网关不读 `FLARE_ADMIN_GATEWAY_AUTH_*`（没有 `core_jwt`、`http_hook`），也不读 `FLARE_ADMIN_GATEWAY_RATE_LIMIT_*`（网关不做限流）；配了其中任何一个，启动日志记一条带变量名的 WARN，照常启动。

下游调用不带调用方自报的身份。会话、在线、能力、插件、Hook 与消息路由把同一枚上下文原样转发，由下游服务再验一次并复核权限码。Media 不认 Staff 上下文：媒资路由用 `FLARE_INTERNAL_CTX_PRIVATE_KEY` 按 `kid=admin-gateway` 把该员工签成应用的 `tenant_admin`，作为 Cell 内部上下文发给 Media；没有这把私钥时媒资路由回 500 `MEDIA_SIGNER_UNAVAILABLE`，不会调用 Media。每次下游调用受 `FLARE_ADMIN_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS`（缺省 5 秒）限时。详见 [docs/admin-api.md](../docs/admin-api.md) 与 [SEC02](../docs/SEC02-console-control-auth.md)。

Admin 写操作还必须携带：

```http
x-audit-reason: compliance-investigation
idempotency-key: request-1
```

也可以用 `x-request-id` 替代 `idempotency-key`。两者都只用于审计日志关联，网关不保存也不去重，同一个键重放会再执行一次。写操作成功后网关向 `FLARE_AUDIT_NATS_URL` 发一条 `flare.audit.effect.v1` 事件（未设则不记审计，启动告警一次）。
两个受限的只读 POST：`/messages/query` 与 `/messages/write-ledger/query`，要求带 `core:messages:read` 的 Staff 上下文，但不要求写操作审计头；`/messages/export` 仍按受保护的写类操作处理（`core:messages:export`）。

## 消息写链路账本查询

`POST /api/v1/admin/messages/write-ledger/query` 用于排查消息从 broker accepted 到 archive/storage/WAL/ACK 的写入状态。该接口是只读内部运维 API，按 Staff 上下文里的应用限定范围，并且请求体至少包含 `server_id`、`conversation_id`、`write_state`、`failed_only=true` 或 `updated_after/updated_before` 之一，避免误触发大表扫描。

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
