# Flare Admin Gateway

English · [中文](README.zh-CN.md)

`flare-admin-gateway` is the internal management API entry point of Flare IM Core. It only provides a secure API boundary, a typed HTTP facade, audit-context validation, and read-only operational snapshots. It does not provide an admin console UI, business administrators, role approvals, or a menu system.

## Boundary

| Service | Exposed surface | Primary responsibility |
|------|--------|----------|
| `flare-api-gateway` | public/third-party HTTP API | Business access APIs for messages, conversations, presence, media, etc. |
| `flare-admin-gateway` | internal Admin HTTP API | Management queries, export tasks, gateway operational snapshots, capability discovery |
| Business admin console | Business side | UI, roles, approvals, menus, administrator lifecycle |

For production deployment, it is recommended to place `flare-admin-gateway` behind an internal network, VPN, mTLS, or a service mesh policy, and not share the public exposure surface with the public gateway.

## Default endpoints

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

The conversation, session, kick, tenant runtime, capability, plugin, hook and media routes are described in [docs/admin-api.md](../docs/admin-api.md). The route set is the shared Core admin route table `core_admin::ROUTES`; a test keeps this list equal to the OpenAPI document and to that table.

Locally it listens on `0.0.0.0:50051` by default, and its configuration file is `config/services/admin-gateway.toml`.

## API docs

`GET /swagger-ui` serves Swagger UI for `GET /api-doc/openapi.json`. The Swagger UI stylesheet and script are embedded in the binary and served from `/swagger-ui/swagger-ui.css` and `/swagger-ui/swagger-ui-bundle.js`, so the page loads no external resources and works on isolated networks. They are unmodified Swagger UI 5.3.1 `dist/` files under Apache-2.0; the license and NOTICE are kept in `assets/swagger-ui/`.

## Authentication

The Admin Gateway does not issue, refresh or store tokens, and it holds no platform key. Every `/api/v1/admin/**` request carries exactly one Staff context signed by the Social gateway admin partition, the only caller of this surface:

```http
x-flare-ctx: <staff context>
```

- the cell-internal signed context (EdDSA, lifetime at most 60 seconds) with `actor_type=staff`, signed with the Cell key registered as `kid=gateway`;
- `tenant_id` is the application being administered, one concrete tenant (empty and `*` are refused);
- `permission` is the route's code from the shared route table `core_admin::ROUTES` (for example `core:users:kick`); `org_id` is absent for platform staff, and the two `plugins/*` writes are for platform staff only.

The gateway verifies the context with the Cell key set (`FLARE_INTERNAL_CTX_PUBLIC_KEYS` layered over `FLARE_INTERNAL_CTX_PUBLIC_KEY`, Staff pinned to `kid=gateway`), which it requires at startup, resolves the route with the same table the admin partition used, and compares the signed code with the route's. It then drops every identity header from the request. A missing, duplicated, malformed, expired, foreign-signed or hook-signed context is 401; another route's code is 403 `ADMIN_PERMISSION_DENIED`; organization staff on a platform-only route is 403 `ADMIN_SCOPE_DENIED`. `x-tenant-id` and `x-actor-id` are neither required nor read. The gateway does not read `FLARE_ADMIN_GATEWAY_AUTH_*` (no `core_jwt` or `http_hook`) or `FLARE_ADMIN_GATEWAY_RATE_LIMIT_*` (it applies no rate limit); if one is set, startup logs a WARN with the variable name and goes on.

Downstream calls carry no caller-supplied identity. The conversation, online, capability, plugin, hook and message routes forward the same context unchanged, and those services verify it again and re-check the code. Media does not accept the staff context: the media routes sign the employee as `tenant_admin` of the application in a Cell internal context under `kid=admin-gateway`, using `FLARE_INTERNAL_CTX_PRIVATE_KEY`. Without that key they answer 500 `MEDIA_SIGNER_UNAVAILABLE` and never call Media. Every downstream call is bounded by `FLARE_ADMIN_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS` (default 5 seconds). See [docs/admin-api.md](../docs/admin-api.md) and [SEC02](../docs/SEC02-console-control-auth.md).

Admin write operations must also carry:

```http
x-audit-reason: compliance-investigation
idempotency-key: request-1
```

`x-request-id` may replace `idempotency-key`. Either one only correlates the request in audit logs; the gateway does not store or deduplicate it, so a replay runs again. After a successful write the gateway publishes a `flare.audit.effect.v1` event to `FLARE_AUDIT_NATS_URL` (unset: not audited, one startup warning).
The two bounded read-only POSTs, `/messages/query` and `/messages/write-ledger/query`, require the staff context with `core:messages:read` but not write-operation audit headers. `/messages/export` remains a protected write-class operation (`core:messages:export`).

## Message write-path ledger query

`POST /api/v1/admin/messages/write-ledger/query` is used to troubleshoot the write status of a message from broker-accepted through archive/storage/WAL/ACK. This endpoint is a read-only internal operations API scoped to the application in the staff context, and the request body must include at least one of `server_id`, `conversation_id`, `write_state`, `failed_only=true`, or `updated_after/updated_before`, to avoid accidentally triggering a large table scan.

Example:

```json
{
  "conversation_id": "conv-1",
  "failed_only": true,
  "updated_after": 1700000000,
  "limit": 100
}
```

Typical `write_state` values include `broker_accepted`, `archive_persisted`, `storage_persisted`, `wal_cleaned`, `ack_published`, as well as failure states such as `wal_cleanup_failed` and `ack_publish_failed`.
