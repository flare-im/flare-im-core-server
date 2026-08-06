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
```

Locally it listens on `0.0.0.0:50051` by default, and its configuration file is `config/services/admin-gateway.toml`.

## Authentication

The Admin Gateway does not issue, refresh, or store tokens. It only restores the standard principal via `FLARE_ADMIN_GATEWAY_AUTH_MODE=core_jwt` or `FLARE_ADMIN_GATEWAY_AUTH_MODE=http_hook`.

Example:

```bash
FLARE_ADMIN_GATEWAY_SERVER_PORT=50051
FLARE_ADMIN_GATEWAY_AUTH_MODE=http_hook
FLARE_ADMIN_GATEWAY_AUTH_HOOK_URL=http://127.0.0.1:8088/internal/admin/auth/validate
FLARE_ADMIN_GATEWAY_AUTH_HOOK_TIMEOUT_MS=800
FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET=change-me
FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET_HEADER=x-flare-admin-auth-hook-secret
```

It is recommended that the business authentication system return the following scopes:

```text
admin_gateway:admin
admin_gateway:admin:*
```

Admin write operations must carry:

```http
x-tenant-id: tenant-1
x-actor-id: admin-1
x-audit-reason: compliance-investigation
x-request-id: request-1
```

You may also use `idempotency-key` instead of `x-request-id` for idempotency and audit correlation.

## Message write-path ledger query

`POST /api/v1/admin/messages/write-ledger/query` is used to troubleshoot the write status of a message from broker-accepted through archive/storage/WAL/ACK. This endpoint is a read-only internal operations API. It must carry `x-tenant-id`, and the request body must include at least one of `server_id`, `conversation_id`, `write_state`, `failed_only=true`, or `updated_after/updated_before`, to avoid accidentally triggering a large table scan.

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
