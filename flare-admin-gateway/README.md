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
GET  /api/v1/admin/capabilities/tenant
GET  /api/v1/admin/plugins
GET  /api/v1/admin/medias/bucket
GET  /api/v1/admin/medias/objects
POST /api/v1/admin/medias/cleanup-orphaned-assets
POST /api/v1/admin/medias/object-acl
```

The conversation, session, kick, tenant runtime, capability switch and plugin routes (ADM-01) and the media routes are described in [docs/admin-api.md](../docs/admin-api.md). A test keeps this list equal to the OpenAPI document.

Locally it listens on `0.0.0.0:50051` by default, and its configuration file is `config/services/admin-gateway.toml`.

## API docs

`GET /swagger-ui` serves Swagger UI for `GET /api-doc/openapi.json`. The Swagger UI stylesheet and script are embedded in the binary and served from `/swagger-ui/swagger-ui.css` and `/swagger-ui/swagger-ui-bundle.js`, so the page loads no external resources and works on isolated networks. They are unmodified Swagger UI 5.3.1 `dist/` files under Apache-2.0; the license and NOTICE are kept in `assets/swagger-ui/`.

## Authentication

The Admin Gateway does not issue, refresh or store tokens, and it holds no key to sign them. Every `/api/v1/admin/**` request carries an admin assertion signed by Console (the Control backend):

```http
authorization: Bearer <admin assertion>
x-tenant-id: tenant-1
```

- EdDSA, issuer `flare-im-console`, audience `flare-core-admin`, `token_use=admin`, lifetime at most 60 seconds;
- the subject is an employee (the projection worker and scheduler subjects are refused), and the token names one concrete tenant (`*` is refused);
- `x-tenant-id` appears exactly once and equals the token tenant.

The gateway verifies the assertion with `FLARE_CONSOLE_PUBLIC_KEY`, which it requires at startup. It then drops every identity header from the request and puts back the subject and tenant from the token. A missing, malformed, expired or foreign-signed token is 401; a tenant mismatch is 403.

Downstream calls carry no caller-supplied identity. The conversation, online, capability and plugin routes forward the same assertion unchanged, and those services verify it again. Media does not accept the assertion: the media routes sign the employee as `tenant_admin` of the token tenant in a Cell internal context, using `FLARE_INTERNAL_CTX_PRIVATE_KEY`. Without that key they answer 500 `MEDIA_SIGNER_UNAVAILABLE` and never call Media. See [docs/admin-api.md](../docs/admin-api.md) and [SEC02](../docs/SEC02-console-control-auth.md).

Admin write operations must also carry:

```http
x-actor-id: <token subject>
x-audit-reason: compliance-investigation
idempotency-key: request-1
```

`x-request-id` may replace `idempotency-key`. Either one only correlates the request in audit logs; the gateway does not store or deduplicate it, so a replay runs again.
The two bounded read-only POSTs, `/messages/query` and `/messages/write-ledger/query`, require the signed employee token and concrete tenant but not write-operation audit headers. `/messages/export` remains a protected write-class operation.

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
