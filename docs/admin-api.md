# Admin API (Admin Gateway)

`flare-admin-gateway` serves the internal management HTTP API under `/api/v1/admin`. Console (the Control backend) is its only intended caller. It is not published through Edge; deploy it on a private network (the single-host template gives Control `CORE_ADMIN_GATEWAY_URL=http://core:50051`).

The machine-readable contract is the OpenAPI document at `GET /api-doc/openapi.json` (Swagger UI at `/swagger-ui`, served without external resources). This page explains what the document cannot: who may call, which headers are enforced, how downstream failures surface, and how the contract is pinned by tests.

## Authentication and tenant scope

Every `/api/v1/admin/**` request needs `authorization: Bearer <admin assertion>` signed by Console:

- EdDSA, issuer `flare-im-console`, audience `flare-core-admin`, `token_use=admin`, lifetime at most 60 seconds;
- an employee subject (the projection worker and scheduler subjects are refused);
- one concrete tenant in the token (`*` is refused).

The request must also carry exactly one `x-tenant-id` equal to the token tenant. The gateway verifies with `FLARE_CONSOLE_PUBLIC_KEY` only and holds no signing key. After verification it drops every identity header from the request (`x-user-id`, `x-tenant-id`, `x-actor-*`, device and session headers) and re-inserts the signed subject and tenant.

For the ADM-01 routes below, the gateway forwards the same admin assertion, unchanged, as the only identity on the downstream gRPC call. Conversation, Online and Capability verify it again and scope every read or kick to the token tenant; requests carry no tenant or caller ID of their own. See [SEC02](SEC02-console-control-auth.md#forwarded-admin-assertion-adm-01) for the downstream side.

| Condition | Status | `reason` |
| --- | --- | --- |
| Missing, malformed, expired or foreign-signed token; wrong audience or `token_use` | 401 | `UNAUTHORIZED` |
| Token subject is not an employee or its tenant is `*`; `x-tenant-id` is missing, duplicated, `*` or differs from the token tenant | 403 | `FORBIDDEN` |
| Write request without `x-actor-id` equal to the token subject, without `x-audit-reason`, or without both `idempotency-key` and `x-request-id` | 403 | `FORBIDDEN` |

## Write operations

A write is any `POST`, `PUT`, `PATCH` or `DELETE`, except the two bounded read-only queries `POST /messages/query` and `POST /messages/write-ledger/query`. Writes must send:

```http
x-tenant-id: <token tenant>
x-actor-id: <token subject>
x-audit-reason: <why the operator is doing this>
idempotency-key: <stable key>      # or x-request-id
```

The gateway writes one `admin_audit` log line per accepted write (route, actor, tenant, target, reason, request ID, idempotency key). The three kick routes are naturally idempotent: a repeated kick finds the device already offline. The JWT alone does not deduplicate a replay inside its 60-second window.

## Responses and errors

Success is `{"code": 0, "data": {...}}`. Errors are `{"code": <non-zero>, "reason": "<TYPED_REASON>", "message": ...}`; downstream error text is never echoed.

| Downstream answer | Status | `reason` |
| --- | --- | --- |
| `PERMISSION_DENIED` (the downstream refused the tenant scope) | 403 | `FORBIDDEN` |
| `NOT_FOUND` (a conversation of another tenant answers the same as a missing one) | 404 | `NOT_FOUND` |
| `INVALID_ARGUMENT` (bad cursor, blank ID) | 400 | `BAD_REQUEST` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, connection failure | 503 | `ADMIN_UPSTREAM_UNAVAILABLE` |
| `UNAUTHENTICATED` for an assertion the gateway already verified (deployment error, e.g. the downstream lacks `FLARE_CONSOLE_PUBLIC_KEY`) | 502 | `ADMIN_UPSTREAM_REJECTED_ASSERTION` |
| Anything else | 500 | a fixed per-route reason such as `ADMIN_CONVERSATION_LIST_FAILED` |
| Capability downstream disabled | 503 | `ADMIN_CAPABILITY_DISABLED` |
| `limit` below 1 | 400 | `ADMIN_PAGE_LIMIT_INVALID` |

Times are Unix milliseconds. Enums are lower-case strings (`unspecified` for values the gateway does not know), never proto numbers.

## ADM-01 routes

All paths are under `/api/v1/admin`.

| Method and path | Downstream RPC | Write |
| --- | --- | --- |
| `GET /conversations` | `ConversationAdminService.AdminListConversations` | no |
| `GET /conversations/{conversation_id}` | `ConversationAdminService.AdminGetConversation` | no |
| `GET /conversations/{conversation_id}/participants` | `ConversationAdminService.AdminListParticipants` | no |
| `GET /users/{user_id}/devices` | `OnlineAdminService.AdminListUserDevices` | no |
| `GET /users/{user_id}/presence` | `OnlineAdminService.AdminGetUserPresence` | no |
| `POST /users/{user_id}/kick` | `AdminListUserDevices`, then `OnlineService.KickDevice` per device | yes |
| `POST /users/{user_id}/devices/{device_id}/kick` | `OnlineService.KickDevice` | yes |
| `POST /tenant/kick-all` | `OnlineService.KickTenant` (tenant from the token) | yes |
| `GET /tenant/runtime` | `CapabilityAdminService.GetTenantRuntime` | no |
| `GET /capabilities/tenant` | `CapabilityAdminService.ListTenantCapabilitySwitches` | no |
| `GET /plugins` | `CapabilityService.ListRegisteredPlugins` (tenant from the token) | no |

### Conversations

`GET /conversations?cursor=&limit=&include_deleted=` returns `{conversations, next_cursor, has_more, limit}`, newest update first. `limit` defaults to 50 and is capped at 200; `next_cursor` is `null` on the last page. Each conversation has `conversation_id`, `conversation_type`, `channel_id`, `display_name`, `avatar_url`, `description`, `visibility` (`private`, `tenant`, `public`), `lifecycle_state` (`active`, `suspended`, `archived`, `deleted`), `member_count` (current members), `created_at`, `updated_at` and `attributes`. Conversations updated while you page can move to the front and be skipped or repeated; this is accepted for an operator view.

`GET /conversations/{conversation_id}` returns `{conversation}` with the same fields and no member list.

`GET /conversations/{conversation_id}/participants?cursor=&limit=&include_removed=` returns `{conversation_id, participants, next_cursor, has_more, member_count, limit}`, oldest join first. `limit` defaults to 200 and is capped at 500. Each participant has `user_id`, `roles`, `muted`, `pinned`, `mention_only`, `visible_from_seq` (0 = whole history), `joined_at` and `attributes`.

### Sessions and presence

`GET /users/{user_id}/devices` returns `{user_id, devices}`. A device has `device_id`, `platform`, `model`, `os_version`, `priority` (`low`, `normal`, `high`, `critical`), `token_version`, `last_active_at`, `gateway_id` and `server_id`.

`GET /users/{user_id}/presence` returns `{user_id, online, last_seen_at, devices}`. A user Online has never seen reads as offline with `last_seen_at: null`.

`POST /users/{user_id}/kick` lists the user's devices and kicks them one by one. It returns `{user_id, kicked, already_offline}`; `already_offline` holds devices that went offline between the list and the kick. Any other failure (for example Online refusing the tenant scope) stops the request and is returned as an error; devices kicked before it stay offline, and a retry only sees the rest.

`POST /users/{user_id}/devices/{device_id}/kick` returns `{user_id, device_id, kicked}`, or 404 when the device is not online.

`POST /tenant/kick-all` kicks every connection of the token tenant and returns `{tenant_id, kicked}`, where `kicked` counts (user, device) pairs. Clients receive `KICKED` with reason `TENANT_UNAVAILABLE`, the same signal as a tenant suspension. `x-audit-reason` is passed to Online as the kick reason for its logs.

### Tenant runtime, capability switches and plugins

These three routes use the Admin-Gateway-only Capability downstream. When it is disabled they answer 503 `ADMIN_CAPABILITY_DISABLED`; the other routes keep working.

`GET /tenant/runtime` returns the tenant projection Capability holds: `{tenant_id, projected, name, status, quota, settings_json, version, projected_at, updated_at}`. `status` is `active`, `suspended` or `deleting`. `quota` is flattened to `max_users`, `max_groups`, `max_group_members`, `send_qps`, `sync_pull_qps` and `max_online_devices_per_user`, where 0 means unlimited. `settings_json` is the Control-supplied JSON text, which Core does not interpret. `projected=false` means Capability has no row for the tenant and every other field is zero.

`GET /capabilities/tenant` returns `{tenant_id, switches}`, sorted by `capability_id`. Each switch has `capability_id`, `enabled` and `updated_at`. `enabled=false` means installed but switched off; a capability that is not listed was never set.

`GET /plugins?capability_id=` returns `{tenant_id, plugins}`, sorted by `plugin_id`. Each plugin has `plugin_id`, `capability_id`, `grpc_authority`, `plugin_version`, `api_version`, `manifest_sha256`, `declared_operations`, `unverified`, `seat_model` and `labels`. With `capability_id`, only instances serving that capability are listed, whether by registration or by declared operation.

## Other routes

`auth/check`, `capabilities` (this contract as JSON), `gateway/{health,upstreams,routes,config}`, `messages/*` and `medias/*` predate ADM-01. `GET /capabilities` and `GET /gateway/routes` list every operation with its write flag; tests keep them equal to the OpenAPI document.

## Downstream configuration

| Downstream | Variable | Static-mode address in the Core container |
| --- | --- | --- |
| Conversation | `FLARE_ADMIN_GATEWAY_GRPC_CONVERSATION_SERVICE_URL` | `http://127.0.0.1:50090` |
| Online | `FLARE_ADMIN_GATEWAY_GRPC_ONLINE_SERVICE_URL` | `http://127.0.0.1:50061` |
| Capability (optional; an empty value disables it) | `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` | `http://127.0.0.1:50110` |

The other four downstreams and their fallbacks are listed in `config/services/admin-gateway.toml` and `release/README.md`. The flare-deploy single-host template pins all seven `FLARE_ADMIN_GATEWAY_GRPC_*_SERVICE_URL` values to these in-container addresses in Compose, and its preflight refuses any other value. Conversation, Online and Capability each need `FLARE_CONSOLE_PUBLIC_KEY`; without it they answer the forwarded assertion with `UNAUTHENTICATED`, which the gateway reports as 502.

## Contract tests

- `flare-admin-gateway/tests/admin_ops_contract.rs` starts stand-in Conversation, Online and Capability services on `127.0.0.1` and drives the router. For every ADM-01 route it checks the missing-token 401, the foreign-tenant 403 and, for writes, the audit-header 403s. It also checks the exact response fields, what the downstream received, and that only the verified assertion reaches the downstream.
- `flare-admin-gateway/tests/admin_openapi_snapshot.rs` compares the `AdminApiDoc` operation set with `tests/snapshots/admin_openapi_operations.txt`. It also requires `GET /capabilities` and `GET /gateway/routes` to list the same operations, and every documented operation to be routed behind admin authentication (401 without a token, never 404). After an intended API change, rerun it with `FLARE_UPDATE_SNAPSHOTS=1` and review the snapshot diff together with the change.

## Known limits

- Admin reads are logged with `tracing` by the downstream services and are not written to the Capability audit table.
- `POST /users/{user_id}/kick` kicks devices sequentially within one request; a user with many devices takes one Online round trip per device.
