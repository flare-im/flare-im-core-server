# Admin API (Admin Gateway)

`flare-admin-gateway` serves the internal management HTTP API under `/api/v1/admin`. Console (the Control backend) is its only intended caller. It is not published through Edge; deploy it on a private network (the single-host template gives Control `CORE_ADMIN_GATEWAY_URL=http://core:50051`).

The machine-readable contract is the OpenAPI document at `GET /api-doc/openapi.json` (Swagger UI at `/swagger-ui`, served without external resources). This page explains what the document cannot: who may call, which headers are enforced, how downstream failures surface, and how the contract is pinned by tests.

## Authentication and tenant scope

Every `/api/v1/admin/**` request needs `authorization: Bearer <admin assertion>` signed by Console:

- EdDSA, issuer `flare-im-console`, audience `flare-core-admin`, `token_use=admin`, lifetime at most 60 seconds;
- an employee subject (the projection worker and scheduler subjects are refused);
- one concrete tenant in the token (`*` is refused).

The request must also carry exactly one `x-tenant-id` equal to the token tenant. The gateway verifies with `FLARE_CONSOLE_PUBLIC_KEY` only and holds no signing key. After verification it drops every identity header from the request (`x-user-id`, `x-tenant-id`, `x-actor-*`, device and session headers) and re-inserts the signed subject and tenant.

For the ADM-01 routes below and the five `messages/*` routes, the gateway forwards the same admin assertion, unchanged, as the only identity on the downstream gRPC call. Conversation, Online, Capability and the Storage Reader verify it again and scope every read or kick to the token tenant; requests carry no tenant or caller ID of their own (the `messages/*` calls add only `x-request-id` and `x-trace-id`, which the Storage Reader uses to correlate logs and to keep one export task per request ID). See [SEC02](SEC02-console-control-auth.md#forwarded-admin-assertion-adm-01) for the downstream side. Media does not accept the assertion; the media routes sign their own Cell internal context instead (see [Media routes](#media-routes)).

| Condition | Status | `reason` |
| --- | --- | --- |
| Missing, malformed, expired or foreign-signed token; wrong audience or `token_use` | 401 | `UNAUTHORIZED` |
| Token subject is not an employee or its tenant is `*`; `x-tenant-id` is missing, duplicated, `*` or differs from the token tenant | 403 | `FORBIDDEN` |
| Write request without `x-actor-id` equal to the token subject, without `x-audit-reason`, or without both `idempotency-key` and `x-request-id` | 403 | `FORBIDDEN` |
| Any request whose `x-audit-reason` is longer than 512 bytes, or whose `x-request-id` or `idempotency-key` is longer than 128 bytes (these headers go into the audit log; they are not truncated) | 400 | `ADMIN_AUDIT_HEADER_TOO_LONG` |

## Write operations

A write is any `POST`, `PUT`, `PATCH` or `DELETE`, except the two bounded read-only queries `POST /messages/query` and `POST /messages/write-ledger/query`. Writes must send:

```http
x-tenant-id: <token tenant>
x-actor-id: <token subject>
x-audit-reason: <why the operator is doing this>
idempotency-key: <correlation key>      # or x-request-id
```

`idempotency-key` (or `x-request-id`) only correlates the request with the caller's records and the gateway's audit log. The gateway does not store or deduplicate it: a replay with the same key runs again, and so does a replay of the same JWT inside its 60-second window. Repeating a kick is harmless because the device is already offline. Repeating orphan cleanup is harmless because it only deletes assets that are orphaned and past their grace period.

What the gateway logs for writes, and where the durable audit row lives, is described under [Audit](#audit).

## Responses and errors

Success is `{"code": 0, "data": {...}}`. Errors are `{"code": <non-zero>, "reason": "<TYPED_REASON>", "message": ...}`; downstream error text is never echoed.

| Downstream answer | Status | `reason` |
| --- | --- | --- |
| `PERMISSION_DENIED` (the downstream refused the tenant scope) | 403 | `FORBIDDEN` |
| `NOT_FOUND` (a conversation of another tenant answers the same as a missing one) | 404 | `NOT_FOUND` |
| `INVALID_ARGUMENT` (bad cursor, blank ID) | 400 | `BAD_REQUEST` |
| `UNAVAILABLE`, connection failure | 503 | `ADMIN_UPSTREAM_UNAVAILABLE` |
| `DEADLINE_EXCEEDED` (the same as API Gateway's Media routes, which answer 504 `MEDIA_TIMEOUT`) | 504 | `ADMIN_UPSTREAM_TIMEOUT` |
| `UNAUTHENTICATED` for an assertion the gateway already verified (deployment error, e.g. the downstream lacks `FLARE_CONSOLE_PUBLIC_KEY`, or Media's `FLARE_INTERNAL_CTX_PUBLIC_KEY` does not pair with the gateway's private key) | 502 | `ADMIN_UPSTREAM_REJECTED_ASSERTION` |
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

`POST /users/{user_id}/kick` lists the user's devices and kicks them one by one. It returns `{user_id, kicked, already_offline}`; `already_offline` holds devices that went offline between the list and the kick. Any other failure (for example Online refusing the tenant scope, or Online becoming unavailable) stops the request and is returned as an error. The request is not transactional: devices kicked before the failure are not rolled back, because a kick cannot be undone, and the error response does not say which ones they were. Read `GET /users/{user_id}/devices` to see what is still online; a retry lists and kicks only those devices.

`POST /users/{user_id}/devices/{device_id}/kick` returns `{user_id, device_id, kicked}`, or 404 when the device is not online.

`POST /tenant/kick-all` kicks every connection of the token tenant and returns `{tenant_id, kicked}`, where `kicked` counts (user, device) pairs. Clients receive `KICKED` with reason `TENANT_UNAVAILABLE`, the same signal as a tenant suspension. `x-audit-reason` is passed to Online as the kick reason for its logs.

### Tenant runtime, capability switches and plugins

These three routes use the Admin-Gateway-only Capability downstream. When it is disabled they answer 503 `ADMIN_CAPABILITY_DISABLED`; the other routes keep working.

`GET /tenant/runtime` returns the tenant projection Capability holds: `{tenant_id, projected, name, status, quota, settings_json, version, projected_at, updated_at}`. `status` is `active`, `suspended` or `deleting`. `quota` is flattened to `max_users`, `max_groups`, `max_group_members`, `send_qps`, `sync_pull_qps` and `max_online_devices_per_user`, where 0 means unlimited. `settings_json` is the Control-supplied JSON text, which Core does not interpret. `projected=false` means Capability has no row for the tenant and every other field is zero.

`GET /capabilities/tenant` returns `{tenant_id, switches}`, sorted by `capability_id`. Each switch has `capability_id`, `enabled` and `updated_at`. `enabled=false` means installed but switched off; a capability that is not listed was never set.

`GET /plugins?capability_id=` returns `{tenant_id, plugins}`, sorted by `plugin_id`. Each plugin has `plugin_id`, `capability_id`, `grpc_authority`, `plugin_version`, `api_version`, `manifest_sha256`, `declared_operations`, `unverified`, `seat_model` and `labels`. With `capability_id`, only instances serving that capability are listed, whether by registration or by declared operation.

## Media routes

The four `medias/*` routes moved here from API Gateway with SEC-05. All paths are under `/api/v1/admin`.

| Method and path | Media RPC | Write | Contract `status` |
| --- | --- | --- | --- |
| `GET /medias/bucket?bucket=` | `MediaService.DescribeBucket` | no | `available` |
| `GET /medias/objects?bucket=&prefix=` | `MediaService.ListObjects` | no | `not_implemented` |
| `POST /medias/cleanup-orphaned-assets` | `MediaService.CleanupOrphanedAssets` | yes | `available` |
| `POST /medias/object-acl` | `MediaService.SetObjectAcl` | yes | `not_implemented` |

Authentication, tenant scope and write headers are the same as for every other route. The downstream identity differs: Media accepts only a Cell-signed internal context (`x-flare-ctx`, EdDSA, 60 seconds), so the gateway signs a fresh one for every RPC with `FLARE_INTERNAL_CTX_PRIVATE_KEY`. It names the token subject as `tenant_admin` of the token tenant and carries the request ID. The admin assertion itself is not sent to Media, and caller-supplied `x-user-id`, `x-actor-*` or `x-flare-ctx` headers never reach it. Without the private key every media route answers 500 `MEDIA_SIGNER_UNAVAILABLE` and does not call Media. Media's side of this boundary is described in [SEC05](SEC05-media-tenant-isolation.md).

Media rejections use the error table above. The 500 reasons are `ADMIN_MEDIA_BUCKET_FAILED`, `ADMIN_MEDIA_LIST_OBJECTS_FAILED`, `ADMIN_MEDIA_CLEANUP_FAILED` and `ADMIN_MEDIA_OBJECT_ACL_FAILED`.

`GET /medias/bucket?bucket=` returns `{bucket, region, storage_class, versioning_enabled, metadata}` for the bucket Media is configured with. `metadata.tenant_prefix` is the token tenant's object-key prefix. A different bucket name is 403; a missing `bucket` parameter is 400. It reports no object statistics.

`GET /medias/objects?bucket=&prefix=` returns `{files}`, each with only `file_id`, `file_name`, `mime_type`, `size` and `object_key`; signed URLs, CDN URLs and file metadata are not passed on. `prefix` is relative to the tenant and optional. The current Media does not implement the tenant object inventory and refuses it, so the route answers 500 `ADMIN_MEDIA_LIST_OBJECTS_FAILED`, never an empty list.

`POST /medias/cleanup-orphaned-assets` takes `{"limit": <n>, "dry_run": false}`; both fields are required (422 otherwise). `limit` 0 means 100, and Media caps it at 1,000. Media does not honor `dry_run` and would delete as in a real run, so the gateway refuses `dry_run: true` with 400 `ADMIN_MEDIA_DRY_RUN_UNSUPPORTED` without calling Media. It returns `{file_ids, success, error_message, scanned}`: `file_ids` are the orphaned message assets of the token tenant that this pass deleted, and `scanned` is their count. The audit target is the tenant.

`POST /medias/object-acl` takes `{file_id, entries: [{principal, permissions}]}` and returns `data: true`. The current Media does not implement ACL changes and refuses every request, so the route answers 500 `ADMIN_MEDIA_OBJECT_ACL_FAILED`, never a successful no-op. The audit target is the file ID.

## Audit

Core has no audit table for Admin Gateway calls. The gateway records structured log events with the tracing target `admin_audit`; the durable audit row belongs to the caller, Control.

| Request | `admin_audit` event |
| --- | --- |
| Writes: the three kick routes, orphan cleanup and object ACL | `access=write`, `route`, `actor_id`, `tenant_id`, `target`, `audit_reason`, `request_id`, `idempotency_key` |
| Sensitive reads: conversation participants, user devices, user presence | `access=read` with the same fields except `idempotency_key`; `audit_reason` is empty unless the caller sent `x-audit-reason`, which reads do not require |
| Every other read | none |

`target` names what the request acts on: the conversation or user ID, `<user_id>/<device_id>` for a device kick, the tenant for kick-all and orphan cleanup, and the file ID for object ACL. The IDs come from percent-decoded path segments (the file ID from the request body), so they can hold any UTF-8, including line breaks, control characters and `/`. The gateway escapes every ID before it is logged:

- RFC 3986 unreserved characters (`A-Z a-z 0-9 - . _ ~`) are kept; every other UTF-8 byte becomes an upper-case `%XX`. Ordinary IDs such as `u1`, `tenant-a` or a UUID are unchanged, and percent-decoding an escaped ID gives back the original.
- In a device kick, each ID is escaped separately before the two are joined with `/`, so `/` in `target` is always the separator: user `u1/d1` with device `x` logs as `u1%2Fd1/x`, and user `u1` with device `d1/x` as `u1/d1%2Fx`.
- An ID longer than 256 bytes after escaping is cut after the last whole character or `%XX` that fits and gets the suffix `!truncated:<original byte length>`. `!` and `:` are always escaped inside an ID, so the suffix cannot be mistaken for content. Truncated targets are not unique.

The three sensitive reads show which conversations a person is in, which devices they use and when they are online, so each one is recorded like a write. The tenant-wide lists, the tenant runtime, capability switches, plugins, the media bucket and object list are not. The events are written after the gateway has accepted the request and before the downstream call, so a request that fails downstream is still recorded; a request the gateway refuses itself (for example 401, 403 or 400) is not. Events never contain the token.

`POST /messages/export` and the two read-only message query POSTs produce no `admin_audit` event.

Control's audit middleware reserves a durable row for every non-GET Console request before it calls the data plane, and completes the row with the result. It writes no row for GET requests. Today Control calls only `gateway/*` and `POST /messages/query` on Admin Gateway. The Console pages that will call the writes and the three sensitive reads (ADM-03) must keep the writes behind non-GET Console requests and write their own audit rows for the sensitive reads.

## Other routes

`auth/check`, `capabilities` (this contract as JSON), `gateway/{health,upstreams,routes,config}` and `messages/*` predate ADM-01 and are described only by the OpenAPI document. `GET /capabilities` and `GET /gateway/routes` list every operation with its write flag; tests keep them equal to the OpenAPI document.

The five `messages/*` routes call the Storage Reader and map its failures with the error table above, so they never return the Storage Reader's error text. Their 500 reasons are `ADMIN_MESSAGE_QUERY_FAILED`, `ADMIN_MESSAGE_DETAIL_FAILED`, `ADMIN_MESSAGE_EVENTS_FAILED`, `ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED` and `ADMIN_MESSAGE_EXPORT_FAILED`. The Storage Reader reports a database it cannot reach (no connection from the pool, a refused or dropped connection, a server shutting down) as `UNAVAILABLE`, which becomes 503 `ADMIN_UPSTREAM_UNAVAILABLE`; other database failures stay `INTERNAL` and 500. Each call takes its own clone of the Storage Reader client, so a slow search does not hold up other admin searches.

The `auth` block of `GET /gateway/config` reports `mode: "console_ed25519"`, the Console public-key verification described under [Authentication and tenant scope](#authentication-and-tenant-scope). The four `hook_*` fields stay only to keep the response shape Control has registered, and always read `null`, `0`, `""` and `null`. The gateway applies no request rate limit, so the `rate_limit` block always reads `enabled: false` with `requests_per_second` and `burst_capacity` at 0. `redacted_fields` is empty because the snapshot holds no secret.

The gateway does not read `FLARE_ADMIN_GATEWAY_AUTH_*` or `FLARE_ADMIN_GATEWAY_RATE_LIMIT_*`. When one of them is set, it logs a WARN with the variable name (never the value) and starts normally; a misconfigured `http_hook` without a URL no longer stops it.

## Downstream configuration

| Downstream | Variable | Static-mode address in the Core container |
| --- | --- | --- |
| Conversation | `FLARE_ADMIN_GATEWAY_GRPC_CONVERSATION_SERVICE_URL` | `http://127.0.0.1:50090` |
| Online | `FLARE_ADMIN_GATEWAY_GRPC_ONLINE_SERVICE_URL` | `http://127.0.0.1:50061` |
| Capability (optional; an empty value disables it) | `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` | `http://127.0.0.1:50110` |
| Storage Reader | `FLARE_ADMIN_GATEWAY_GRPC_STORAGE_READER_SERVICE_URL` | `http://127.0.0.1:60083` |

The other three downstreams, Media among them, and their fallbacks are listed in `config/services/admin-gateway.toml` and `release/README.md`. The flare-deploy single-host template pins all seven `FLARE_ADMIN_GATEWAY_GRPC_*_SERVICE_URL` values to these in-container addresses in Compose, and its preflight refuses any other value. Conversation, Online, Capability and the Storage Reader each need `FLARE_CONSOLE_PUBLIC_KEY`; without it they answer the forwarded assertion with `UNAUTHENTICATED`, which the gateway reports as 502. The media routes need `FLARE_INTERNAL_CTX_PRIVATE_KEY` in the gateway and the paired `FLARE_INTERNAL_CTX_PUBLIC_KEY` in Media.

## Contract tests

- `flare-admin-gateway/tests/admin_ops_contract.rs` starts stand-in Conversation, Online and Capability services on `127.0.0.1` and drives the router. For every ADM-01 route it checks the missing-token 401, the foreign-tenant 403 and, for writes, the audit-header 403s. It also checks the exact response fields, what the downstream received, and that only the verified assertion reaches the downstream.
- `flare-admin-gateway/tests/admin_media_contract.rs` does the same for the four media routes against a stand-in Media on `127.0.0.1`: missing-token 401, foreign-tenant 403, write-header 403s, response fields, the request Media received, the refused `dry_run`, and the typed rejections that never echo Media's text. It verifies the signed context Media received with the Cell public key, checks that Media's real signed-context layer accepts it and refuses a context signed by another Cell key, and that no admin assertion or forged header reaches Media.
- `flare-admin-gateway/tests/admin_audit_log.rs` captures the `admin_audit` events: one per write and per sensitive read, none for other reads or for writes the gateway refused, and no token in any event. It also sends IDs with a line break, control characters, an encoded `/` and 1,000 bytes, and checks the escaped `target` of each.
- `flare-admin-gateway/tests/admin_gateway_config_contract.rs` configures an unused `http_hook` with a URL and secret and an enabled rate limit, and checks that `GET /gateway/config` still reports `console_ed25519` with no hook values and no rate limit, and that the OpenAPI `AdminAuthConfigSnapshot` keeps the five fields Control has registered.
- `flare-admin-gateway/tests/admin_messages_contract.rs` starts a stand-in Storage Reader on `127.0.0.1` and checks, for each of the five `messages/*` routes, every downstream status in the error table and the route's fixed 500 reason, with no downstream text in the response; that only the verified assertion and the request and trace IDs reach the Storage Reader; and that the other four routes answer while a search is held in the Storage Reader.
- `flare-admin-gateway/tests/admin_openapi_snapshot.rs` compares the `AdminApiDoc` operation set with `tests/snapshots/admin_openapi_operations.txt`. It also requires `GET /capabilities` and `GET /gateway/routes` to list the same operations, every documented operation to be routed behind admin authentication (401 without a token, never 404), the endpoint lists in both Admin Gateway READMEs to match the document, and the route tables on this page to cover the ADM-01 and media routes and nothing else. After an intended API change, rerun it with `FLARE_UPDATE_SNAPSHOTS=1` and review the snapshot diff together with the change.

## Known limits

- Core writes no durable audit row for admin calls, reads or writes; see [Audit](#audit). The `capability_audit_log` table records capability policy changes only.
- Idempotency keys are not deduplicated; see [Write operations](#write-operations).
- `GET /medias/objects` and `POST /medias/object-acl` fail until Media implements them.
- `POST /users/{user_id}/kick` kicks devices sequentially within one request; a user with many devices takes one Online round trip per device.
