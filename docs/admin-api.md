# Admin API (Admin Gateway)

`flare-admin-gateway` serves the internal management HTTP API under `/api/v1/admin`. The Social gateway admin partition is its only intended caller: it resolves `/admin/v1/apps/{app}/<path>` against the shared Core admin route table, signs the employee into a Staff context, and proxies to this surface. It is not published through Edge; deploy it on a private network (the single-host template gives the gateway `FLARE_CORE_ADMIN_URL=http://core:50051`).

The machine-readable contract is the OpenAPI document at `GET /api-doc/openapi.json` (Swagger UI at `/swagger-ui`, served without external resources). This page explains what the document cannot: who may call, which headers are enforced, how downstream failures surface, which audit events a write produces, and how the contract is pinned by tests.

## Authentication and tenant scope

Every `/api/v1/admin/**` request carries exactly one `x-flare-ctx`: the cell-internal signed context (EdDSA, lifetime at most 60 seconds) with `actor_type=staff`, signed by the Social gateway admin partition with the Cell key registered as `kid=gateway`. The gateway verifies it with the Cell key set (`FLARE_INTERNAL_CTX_PUBLIC_KEYS` layered over `FLARE_INTERNAL_CTX_PUBLIC_KEY`, Staff pinned to `kid=gateway`) and then checks the signed grant against the route:

- `tenant_id` is the application being administered: one concrete tenant (empty and `*` are refused);
- `permission` is the route's code from `flare_core_infra::auth::core_admin::ROUTES`, the table both ends resolve; the routes and codes are listed below;
- `org_id` is the staff member's organization, absent for platform staff; the two plugin registration routes are for platform staff only;
- `actor_id` is the employee, recorded as the operator everywhere downstream; `request_id` and `trace_id` are its correlation ids.

The gateway holds no platform key and reads no `authorization` header: a platform-issued assertion of any shape is not an identity here. It does not read `x-tenant-id` or `x-actor-id` either; after verification it drops every identity header (`x-user-id`, `x-tenant-id`, `x-actor-*`, device and session headers, `authorization`) before the handlers run.

Every downstream call forwards the same context, unchanged, as the only identity: Conversation, Online, Capability and the Storage Reader verify it again with their own copy of the key set and compare the code with the operation they serve (the `messages/*` calls add only `x-request-id` and `x-trace-id` from the claims). Online's `AdminListUserDevices` admits `core:users:read` and `core:users:kick`, because `POST /users/{user_id}/kick` lists the devices with its kick context before it kicks them; `AdminGetUserPresence` admits only the read code. See [SEC02](SEC02-console-control-auth.md#the-staff-context) for the downstream side. Media does not accept the staff context; the media routes sign their own Cell internal context instead (see [Media routes](#media-routes)).

| Condition | Status | `reason` |
| --- | --- | --- |
| No `x-flare-ctx` | 401 | `UNAUTHORIZED` |
| Two `x-flare-ctx` headers; a malformed, expired or foreign-signed context; a Staff context signed with a key id other than `gateway`; a verified context that is not Staff (for example a service context) | 401 | `ADMIN_STAFF_CONTEXT_INVALID` |
| The path is not an admin route, or a path parameter is not an identifier (percent-encoded, longer than 128 bytes, or outside `A-Z a-z 0-9 _ - . :`) | 404 | `NOT_FOUND` |
| The signed `permission` is not the route's code | 403 | `ADMIN_PERMISSION_DENIED` |
| Organization staff (`org_id` present) on a platform-only route | 403 | `ADMIN_SCOPE_DENIED` |
| The signed application is empty or `*` | 403 | `ADMIN_IDENTITY_REQUIRED` |
| Write request without `x-audit-reason` (or with one that is blank once decoded), or without both `idempotency-key` and `x-request-id` | 403 | `FORBIDDEN` |
| Any request whose `x-audit-reason` is longer than 512 bytes once percent-decoded or 1536 bytes as sent, or whose `x-request-id` or `idempotency-key` is longer than 128 bytes (these headers go into the audit log; they are not truncated) | 400 | `ADMIN_AUDIT_HEADER_TOO_LONG` |
| Any request with more than one `x-audit-reason`, or one that is not percent-encoded UTF-8: a `%` not followed by two hex digits, decoded bytes that are not UTF-8, or a raw byte outside visible ASCII | 400 | `ADMIN_AUDIT_REASON_INVALID` |

Requests the gateway cannot parse are refused before any downstream call, with the same error envelope as every other error rather than the framework's plain-text 400, 415 or 422. The message is a fixed sentence; the parser's text, which quotes values and positions from the request, is neither returned nor logged.

| Condition | Status | `reason` |
| --- | --- | --- |
| Body not sent as `application/json`, not valid JSON, or a field missing or of the wrong type (for example a number where `POST /messages/query` expects a string) | 400 | `ADMIN_INVALID_JSON_BODY` |
| Body larger than `FLARE_ADMIN_GATEWAY_SERVER_MAX_BODY_SIZE` (10 MiB by default) | 400 | `ADMIN_REQUEST_BODY_TOO_LARGE` |
| Query parameter missing where required (`bucket` on the media routes) or of the wrong type (`limit=abc`, `include_deleted=maybe`) | 400 | `ADMIN_INVALID_QUERY` |
| Path parameter that axum cannot extract after the route table accepted it | 400 | `ADMIN_INVALID_PATH` |

`GET /auth/check` (`core:gateway:read`) echoes what the context signs: `user_id` and `actor_id` (the employee), `tenant_id`, `org_id`, `permission`, `request_id`, `trace_id`, `scopes`.

## Write operations

A write is every route the shared table marks `write`: every `POST`, `PUT` and `DELETE` except the two bounded read-only queries `POST /messages/query` and `POST /messages/write-ledger/query`. Writes must send:

```http
x-flare-ctx: <staff context carrying the route's write code>
x-audit-reason: <why the operator is doing this, percent-encoded UTF-8>
idempotency-key: <correlation key>      # or x-request-id
```

`x-audit-reason` carries the operator's free text as percent-encoded UTF-8, which is what JavaScript `encodeURIComponent` produces, so a reason in Chinese or any other script travels as visible ASCII through the browser and the Social gateway: `清理违规 spam` is sent as `%E6%B8%85%E7%90%86%E8%BF%9D%E8%A7%84%20spam`. Plain ASCII without `%` decodes to itself, and `+` stays `+`. The Social gateway admin partition checks the same rules and forwards the encoded value unchanged; this gateway decodes it. The rules: exactly one header, at most 1536 bytes as sent, valid UTF-8 of at most 512 bytes once decoded, and not blank after trimming. The 1536-byte limit is not a separate rule but a cheap check made before decoding: percent-encoding takes at most three bytes per decoded byte, so every well-formed value over 1536 bytes is also over 512 bytes once decoded. Its only visible effect is on a value over 1536 bytes that is also malformed or repeated: it gets `ADMIN_AUDIT_HEADER_TOO_LONG`, without being decoded, instead of `ADMIN_AUDIT_REASON_INVALID`. The decoded, trimmed reason is what the audit log records and what Online receives as the kick reason. Reads do not require a reason, but one that is sent is checked the same way.

`idempotency-key` (or `x-request-id`) only correlates the request with the caller's records and the gateway's audit log. The gateway does not store or deduplicate it: a replay with the same key runs again, and so does a replay of the same context inside its 60-second window. Repeating a kick is harmless because the device is already offline. Repeating orphan cleanup is harmless because it only deletes assets that are orphaned and past their grace period.

What the gateway logs for writes and which effect event it publishes is described under [Audit](#audit).

## Responses and errors

Success is `{"code": 0, "data": {...}}`. Errors are `{"code": <non-zero>, "reason": "<TYPED_REASON>", "message": ...}`; downstream error text is never echoed.

| Downstream answer | Status | `reason` |
| --- | --- | --- |
| `PERMISSION_DENIED` (the downstream refused the application, or a hook or session of another application) | 403 | `FORBIDDEN` |
| `NOT_FOUND` (a conversation or hook of another application answers the same as a missing one) | 404 | `NOT_FOUND` |
| `INVALID_ARGUMENT` (bad cursor, blank ID, a capability outside a plugin's declared operations) | 400 | `BAD_REQUEST` |
| `UNAVAILABLE`, connection failure | 503 | `ADMIN_UPSTREAM_UNAVAILABLE` |
| `DEADLINE_EXCEEDED`, including the gateway's own per-call timeout `FLARE_ADMIN_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS` (5 seconds by default; the same as API Gateway's Media routes, which answer 504 `MEDIA_TIMEOUT`) | 504 | `ADMIN_UPSTREAM_TIMEOUT` |
| `UNAUTHENTICATED` for a context the gateway already verified (deployment error, e.g. the downstream's key set does not hold the gateway key, or Media's key set does not hold the admin-gateway key) | 502 | `ADMIN_UPSTREAM_REJECTED_ASSERTION` |
| Anything else, including hook governance disabled in Capability | 500 | a fixed per-route reason such as `ADMIN_CONVERSATION_LIST_FAILED` |
| Capability downstream disabled | 503 | `ADMIN_CAPABILITY_DISABLED` |
| `limit` below 1 | 400 | `ADMIN_PAGE_LIMIT_INVALID` |

Times are Unix milliseconds. Enums are lower-case strings (`unspecified` for values the gateway does not know), never proto numbers; hook points use the proto names (`HOOK_POINT_MESSAGE_PRE_SEND`).

## ADM-01 routes

All paths are under `/api/v1/admin`. The code column is the signed `permission` the route requires.

| Method and path | Code | Downstream RPC | Write |
| --- | --- | --- | --- |
| `GET /conversations` | `core:conversations:read` | `ConversationAdminService.AdminListConversations` | no |
| `GET /conversations/{conversation_id}` | `core:conversations:read` | `ConversationAdminService.AdminGetConversation` | no |
| `GET /conversations/{conversation_id}/participants` | `core:conversations:read` | `ConversationAdminService.AdminListParticipants` | no |
| `GET /users/{user_id}/devices` | `core:users:read` | `OnlineAdminService.AdminListUserDevices` | no |
| `GET /users/{user_id}/presence` | `core:users:read` | `OnlineAdminService.AdminGetUserPresence` | no |
| `POST /users/{user_id}/kick` | `core:users:kick` | `AdminListUserDevices`, then `OnlineService.KickDevice` per device | yes |
| `POST /users/{user_id}/devices/{device_id}/kick` | `core:users:kick` | `OnlineService.KickDevice` | yes |
| `POST /tenant/kick-all` | `core:tenant:kick` | `OnlineService.KickTenant` (tenant from the context) | yes |
| `GET /tenant/runtime` | `core:tenant:read` | `CapabilityAdminService.GetTenantRuntime` | no |
| `GET /capabilities/catalog` | `core:capabilities:read` | `CapabilityService.ListCapabilities` | no |
| `GET /capabilities/tenant` | `core:capabilities:read` | `CapabilityAdminService.ListTenantCapabilitySwitches` | no |
| `PUT /capabilities/tenant/{capability_id}` | `core:capabilities:write` | `CapabilityService.SetTenantCapabilitySwitch` (tenant from the context) | yes |
| `GET /plugins` | `core:plugins:read` | `CapabilityService.ListRegisteredPlugins` (tenant from the context) | no |
| `POST /plugins/register` | `core:plugins:write`, platform staff only | `CapabilityService.RegisterPluginEndpoint` (tenant and request id from the context) | yes |
| `POST /plugins/deregister` | `core:plugins:write`, platform staff only | `CapabilityService.DeregisterPluginEndpoint` (tenant and request id from the context) | yes |
| `GET /hooks` | `core:hooks:read` | `CapabilityService.Administer` `flare.extension.v1.hook_config.list` (tenant from the context) | no |
| `POST /hooks` | `core:hooks:write` | `Administer` `hook_config.create` (tenant from the context) | yes |
| `GET /hooks/{hook_id}` | `core:hooks:read` | `Administer` `hook_config.get` | no |
| `PUT /hooks/{hook_id}` | `core:hooks:write` | `Administer` `hook_config.update` | yes |
| `DELETE /hooks/{hook_id}` | `core:hooks:write` | `Administer` `hook_config.delete` | yes |
| `PUT /hooks/{hook_id}/status` | `core:hooks:write` | `Administer` `hook_config.set_status` | yes |
| `GET /hooks/{hook_id}/statistics` | `core:hooks:read` | `Administer` `hook_config.get_statistics` | no |
| `GET /hooks/{hook_id}/executions` | `core:hooks:read` | `Administer` `hook_config.query_executions` | no |
| `POST /plugin-admin/{plugin_id}/query` | `plugin:<plugin_id>:admin` | `Administer` `flare.plugin.v1.admin.query` (tenant from the context) | no |
| `POST /plugin-admin/{plugin_id}/dispatch` | `plugin:<plugin_id>:admin` | `Administer` `flare.plugin.v1.admin.dispatch` (tenant from the context) | yes |

### Conversations

`GET /conversations?cursor=&limit=&include_deleted=` returns `{conversations, next_cursor, has_more, limit}`, newest update first. `limit` defaults to 50 and is capped at 200; `next_cursor` is `null` on the last page. Each conversation has `conversation_id`, `conversation_type`, `channel_id`, `display_name`, `avatar_url`, `description`, `visibility` (`private`, `tenant`, `public`), `lifecycle_state` (`active`, `suspended`, `archived`, `deleted`), `member_count` (current members), `created_at`, `updated_at` and `attributes`. Conversations updated while you page can move to the front and be skipped or repeated; this is accepted for an operator view.

`GET /conversations/{conversation_id}` returns `{conversation}` with the same fields and no member list.

`GET /conversations/{conversation_id}/participants?cursor=&limit=&include_removed=` returns `{conversation_id, participants, next_cursor, has_more, member_count, limit}`, oldest join first. `limit` defaults to 200 and is capped at 500. Each participant has `user_id`, `roles`, `muted`, `pinned`, `mention_only`, `visible_from_seq` (0 = whole history), `joined_at` and `attributes`.

### Sessions and presence

`GET /users/{user_id}/devices` returns `{user_id, devices}`. A device has `device_id`, `platform`, `model`, `os_version`, `priority` (`low`, `normal`, `high`, `critical`), `token_version`, `last_active_at`, `gateway_id` and `server_id`.

`GET /users/{user_id}/presence` returns `{user_id, online, last_seen_at, devices}`. A user Online has never seen reads as offline with `last_seen_at: null`.

`POST /users/{user_id}/kick` lists the user's devices and kicks them one by one. It returns `{user_id, kicked, already_offline}`; `already_offline` holds devices that went offline between the list and the kick. Any other failure (for example Online refusing the application, or Online becoming unavailable) stops the request and is returned as an error. The request is not transactional: devices kicked before the failure are not rolled back, because a kick cannot be undone, and the error response does not say which ones they were. Read `GET /users/{user_id}/devices` to see what is still online; a retry lists and kicks only those devices.

`POST /users/{user_id}/devices/{device_id}/kick` returns `{user_id, device_id, kicked}`, or 404 when the device is not online.

`POST /tenant/kick-all` kicks every connection of the signed application and returns `{tenant_id, kicked}`, where `kicked` counts (user, device) pairs. Clients receive `KICKED` with reason `TENANT_UNAVAILABLE`, the same signal as a tenant suspension. The decoded `x-audit-reason` is passed to Online as the kick reason for its logs.

### Tenant runtime, capabilities and plugins

These routes use the Admin-Gateway-only Capability downstream. When it is disabled they answer 503 `ADMIN_CAPABILITY_DISABLED`; the other routes keep working.

`GET /tenant/runtime` returns the tenant projection Capability holds: `{tenant_id, projected, name, status, quota, settings_json, version, projected_at, updated_at}`. `status` is `active`, `suspended` or `deleting`. `quota` is flattened to `max_users`, `max_groups`, `max_group_members`, `send_qps`, `sync_pull_qps` and `max_online_devices_per_user`, where 0 means unlimited. `settings_json` is the platform-supplied JSON text, which Core does not interpret. `projected=false` means Capability has no row for the tenant and every other field is zero.

`GET /capabilities/catalog` returns `{capabilities}`, the descriptors registered in this deployment, sorted by `capability_id`: `capability_id`, `plugin_id`, `version`, `scope`, `visibility`, `permissions`, `message_types`, `timeout_ms` and `description`.

`GET /capabilities/tenant` returns `{tenant_id, switches}`, sorted by `capability_id`. Each switch has `capability_id`, `enabled` and `updated_at`. `enabled=false` means installed but switched off; a capability that is not listed was never set.

`PUT /capabilities/tenant/{capability_id}` takes `{"enabled": true|false}` and sets the switch of the signed application. It returns `{tenant_id, capability_id, enabled, message}`; `message` is Capability's confirmation text.

`GET /plugins?capability_id=` returns `{tenant_id, plugins}`, sorted by `plugin_id`. Each plugin has `plugin_id`, `capability_id`, `grpc_authority`, `plugin_version`, `api_version`, `manifest_sha256`, `declared_operations`, `unverified`, `seat_model` and `labels`. With `capability_id`, only instances serving that capability are listed, whether by registration or by declared operation.

`POST /plugins/register` (platform staff only) takes `{plugin_id, capability_id, grpc_authority, labels, plugin_version, api_version, manifest_sha256, declared_operations, seat_model}`; the first three are required (400 `ADMIN_PLUGIN_FIELD_REQUIRED` otherwise), and `plugin_id`, `capability_id` and every `declared_operations` entry must be 1 to 128 ASCII letters, digits or `_ - . :` (400 `ADMIN_PLUGIN_FIELD_INVALID` otherwise; Capability applies the same rule and answers `INVALID_ARGUMENT`); the tenant and the request id come from the context. It returns `{tenant_id, plugin_id, accepted, message}`; `message` says `registered (unverified)` when `declared_operations` was empty, and Capability refuses a `capability_id` outside a non-empty declaration with 400 `BAD_REQUEST`. `POST /plugins/deregister` takes `{plugin_id}` and returns `{tenant_id, plugin_id, accepted}`; `accepted=false` means the instance was not registered.

Plugin registrations persist across Capability restarts: Capability writes them to `capability_plugin_endpoints` (migration 0011) before it accepts them, and every Capability instance re-reads the table, so a registration made through one instance is listed by all of them within `FLARE_CAPABILITY_PLUGIN_ROUTE_REFRESH_SECS` (10 seconds by default). A registration that is neither re-registered nor healthy within `FLARE_CAPABILITY_PLUGIN_ROUTE_TTL_SECS` (one day by default) is removed. When Capability cannot write the table, register and deregister answer 503 `ADMIN_UPSTREAM_UNAVAILABLE` and nothing changes. Endpoints that Capability discovers from its own configuration are listed too, but are never stored and never expire; see the Capability README.

### Plugin admin pages

The two `plugin-admin/*` routes serve a plugin's `admin-page` contribution. The signed permission is the plugin's own code, `plugin:<plugin_id>:admin` (the shared route table builds it from the path, `core_admin::route_permission`); a plugin id whose code would exceed 128 bytes does not resolve. Both take `{"operation": "...", "input": {...}}` (`input` defaults to `{}`, at most 16 KiB with the plugin id and operation; 400 `ADMIN_PLUGIN_FIELD_INVALID` / `ADMIN_PLUGIN_INPUT_TOO_LARGE`) and return `{tenant_id, plugin_id, operation, request_id, success, data, error_code}`. `query` is a read: the operation must be a rows source or a detail operation of the plugin's admin pages. `dispatch` is a write (audit reason and correlation key required): the operation must be an action target of its admin pages. Capability checks that the plugin is enabled for the signed application and the grant approves the operation, signs a staff ticket (`flare.plugin.staff-call.v1`, no conversation) and calls the plugin; anything else is 403 `FORBIDDEN`. A plugin rejection is a 200 with `success=false` and the plugin's `error_code`. When an action's result carries `data.sharedState`, Capability publishes it to every card bound to the entity.

### Hooks

The eight `hooks/*` routes govern Hook configurations through Capability's `Administer` RPC; Capability checks the stored configuration's tenant before any hook-id operation, so a hook of another application answers 403 `FORBIDDEN` and a missing one 404 `NOT_FOUND`. A `hook_id` is the numeric id or `hook_type:name`.

`GET /hooks?hook_type=&enabled_only=&limit=` returns `{hooks, limit, has_more, total}`. `limit` defaults to 100 and is capped at 1000 (Capability pages by limit only). A hook has `hook_id`, `name`, `hook_type`, `tenant_id`, `priority`, `enabled`, `transport`, `selector`, `retry_policy`, `created_at`, `updated_at`, `hook_points` and `config_extensions`. `transport` carries `type` (`grpc`, `webhook`, `local`), `service_name`, `endpoint`, `registry_type`, `namespace`, `load_balance`, `headers`, `target`, `timeout_ms`, `metadata` and `secret_configured`; the webhook `secret` is accepted on create and update and never returned. `selector` has `tenants`, `conversation_types`, `message_types`, `business_types`, `hook_points`, `app_ids` and `conversation_ids`; `retry_policy` has `max_retries`, `retry_interval_ms` and `backoff_strategy`; `config_extensions` has `contract_version`, `attributes` and `extensions` (a map of `{type_url, value_base64}`).

`POST /hooks` takes `{name, hook_type, priority, transport, selector, retry_policy, hook_points, config_extensions}` (the tenant comes from the context; a `tenant_id` in the body is ignored) and returns `{hook}`. A blank `name` or `hook_type` is 400 `ADMIN_HOOK_NAME_REQUIRED` / `ADMIN_HOOK_TYPE_REQUIRED`, an unknown hook point 400 `ADMIN_HOOK_POINT_INVALID`, an extension that is not base64 400 `ADMIN_HOOK_EXTENSION_INVALID`. Capability refuses a `name` longer than 128 characters or holding a control character (`char::is_control`: line breaks, tab, ESC, DEL, C1) on create and on a rename, which comes back as 400 `BAD_REQUEST`; names stored or configured before this rule still load and run, and Capability escapes control characters wherever it logs a hook name. `GET /hooks/{hook_id}` returns `{hook}`; `PUT /hooks/{hook_id}` takes the same fields minus `hook_type` and returns `{hook}` after the update; `DELETE /hooks/{hook_id}` returns `{hook_id, deleted}`; `PUT /hooks/{hook_id}/status` takes `{"enabled": true|false}` and returns `{hook_id, enabled, updated}`.

`GET /hooks/{hook_id}/statistics?start_time=&end_time=` returns `{hook_id, total_executions, success_count, failure_count, avg_latency_ms, p99_latency_ms, rate_limit_count, circuit_break_count, error_count_by_code}`; without a range Capability takes the last 24 hours, and `end_time` before `start_time` is 400 `ADMIN_HOOK_TIME_RANGE_INVALID`. Times are Unix milliseconds from 0 through 253402300799999 (the end of year 9999); Capability answers a time outside that range, or a `start_time` in the future without an `end_time`, with 400 `BAD_REQUEST`. `GET /hooks/{hook_id}/executions?message_id=&success_only=&start_time=&end_time=&limit=` returns `{hook_id, executions, limit, has_more}`, newest first; an execution has `execution_id`, `hook_id`, `message_id`, `success`, `latency_ms`, `error_code`, `error_message` and `executed_at`.

Both read the `hook_executions` rows Capability records for the application's own hook (migration 0012; see the Capability README, "Execution evidence and circuit breakers"). Rejections, failures and timeouts are recorded in full; allowed calls and circuit-open short circuits are sampled (1 in 100 by default), so the counts are weighted estimates. `success` means the hook answered (allowed or rejected): a rejection has `error_code` `HOOK_REJECTED` and the hook's reason code as `error_message`; a failure has its error code and a fixed message, never the downstream error text; `HOOK_TIMEOUT` and `HOOK_CIRCUIT_OPEN` mark timeouts and short circuits. `circuit_break_count` counts short circuits, `error_count_by_code` counts failures only, `rate_limit_count` is always 0, and average and p99 latency leave short circuits out. Rows older than the retention period (7 days by default) are deleted.

## Media routes

The four `medias/*` routes moved here from API Gateway with SEC-05. All paths are under `/api/v1/admin`.

| Method and path | Code | Media RPC | Write | Contract `status` |
| --- | --- | --- | --- | --- |
| `GET /medias/bucket?bucket=` | `core:medias:read` | `MediaService.DescribeBucket` | no | `available` |
| `GET /medias/objects?bucket=&prefix=` | `core:medias:read` | `MediaService.ListObjects` | no | `not_implemented` |
| `POST /medias/cleanup-orphaned-assets` | `core:medias:write` | `MediaService.CleanupOrphanedAssets` | yes | `available` |
| `POST /medias/object-acl` | `core:medias:write` | `MediaService.SetObjectAcl` | yes | `not_implemented` |

Authentication, tenant scope and write headers are the same as for every other route. The downstream identity differs: Media accepts only a Cell-signed internal context (`x-flare-ctx`, EdDSA, 60 seconds) from a key it holds, so the gateway signs a fresh one for every RPC with `FLARE_INTERNAL_CTX_PRIVATE_KEY` under `kid=admin-gateway`. It names the employee as `tenant_admin` of the signed application and carries the signed request id. The staff context itself is not sent to Media, and caller-supplied `x-user-id`, `x-actor-*` or `x-flare-ctx` headers never reach it. Without the private key every media route answers 500 `MEDIA_SIGNER_UNAVAILABLE` and does not call Media. Media's side of this boundary is described in [SEC05](SEC05-media-tenant-isolation.md).

Media rejections use the error table above. The 500 reasons are `ADMIN_MEDIA_BUCKET_FAILED`, `ADMIN_MEDIA_LIST_OBJECTS_FAILED`, `ADMIN_MEDIA_CLEANUP_FAILED` and `ADMIN_MEDIA_OBJECT_ACL_FAILED`.

`GET /medias/bucket?bucket=` returns `{bucket, region, storage_class, versioning_enabled, metadata}` for the bucket Media is configured with. `metadata.tenant_prefix` is the application's object-key prefix. A different bucket name is 403; a missing `bucket` parameter is 400 `ADMIN_INVALID_QUERY`. It reports no object statistics.

`GET /medias/objects?bucket=&prefix=` returns `{files}`, each with only `file_id`, `file_name`, `mime_type`, `size` and `object_key`; signed URLs, CDN URLs and file metadata are not passed on. `prefix` is relative to the application and optional. The current Media does not implement the tenant object inventory and refuses it, so the route answers 500 `ADMIN_MEDIA_LIST_OBJECTS_FAILED`, never an empty list.

`POST /medias/cleanup-orphaned-assets` takes `{"limit": <n>, "dry_run": false}`; both fields are required (400 `ADMIN_INVALID_JSON_BODY` otherwise). `limit` 0 means 100, and Media caps it at 1,000. Media does not honor `dry_run` and would delete as in a real run, so the gateway refuses `dry_run: true` with 400 `ADMIN_MEDIA_DRY_RUN_UNSUPPORTED` without calling Media. It returns `{file_ids, success, error_message, scanned}`: `file_ids` are the orphaned message assets of the application that this pass deleted, and `scanned` is their count. The audit target is the tenant.

`POST /medias/object-acl` takes `{file_id, entries: [{principal, permissions}]}` and returns `data: true`. The current Media does not implement ACL changes and refuses every request, so the route answers 500 `ADMIN_MEDIA_OBJECT_ACL_FAILED`, never a successful no-op. The audit target is the file ID.

## Audit

Core has no audit table for Admin Gateway calls. Two things are recorded:

- a structured log event with the tracing target `admin_audit` for every accepted write and for the three sensitive reads, written after the gateway accepted the request and before the downstream call (a request the gateway refuses itself is not recorded, and a request that fails downstream still is);
- an effect event `flare.audit.effect.v1` published after a write succeeded, which the platform aggregates with the gateway admin partition's `flare.audit.request.v1` / `flare.audit.result.v1` by `request_id` into one durable audit record.

| Request | `admin_audit` event |
| --- | --- |
| Every write route | `access=write`, `route`, `actor_id`, `tenant_id`, `target`, `audit_reason` (the decoded reason), `request_id`, `idempotency_key` |
| Sensitive reads: conversation participants, user devices, user presence | `access=read` with the same fields except `idempotency_key`; `audit_reason` is empty unless the caller sent `x-audit-reason`, which reads do not require |
| Every other read | none |

`route` values: `users.kick`, `devices.kick`, `tenant.kick_all`, `capabilities.tenant_switch`, `plugins.register`, `plugins.deregister`, `hooks.create`, `hooks.update`, `hooks.delete`, `hooks.set_status`, `messages.export`, `medias.cleanup_orphaned_assets`, `medias.object_acl`; reads `conversations.participants`, `users.devices`, `users.presence`.

`target` names what the request acts on: the conversation, user, hook, plugin or capability ID, `<user_id>/<device_id>` for a device kick, the tenant for kick-all and orphan cleanup, the conversation for an export, the file ID for object ACL and the hook name for a hook creation. Path parameters are identifiers by the route table (`A-Z a-z 0-9 _ - . :`, at most 128 bytes), so they are logged as they are; IDs that come from a request body (the file ID, the hook name) can hold any UTF-8 and are escaped before they are logged:

- RFC 3986 unreserved characters (`A-Z a-z 0-9 - . _ ~`) are kept; every other UTF-8 byte becomes an upper-case `%XX`. Ordinary IDs such as `u1`, `tenant-a` or a UUID are unchanged, and percent-decoding an escaped ID gives back the original.
- In a device kick, each ID is escaped separately before the two are joined with `/`, so `/` in `target` is always the separator.
- An ID longer than 256 bytes after escaping is cut after the last whole character or `%XX` that fits and gets the suffix `!truncated:<original byte length>`. `!` and `:` are always escaped inside a body-supplied ID, so the suffix cannot be mistaken for content; a hook id of the form `hook_type:name` logs as `hook_type%3Aname`. Truncated targets are not unique.

`audit_reason` is the decoded, trimmed reason: Chinese and other text is logged as written. Control characters that a percent-encoded reason can carry (a line break, for example) are logged as Rust escapes (`\n`), so a reason cannot forge a log line. Online logs the kick reason it receives (`reason` on its `device kicked` and `tenant kicked` lines) escaped the same way.

Events never contain the context token.

### Effect events

The effect sink connects to `FLARE_AUDIT_NATS_URL` at startup (JetStream stream `flare_audit`, subjects `flare.audit.v1.>`, 30 days retention, 10 minute duplicate window). Unset, the sink is disabled with one startup warning and writes are not audited; set but unreachable, startup fails. A publish failure after a successful write is only logged: the effect has already happened. Each event is published to `flare.audit.v1.<tenant_id>` with the message id `{request_id}:effect:admin-gateway:{aggregate_id}`, so a retried publish is deduplicated, and the JSON body:

```json
{
  "event": "flare.audit.effect.v1",
  "request_id": "<signed request id>",
  "at_ms": 1700000000000,
  "scope": "<tenant_id>",
  "source": "admin-gateway",
  "actor_id": "<employee>",
  "actor_type": "staff",
  "permission": "<signed route code>",
  "aggregate": "<see below>",
  "aggregate_id": "<see below>",
  "action": "<see below>",
  "outcome": "ok"
}
```

| Route | `aggregate` | `aggregate_id` | `action` |
| --- | --- | --- | --- |
| `POST /users/{user_id}/kick` | `user_session` | the user id | `kick_user` |
| `POST /users/{user_id}/devices/{device_id}/kick` | `user_session` | `<user_id>/<device_id>` | `kick_device` |
| `POST /tenant/kick-all` | `tenant_sessions` | the tenant | `kick_all` |
| `PUT /capabilities/tenant/{capability_id}` | `tenant_capability` | the capability id | `enable` or `disable` |
| `POST /plugins/register` (when `accepted`) | `plugin_endpoint` | the plugin id | `register` |
| `POST /plugins/deregister` (when `accepted`) | `plugin_endpoint` | the plugin id | `deregister` |
| `POST /hooks` | `hook_config` | the new hook id | `create` |
| `PUT /hooks/{hook_id}` | `hook_config` | the hook id | `update` |
| `DELETE /hooks/{hook_id}` (when `deleted`) | `hook_config` | the hook id | `delete` |
| `PUT /hooks/{hook_id}/status` (when `updated`) | `hook_config` | the hook id | `enable` or `disable` |
| `POST /messages/export` | `message_export` | the export task id | `export` |
| `POST /medias/cleanup-orphaned-assets` | `media_assets` | the tenant | `cleanup_orphaned` |
| `POST /medias/object-acl` | `media_object` | the file id | `set_acl` |

The three sensitive reads produce no effect event; the two read-only message query POSTs produce neither an `admin_audit` event nor an effect.

## Other routes

`auth/check`, `capabilities` (this contract as JSON), `gateway/{health,upstreams,routes,config}` and `messages/*` predate ADM-01 and are described only by the OpenAPI document. `GET /capabilities` and `GET /gateway/routes` list every operation with its write flag; tests keep them equal to the OpenAPI document and to the shared route table. The `required_headers` block of `GET /capabilities` names `x-flare-ctx` as the identity header and the three audit headers.

The five `messages/*` routes call the Storage Reader (the four queries with `core:messages:read`, the export with `core:messages:export`) and map its failures with the error table above, so they never return the Storage Reader's error text. Their 500 reasons are `ADMIN_MESSAGE_QUERY_FAILED`, `ADMIN_MESSAGE_DETAIL_FAILED`, `ADMIN_MESSAGE_EVENTS_FAILED`, `ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED` and `ADMIN_MESSAGE_EXPORT_FAILED`. The Storage Reader reports a database it cannot reach (no connection from the pool, a refused or dropped connection, a server shutting down) as `UNAVAILABLE`, which becomes 503 `ADMIN_UPSTREAM_UNAVAILABLE`; a refused argument is `INVALID_ARGUMENT` (400 `BAD_REQUEST`), a timeout `DEADLINE_EXCEEDED` (504), and other database or decoding failures stay `INTERNAL` and 500. `NOT_FOUND` (404) is an answer only when one message is read by id (its detail or event chain); a list, search, export or write-ledger query has nothing to not find, so a "not found" from storage there is an internal inconsistency and stays `INTERNAL`. Its statuses carry a fixed sentence only: database messages, SQL fragments and request values stay in its own error log. Each call takes its own clone of the Storage Reader client, so a slow search does not hold up other admin searches.

The `auth` block of `GET /gateway/config` reports `mode: "staff_context_ed25519"`, the Cell key-set verification described under [Authentication and tenant scope](#authentication-and-tenant-scope). The four `hook_*` fields stay only to keep the response shape the platform has registered, and always read `null`, `0`, `""` and `null`. The gateway applies no request rate limit, so the `rate_limit` block always reads `enabled: false` with `requests_per_second` and `burst_capacity` at 0. `redacted_fields` is empty because the snapshot holds no secret. The `grpc` block and `GET /gateway/upstreams` list only the downstreams the gateway connects: Media, Conversation, Online, the Storage Reader and, when enabled, Capability; it never dials Message Ingest or the Orchestrator.

The gateway does not read `FLARE_ADMIN_GATEWAY_AUTH_*` or `FLARE_ADMIN_GATEWAY_RATE_LIMIT_*`. When one of them is set, it logs a WARN with the variable name (never the value) and starts normally; a misconfigured `http_hook` without a URL no longer stops it.

## Downstream configuration

| Downstream | Variable | Static-mode address in the Core container |
| --- | --- | --- |
| Conversation | `FLARE_ADMIN_GATEWAY_GRPC_CONVERSATION_SERVICE_URL` | `http://127.0.0.1:50090` |
| Online | `FLARE_ADMIN_GATEWAY_GRPC_ONLINE_SERVICE_URL` | `http://127.0.0.1:50061` |
| Capability (optional; an empty value disables it) | `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` | `http://127.0.0.1:50110` |
| Storage Reader | `FLARE_ADMIN_GATEWAY_GRPC_STORAGE_READER_SERVICE_URL` | `http://127.0.0.1:60083` |
| Media | `FLARE_ADMIN_GATEWAY_GRPC_MEDIA_SERVICE_URL` | `http://127.0.0.1:60081` |

Every call is bounded by `FLARE_ADMIN_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS` (default 5, 0 refuses startup). The fallbacks are listed in `config/services/admin-gateway.toml` and `release/README.md`. Conversation, Online, Capability and the Storage Reader each need the Cell key set with the gateway key; without it they answer the forwarded context with `UNAUTHENTICATED`, which the gateway reports as 502. The media routes need `FLARE_INTERNAL_CTX_PRIVATE_KEY` in the gateway and a key set in Media that resolves `kid=admin-gateway` (the single Cell key does). The gateway itself needs `FLARE_INTERNAL_CTX_PUBLIC_KEY` or `FLARE_INTERNAL_CTX_PUBLIC_KEYS` and refuses to start without one; `FLARE_AUDIT_NATS_URL` enables the effect events; the supervisor starts the process only when `FLARE_ADMIN_GATEWAY_ENABLED` is set.

## Contract tests

- `flare-admin-gateway/tests/admin_staff_auth.rs` drives the boundary with contexts signed by the gateway key, by the hook key and by the platform key (as a bearer): platform assertions of the retired admin shape and of the control shape are 401, a hook-signed staff context is 401, a service context and a duplicated context are 401, another route's code and a staff context with no permission claim are 403 `ADMIN_PERMISSION_DENIED`, organization staff on a plugin route is 403 `ADMIN_SCOPE_DENIED`, and on a single-key deployment a `kid=gateway` context passes while a kid-less one is refused. Every harness builds its verifier with the production constructor (`admin_staff_verifier_from_sources`).
- `flare-admin-gateway/tests/admin_ops_contract.rs` starts stand-in Conversation, Online and Capability services on `127.0.0.1` and drives the router. For every ADM-01 route it checks the missing-context 401, the wrong-code 403 and, for writes, the audit-header 403s. It also checks the exact response fields, what the downstream received, and that only the verified context reaches the downstream. The stand-ins gate like the real services: they verify the forwarded context with the same key set and refuse, with `PERMISSION_DENIED`, a code the real RPC does not admit (and a tenant other than the signed one), so a code the gateway forwards that a downstream would refuse fails here; `POST /users/{user_id}/kick` is driven end to end through those gates. It also covers the audit reason encoding: a percent-encoded Chinese reason reaches Online decoded, and a malformed, repeated or over-long reason is a 400 on reads and writes.
- `flare-admin-gateway/tests/admin_capability_contract.rs` does the same for the capability catalog, the tenant switch, plugin registration (including the platform-only rule) and the eight hook routes, with a stand-in `Administer` that decodes each operation's payload and records it.
- `flare-admin-gateway/tests/admin_media_contract.rs` does the same for the four media routes against a stand-in Media on `127.0.0.1`: missing-context 401, wrong-code 403, write-header 403s, response fields, the request Media received, the refused `dry_run`, and the typed rejections that never echo Media's text. It verifies the signed context Media received with the Cell public key (`kid=admin-gateway`), checks that Media's real signed-context layer accepts it and refuses a context signed by another Cell key, and that no staff context or forged header reaches Media. The stand-in Media verifies every context itself and, like Media, refuses an actor that is not a tenant administrator.
- `flare-admin-gateway/tests/admin_audit_log.rs` captures the `admin_audit` events: one per write (including the capability, plugin and hook writes) and per sensitive read, none for other reads or for writes the gateway refused, no token in any event, the escaped `target` of body-supplied IDs with a line break, an encoded `/` and 1,000 bytes, and the decoded `audit_reason` of a percent-encoded Chinese reason and of one carrying an encoded line break.
- `flare-admin-gateway/tests/admin_gateway_config_contract.rs` configures an unused `http_hook` with a URL and secret and an enabled rate limit, and checks that `GET /gateway/config` still reports `staff_context_ed25519` with no hook values and no rate limit, and that the OpenAPI `AdminAuthConfigSnapshot` keeps the five fields the platform has registered.
- `flare-admin-gateway/tests/admin_messages_contract.rs` starts a stand-in Storage Reader on `127.0.0.1` and checks, for each of the five `messages/*` routes, every downstream status in the error table and the route's fixed 500 reason, with no downstream text in the response; that only the verified context (with the read or export code, which the stand-in enforces per RPC as the Storage Reader does) and the signed request and trace ids reach the Storage Reader; and that the other four routes answer while a search is held in the Storage Reader. It also sends the three body routes a field of the wrong type, broken JSON, an empty body and a body without `application/json`, an oversized body, and the events route a malformed `limit`, and checks the typed 400s above, with no request value or parser text in the answer and no Storage Reader call.
- `flare-admin-gateway/tests/admin_openapi_snapshot.rs` compares the `AdminApiDoc` operation set with `tests/snapshots/admin_openapi_operations.txt` and with the shared route table, and every documented response of every operation (status and description, one line each) with `tests/snapshots/admin_openapi_responses.txt`. Every operation documents the 400 `ADMIN_AUDIT_HEADER_TOO_LONG` and `ADMIN_AUDIT_REASON_INVALID`, the eleven body routes `ADMIN_INVALID_JSON_BODY` and `ADMIN_REQUEST_BODY_TOO_LARGE`, the nine routes with query parameters `ADMIN_INVALID_QUERY`, the fifteen routes with path parameters `ADMIN_INVALID_PATH`, and none documents 422; the 32 downstream routes name each typed downstream answer from the error table. It also requires `GET /capabilities` and `GET /gateway/routes` to list the same operations with the table's write flags, every documented operation to be routed behind admin authentication (401 without a context, never 404), the endpoint lists in both Admin Gateway READMEs to match the document, and the route tables on this page to cover the ADM-01, capability, hook and media routes and nothing else. After an intended API change, rerun it with `FLARE_UPDATE_SNAPSHOTS=1` and review both snapshot diffs together with the change.

## Known limits

- Core writes no durable audit row for admin calls; the durable record is the platform's aggregation of the gateway's request/result events and the effect events above. The `capability_audit_log` table records capability policy changes only.
- Idempotency keys are not deduplicated; see [Write operations](#write-operations).
- `GET /medias/objects` and `POST /medias/object-acl` fail until Media implements them.
- `POST /users/{user_id}/kick` kicks devices sequentially within one request; a user with many devices takes one Online round trip per device.
- `GET /hooks` and `GET /hooks/{hook_id}/executions` page by limit only; Capability does not implement cursors for them.
