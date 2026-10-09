# Platform → Core control authentication (SEC-02, v4 R3)

Two identities reach Core's management surfaces, and they are verified with two different keys.

| Identity | Who signs it | Where it is carried | Core verifies it with | Who accepts it |
| --- | --- | --- | --- | --- |
| Staff context | The Social gateway admin partition, with the Cell key registered as `kid=gateway` | `x-flare-ctx` (the cell-internal signed context, `actor_type=staff`) | The Cell key set: `FLARE_INTERNAL_CTX_PUBLIC_KEYS` layered over `FLARE_INTERNAL_CTX_PUBLIC_KEY`, with `Staff` pinned to `kid=gateway` | Admin Gateway and the admin RPCs of Conversation, Online, Capability and the Storage Reader |
| Platform service assertion | The platform (`flare-platform`), with its own Ed25519 key | `authorization: Bearer <token>` | `FLARE_PLATFORM_PUBLIC_KEY` | Capability `TenantProjection` (subject `flare-platform-worker`) and Online `KickTenant` (subject `flare-platform-control-scheduler`) |

There is no platform-issued employee assertion any more. The former `token_use=admin` / audience `flare-core-admin` token, the console public-key variable and the kit's console verifier are gone; a bearer with an employee subject is `UNAUTHENTICATED` everywhere in Core. Rollback is a redeploy of the previous image set of Core, platform, gateway and console together; there is no compatibility flag that keeps both schemes alive.

## The staff context

The gateway admin partition authenticates the employee (staff session or personal access token), resolves the request against the shared Core admin route table `flare_core_infra::auth::core_admin::ROUTES` (the same table Admin Gateway resolves `/api/v1/admin/<path>` with, so the two cannot drift), checks the employee's role bundle, and signs:

| Claim | Value |
| --- | --- |
| `actor_type` | `staff` |
| `actor_id` | the employee id (the acting user of every downstream operation) |
| `tenant_id` | the application being administered: one concrete IM tenant, never empty, never `*` |
| `permission` | the route's code from `core_admin::ROUTES` (`core:conversations:read`, `core:messages:read`, `core:messages:export`, `core:users:read`, `core:users:kick`, `core:tenant:read`, `core:tenant:kick`, `core:capabilities:read`, `core:capabilities:write`, `core:plugins:read`, `core:plugins:write`, `core:hooks:read`, `core:hooks:write`, `core:medias:read`, `core:medias:write`, `core:gateway:read`) |
| `org_id` | the staff member's organization; absent for platform staff |
| `roles`, `credential_id`, `perm_version` | the grant description |
| `request_id`, `trace_id` | correlation; the downstream services log them and the Storage Reader keys export tasks by the request id |
| JWT header `kid` | `gateway` |

The token has the usual cell-internal shape (`iss`/`aud` `flare-internal`, `token_use=internal_context`, EdDSA, lifetime at most 60 seconds) and verifies with `SignedContextVerifier::from_env().require_staff_kid([kid::GATEWAY])`. The pin reads the JWT header: a Staff token without a `kid` is `legacy` and is refused even on a deployment that holds a single Cell key, so the admin partition sets `kid=gateway` on every deployment. On a single-key deployment a token naming `kid=gateway` resolves to that one key (`KeySet` fallback, removed in R6-4 once per-component keys exist); a Staff token signed with any other component key (`hook`, `bridge`, `ingest`, `admin-gateway`) is refused by kid.

`flare_im_service_kit::staff_context` is the shared gate: `admin_staff_verifier_from_env(service)` builds the pinned verifier (a configuration error naming both key variables when neither is set), `require_staff(request, verifier, code)` verifies exactly one `x-flare-ctx`, requires a `Staff` actor whose signed `permission` is `code` (`flare_server_core::require_staff_permission`), and refuses an empty or wildcard application. Missing, duplicate or invalid tokens (including a wrong kid) are `UNAUTHENTICATED`; a verified non-staff actor, another code, or no application is `PERMISSION_DENIED`. `staff_context_request` / `staff_traced_request` forward the verified token unchanged as `x-flare-ctx` to the next admin service, with nothing else.

### Admin Gateway

`admin_auth_middleware` on `/api/v1/admin/**`, in order: exactly one `x-flare-ctx` (401 `UNAUTHORIZED` when missing, 401 `ADMIN_STAFF_CONTEXT_INVALID` when duplicated, malformed, expired, non-staff or signed with a kid the verifier does not accept for staff); the route exists in `core_admin::ROUTES` with a valid parameter (otherwise 404 `NOT_FOUND`; percent-encoded, over-long or non-identifier parameters are not admin routes); the signed `permission` equals the route's code (403 `ADMIN_PERMISSION_DENIED`); a `platform_only` route is called by platform staff, that is without `org_id` (403 `ADMIN_SCOPE_DENIED`); the signed application is one concrete tenant (403 `ADMIN_IDENTITY_REQUIRED`); a write route carries `x-audit-reason` and `x-request-id` or `idempotency-key` (403 `FORBIDDEN`). Before the route checks, on reads too, the audit headers must be within their limits (400 `ADMIN_AUDIT_HEADER_TOO_LONG`) and `x-audit-reason` must be one header of percent-encoded UTF-8 (400 `ADMIN_AUDIT_REASON_INVALID`); the gateway logs it decoded. `x-tenant-id` and `x-actor-id` are neither required nor read; every identity header is stripped before the handlers run. The gateway forwards the same token, byte for byte, to Conversation, Online, Capability and the Storage Reader; for Media it re-signs the verified staff as `tenant_admin` with `FLARE_INTERNAL_CTX_PRIVATE_KEY` under `kid=admin-gateway`. See [admin-api.md](admin-api.md).

### Downstream admin RPCs

Each service re-verifies the forwarded token with its own copy of the Cell key set (Staff pinned to `gateway`) and compares the signed code with the operation it serves. The tenant is always the signed application; requests carry no tenant of their own, or must name the signed one.

| Service and RPCs | Code |
| --- | --- |
| Conversation `ConversationAdminService.{AdminListConversations, AdminGetConversation, AdminListParticipants}` | `core:conversations:read` |
| Online `OnlineAdminService.AdminListUserDevices` | `core:users:read`, or `core:users:kick` (the whole-user kick lists the devices with its kick context first) |
| Online `OnlineAdminService.AdminGetUserPresence` | `core:users:read` |
| Online `OnlineService.KickDevice` | `core:users:kick` (the session must belong to the signed application) |
| Online `OnlineService.KickTenant` | `core:tenant:kick` with the signed application equal to the request tenant; or the scheduler's platform assertion (see below). A call with `x-flare-ctx` takes the staff branch, one without takes the scheduler branch; each fails closed on its own key |
| Capability `CapabilityAdminService.GetTenantRuntime` | `core:tenant:read` |
| Capability `CapabilityAdminService.ListTenantCapabilitySwitches`, `CapabilityService.{ListCapabilities, ListUserCapabilities}` | `core:capabilities:read` |
| Capability `CapabilityService.{GrantUserCapability, RevokeUserCapability, SetTenantCapabilitySwitch}` | `core:capabilities:write` |
| Capability `CapabilityService.ListRegisteredPlugins` | `core:plugins:read`, or a Cell-signed `Service` context whose signed tenant is the request tenant |
| Capability `CapabilityService.{RegisterPluginEndpoint, DeregisterPluginEndpoint}` | `core:plugins:write` from platform staff (`org_id` absent), or the plugin host's own `Service` context for its own tenant |
| Capability `CapabilityService.Administer` | `core:hooks:read` for `flare.extension.v1.hook_config.{get, list, get_statistics, query_executions}`, `core:hooks:write` for `{create, update, delete, set_status}`; the code is checked before the payload is decoded, and hook-id operations first resolve the stored configuration's tenant. The in-process governance call gets a context rebuilt from the verified claims (actor type `Staff`, `user_id` = the employee), never from `GenericRequest.metadata` |
| Storage Reader `StorageReaderService.{SearchMessages, GetMessage, QueryMessageEvents, QueryMessageWriteLedger}` | `core:messages:read` |
| Storage Reader `StorageReaderService.ExportMessages` | `core:messages:export` |

The Storage Reader's five admin RPCs stay exempt from its signed-first read boundary (whose verifier admits only User, TenantAdmin and Service actors) and verify the staff context in the handler; request and trace ids come from the claims first and from `x-request-id` / `x-trace-id` only as a fallback.

## The platform service assertion

`flare_im_service_kit::control_auth::PlatformAssertionVerifier` holds `FLARE_PLATFORM_PUBLIC_KEY` and verifies EdDSA, issuer `flare-platform`, audience `flare-core-projection`, `token_use=control`, `exp - iat <= 60s`, and a subject that is exactly `flare-platform-worker` or `flare-platform-control-scheduler`; every other subject is `UNAUTHENTICATED`. `require_platform_assertion` is the RPC gate; the method still checks the kind and the tenant:

- `TenantProjection.UpsertTenant` / `UpsertCredentials` / `UpsertFeatureFlags`: worker only; the token tenant must equal the canonical body tenant (`UpsertFeatureFlags` and `UpsertRoleBundle` stay `UNIMPLEMENTED` after authentication).
- `TenantProjection.GetVersions`: worker only; one requested tenant must equal the token tenant; several or none require token tenant `*`.
- `OnlineService.KickTenant`: scheduler only, with the token tenant equal to the request tenant. The scheduler cannot call any other RPC.

Only Capability and Online receive the platform key. `release/supervise-core.toml`, `deploy/docker-compose.stack.yml` and `scripts/start_server.sh` hand it to exactly those two; the supervisor forbids `FLARE_PLATFORM_SIGNING_KEY` and `FLARE_PLATFORM_PRIVATE_KEY` (and the retired console names) in its environment. `flare-supervise`'s `core_manifest_hands_the_platform_public_key_to_the_two_service_verifiers_only` and the service kit's `platform_public_key_reaches_only_the_two_service_verifiers_on_both_launch_paths` pin both lists.

## Keys and launch

Every signed-context verifier (Online, Route, Capability, Conversation, Message Ingest, Storage Reader, Sync Orchestrator, Push Server/Proxy/Worker, Media, the signaling gateway, Admin Gateway) inherits `FLARE_INTERNAL_CTX_PUBLIC_KEY` and the optional `FLARE_INTERNAL_CTX_PUBLIC_KEYS`. Conversation, the Storage Reader and Admin Gateway started without a key answer every admin RPC with `UNAUTHENTICATED` (Admin Gateway refuses to start, since the key set is its only identity source); the manifest requires the single key for all of them. Admin Gateway is started only when `FLARE_ADMIN_GATEWAY_ENABLED` is set, needs `FLARE_INTERNAL_CTX_PRIVATE_KEY` for its Media calls, and publishes `flare.audit.effect.v1` to `FLARE_AUDIT_NATS_URL` when that is set.

TLS/private listener protection remains required; a valid short-lived token is not a substitute for network isolation. Retries of writes also need stable idempotency keys; a token alone does not deduplicate a replay inside its validity window. The platform must sign a worker or scheduler assertion only for its own jobs, and the gateway must sign a staff context only after its own IAM authorization: Core verifies signatures and codes, it cannot look up the employee directory. Rotate the Cell and platform keys through coordinated public-key rollouts; do not reuse one for the other.
