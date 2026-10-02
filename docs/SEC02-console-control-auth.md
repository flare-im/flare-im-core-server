# Console → Core control authentication (SEC-02)

The Console/Control backend is the sole issuer of Core management assertions. Core processes receive only `FLARE_CONSOLE_PUBLIC_KEY` (Ed25519 public key, PEM or base64). The private `FLARE_CONSOLE_SIGNING_KEY` must never be mounted in Core, Social data-plane, or a browser. Missing public key makes protected mixed-service RPCs return `UNAUTHENTICATED`; Admin Gateway refuses startup. Invalid public-key material fails startup.

| Boundary | JWT `iss` | `aud` | `token_use` | Allowed subject / tenant |
| --- | --- | --- | --- | --- |
| Capability management, TenantProjection, Online Kick gRPC | `flare-im-console` | `flare-core-control` | `control` | See method policy below |
| Admin Gateway HTTP `/api/v1/admin/**` | `flare-im-console` | `flare-core-admin` | `admin` | Real employee subject and one concrete, matching `x-tenant-id` |
| Admin reads forwarded by Admin Gateway (Conversation, Online, Capability admin services, Capability `ListRegisteredPlugins`), Online Kick gRPC | `flare-im-console` | `flare-core-admin` | `admin` | Employee subject, one concrete tenant; see "Forwarded admin assertion" below |

Every assertion has `sub`, `tenant_id`, `iat`, `exp`, `jti`; Core enforces EdDSA, exact issuer/audience/use, and `exp - iat <= 60s`. Plain user-access JWTs, the cell-internal `x-flare-ctx`, old shared-secret headers, missing assertions, and duplicate Authorization headers do not authorize control operations. TLS/private listener protection remains required; a valid short-lived assertion is not a substitute for network isolation. Retries of writes also need stable idempotency keys; the JWT alone does not deduplicate a replay inside its validity window.

## Method policy

- `TenantProjection.UpsertTenant` / `UpsertFeatureFlags`: subject exactly `flare-platform-control-worker`; token tenant must equal the canonical body tenant. `UpsertFeatureFlags` remains explicitly `UNIMPLEMENTED` in Core, after authentication.
- `TenantProjection.GetVersions`: worker only; one requested tenant must equal token tenant; multiple or no tenant IDs require token tenant `*`. Non-canonical IDs are rejected.
- `CapabilityService.ListCapabilities`: employee assertion (platform-wide Control listing may use token tenant `*`). `ListUserCapabilities`, `GrantUserCapability`, `RevokeUserCapability`, `SetTenantCapabilitySwitch`, `RegisterPluginEndpoint`, `DeregisterPluginEndpoint`, `ListRegisteredPlugins`, `Administer`: employee subject, concrete token tenant exactly matching the requested tenant. `ListRegisteredPlugins` is read-only and also accepts the forwarded admin assertion (Admin Gateway `GET /api/v1/admin/plugins`); registering and deregistering plugins stay control-only. Hook ID operations resolve the stored Hook config tenant before read/change; `GenericRequest.metadata` is not identity. In-process Hook governance calls receive a context reconstructed from the verified assertion, preserving the real actor for audit.
- `OnlineService.KickDevice`: employee only, with a concrete token tenant; the live session's tenant must match before removal. The request proto does not carry a tenant field. Accepts the control assertion or the forwarded admin assertion.
- `OnlineService.KickTenant`: employee with exact tenant, or subject exactly `flare-platform-control-scheduler` with exact tenant. Scheduler cannot call `KickDevice` or other management operations. Accepts the control assertion or the forwarded admin assertion; the scheduler is only accepted on the control assertion.
- `ConversationAdminService` (`AdminListConversations`, `AdminGetConversation`, `AdminListParticipants`), `OnlineAdminService` (`AdminListUserDevices`, `AdminGetUserPresence`), `CapabilityAdminService` (`ListTenantCapabilitySwitches`, `GetTenantRuntime`): forwarded admin assertion only. A control assertion is rejected with `UNAUTHENTICATED`. None of these requests carries a tenant ID or a caller user ID; every read is scoped to the token tenant, and a conversation that belongs to another tenant is answered exactly like a missing one (`NOT_FOUND`).
- Admin Gateway: employee with concrete matching `x-tenant-id`. Write requests additionally require `x-actor-id` equal to signed `sub`, an audit reason, and `x-request-id` or `idempotency-key`. After verification the gateway replaces identity headers with the signed subject/tenant before downstream handlers run; no legacy API-Gateway JWT fallback remains.

## Forwarded admin assertion (ADM-01)

Admin Gateway verifies the admin assertion on `/api/v1/admin/**` and then forwards the same token, byte for byte, as the only identity on downstream gRPC calls: `authorization: Bearer <admin token>`. The service kit's admin client methods (`OnlineServiceClientWrapper::{kick_device, kick_tenant, admin_*}`, `ConversationReadServiceClientWrapper::admin_*`, `CapabilityAdminClientWrapper`, including its `list_registered_plugins`) attach nothing else; no `x-flare-ctx`, `x-tenant-id` or actor metadata is sent.

Each receiving service (Online, Capability, Conversation) adds the same branch through `flare_im_service_kit::control_auth`:

1. exactly one `authorization` entry with a `Bearer` token;
2. `verify_token(token, "admin", CORE_ADMIN_AUDIENCE)` with the Console public key — EdDSA, issuer `flare-im-console`, `token_use=admin`, audience `flare-core-admin`, lifetime at most 60 seconds;
3. `require_kind(Employee)` — the projection worker and scheduler subjects never pass the admin branch;
4. `require_tenant(actor.tenant_id)` — the token names one concrete, canonical tenant; `*` is rejected.

`require_grpc_admin` implements the admin-only form; `require_grpc_control_or_admin` tries the control assertion first and falls back to the admin branch for the two Online kicks and Capability `ListRegisteredPlugins`. Missing or malformed tokens and wrong audience or purpose map to `UNAUTHENTICATED`; a non-employee subject, a `*` tenant or a tenant mismatch maps to `PERMISSION_DENIED`.

What this deliberately avoids:

- These calls need no signing key in Admin Gateway: it signs neither a cell-internal context nor a Control assertion for them, so it cannot mint or widen an identity; it can only replay what Console signed, within the assertion's 60-second lifetime. (The admin media routes still sign a cell-internal context with `FLARE_INTERNAL_CTX_PRIVATE_KEY`, see SEC05; that path is unchanged here.)
- Downstream services no longer derive an admin identity from unsigned context metadata. The conversation service still installs `ContextLayer::allow_missing` for its user-facing services, but the admin service ignores that context. The Storage Reader does the same for its five admin retrieval RPCs (`SearchMessages`, `GetMessage`, `QueryMessageEvents`, `QueryMessageWriteLedger`, `ExportMessages`, used only by Admin Gateway's `messages/*` routes): they verify the forwarded assertion with `require_grpc_admin` and take the tenant and actor from it; only `x-request-id` and `x-trace-id` are read from metadata, for log correlation and export-task deduplication. Its sync and history RPCs keep their existing context.
- Capability serves tenant switches and the tenant runtime view from its own `capability_tenant_switches` table and `tenants` projection, so Admin Gateway needs no database role.

All three Core launch paths hand `FLARE_CONSOLE_PUBLIC_KEY` to exactly the five verifiers (Online, Capability, Conversation, Storage Reader, Admin Gateway) and to nothing else: `release/supervise-core.toml` through `inherit_env` (supervised children start from an empty environment), `deploy/docker-compose.stack.yml`, and `scripts/start_server.sh`. Online, Capability, Conversation or Storage Reader started without the key keeps serving its user-facing RPCs but answers every Control and admin RPC with `UNAUTHENTICATED` (only Admin Gateway refuses to start), so a missing entry does not show up at deploy time. `flare-supervise`'s `core_manifest_hands_console_public_key_to_every_admin_verifier` and the service kit's `console_public_key_reaches_every_admin_verifier_on_both_launch_paths` pin the list.

The admin-only Capability downstream is configured with `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` (default `discovery://flare-capability`; an empty value disables it) and `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_STATIC_FALLBACK` (default `http://127.0.0.1:50110`). API Gateway never reads these variables and never connects to Capability.

Control must issue an employee assertion only after its own IAM authorization. Core's public-key verifier distinguishes the two reserved service subjects from employee subjects, but cannot independently look up Control's employee directory; issuer key protection and Control's IAM policy are part of this trust boundary. Rotate the signing key through a coordinated public-key rollout; do not reuse the cell-internal context key.
