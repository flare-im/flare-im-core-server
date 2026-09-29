# Console → Core control authentication (SEC-02)

The Console/Control backend is the sole issuer of Core management assertions. Core processes receive only `FLARE_CONSOLE_PUBLIC_KEY` (Ed25519 public key, PEM or base64). The private `FLARE_CONSOLE_SIGNING_KEY` must never be mounted in Core, Social data-plane, or a browser. Missing public key makes protected mixed-service RPCs return `UNAUTHENTICATED`; Admin Gateway refuses startup. Invalid public-key material fails startup.

| Boundary | JWT `iss` | `aud` | `token_use` | Allowed subject / tenant |
| --- | --- | --- | --- | --- |
| Capability management, TenantProjection, Online Kick gRPC | `flare-im-console` | `flare-core-control` | `control` | See method policy below |
| Admin Gateway HTTP `/api/v1/admin/**` | `flare-im-console` | `flare-core-admin` | `admin` | Real employee subject and one concrete, matching `x-tenant-id` |

Every assertion has `sub`, `tenant_id`, `iat`, `exp`, `jti`; Core enforces EdDSA, exact issuer/audience/use, and `exp - iat <= 60s`. Plain user-access JWTs, the cell-internal `x-flare-ctx`, old shared-secret headers, missing assertions, and duplicate Authorization headers do not authorize control operations. TLS/private listener protection remains required; a valid short-lived assertion is not a substitute for network isolation. Retries of writes also need stable idempotency keys; the JWT alone does not deduplicate a replay inside its validity window.

## Method policy

- `TenantProjection.UpsertTenant` / `UpsertFeatureFlags`: subject exactly `flare-platform-control-worker`; token tenant must equal the canonical body tenant. `UpsertFeatureFlags` remains explicitly `UNIMPLEMENTED` in Core, after authentication.
- `TenantProjection.GetVersions`: worker only; one requested tenant must equal token tenant; multiple or no tenant IDs require token tenant `*`. Non-canonical IDs are rejected.
- `CapabilityService.ListCapabilities`: employee assertion (platform-wide Control listing may use token tenant `*`). `ListUserCapabilities`, `GrantUserCapability`, `RevokeUserCapability`, `SetTenantCapabilitySwitch`, `RegisterPluginEndpoint`, `DeregisterPluginEndpoint`, `ListRegisteredPlugins`, `Administer`: employee subject, concrete token tenant exactly matching the requested tenant. Hook ID operations resolve the stored Hook config tenant before read/change; `GenericRequest.metadata` is not identity. In-process Hook governance calls receive a context reconstructed from the verified assertion, preserving the real actor for audit.
- `OnlineService.KickDevice`: employee only, with a concrete token tenant; the live session's tenant must match before removal. The request proto does not carry a tenant field.
- `OnlineService.KickTenant`: employee with exact tenant, or subject exactly `flare-platform-control-scheduler` with exact tenant. Scheduler cannot call `KickDevice` or other management operations.
- Admin Gateway: employee with concrete matching `x-tenant-id`. Write requests additionally require `x-actor-id` equal to signed `sub`, an audit reason, and `x-request-id` or `idempotency-key`. After verification the gateway replaces identity headers with the signed subject/tenant before downstream handlers run; no legacy API-Gateway JWT fallback remains.

Control must issue an employee assertion only after its own IAM authorization. Core's public-key verifier distinguishes the two reserved service subjects from employee subjects, but cannot independently look up Control's employee directory; issuer key protection and Control's IAM policy are part of this trust boundary. Rotate the signing key through a coordinated public-key rollout; do not reuse the cell-internal context key.
