# Access token issuance and refresh: moving it off the client

Chinese version: [AUTH-TOKEN-ISSUANCE.zh-CN.md](AUTH-TOKEN-ISSUANCE.zh-CN.md)

## Problem

The core SDK used to mint the access JWT locally (`generate_core_token`, HS256). That puts the signing
secret either inside the app bundle or into a login form. Both are wrong: anyone holding the bundle can
forge any identity, and production users do not know the secret. Refresh logic was also duplicated per
client with inconsistent behaviour.

## Three deployment shapes, one client contract

| Shape | Issues / refreshes | Validates | How the client gets a token |
|---|---|---|---|
| core only | `flare-api-gateway` exposes `POST /api/v1/auth/tokens` and `POST /api/v1/auth/tokens/refresh`; the lifecycle lives in `flare-server-core::auth::issuer`, the gateway only routes | gateways in `core_jwt` mode | **SDK-managed**: set `auth.tokenEndpoint`, call `login(userId)` without a token; the SDK fetches and refreshes before expiry |
| flare-social | `flare-user` login returns `im_connect_token`; refresh via `flare-user` | gateways trust the social issuer or use `http_hook` | **App-managed**: the social SDK passes the token to `login`, calls `update_access_token` after refresh (already the case) |
| custom business backend | the backend signs (holds the secret or calls the gateway issue API with an app credential) | `core_jwt` trusted issuer or `http_hook` | **App-managed**: same as above |

Clients never touch a signing secret. `sdk.generate_core_token` is removed from every binding.

## Gateway API (core-only shape)

Per `GATEWAY_SPEC.md` the gateway owns no token lifecycle: issue/refresh/revoke are implemented by
`TokenIssuer` in server-core; the gateway does inbound auth, validation and rate limiting.

- `POST /api/v1/auth/tokens` body `{userId, tenantId?, deviceId?, ttlSecs?}`. Callers: a business backend
  with `x-app-id` + `x-app-secret` (`FLARE_API_GATEWAY_AUTH_APP_CREDENTIALS="app:secret,..."`), or, for
  local development only, a gateway started with `FLARE_API_GATEWAY_AUTH_DEV_ISSUE=true` (default false,
  loud startup warning). Response `{token, expiresAt, userId, tenantId?, deviceId?}`.
- `POST /api/v1/auth/tokens/refresh` with `Authorization: Bearer <current token>`. Signature and issuer must
  verify; `exp` may be past by at most `AUTH_REFRESH_GRACE_SECS` (default 7 days); with a token store the old
  `jti` must not be revoked and is revoked after rotation. The new token keeps `sub/tenant_id/device_id`.
- In `http_hook` auth mode both calls are forwarded to `AUTH_HOOK_ISSUE_URL` with the hook secret header;
  without that URL the endpoints answer `501 TOKEN_ISSUE_DELEGATED`.

## SDK changes

- `SdkConfig.auth = { tokenEndpoint?, appId? }` (`tokenEndpoint` defaults to `httpUrl`).
- `login(userId, token?)`: no token + `tokenEndpoint` configured → the SDK issues via the gateway; an explicit
  token is used as-is; neither → a configuration error. No more env-var or `dev-test-token` fallbacks.
- Refresh belongs to the core: it parses `exp`, refreshes five minutes before expiry (configurable) and
  calls `update_access_token`; it also refreshes before reconnecting with an expired token and on
  `TOKEN_EXPIRED`. In the app-managed shape the core only emits the event.
- Contract: `sdk.generate_core_token` removed, `client_config.json` gains an `auth` section, all bindings regenerated.
- The kit and the five example apps keep only the user id field (a "paste access token" field stays in the
  advanced section for the app-managed shape); the signing-secret field and local minting are gone.

## Verification

Unit tests for issue/refresh/rotation/grace/revocation in server-core; gateway route and handler tests;
SDK login-without-token and refresh scheduling tests; production check on 118.107.9.221 (gateway image
rebuilt with `AUTH_DEV_ISSUE=true`, web and iOS simulator log in with a user id only); flare-social e2e regression.

## 6. Token validation: core-standalone / third-party / hybrid

Beyond issuance, **validation** is also dual-mode and composable. Gateways obtain a `TokenValidator`
via `build_token_validator(&auth, token_service, trusted_issuers)`.

| Shape | Config | Behavior |
|---|---|---|
| **Core standalone** | `AUTH_MODE=core_jwt`, no hook | Local HS256 validation of core-signed tokens (+ `trusted_token_issuers`). Zero network. |
| **Third-party hook (pure delegation)** | `AUTH_MODE=http_hook` + `AUTH_HOOK_URL` | Every token is POSTed to the business hook; the business owns the whole token flow. |
| **Hybrid (core-local + third-party fallback)** | `AUTH_MODE=core_jwt` + `AUTH_HOOK_URL` | `ChainedTokenValidator`: local first (core JWT + trusted issuers, zero network for tokens it recognizes), falling back to the hook only for tokens it cannot validate locally (third-party opaque tokens). One deployment validates its own tokens AND third-party tokens. |

**Validation hook contract**: `POST {AUTH_HOOK_URL}` with header `{AUTH_HOOK_SECRET_HEADER}` →
`{ "active", "userId", "tenantId?", "deviceId?", "appId?", "expiresAt?", "scopes", "metadata" }`.
`active=false`/401/403 → reject; 5xx/timeout → `ProviderUnavailable` (retryable, never a permanent reject).

**Refresh tokens**: `core_jwt` issues a short access token + a long refresh token (`token_use=refresh`,
`refresh_token_ttl_seconds`, default 30d); the refresh endpoint accepts the refresh token, mints a new access
token and rotates the refresh token. In `http_hook` mode the business hook owns refresh.
