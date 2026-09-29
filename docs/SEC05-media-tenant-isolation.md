# SEC-05 Media tenant / owner boundary

## Changed boundary

- Public API Gateway no longer registers `POST /api/v1/medias/cleanup-orphaned-assets`, `POST .../object-acl`, `GET .../objects`, or `GET .../bucket`. Their administrative counterparts are under `/api/v1/admin/medias` in Admin Gateway and require its control-plane authorization, one concrete authorized tenant and, for writes, actor / audit reason / request correlation.
- All Media HTTP routes, including `GET /api/v1/medias/files/{file_id}`, require a validated bearer principal with a concrete tenant and nonempty user. A caller-supplied tenant/user header cannot fill missing token claims.
- Every Media client RPC carries the validated request Context. Media PostgreSQL and cache reads are tenant-scoped. Private metadata / URL / byte reads require the typed authenticated `owner_id`; public files remain readable by authenticated principals within the same tenant. Public does **not** grant mutation rights. Shared-conversation attachment authorization is not inferred from arbitrary metadata; it needs a future typed authorization contract.
- Streaming, multipart and direct upload bind tenant and owner from Context, not request metadata. Continuation / status / complete / abort verify that binding before storage access. A nonexistent multipart session is a no-op abort and cannot cause directory cleanup. Chunk directories accept only canonical UUID upload IDs.
- New S3 keys begin with an injectively encoded tenant path segment. `/`, `.`, `%` and non-ASCII bytes are percent-encoded; case is preserved. Both stream and direct upload use the same key builder. Existing objects are read by their persisted key without adding a prefix.
- Orphan cleanup selects only the authorized tenant, with a default limit of 100 and maximum 1,000. PostgreSQL, reference and cache mutations use the same tenant. Storage errors propagate instead of reporting false cleanup success.
- Object listing and ACL writes were prototypes (empty inventory / successful no-op). They now explicitly fail unsupported and are marked `not_implemented` in Admin capabilities. Bucket inspection describes only the configured bucket and this tenant's prefix; it is not global object inventory.

## Upgrade gates and deliberate compatibility changes

1. Back up metadata; apply `deploy/patches/20260929_media_authenticated_owner.sql` **before** starting the new Media binary. The patch is additive and repeatable. Fresh databases use `deploy/init.sql`; never run that destructive bootstrap against an existing database.
2. Stop accepting new uploads on old gateways, drain or expire uploads, then deploy Media and both gateways together. Mixed old wrappers omit Context and will be rejected. Existing sessions without `tenant_id` must restart; do not backfill them from client-controlled metadata.
3. Historical private assets have `owner_id = NULL` and fail closed. Audit ownership against trusted application records or re-upload; do not bulk-copy historical `metadata.owner_id` / `metadata.user_id`. Unknown assets stay unavailable. Any administrative backfill must include tenant + file ID, have a recorded approval/source, and invalidate the corresponding new cache entry.
4. Redis metadata uses the `media:metadata:v2` namespace. Old entries are ignored; operator cleanup can remove the retired namespace after rollout. Do not flush a shared Redis database. Rolling back only the binary is unsafe because it restores anonymous access and unbound sessions.
5. Anonymous local file links now require bearer authentication, including `<img src>` / browser downloads that previously depended on anonymous access. Clients must use authenticated fetching or a separately designed short-lived delivery token. Already-issued S3 presigned URLs remain valid until their expiry; revoke/rotate storage credentials or deny objects separately if immediate revocation is required. Do not expose private bucket/CDN origins with public-read policies.
6. **Media gRPC remains private infrastructure**: its current Context layer is not a signed caller authenticator. Network policy must permit only trusted gateway/internal service peers. These changes do not authorize exposing Media gRPC to tenants, browsers or an untrusted service network. A typed SignedContext/service-auth rollout is a separate gate, not implied by tenant filtering.

## Deterministic gates

From the Core repository, with the shared Rust build lock free:

```sh
RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 cargo check --locked --offline -p flare-media -p flare-api-gateway -p flare-admin-gateway --all-targets
RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 cargo test --locked --offline -p flare-api-gateway -p flare-admin-gateway --lib
RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 cargo test --locked --offline -p flare-media --lib
python3 -m unittest discover -s scripts -p test_media_isolation_http_test.py -v
```

Router tests use the actual route tree with lazy clients and no upstream connection. Domain tests exercise real upload/download/session/cleanup orchestration with in-memory ports. S3 key and presigning tests do not contact S3. None substitutes for the external gates below.

## Real PostgreSQL gate

Start a disposable PostgreSQL database and set `FLARE_MEDIA_TEST_DATABASE_URL` in the environment (do not put credentials in command history). Run:

```sh
RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 cargo test --locked --offline -p flare-media --lib infrastructure::persistence::postgres_metadata::tests::postgres_media_owner_migration_and_tenant_cleanup -- --ignored --exact
```

The ignored test requires the URL; missing infrastructure is not reported as a pass. It extracts the authoritative schema, creates only connection-local TEMP tables, applies the owner migration twice, and executes the real repository for same-file-ID/different-tenant reads, immutable ownership, missing-context rejection, bounded cleanup selection and tenant-specific deletion. Its connection closure removes the temporary tables. It does not touch durable media tables.

## Real HTTP / storage gate

Run API Gateway, Admin Gateway and Media against **disposable** active tenants A/B, PostgreSQL and the intended local/S3 store. Provision current tokens through the real auth path; do not forge production tokens. Set these environment variables:

- `FLARE_MEDIA_E2E_DISPOSABLE=1`
- `FLARE_MEDIA_E2E_TENANT_A`, `FLARE_MEDIA_E2E_TENANT_B`: distinct fixture tenants
- `FLARE_MEDIA_E2E_USER_A`, `FLARE_MEDIA_E2E_USER_B`: normal user bearer tokens (prefer the same user ID in the two tenants)
- `FLARE_MEDIA_E2E_OTHER_A`: a different user in tenant A
- `FLARE_MEDIA_E2E_ADMIN_A`: current Admin Gateway assertion authorized for tenant A only

```sh
python3 scripts/test_media_isolation_http.py \
  --api-url http://127.0.0.1:8080 --admin-url http://127.0.0.1:8081 \
  --bucket fixture-bucket --storage-origin http://127.0.0.1:9000
```

Ports are examples; use the actual isolated deployment. Remote services must use HTTPS. The script rejects automatic redirects, never forwards an application bearer to S3, downloads only from the explicitly allowed storage origin, and never logs tokens, bodies or signed URLs. It creates one random fixture and deletes only that file. It deliberately does **not** invoke tenant-wide orphan cleanup. Repeat once for local storage and once for S3; complete multipart/direct transport tests in the existing client integration suite before release.

## Remaining limitations to track explicitly

- Generic gRPC error conversion still maps some denied / unsupported operations to HTTP 500. The external test asserts no data/redirect plus a failing status, not a fabricated stable 403 contract.
- Cleanup still uses the existing read-then-delete lifecycle. Concurrent reference creation requires a future transactional claim/tombstone protocol; schedule cleanup in a controlled window until that change is implemented. Tenant scoping does not solve this lifecycle race.
- Direct upload URL generation / ACL / object inventory and typed cross-user attachment grants need separate completed contracts. The SEC-05 patch does not claim these prototypes are production-complete.
- Actual PostgreSQL, HTTP and S3 acceptance must be recorded separately from source checks and in-memory tests. A skipped external test is not an acceptance result.
