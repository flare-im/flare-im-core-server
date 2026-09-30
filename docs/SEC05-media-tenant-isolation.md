# SEC-05 Media tenant / owner boundary

## Changed boundary

- Public API Gateway no longer registers `POST /api/v1/medias/cleanup-orphaned-assets`, `POST .../object-acl`, `GET .../objects`, or `GET .../bucket`. Their administrative counterparts are under `/api/v1/admin/medias` in Admin Gateway and require its control-plane authorization, one concrete authorized tenant and, for writes, actor / audit reason / request correlation.
- All Media HTTP routes, including `GET /api/v1/medias/files/{file_id}`, require a validated bearer principal with a concrete tenant and nonempty user. A caller-supplied tenant/user header cannot fill missing token claims.
- Media gRPC authenticates its caller. The server is wrapped in `SignedContextLayer`: only a Cell-signed internal context (`x-flare-ctx`, EdDSA, 60 s) establishes tenant and actor; a missing, forged, expired or duplicated token is `UNAUTHENTICATED`, and caller-supplied `x-tenant-id` / `x-user-id` / `x-actor-*` metadata is stripped before the handler. The handler also refuses any Context that the layer did not verify. Orphan cleanup, object listing, ACL and bucket description additionally require a `tenant_admin` (or `system`) actor; a validly signed end user gets `PERMISSION_DENIED`.
- Core's Media client (`MediaCaller`) signs a fresh token for every RPC, so a slow upload body or several calls in one handler never reuse an expired token. API Gateway signs the token-bound user (`user` actor, tenant exactly as in the access token; admin-scoped principals are refused). Admin Gateway signs the Control-verified employee as `tenant_admin` of its own tenant. Without a configured signer both gateways fail the Media route instead of sending an unsigned RPC.
- Media PostgreSQL and cache reads are tenant-scoped. Private metadata / URL / byte reads require the typed authenticated `owner_id`; public files remain readable by authenticated principals within the same tenant. Public does **not** grant mutation rights. Shared-conversation attachment authorization is not inferred from arbitrary metadata; it needs a future typed authorization contract.
- Streaming, multipart and direct upload bind tenant and owner from Context, not request metadata. Continuation / status / complete / abort verify that binding before storage access. A nonexistent multipart session is a no-op abort and cannot cause directory cleanup. Chunk directories accept only canonical UUID upload IDs.
- New S3 keys begin with an injectively encoded tenant path segment. `/`, `.`, `%` and non-ASCII bytes are percent-encoded; case is preserved. Both stream and direct upload use the same key builder. Existing objects are read by their persisted key without adding a prefix.
- Orphan cleanup selects only the authorized tenant, with a default limit of 100 and maximum 1,000. PostgreSQL, reference and cache mutations use the same tenant. Storage errors propagate instead of reporting false cleanup success.
- Cleanup and reference creation are serialized per asset. The candidate list is only a snapshot; each candidate is then *claimed* in its own transaction: `SELECT ... FOR UPDATE` re-checks `reference_count = 0`, `pending` and an expired grace period, and a following statement (fresh snapshot, after the lock) deletes the row only if no `media_references` row exists. The object is deleted while the claim is held, then the claim commits; a storage error rolls the claim back so the metadata stays for a retry. Reference creation pins the asset row with `FOR KEY SHARE` in the same transaction as the insert: a reference committed first makes the claim give up; a reference arriving during a claim waits and then fails with not-found instead of pointing at a deleted object. Two concurrent cleaners never report the same asset.
- Object listing and ACL writes were prototypes (empty inventory / successful no-op). They now explicitly fail unsupported and are marked `not_implemented` in Admin capabilities. Bucket inspection describes only the configured bucket and this tenant's prefix; it is not global object inventory.

## Upgrade gates and deliberate compatibility changes

1. Back up metadata; apply `deploy/patches/20260929_media_authenticated_owner.sql` **before** starting the new Media binary. The patch is additive and repeatable. Fresh databases use `deploy/init.sql`; never run that destructive bootstrap against an existing database.
2. Stop accepting new uploads on old gateways, drain or expire uploads, then deploy Media and both gateways together. Old gateways do not sign and are rejected by the new Media (`UNAUTHENTICATED`); new gateways cannot talk to an old Media, which ignores the signed context. Media now refuses to start without `FLARE_INTERNAL_CTX_PUBLIC_KEY`; Admin Gateway needs `FLARE_INTERNAL_CTX_PRIVATE_KEY` for its media routes (the Core supervisor manifest, `deploy/docker-compose.stack.yml` and `scripts/start_server.sh` pass them). Existing sessions without `tenant_id` must restart; do not backfill them from client-controlled metadata.
3. Historical private assets have `owner_id = NULL` and fail closed. Audit ownership against trusted application records or re-upload; do not bulk-copy historical `metadata.owner_id` / `metadata.user_id`. Unknown assets stay unavailable. Any administrative backfill must include tenant + file ID, have a recorded approval/source, and invalidate the corresponding new cache entry.
4. Redis metadata uses the `media:metadata:v2` namespace. Old entries are ignored; operator cleanup can remove the retired namespace after rollout. Do not flush a shared Redis database. Rolling back only the binary is unsafe because it restores anonymous access and unbound sessions.
5. Anonymous local file links now require bearer authentication, including `<img src>` / browser downloads that previously depended on anonymous access. Clients must use authenticated fetching or a separately designed short-lived delivery token. Already-issued S3 presigned URLs remain valid until their expiry; revoke/rotate storage credentials or deny objects separately if immediate revocation is required. Do not expose private bucket/CDN origins with public-read policies.
6. **Media gRPC remains private infrastructure**. Caller authentication now rests on the Cell private key: any process holding it can mint any tenant identity, so it is distributed only to ingress processes (API Gateway, Admin Gateway, Signaling Gateway, and the broker-isolated Push Server/Worker); Media itself only holds the public key. Keep network policy restricted to trusted peers as defense in depth; this does not authorize exposing Media gRPC to tenants, browsers or an untrusted network.

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

The cleanup/reference race needs several connections, so it runs in a fresh throwaway schema of the same disposable database (both tables are asserted to live there, and the schema is dropped afterwards, also on failure):

```sh
RUSTC_WRAPPER= CARGO_BUILD_JOBS=2 cargo test --locked --offline -p flare-media --lib infrastructure::persistence::postgres_metadata::race_tests -- --ignored
```

It pins both interleavings deterministically (a reference created after the candidate snapshot must survive with its object; a reference racing an uncommitted claim must wait and then fail with nothing left behind), checks that a storage failure releases the claim for a retry, and runs a multi-threaded stress of two cleaners against per-asset reference creation, asserting per asset that a successful reference keeps asset, reference and object, and a failed one leaves none of them and is reported exactly once.

## Real HTTP / storage gate

Run API Gateway, Admin Gateway and Media against **disposable** active tenants A/B, PostgreSQL and the intended local/S3 store. Provision current tokens through the real auth path; do not forge production tokens. Set these environment variables:

- `FLARE_MEDIA_E2E_DISPOSABLE=1`
- `FLARE_MEDIA_E2E_TENANT_A`, `FLARE_MEDIA_E2E_TENANT_B`: distinct fixture tenants
- `FLARE_MEDIA_E2E_USER_A`, `FLARE_MEDIA_E2E_USER_B`: normal user bearer tokens (prefer the same user ID in the two tenants)
- `FLARE_MEDIA_E2E_OTHER_A`: a different user in tenant A
- `FLARE_MEDIA_E2E_ADMIN_A`: current Admin Gateway assertion authorized for tenant A only (required only with `--admin-url`)

```sh
python3 scripts/test_media_isolation_http.py \
  --api-url http://127.0.0.1:8080 --admin-url http://127.0.0.1:8081 \
  --bucket fixture-bucket --storage-origin http://127.0.0.1:9000
```

Through an installation's Edge (Core mounted under `/api`, objects under `/flare-media/`), dialing the numeric loopback address while TLS SNI, certificate verification and `Host` keep the installation domain, and trusting only the installation CA:

```sh
python3 scripts/test_media_isolation_http.py \
  --api-url https://p0e2e.127-0-0-1.sslip.io:18443/api \
  --storage-origin https://p0e2e.127-0-0-1.sslip.io:18443/flare-media/ \
  --ca-file ~/.flare-p0e2e/edge/tls/fullchain.pem --connect-address 127.0.0.1
```

- `--api-url` / `--admin-url` are an origin plus an optional path prefix (plain segments only; no query, encoding or dot segments).
- `--storage-origin` is an origin plus an optional path prefix. The owner's presigned `file-url` and the `/files/{id}` 307 target must be absolute URLs under it; both are fetched without the application bearer and must return the uploaded bytes. Without it, a redirect fails the gate and the presigned check prints `presigned: not evaluated`.
- `--connect-address` must be a numeric IP; plaintext is allowed only to loopback.
- Without `--admin-url` the admin half is skipped and the output says `admin: not evaluated`; the result is then not evidence for the Admin Gateway boundary.

Ports are examples; use the actual isolated deployment. Remote services must use HTTPS. The script rejects automatic redirects, ignores proxy settings, never forwards an application bearer to storage, and never logs tokens, bodies or signed URLs. It creates one random fixture and deletes only that file (also when a check fails). It deliberately does **not** invoke tenant-wide orphan cleanup. Repeat once for local storage and once for S3; complete multipart/direct transport tests in the existing client integration suite before release.

## Remaining limitations to track explicitly

- Generic gRPC error conversion still maps some denied / unsupported operations to HTTP 500. The external test asserts no data/redirect plus a failing status, not a fabricated stable 403 contract.
- Only reference *creation* is serialized with cleanup. Reference release and owner deletion still write the whole metadata row with an upsert, so releasing a reference on an asset that a concurrent cleanup has just removed can re-insert a metadata row without an object. `CleanupOrphanedAssetsRequest.dry_run` is not honored: a dry run deletes like a real run.
- `GET /api/v1/medias/files/{id}` redirects to `cdn_url` whenever it is non-empty. With the default empty CDN setting (`cdn_base_url = "${FLARE_S3_CDN_BASE_URL:-}"`) the S3 repository normalizes its own copy to unset, but the Media service configuration keeps `Some("")`, so `cdn_url` becomes the bare object key (also persisted in metadata at upload) and the 307 `Location` is a relative path that clients resolve against the API. The HTTP gate fails on this redirect until it is fixed; the presigned `file-url` path is not affected.
- Direct upload URL generation / ACL / object inventory and typed cross-user attachment grants need separate completed contracts. The SEC-05 patch does not claim these prototypes are production-complete.
- Actual PostgreSQL, HTTP and S3 acceptance must be recorded separately from source checks and in-memory tests. A skipped external test is not an acceptance result.
