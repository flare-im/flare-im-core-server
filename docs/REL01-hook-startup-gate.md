# Hook startup and mandatory PreSend gate

## Runtime contract

- No explicit path: load the embedded, intentionally empty `hooks.core.toml` profile and report `profile=core`, zero definitions.
- Explicit file/directory: all candidates must exist and parse. An explicit directory without a `.toml` profile is an error; use the explicit empty core file to disable business hooks deliberately.
- Unknown fields are configuration errors, including misspelled selectors and transport options. Plugin-specific extensibility remains inside `metadata`/transport metadata maps. TOML errors report file/byte location without echoing source text, secrets, or headers.
- Ingest validates the deployment file before any I/O, then installs it and reads `hook_configs`, all before Redis/MQ initialization, background tasks, or listeners. Invalid Hook configuration cannot be masked by an unavailable broker.
- The loader logs definition counts and source. The Ingest installation log counts **enabled** Hooks, not disabled definitions.
- A PreSend Hook with `require_success=true` answers service unavailability, connection errors, timeouts, an undecodable rewrite and an exhausted budget with a retryable 503. An explicit business rejection always rejects, including optional Hooks. `require_success=false` skips the step in those cases.
- Core ships no business Hook: the packaged `hooks.toml` is the empty core profile. A business server keeps its own Hook registration file (flare-im: `flare-im-server/deploy/core-hooks.toml`, one mandatory PreSend Hook for every tenant) and passes its path to Core through `MESSAGE_INGEST_HOOKS_CONFIG`. That file is not proof that the business Hook service is reachable or correctly authenticated.

## Database hooks

The deployment file is one source; `hook_configs` is the other. With a Core database configured (the `postgres` profile of `[services.message_ingest]` or `MESSAGE_INGEST_TENANT_RUNTIME_POSTGRES_URL`):

- Startup reads all enabled rows after the deployment file. An unreachable database, a missing table or an invalid row stops startup, so the service never becomes ready with half of its hooks.
- At runtime Ingest polls `hook_config_versions` about once a second and reloads the scopes that changed. An unreachable database keeps the last good configuration and warns; a scope with an invalid row keeps its last good hooks and warns while other scopes reload.
- In `FLARE_REGISTRY_MODE=static` a row whose gRPC target is a service name counts as invalid, like a `discovery://` route in the deployment file.
- Without a Core database Ingest runs deployment hooks only, warns, and records no executions.
- After loading, Ingest writes `hook_deployment_summary`. A failed write only warns: the summary is for display.

Capability has no hook file and no config center source. It does not run hooks.

The generic `hooks.example.toml` holds HookConfig only. Capability discovery belongs in `config/services/capability.toml`.

## Focused regression gates

From the Core repository:

```sh
cargo test -p flare-im-core-hooks --lib --test config_fail_closed
cargo test -p flare-message-ingest --lib hook_startup_tests
cargo check -p flare-message-ingest --lib
cargo test -p flare-im-core-hooks --lib hooks::reload
cargo test -p flare-capability --lib infrastructure::config
```

The tests cover absent/invalid paths, absent overlays, empty directory mounts, unknown fields, redacted parse errors, all shipped profiles, real factory installation/selection for multiple tenants, required versus optional infrastructure failures, database reload with last-good fallback, and mandatory-policy startup ordering before missing Redis/MQ. They do not claim to exercise a container or persisted blacklist policy.

The Social SDK blacklist test now establishes the same pair's friendship and waits for a fresh permitted message's exact client ID in the recipient's inbound event. After blocking, it requires a server ACK correlated by client-message ID and conversation ID, not the local queue receipt. The current Core WebSocket mapping is code `60`, reason `pre-send hook rejected`, details `allow=false`; all three must match. This is the current generic Hook rejection contract, not a dedicated Social blacklist error code. Timeout, disconnection, authentication failure, unrelated internal errors, and missing ACKs fail the assertion.

## Remaining release acceptance

Use a newly built immutable Core image and an isolated integration stack with the current SignedContext verification keys, tenant projections, Social services, and temporary fixture data. Do not run these mutation checks against a user's existing database.

1. Start Message Ingest with the business server's Hook registration file mounted and `MESSAGE_INGEST_HOOKS_CONFIG` pointing at it. Require successful readiness **and** `message ingest hooks installed ... pre_send=1`; a loader log from a process that later exits is not success.
2. In a separate disposable container, point that variable to an absent path (or rename only its disposable mounted file). Require nonzero exit with `load hook config` and no service readiness. Also test an empty mounted directory and an invalid TOML field. Pin the image and use `--pull=never`; do not mutate the live stack mount.
3. From Social, run `./scripts/e2e_social_sdk.sh e2e_relation` against that isolated stack. Its fresh fixture must establish successful messaging first, persist the block, then prove the blocked send is rejected and not delivered. Retain the test case result and correlated Ingest/Social request IDs without credentials or message contents.
4. Stop only the isolated Hook service and repeat a send that selects the mandatory policy. Require send rejection during both connection failure and timeout; restoring the service should restore permitted sends.
5. Point Ingest at an isolated database and stop it: a fresh start must not become ready, and a running instance must keep sending with the hooks it holds. Write a hook through `hook_config.create` and require it to run within about two seconds, without a restart.

At the time of this change, no current Core release image or isolated full Social/Core stack was available locally. Container readiness, container missing-file exit, and persisted-blacklist SDK E2E remain release gates; source/unit checks must not mark them passed.
