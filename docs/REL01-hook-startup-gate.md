# Hook startup and mandatory PreSend gate

## Runtime contract

- No explicit path: load the embedded, intentionally empty `hooks.core.toml` profile and report `profile=core`, zero definitions.
- Explicit file/directory: all candidates must exist and parse. An explicit directory without a `.toml` profile is an error; use the explicit empty core file to disable business hooks deliberately.
- Unknown fields are configuration errors, including misspelled selectors and transport options. Plugin-specific extensibility remains inside `metadata`/transport metadata maps. TOML errors report file/byte location without echoing source text, secrets, or headers.
- Ingest validates and installs Hooks before Redis/MQ initialization, background tasks, or listeners. Invalid Hook configuration cannot be masked by an unavailable broker.
- The loader logs definition counts and source. The Ingest installation log counts **enabled** Hooks, not disabled definitions.
- A PreSend Hook with `require_success=true` rejects service unavailability, connection errors and timeouts. An explicit business rejection always rejects, including optional Hooks. `require_success=false` alone permits the existing infrastructure-error/timeout degradation.
- `hooks.social.toml` and the packaged `hooks.toml` select one mandatory Social PreSend Hook for every tenant's supported single/group conversation type. This configuration is not proof that a deployed Social service is reachable or correctly authenticated.

The generic `hooks.example.toml` no longer mixes Capability service configuration into HookConfig. Capability discovery belongs in `config/services/capability.toml`. Business examples use the actual Rust enum values `FailFast` and `Ignore`.

## Focused regression gates

From the Core repository:

```sh
cargo test -p flare-im-hooks --lib --test config_fail_closed
cargo test -p flare-message-ingest --lib hook_startup_tests
cargo check -p flare-message-ingest --lib
```

The tests cover absent/invalid paths, absent overlays, empty directory mounts, unknown fields, redacted parse errors, all shipped profiles, real factory installation/selection for multiple tenants, required versus optional infrastructure failures, and mandatory-policy startup ordering before missing Redis/MQ. They do not claim to exercise a container or persisted blacklist policy.

The Social SDK blacklist test now establishes the same pair's friendship and waits for a fresh permitted message's exact client ID in the recipient's inbound event. After blocking, it requires a server ACK correlated by client-message ID and conversation ID, not the local queue receipt. The current Core WebSocket mapping is code `60`, reason `pre-send hook rejected`, details `allow=false`; all three must match. This is the current generic Hook rejection contract, not a dedicated Social blacklist error code. Timeout, disconnection, authentication failure, unrelated internal errors, and missing ACKs fail the assertion.

## Remaining release acceptance

Use a newly built immutable Core image and an isolated integration stack with the current SignedContext verification keys, tenant projections, Social services, and temporary fixture data. Do not run these mutation checks against a user's existing database.

1. Start Message Ingest with the mounted Social profile and `MESSAGE_INGEST_HOOKS_CONFIG=/etc/flare/config/hooks.toml`. Require successful readiness **and** `message ingest hooks installed ... pre_send=1`; a loader log from a process that later exits is not success.
2. In a separate disposable container, point that variable to an absent path (or rename only its disposable mounted file). Require nonzero exit with `load hook config` and no service readiness. Also test an empty mounted directory and an invalid TOML field. Pin the image and use `--pull=never`; do not mutate the live stack mount.
3. From Social, run `./scripts/e2e_social_sdk.sh e2e_relation` against that isolated stack. Its fresh fixture must establish successful messaging first, persist the block, then prove the blocked send is rejected and not delivered. Retain the test case result and correlated Ingest/Social request IDs without credentials or message contents.
4. Stop only the isolated Hook service and repeat a send that selects the mandatory policy. Require send rejection during both connection failure and timeout; restoring the service should restore permitted sends.

At the time of this change, no current Core release image or isolated full Social/Core stack was available locally. Container readiness, container missing-file exit, and persisted-blacklist SDK E2E remain release gates; source/unit checks must not mark them passed.
