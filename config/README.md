# Flare IM Core Configuration

This directory is the single configuration root consumed by `flare-im-service-kit`.
The project is still deployed as separate microservice binaries; this directory only gives them one shared, typed configuration source.

## Load Order

`load_app_config_from_env()` resolves the root from `FLARE_CONFIG_PATH`, then falls back to `config`, `config.toml`, and `flare-im-core/config`.

For a directory root, TOML fragments are merged in this order:

1. `base.toml`
2. `shared/*.toml`
3. `services/*.toml`
4. `overrides/*.toml`
5. `environments/{FLARE_ENV}.toml`

Later layers override earlier layers recursively. The environment layer is intentionally narrow today: it only merges root `[mq]` and `[object_storage.*]`. Use `overrides/*.toml` for full service/runtime overlays.

String values may reference environment variables with `${ENV_VAR}`. Missing variables fail config loading instead of silently keeping placeholders; errors in the active environment overlay are fatal at startup.

## What Goes Where

| Need | Place |
| --- | --- |
| Shared infrastructure profiles | `base.toml`: `[redis.*]`, `[postgres.*]`, `[jetstream.*]`, `[kafka.*]`, `[object_storage.*]`, `[registry]`, `[logging]` |
| One service's stable runtime knobs | `services/<service>.toml`, under one `[services.<service_key>]` table |
| Local or deployment-specific full overlay | `overrides/*.toml`; keep secrets out of git |
| MQ/object-store environment examples | `environments/{FLARE_ENV}.toml` |
| Hook runtime profiles | `hooks.toml` plus `hooks.*.toml` profiles |

Service files use kebab-case names; TOML table keys keep snake_case.

## Service Index

| File | Table | Runtime package |
| --- | --- | --- |
| `access-gateway.toml` | `[services.access_gateway]` | `flare-signaling-gateway` |
| `admin-gateway.toml` | `[services.admin_gateway]` | `flare-admin-gateway` |
| `api-gateway.toml` | `[services.api_gateway]` | `flare-api-gateway` |
| `capability.toml` | `[services.capability]` | `flare-capability` |
| `conversation.toml` | `[services.conversation]` | `flare-conversation` |
| `media.toml` | `[services.media]` | `flare-media` |
| `message-ingest.toml` | `[services.message_ingest]` | `flare-message-ingest` |
| `message-orchestrator.toml` | `[services.message_orchestrator]` | `flare-orchestrator` |
| `push-proxy.toml` | `[services.push_proxy]` | `flare-push-proxy` |
| `push-server.toml` | `[services.push_server]` | `flare-push-server` |
| `push-worker.toml` | `[services.push_worker]` | `flare-push-worker` |
| `signaling-online.toml` | `[services.signaling_online]` | `flare-signaling-online` |
| `signaling-route.toml` | `[services.signaling_route]` | `flare-signaling-route` |
| `storage-reader.toml` | `[services.storage_reader]` | `flare-storage-reader` |
| `storage-writer.toml` | `[services.storage_writer]` | `flare-storage-writer` |
| `sync-orchestrator.toml` | `[services.sync_orchestrator]` | `flare-sync-orchestrator` |

## Runtime Overrides

Typed TOML owns stable service behavior: ports, profile references, subjects, WAL settings, hook paths, metrics endpoints, auth mode, and object-store/MQ profiles.

Environment variables are reserved for secrets and per-process operational tuning:

- `FLARE_API_GATEWAY_TOKEN_SECRET`, `FLARE_ADMIN_GATEWAY_TOKEN_SECRET`, `ACCESS_GATEWAY_TOKEN_SECRET`
- `FLARE_API_GATEWAY_GRPC_*`, `FLARE_ADMIN_GATEWAY_GRPC_*` for downstream HTTP gateway routes and static fallbacks
- `ACCESS_GATEWAY_*` for long-connection stability limits and auth-hook overrides
- `PUSH_WORKER_OFFLINE_*` for offline outbox stream wiring
- `FLARE_MQ_DEFAULT_BACKEND`

Do not add active TOML keys that are not consumed by `FlareAppConfig` or a service-owned typed settings loader. A non-working knob is worse than no knob.

## Rules

- Commit concise defaults and examples, not secrets.
- Use `overrides/*.toml` for deployment overlays that need the full recursive merge surface.
- Use `environments/{FLARE_ENV}.toml` only for `[mq]` and `[object_storage.*]` until the loader is widened.
- Keep service config files one service per file, kebab-case file names, snake_case table names.
