# Flare IM Core Configuration

This directory is the single configuration root consumed by `flare-im-service-kit`.

## Load Order

When a service calls `load_app_config_from_env()`, the config path is resolved from:

1. `FLARE_CONFIG_PATH`
2. `config`
3. `config.toml`
4. `flare-im-core/config`

For a directory config root, fragments are merged in this order:

1. `base.toml`
2. `shared/*.toml`
3. `services/*.toml`
4. `overrides/*.toml`

Later layers override earlier layers recursively.

After that, `config/environments/{FLARE_ENV}.toml` is applied for the currently supported environment-specific surfaces: root `[mq]` and `[object_storage.*]`. `FLARE_ENV` defaults to `development`.

## Layout

- `base.toml`: shared infrastructure profiles and global defaults.
- `services/*.toml`: one service table per file. File names use kebab-case service keys, while TOML table names keep Rust/config snake_case, for example `api-gateway.toml` contains `[services.api_gateway]`.
- `shared/*.toml`: optional shared fragments that are intentionally separated from `base.toml`.
- `overrides/*.toml`: local or deployment overlays. Do not commit secrets here.
- `environments/*.toml`: environment-level overrides for MQ and object storage only.
- `hooks*.toml`: hook runtime profiles. `hooks.toml` is the active file used by services.

## Rules

- Service config files must be named in kebab-case: `message-ingest.toml`, not `message_ingest.toml`.
- Stable config semantics belong in typed TOML fields, not ad hoc environment variables.
- Environment variables are reserved for secrets, local endpoints, and deployment-specific overrides.
- Secrets must not be committed. Use environment variables or secret managers.
