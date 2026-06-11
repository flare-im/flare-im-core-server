# Environment Overrides

`config/environments/{FLARE_ENV}.toml` is loaded after the main config directory. `FLARE_ENV` defaults to `development`.

Current loader support is intentionally narrow:

- root `[mq]`
- `[object_storage.*]`

Do not put service tables here unless the loader is explicitly extended to merge them. Use `overrides/*.toml` for full recursive overrides.
