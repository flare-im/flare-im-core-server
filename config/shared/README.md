# Shared Config Fragments

Optional `.toml` files in this directory are merged after `base.toml` and before `services/*.toml`.

Use this layer only when a shared concern is too large or too deployment-specific for `base.toml`, for example a reusable observability profile or a regional registry profile.
