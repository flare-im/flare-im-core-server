#!/usr/bin/env bash
# Builds the Capability service that links the plugin service router
# (flare-capability/with-plugin-host, its own workspace) into the given target directory. It
# produces the same `flare-capability` binary as the open workspace and replaces it there.
#
# FLARE_CAPABILITY_PLUGIN_HOST:
#   auto (default)  build it when ../flare-plugin/flare-plugin-host is checked out, otherwise keep
#                   the open binary, which refuses plugin.service.dispatch;
#   1               require it (fail when flare-plugin-host is missing);
#   0               keep the open binary.
#
# Usage: flare_build_capability_with_plugin_host <project_root> <target_dir> [cargo build args...]
flare_build_capability_with_plugin_host() {
    local root="$1" target_dir="$2"
    shift 2
    local mode="${FLARE_CAPABILITY_PLUGIN_HOST:-auto}"
    local host="$root/../flare-plugin/flare-plugin-host/Cargo.toml"
    case "$mode" in
        0)
            echo "flare-capability: open build (FLARE_CAPABILITY_PLUGIN_HOST=0), plugin.service.dispatch is refused" >&2
            return 0
            ;;
        1 | auto) ;;
        *)
            echo "flare-capability: FLARE_CAPABILITY_PLUGIN_HOST must be auto, 1 or 0 (got $mode)" >&2
            return 2
            ;;
    esac
    if [ ! -f "$host" ]; then
        if [ "$mode" = 1 ]; then
            echo "flare-capability: FLARE_CAPABILITY_PLUGIN_HOST=1 but $host is missing" >&2
            return 1
        fi
        echo "flare-capability: flare-plugin-host is not checked out, open build, plugin.service.dispatch is refused" >&2
        return 0
    fi
    echo "flare-capability: building with the plugin service router (flare-capability/with-plugin-host)" >&2
    cargo build --manifest-path "$root/flare-capability/with-plugin-host/Cargo.toml" \
        --target-dir "$target_dir" "$@"
}
