#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$RELEASE_ROOT/.." && pwd)"
WORKSPACE_ROOT="$(cd "$PROJECT_ROOT/.." && pwd)"

# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

profile="release"
skip_build=0
dry_run=0
force=0
jobs="${CARGO_BUILD_JOBS:-1}"
package_dir="$RELEASE_ROOT/dist/flare-im-core-cloud-4c4g-$(date +%Y%m%d%H%M%S)"
build_target="${FLARE_RELEASE_BUILD_TARGET:-host}"

usage() {
    cat <<'USAGE'
Usage: build_release_bundle.sh [options]

Build a 4-core/4GB friendly Flare IM Core release bundle.

Options:
  --package-dir DIR   Output bundle directory.
  --profile PROFILE   Cargo profile: release or debug. Default: release.
  --jobs N            Cargo build jobs. Default: CARGO_BUILD_JOBS or 1.
  --skip-build        Do not run cargo build; copy existing target artifacts.
  --dry-run           Print the release bundle plan without writing files.
  --force             Replace an existing package directory.
  -h, --help          Show this help.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --package-dir)
            [ "$#" -ge 2 ] || flare_die "--package-dir requires a value"
            package_dir="$2"
            shift 2
            ;;
        --profile)
            [ "$#" -ge 2 ] || flare_die "--profile requires a value"
            profile="$2"
            shift 2
            ;;
        --jobs)
            [ "$#" -ge 2 ] || flare_die "--jobs requires a value"
            jobs="$2"
            shift 2
            ;;
        --skip-build)
            skip_build=1
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        --force)
            force=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            flare_die "unknown option: $1"
            ;;
    esac
done

case "$profile" in
    release|debug) ;;
    *) flare_die "--profile must be release or debug" ;;
esac

case "$jobs" in
    ''|*[!0-9]*) flare_die "--jobs must be a positive integer" ;;
    0) flare_die "--jobs must be greater than 0" ;;
esac

build_label="run"
[ "$skip_build" -eq 1 ] && build_label="skip"

if [ "$dry_run" -eq 1 ]; then
    cat <<EOF
release bundle plan
project_root=$PROJECT_ROOT
workspace_root=$WORKSPACE_ROOT
profile=$profile
jobs=$jobs
build=$build_label
build_target=$build_target
config_layout=cloud-4c4g
package_dir=$package_dir
EOF
    exit 0
fi

[ -f "$PROJECT_ROOT/Cargo.toml" ] || flare_die "missing Cargo.toml: $PROJECT_ROOT/Cargo.toml"

if [ "$skip_build" -ne 1 ]; then
    if [ -z "${PROTOC:-}" ]; then
        if command -v protoc >/dev/null 2>&1; then
            export PROTOC
            PROTOC="$(command -v protoc)"
        elif [ -x /opt/homebrew/bin/protoc ]; then
            export PROTOC=/opt/homebrew/bin/protoc
        elif [ -x /usr/local/bin/protoc ]; then
            export PROTOC=/usr/local/bin/protoc
        else
            flare_die "missing protoc; install protobuf or set PROTOC"
        fi
    fi

    cargo_args=(
        build
        --manifest-path "$PROJECT_ROOT/Cargo.toml"
        --jobs "$jobs"
        -p flare-signaling-online
        -p flare-signaling-route
        -p flare-capability
        -p flare-conversation
        -p flare-message-ingest
        -p flare-orchestrator
        -p flare-storage-writer
        -p flare-storage-reader
        -p flare-sync-orchestrator
        -p flare-push-server
        -p flare-push-worker
        -p flare-media
        -p flare-api-gateway
        -p flare-signaling-gateway
        --bins
    )
    if [ "$profile" = "release" ]; then
        cargo_args+=(--release)
    fi

    flare_log "building release binaries with cargo --jobs $jobs"
    PROTOC="$PROTOC" cargo "${cargo_args[@]}"
fi

metadata="$(cargo metadata --manifest-path "$PROJECT_ROOT/Cargo.toml" --format-version=1 --no-deps)"
if command -v jq >/dev/null 2>&1; then
    target_dir="$(printf '%s' "$metadata" | jq -r '.target_directory')"
else
    target_dir="$(printf '%s' "$metadata" | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
fi
[ -n "$target_dir" ] && [ "$target_dir" != "null" ] || flare_die "could not resolve cargo target directory"
target_profile_dir="$target_dir/$profile"

if [ -e "$package_dir" ]; then
    [ "$force" -eq 1 ] || flare_die "package dir already exists; pass --force: $package_dir"
    case "$package_dir" in
        /|/tmp|/var|/Users|"$PROJECT_ROOT"|"$WORKSPACE_ROOT"|"$RELEASE_ROOT")
            flare_die "refusing to remove unsafe package dir: $package_dir"
            ;;
    esac
    rm -rf "$package_dir"
fi

mkdir -p \
    "$package_dir/bin" \
    "$package_dir/config" \
    "$package_dir/logs" \
    "$package_dir/run" \
    "$package_dir/data" \
    "$package_dir/scripts/lib" \
    "$package_dir/sql" \
    "$package_dir/proto/flare-grpc-proto" \
    "$package_dir/proto/flare-proto"

copy_release_config() {
    local config_dir="$1"
    mkdir -p "$config_dir/services" "$config_dir/environments" "$config_dir/shared" "$config_dir/overrides"

    cp "$PROJECT_ROOT/config/base.toml" "$config_dir/base.toml"
    cp "$PROJECT_ROOT/config/hooks.core.toml" "$config_dir/hooks.core.toml"
    ln -sfn hooks.core.toml "$config_dir/hooks.toml"

    cat > "$config_dir/services/cloud-4c4g.toml" <<'HEADER'
# Generated by release/scripts/build_release_bundle.sh.
# Keep the server bundle operationally simple: all runtime service fragments live here.
HEADER

    service_files=(
        access-gateway
        api-gateway
        capability
        conversation
        media
        message-ingest
        message-orchestrator
        push-server
        push-worker
        signaling-online
        signaling-route
        storage-reader
        storage-writer
        sync-orchestrator
    )

    for service_name in "${service_files[@]}"; do
        source_file="$PROJECT_ROOT/config/services/$service_name.toml"
        [ -f "$source_file" ] || flare_die "missing service config: $source_file"
        {
            printf '\n# ---- %s ----\n' "$service_name"
            sed '/^#/d; /^[[:space:]]*$/d' "$source_file"
        } >> "$config_dir/services/cloud-4c4g.toml"
    done

    if compgen -G "$PROJECT_ROOT/config/environments/*.toml" >/dev/null; then
        cp "$PROJECT_ROOT"/config/environments/*.toml "$config_dir/environments/"
    fi

    cat > "$config_dir/README.md" <<'README'
# Release Runtime Config

This directory is generated for the 4C4G single-server release bundle.

- `base.toml`: shared infrastructure profiles.
- `services/cloud-4c4g.toml`: all runtime service fragments in one file.
- `hooks.toml -> hooks.core.toml`: business-neutral hook profile.
- `environments/*.toml`: narrow MQ/object-store environment overlays.

Prefer editing the release env file and `shared/.env` over changing these TOML files on the server.
README
}

cp "$RELEASE_ROOT/README.md" "$package_dir/README.md"
cp "$RELEASE_ROOT/.env.example" "$package_dir/.env.example"
cp "$RELEASE_ROOT/docker-compose.infra.yml" "$package_dir/docker-compose.infra.yml"
cp -R "$RELEASE_ROOT/nats" "$package_dir/nats"

cp "$SCRIPT_DIR/start.sh" "$package_dir/scripts/start.sh"
cp "$SCRIPT_DIR/stop.sh" "$package_dir/scripts/stop.sh"
cp "$SCRIPT_DIR/status.sh" "$package_dir/scripts/status.sh"
cp "$SCRIPT_DIR/smoke.sh" "$package_dir/scripts/smoke.sh"
cp "$SCRIPT_DIR/lib/common.sh" "$package_dir/scripts/lib/common.sh"
cp "$PROJECT_ROOT/scripts/smoke_message_flow.sh" "$package_dir/scripts/smoke_message_flow.sh"

copy_release_config "$package_dir/config"
cp "$PROJECT_ROOT/deploy/init.sql" "$package_dir/sql/init.sql"
cp -R "$WORKSPACE_ROOT/flare-grpc-proto/proto" "$package_dir/proto/flare-grpc-proto/proto"
cp -R "$WORKSPACE_ROOT/flare-proto/proto" "$package_dir/proto/flare-proto/proto"

while IFS= read -r bin; do
    src="$target_profile_dir/$bin"
    [ -x "$src" ] || flare_die "missing binary: $src"
    cp "$src" "$package_dir/bin/$bin"
done < <(flare_release_required_bins)

chmod +x "$package_dir"/scripts/*.sh "$package_dir/scripts/lib/common.sh"

cat > "$package_dir/manifest.txt" <<EOF
name=flare-im-core-cloud-4c4g
created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
profile=$profile
build_target=$build_target
mq_backend=nats
required_infra=consul,redis,postgres,nats,rustfs
optional_infra=kafka,prometheus,loki,tempo,grafana
remote_layout=current+shared
config_layout=cloud-4c4g
EOF

flare_log "release bundle created: $package_dir"
flare_log "next: copy .env.example to .env, set secrets, then run ./scripts/start.sh"
