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
docker_image="${FLARE_RELEASE_DOCKER_IMAGE:-flare-im-core-release-builder:rust-1.94.1}"
docker_platform="${FLARE_RELEASE_DOCKER_PLATFORM:-linux/amd64}"
dockerfile="${FLARE_RELEASE_DOCKERFILE:-$RELEASE_ROOT/Dockerfile.bundle}"
cargo_registry_mirror="${FLARE_DOCKER_CARGO_REGISTRY_MIRROR:-}"

usage() {
    cat <<'USAGE'
Usage: build_linux_bundle_docker.sh [options]

Build the Flare IM Core release bundle inside a Linux Docker container.

Options:
  --package-dir DIR       Output bundle directory on the host.
  --profile PROFILE       Cargo profile: release or debug. Default: release.
  --jobs N                Cargo build jobs. Default: CARGO_BUILD_JOBS or 1.
  --skip-build            Copy existing artifacts from the Docker target cache.
  --docker-image IMAGE    Builder image tag. Default: FLARE_RELEASE_DOCKER_IMAGE or flare-im-core-release-builder:rust-1.94.1.
  --platform PLATFORM     Docker platform. Default: FLARE_RELEASE_DOCKER_PLATFORM or linux/amd64.
  --dry-run               Print the Docker bundle plan without writing files.
  --force                 Replace an existing package directory.
  -h, --help              Show this help.

Environment:
  FLARE_DOCKER_CARGO_REGISTRY_MIRROR=sparse+https://mirrors.ustc.edu.cn/crates.io-index/
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
        --docker-image)
            [ "$#" -ge 2 ] || flare_die "--docker-image requires a value"
            docker_image="$2"
            shift 2
            ;;
        --platform)
            [ "$#" -ge 2 ] || flare_die "--platform requires a value"
            docker_platform="$2"
            shift 2
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

[ -f "$dockerfile" ] || flare_die "missing Dockerfile: $dockerfile"

package_parent="$(dirname "$package_dir")"
package_base="$(basename "$package_dir")"
mkdir -p "$package_parent"
package_parent="$(cd "$package_parent" && pwd)"
package_dir="$package_parent/$package_base"

build_label="run"
[ "$skip_build" -eq 1 ] && build_label="skip"

if [ "$dry_run" -eq 1 ]; then
    cat <<EOF
docker release bundle plan
workspace_root=$WORKSPACE_ROOT
project_root=$PROJECT_ROOT
dockerfile=$dockerfile
docker_image=$docker_image
platform=$docker_platform
profile=$profile
jobs=$jobs
build=$build_label
package_dir=$package_dir
cargo_registry_mirror=${cargo_registry_mirror:-crates.io}
EOF
    exit 0
fi

flare_require_command docker

container_package_dir="/out/$package_base"
container_args=(
    --package-dir "$container_package_dir"
    --profile "$profile"
    --jobs "$jobs"
)

if [ "$skip_build" -eq 1 ]; then
    container_args+=(--skip-build)
fi
if [ "$force" -eq 1 ]; then
    container_args+=(--force)
fi

registry_volume="${FLARE_RELEASE_DOCKER_REGISTRY_VOLUME:-flare-im-core-cargo-registry}"
git_volume="${FLARE_RELEASE_DOCKER_GIT_VOLUME:-flare-im-core-cargo-git}"
target_volume="${FLARE_RELEASE_DOCKER_TARGET_VOLUME:-flare-im-core-target-${docker_platform//[^A-Za-z0-9_.-]/-}}"

flare_log "building Docker release builder image: $docker_image ($docker_platform)"
docker build \
    --platform "$docker_platform" \
    -f "$dockerfile" \
    -t "$docker_image" \
    "$RELEASE_ROOT"

flare_log "building Linux release bundle in Docker: $package_dir"
docker run --rm \
    --platform "$docker_platform" \
    -e CARGO_BUILD_JOBS="$jobs" \
    -e CARGO_REGISTRY_MIRROR="$cargo_registry_mirror" \
    -e FLARE_RELEASE_BUILD_TARGET="docker:$docker_platform" \
    -e RUSTC_WRAPPER=/tmp/flare-rustc-wrapper \
    -e CARGO_BUILD_RUSTC_WRAPPER=/tmp/flare-rustc-wrapper \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -e PACKAGE_DIR="$container_package_dir" \
    -v "$WORKSPACE_ROOT:/workspace/flare-im:ro" \
    -v "$package_parent:/out" \
    -v "$registry_volume:/usr/local/cargo/registry" \
    -v "$git_volume:/usr/local/cargo/git" \
    -v "$target_volume:/cargo-target" \
    "$docker_image" \
    bash -lc '
set -euo pipefail
if [ -n "${CARGO_REGISTRY_MIRROR:-}" ]; then
    cat > /usr/local/cargo/config.toml <<EOF
[source.crates-io]
replace-with = "flare-mirror"

[source.flare-mirror]
registry = "${CARGO_REGISTRY_MIRROR}"

[net]
git-fetch-with-cli = true
EOF
fi
export CARGO_TARGET_DIR=/cargo-target
export PROTOC=/usr/bin/protoc
export PATH=/usr/local/cargo/bin:$PATH
cat > /tmp/flare-rustc-wrapper <<EOF
#!/usr/bin/env bash
exec "\$@"
EOF
chmod +x /tmp/flare-rustc-wrapper
export RUSTC_WRAPPER=/tmp/flare-rustc-wrapper
export CARGO_BUILD_RUSTC_WRAPPER=/tmp/flare-rustc-wrapper
/workspace/flare-im/flare-im-core/release/scripts/build_release_bundle.sh "$@"
if [ "$(id -u)" = "0" ]; then
    chown -R "${HOST_UID}:${HOST_GID}" "${PACKAGE_DIR}"
fi
' bash "${container_args[@]}"

flare_log "Docker release bundle created: $package_dir"
