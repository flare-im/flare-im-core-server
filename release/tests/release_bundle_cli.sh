#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

fail() {
    echo "release_bundle_cli: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    if ! grep -Fq -- "$needle" <<<"$haystack"; then
        fail "expected output to contain: $needle"
    fi
}

assert_executable() {
    local path="$1"
    if [ ! -x "$path" ]; then
        fail "missing executable script: $path"
    fi
}

assert_executable "$RELEASE_ROOT/scripts/build_release_bundle.sh"
assert_executable "$RELEASE_ROOT/scripts/start.sh"
assert_executable "$RELEASE_ROOT/scripts/stop.sh"
assert_executable "$RELEASE_ROOT/scripts/status.sh"
assert_executable "$RELEASE_ROOT/scripts/smoke.sh"
assert_executable "$RELEASE_ROOT/flarectl.sh"

help_output="$("$RELEASE_ROOT/scripts/build_release_bundle.sh" --help)"
assert_contains "$help_output" "Usage:"
assert_contains "$help_output" "--package-dir"
assert_contains "$help_output" "--skip-build"
assert_contains "$help_output" "--dry-run"

dry_run_output="$("$RELEASE_ROOT/scripts/build_release_bundle.sh" --dry-run --skip-build --package-dir /tmp/flare-im-core-release-test)"
assert_contains "$dry_run_output" "release bundle plan"
assert_contains "$dry_run_output" "build=skip"
assert_contains "$dry_run_output" "config_layout=cloud-4c4g"
assert_contains "$dry_run_output" "package_dir=/tmp/flare-im-core-release-test"

for script in start stop status smoke; do
    output="$("$RELEASE_ROOT/scripts/${script}.sh" --help)"
    assert_contains "$output" "Usage:"
done

flarectl_help="$("$RELEASE_ROOT/flarectl.sh" --help)"
assert_contains "$flarectl_help" "deploy"
assert_contains "$flarectl_help" "update"
assert_contains "$flarectl_help" "restart"
assert_contains "$flarectl_help" "--env-file"
assert_contains "$flarectl_help" "--host"
assert_contains "$flarectl_help" "--password"

remote_plan="$("$RELEASE_ROOT/flarectl.sh" deploy --dry-run --host 192.0.2.10 --user root --password secret --package-dir /tmp/flare-bundle)"
assert_contains "$remote_plan" "remote operation plan"
assert_contains "$remote_plan" "action=deploy"
assert_contains "$remote_plan" "host=192.0.2.10"
assert_contains "$remote_plan" "user=root"
assert_contains "$remote_plan" "auth=password"
assert_contains "$remote_plan" "remote_dir=/opt/flare-im-core"
assert_contains "$remote_plan" "package_dir=/tmp/flare-bundle"

env_file="$(mktemp "${TMPDIR:-/tmp}/flarectl-env.XXXXXX")"
cat > "$env_file" <<'ENV'
FLARE_DEPLOY_HOST=198.51.100.20
FLARE_DEPLOY_USER=deploy
FLARE_DEPLOY_PASSWORD=env-secret
FLARE_DEPLOY_REMOTE_DIR=/srv/flare-im-core
FLARE_DEPLOY_PACKAGE_DIR=/tmp/env-package
FLARE_DEPLOY_PROFILE=debug
FLARE_DEPLOY_JOBS=2
FLARE_DEPLOY_SMOKE=1
ENV

env_plan="$("$RELEASE_ROOT/flarectl.sh" deploy --dry-run --env-file "$env_file")"
assert_contains "$env_plan" "host=198.51.100.20"
assert_contains "$env_plan" "user=deploy"
assert_contains "$env_plan" "auth=password"
assert_contains "$env_plan" "remote_dir=/srv/flare-im-core"
assert_contains "$env_plan" "package_dir=/tmp/env-package"
assert_contains "$env_plan" "profile=debug"
assert_contains "$env_plan" "jobs=2"
assert_contains "$env_plan" "smoke=1"

error_file="$(mktemp "${TMPDIR:-/tmp}/release-bundle-cli.XXXXXX")"
trap 'rm -f "$error_file" "$env_file"' EXIT

if "$RELEASE_ROOT/scripts/build_release_bundle.sh" --not-a-real-option >"$error_file" 2>&1; then
    fail "unknown option should fail"
fi
assert_contains "$(cat "$error_file")" "unknown option"

echo "release_bundle_cli: pass"
