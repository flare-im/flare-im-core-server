#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

host=""
user=""
password=""
password_file=""
identity_file=""
ssh_port=""
remote_dir=""
package_dir=""
profile=""
jobs=""
skip_build=0
dry_run=0
run_smoke=0
tail_lines=""
env_file="${FLARE_DEPLOY_ENV_FILE:-}"
build_mode="${FLARE_DEPLOY_BUILD_MODE:-auto}"

usage() {
    cat <<'USAGE'
Usage: flarectl.sh <command> [options]

Single-server Flare IM Core deploy/update/restart tool.

Commands:
  deploy      Build or use a release bundle, upload it, switch current, and start.
  update      Alias of deploy for upgrade workflows.
  upgrade     Alias of deploy.
  restart     Restart the remote current release.
  status      Check remote status.
  smoke       Run remote message-flow smoke test.
  logs        Print recent remote logs.
  stop        Stop remote core processes.

Connection options:
  --env-file FILE         Load deploy defaults from env file. Default: release/deploy.env when present.
  --host HOST             Server address.
  --user USER             SSH user.
  --password PASSWORD     SSH password. Prefer FLARE_DEPLOY_PASSWORD in shell history sensitive environments.
  --password-file FILE    Read SSH password from file.
  --identity-file FILE    SSH private key file. If omitted with no password, SSH agent/default keys are used.
  --port PORT             SSH port. Default: 22.
  --remote-dir DIR        Remote install root. Default: /opt/flare-im-core.

Deploy options:
  --package-dir DIR       Existing release bundle directory. If omitted, deploy builds one.
  --build-mode MODE       Bundle build mode: auto, host, or docker. Default: auto.
  --skip-build            Copy existing target artifacts into the generated bundle.
  --profile PROFILE       Cargo profile for generated bundle: release or debug. Default: release.
  --jobs N                Cargo build jobs. Default: CARGO_BUILD_JOBS or 1.
  --smoke                 Run smoke test after remote start.

Other:
  --tail-lines N          Lines per log file for logs command. Default: 120.
  --dry-run               Print the operation plan only.
  -h, --help              Show this help.

Examples:
  ./release/flarectl.sh deploy --smoke
  ./release/flarectl.sh deploy --env-file ./release/deploy.env --smoke
  FLARE_DEPLOY_PASSWORD='secret' ./release/flarectl.sh restart --host 203.0.113.10 --user root
USAGE
}

die() {
    printf '[flarectl][error] %s\n' "$*" >&2
    exit 1
}

log() {
    printf '[flarectl] %s\n' "$*"
}

quote() {
    printf '%q' "$1"
}

join_shell_words() {
    local word
    local joined=""
    for word in "$@"; do
        joined="$joined $(quote "$word")"
    done
    printf '%s' "${joined# }"
}

load_env_file() {
    local file="$1"
    [ -f "$file" ] || die "env file not found: $file"
    # shellcheck disable=SC1090
    set -a
    . "$file"
    set +a
}

is_truthy() {
    case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes|y|on) return 0 ;;
        *) return 1 ;;
    esac
}

resolve_build_mode() {
    case "$build_mode" in
        auto)
            if [ "$(uname -s)" = "Linux" ]; then
                printf '%s\n' "host"
            else
                printf '%s\n' "docker"
            fi
            ;;
        host|docker)
            printf '%s\n' "$build_mode"
            ;;
        *)
            die "--build-mode must be auto, host, or docker"
            ;;
    esac
}

action="${1:-}"
if [ -z "$action" ]; then
    usage
    exit 1
fi

case "$action" in
    -h|--help)
        usage
        exit 0
        ;;
    deploy|update|upgrade|restart|status|smoke|logs|stop)
        shift
        ;;
    *)
        die "unknown command: $action"
        ;;
esac

scan_args=("$@")
scan_index=0
while [ "$scan_index" -lt "${#scan_args[@]}" ]; do
    case "${scan_args[$scan_index]}" in
        --env-file)
            next_index=$((scan_index + 1))
            [ "$next_index" -lt "${#scan_args[@]}" ] || die "--env-file requires a value"
            env_file="${scan_args[$next_index]}"
            scan_index=$((scan_index + 2))
            ;;
        *)
            scan_index=$((scan_index + 1))
            ;;
    esac
done

if [ -z "$env_file" ] && [ -f "$SCRIPT_DIR/deploy.env" ]; then
    env_file="$SCRIPT_DIR/deploy.env"
fi

if [ -n "$env_file" ]; then
    load_env_file "$env_file"
fi

host="${FLARE_DEPLOY_HOST:-}"
user="${FLARE_DEPLOY_USER:-}"
password="${FLARE_DEPLOY_PASSWORD:-}"
password_file="${FLARE_DEPLOY_PASSWORD_FILE:-}"
identity_file="${FLARE_DEPLOY_IDENTITY_FILE:-}"
ssh_port="${FLARE_DEPLOY_SSH_PORT:-22}"
remote_dir="${FLARE_DEPLOY_REMOTE_DIR:-/opt/flare-im-core}"
package_dir="${FLARE_DEPLOY_PACKAGE_DIR:-}"
build_mode="${FLARE_DEPLOY_BUILD_MODE:-$build_mode}"
profile="${FLARE_DEPLOY_PROFILE:-release}"
jobs="${FLARE_DEPLOY_JOBS:-${CARGO_BUILD_JOBS:-1}}"
tail_lines="${FLARE_DEPLOY_TAIL_LINES:-120}"
if is_truthy "${FLARE_DEPLOY_SKIP_BUILD:-}"; then
    skip_build=1
fi
if is_truthy "${FLARE_DEPLOY_SMOKE:-}"; then
    run_smoke=1
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --env-file)
            [ "$#" -ge 2 ] || die "--env-file requires a value"
            env_file="$2"
            shift 2
            ;;
        --host)
            [ "$#" -ge 2 ] || die "--host requires a value"
            host="$2"
            shift 2
            ;;
        --user)
            [ "$#" -ge 2 ] || die "--user requires a value"
            user="$2"
            shift 2
            ;;
        --password)
            [ "$#" -ge 2 ] || die "--password requires a value"
            password="$2"
            shift 2
            ;;
        --password-file)
            [ "$#" -ge 2 ] || die "--password-file requires a value"
            password_file="$2"
            shift 2
            ;;
        --identity-file)
            [ "$#" -ge 2 ] || die "--identity-file requires a value"
            identity_file="$2"
            shift 2
            ;;
        --port)
            [ "$#" -ge 2 ] || die "--port requires a value"
            ssh_port="$2"
            shift 2
            ;;
        --remote-dir)
            [ "$#" -ge 2 ] || die "--remote-dir requires a value"
            remote_dir="$2"
            shift 2
            ;;
        --package-dir)
            [ "$#" -ge 2 ] || die "--package-dir requires a value"
            package_dir="$2"
            shift 2
            ;;
        --build-mode)
            [ "$#" -ge 2 ] || die "--build-mode requires a value"
            build_mode="$2"
            shift 2
            ;;
        --skip-build)
            skip_build=1
            shift
            ;;
        --profile)
            [ "$#" -ge 2 ] || die "--profile requires a value"
            profile="$2"
            shift 2
            ;;
        --jobs)
            [ "$#" -ge 2 ] || die "--jobs requires a value"
            jobs="$2"
            shift 2
            ;;
        --smoke)
            run_smoke=1
            shift
            ;;
        --tail-lines)
            [ "$#" -ge 2 ] || die "--tail-lines requires a value"
            tail_lines="$2"
            shift 2
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

case "$action" in
    update|upgrade) action="deploy" ;;
esac

[ -n "$host" ] || die "--host is required"
[ -n "$user" ] || die "--user is required"

if [ -n "$password_file" ]; then
    [ -f "$password_file" ] || die "password file not found: $password_file"
    password="$(tr -d '\r\n' < "$password_file")"
fi

case "$profile" in
    release|debug) ;;
    *) die "--profile must be release or debug" ;;
esac

case "$build_mode" in
    auto|host|docker) ;;
    *) die "--build-mode must be auto, host, or docker" ;;
esac

case "$jobs" in
    ''|*[!0-9]*) die "--jobs must be a positive integer" ;;
    0) die "--jobs must be greater than 0" ;;
esac

case "$ssh_port" in
    ''|*[!0-9]*) die "--port must be a positive integer" ;;
    0) die "--port must be greater than 0" ;;
esac

case "$tail_lines" in
    ''|*[!0-9]*) die "--tail-lines must be a positive integer" ;;
    0) die "--tail-lines must be greater than 0" ;;
esac

if [ -n "$password" ] && [ -n "$identity_file" ]; then
    die "use either --password or --identity-file, not both"
fi

auth="ssh-agent"
if [ -n "$password" ]; then
    auth="password"
elif [ -n "$identity_file" ]; then
    auth="identity-file"
fi

build_label="auto"
[ "$skip_build" -eq 1 ] && build_label="skip"
[ -n "$package_dir" ] && build_label="provided"

if [ "$dry_run" -eq 1 ]; then
    effective_build_mode="$(resolve_build_mode)"
    cat <<EOF
remote operation plan
action=$action
host=$host
user=$user
port=$ssh_port
auth=$auth
remote_dir=$remote_dir
package_dir=${package_dir:-auto}
profile=$profile
jobs=$jobs
build=$build_label
build_mode=$build_mode
effective_build_mode=$effective_build_mode
smoke=$run_smoke
EOF
    exit 0
fi

ssh_opts=(-p "$ssh_port" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=20 -o ServerAliveCountMax=3)
scp_opts=(-P "$ssh_port" -o StrictHostKeyChecking=accept-new)
if [ -n "$identity_file" ]; then
    [ -f "$identity_file" ] || die "identity file not found: $identity_file"
    ssh_opts+=(-i "$identity_file")
    scp_opts+=(-i "$identity_file")
fi

if [ -n "$password" ]; then
    command -v sshpass >/dev/null 2>&1 || die "password auth requires sshpass; install sshpass or use --identity-file"
    export SSHPASS="$password"
    ssh_cmd=(sshpass -e ssh "${ssh_opts[@]}")
    scp_cmd=(sshpass -e scp "${scp_opts[@]}")
else
    ssh_cmd=(ssh "${ssh_opts[@]}")
    scp_cmd=(scp "${scp_opts[@]}")
fi

remote="$user@$host"

remote_exec() {
    "${ssh_cmd[@]}" "$remote" "$@"
}

remote_shell() {
    local script="$1"
    "${ssh_cmd[@]}" "$remote" "bash -s" <<EOF
set -euo pipefail
$script
EOF
}

upload_archive() {
    local archive="$1"
    local remote_tmp="$2"

    if command -v rsync >/dev/null 2>&1 && remote_shell "command -v rsync >/dev/null 2>&1"; then
        local rsync_ssh
        rsync_ssh="$(join_shell_words ssh "${ssh_opts[@]}")"
        log "uploading release archive with rsync"
        if [ -n "$password" ]; then
            sshpass -e rsync -P --inplace -e "$rsync_ssh" "$archive" "$remote:$remote_tmp"
        else
            rsync -P --inplace -e "$rsync_ssh" "$archive" "$remote:$remote_tmp"
        fi
        return 0
    fi

    log "uploading release archive with scp"
    "${scp_cmd[@]}" "$archive" "$remote:$remote_tmp"
}

ensure_package_dir() {
    if [ -n "$package_dir" ]; then
        [ -d "$package_dir" ] || die "package dir not found: $package_dir"
        return 0
    fi

    package_dir="$SCRIPT_DIR/dist/flare-im-core-cloud-4c4g-$(date +%Y%m%d%H%M%S)"
    build_args=(--package-dir "$package_dir" --profile "$profile" --jobs "$jobs")
    if [ "$skip_build" -eq 1 ]; then
        build_args+=(--skip-build)
    fi
    local effective_build_mode
    effective_build_mode="$(resolve_build_mode)"
    log "building release bundle ($effective_build_mode): $package_dir"
    case "$effective_build_mode" in
        docker)
            "$SCRIPT_DIR/scripts/build_linux_bundle_docker.sh" "${build_args[@]}"
            ;;
        host)
            "$SCRIPT_DIR/scripts/build_release_bundle.sh" "${build_args[@]}"
            ;;
    esac
}

preflight_remote() {
    local require_smoke="$1"
    local smoke_check=""
    if [ "$require_smoke" -eq 1 ]; then
        smoke_check='command -v grpcurl >/dev/null 2>&1 || { echo "missing grpcurl for smoke test" >&2; exit 1; }
command -v psql >/dev/null 2>&1 || { echo "missing psql for smoke test" >&2; exit 1; }'
    fi

    log "checking remote prerequisites"
    remote_shell "
command -v bash >/dev/null 2>&1 || { echo \"missing bash\" >&2; exit 1; }
command -v tar >/dev/null 2>&1 || { echo \"missing tar\" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo \"missing docker\" >&2; exit 1; }
if ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
    echo \"missing docker compose\" >&2
    exit 1
fi
$smoke_check
"
}

deploy_release() {
    preflight_remote "$run_smoke"
    ensure_package_dir

    local release_id archive remote_tmp
    release_id="release-$(date +%Y%m%d%H%M%S)"
    archive="$(mktemp "${TMPDIR:-/tmp}/flare-im-core-release.XXXXXX.tgz")"
    remote_tmp="$remote_dir/tmp/${release_id}.tgz"
    trap 'rm -f "$archive"' EXIT

    log "packing $package_dir"
    tar -C "$package_dir" -czf "$archive" .

    log "preparing remote directory $remote_dir"
    remote_shell "mkdir -p $(quote "$remote_dir")/releases $(quote "$remote_dir")/shared $(quote "$remote_dir")/tmp"

    upload_archive "$archive" "$remote_tmp"

    local q_remote_dir q_release_id q_release_path q_remote_tmp q_start_args
    q_remote_dir="$(quote "$remote_dir")"
    q_release_id="$(quote "$release_id")"
    q_release_path="$(quote "$remote_dir/releases/$release_id")"
    q_remote_tmp="$(quote "$remote_tmp")"
    q_start_args=""
    if [ "$run_smoke" -eq 1 ]; then
        q_start_args=" --smoke"
    fi

    log "activating release $release_id"
    remote_shell "
remote_dir=$q_remote_dir
release_id=$q_release_id
release_path=$q_release_path
remote_tmp=$q_remote_tmp
mkdir -p \"\$release_path\"
tar -xzf \"\$remote_tmp\" -C \"\$release_path\"
rm -f \"\$remote_tmp\"
mkdir -p \"\$remote_dir/shared/data\" \"\$remote_dir/shared/logs\" \"\$remote_dir/shared/run\"
rm -rf \"\$release_path/data\" \"\$release_path/logs\" \"\$release_path/run\"
ln -s \"\$remote_dir/shared/data\" \"\$release_path/data\"
ln -s \"\$remote_dir/shared/logs\" \"\$release_path/logs\"
ln -s \"\$remote_dir/shared/run\" \"\$release_path/run\"
if [ -f \"\$remote_dir/shared/.env\" ]; then
    ln -sfn \"\$remote_dir/shared/.env\" \"\$release_path/.env\"
fi
chmod +x \"\$release_path\"/scripts/*.sh \"\$release_path\"/scripts/lib/common.sh
if [ -x \"\$remote_dir/current/scripts/stop.sh\" ]; then
    \"\$remote_dir/current/scripts/stop.sh\" --core-only --quiet || true
fi
ln -sfn \"\$release_path\" \"\$remote_dir/current\"
cd \"\$remote_dir/current\"
./scripts/start.sh$q_start_args
"
}

run_remote_current() {
    local command="$1"
    local q_remote_dir
    q_remote_dir="$(quote "$remote_dir")"
    remote_shell "
remote_dir=$q_remote_dir
[ -d \"\$remote_dir/current\" ] || { echo \"missing remote current release: \$remote_dir/current\" >&2; exit 1; }
cd \"\$remote_dir/current\"
$command
"
}

case "$action" in
    deploy)
        deploy_release
        ;;
    restart)
        restart_args=""
        if [ "$run_smoke" -eq 1 ]; then
            restart_args=" --smoke"
        fi
        run_remote_current "./scripts/stop.sh --core-only && ./scripts/start.sh$restart_args"
        ;;
    status)
        run_remote_current "./scripts/status.sh"
        ;;
    smoke)
        run_remote_current "./scripts/smoke.sh"
        ;;
    logs)
        q_lines="$(quote "$tail_lines")"
        q_remote_dir="$(quote "$remote_dir")"
        remote_shell "
remote_dir=$q_remote_dir
tail_lines=$q_lines
if ls \"\$remote_dir/shared/logs\"/flare-*.log >/dev/null 2>&1; then
    tail -n \"\$tail_lines\" \"\$remote_dir/shared/logs\"/flare-*.log
else
    echo \"no flare logs found under \$remote_dir/shared/logs\" >&2
    exit 1
fi
"
        ;;
    stop)
        run_remote_current "./scripts/stop.sh --core-only"
        ;;
esac
