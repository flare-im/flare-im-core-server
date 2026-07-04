#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
STOP_INFRA=0
QUIET=0

usage() {
    cat <<'USAGE'
Usage: stop.sh [options]

Stop Flare IM Core release bundle processes.

Options:
  --core-only   Stop only Flare core processes. This is the default.
  --infra       Also stop Docker infrastructure.
  --quiet       Reduce output.
  -h, --help    Show this help.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --core-only)
            STOP_INFRA=0
            shift
            ;;
        --infra)
            STOP_INFRA=1
            shift
            ;;
        --quiet)
            QUIET=1
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

log_stop() {
    [ "$QUIET" -eq 1 ] || flare_log "$*"
}

stop_one() {
    local service="$1"
    local bin="$2"
    local pid_file="$APP_HOME/run/flare-$service.pid"

    if [ -f "$pid_file" ]; then
        local pid
        pid="$(cat "$pid_file" 2>/dev/null || true)"
        if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
            log_stop "stopping $service pid=$pid"
            kill "$pid" 2>/dev/null || true
            sleep 1
            if ps -p "$pid" >/dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi

    pkill -f "$APP_HOME/bin/$bin" >/dev/null 2>&1 || true
}

while IFS=: read -r service bin _port; do
    stop_one "$service" "$bin"
done < <(flare_release_service_specs)

if [ "$STOP_INFRA" -eq 1 ]; then
    flare_require_command docker
    compose_cmd="$(flare_compose_cmd)" || flare_die "missing docker compose"
    log_stop "stopping Docker infrastructure"
    (cd "$APP_HOME" && $compose_cmd -f docker-compose.infra.yml down)
fi

log_stop "stop complete"
