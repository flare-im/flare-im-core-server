#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
START_INFRA=1
RUN_SMOKE=0

usage() {
    cat <<'USAGE'
Usage: start.sh [options]

Start the Flare IM Core 4-core/4GB release bundle.

Options:
  --no-infra     Do not start Docker infrastructure.
  --smoke        Run message-flow smoke test after startup.
  -h, --help     Show this help.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --no-infra|--skip-infra)
            START_INFRA=0
            shift
            ;;
        --smoke)
            RUN_SMOKE=1
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

flare_load_env_file "$APP_HOME"
flare_configure_runtime_env "$APP_HOME"
flare_ensure_token_secrets "$APP_HOME"

mkdir -p "$APP_HOME/logs" "$APP_HOME/run" "$APP_HOME/data"

if [ "$START_INFRA" -eq 1 ]; then
    flare_require_command docker
    compose_cmd="$(flare_compose_cmd)" || flare_die "missing docker compose"
    flare_log "starting required infrastructure: consul redis postgres nats rustfs"
    (cd "$APP_HOME" && $compose_cmd -f docker-compose.infra.yml up -d consul redis postgres nats rustfs)

    flare_wait_for_port 28500 consul 90
    flare_wait_for_port 26379 redis 90
    flare_wait_for_port 25432 postgres 120
    flare_wait_for_port 24222 nats 90
    flare_wait_for_port 29000 rustfs 120
fi

while IFS= read -r bin; do
    [ -x "$APP_HOME/bin/$bin" ] || flare_die "missing executable binary: $APP_HOME/bin/$bin"
done < <(flare_release_required_bins)

"$SCRIPT_DIR/stop.sh" --core-only --quiet || true

start_service() {
    local service="$1"
    local bin="$2"
    local port="$3"
    local pid_file="$APP_HOME/run/flare-$service.pid"
    local log_file="$APP_HOME/logs/flare-$service.log"

    flare_log "starting $service"
    if [ "$service" = "access-gateway" ]; then
        nohup env \
            PORT="${ACCESS_GATEWAY_PORT:-60051}" \
            GRPC_PORT="${ACCESS_GATEWAY_GRPC_PORT:-60060}" \
            "$APP_HOME/bin/$bin" > "$log_file" 2>&1 &
    else
        nohup "$APP_HOME/bin/$bin" > "$log_file" 2>&1 &
    fi
    echo "$!" > "$pid_file"
    sleep 2

    if ! ps -p "$(cat "$pid_file")" >/dev/null 2>&1; then
        tail -n 80 "$log_file" >&2 || true
        flare_die "$service failed to start"
    fi

    if [ -n "$port" ]; then
        flare_wait_for_port "$port" "$service" 90 || {
            tail -n 80 "$log_file" >&2 || true
            flare_die "$service did not open port $port"
        }
    fi
}

while IFS=: read -r service bin port; do
    start_service "$service" "$bin" "$port"
done < <(flare_release_service_specs)

"$SCRIPT_DIR/status.sh" --core-only

if [ "$RUN_SMOKE" -eq 1 ]; then
    "$SCRIPT_DIR/smoke.sh"
fi

flare_log "startup complete"
