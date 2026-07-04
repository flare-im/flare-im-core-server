#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"
CHECK_INFRA=1
CHECK_CORE=1

usage() {
    cat <<'USAGE'
Usage: status.sh [options]

Check Flare IM Core release bundle status.

Options:
  --core-only    Check only core processes.
  --infra-only   Check only required Docker infrastructure ports.
  -h, --help     Show this help.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --core-only)
            CHECK_INFRA=0
            CHECK_CORE=1
            shift
            ;;
        --infra-only)
            CHECK_INFRA=1
            CHECK_CORE=0
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

failed=0

check_endpoint() {
    local label="$1"
    local port="$2"
    if flare_check_port "$port"; then
        printf 'ok   %-24s port=%s\n' "$label" "$port"
    else
        printf 'fail %-24s port=%s\n' "$label" "$port"
        failed=1
    fi
}

if [ "$CHECK_INFRA" -eq 1 ]; then
    check_endpoint consul 28500
    check_endpoint redis 26379
    check_endpoint postgres 25432
    check_endpoint nats 24222
    check_endpoint rustfs 29000
fi

if [ "$CHECK_CORE" -eq 1 ]; then
    while IFS=: read -r service bin port; do
        pid_file="$APP_HOME/run/flare-$service.pid"
        if [ -f "$pid_file" ] && ps -p "$(cat "$pid_file")" >/dev/null 2>&1; then
            if [ -n "$port" ]; then
                if flare_check_port "$port"; then
                    printf 'ok   %-24s pid=%s port=%s\n' "$service" "$(cat "$pid_file")" "$port"
                else
                    printf 'fail %-24s pid=%s port=%s-not-listening\n' "$service" "$(cat "$pid_file")" "$port"
                    failed=1
                fi
            else
                printf 'ok   %-24s pid=%s\n' "$service" "$(cat "$pid_file")"
            fi
        else
            printf 'fail %-24s missing-process bin=%s\n' "$service" "$bin"
            failed=1
        fi
    done < <(flare_release_service_specs)
fi

exit "$failed"
