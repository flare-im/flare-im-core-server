#!/usr/bin/env bash
# Environment assignments are consumed by bash's builtin export, never placed in
# the long-lived wrapper's argv. Do not enable xtrace when handling credentials.
start_detached_process() {
    local label="$1" log_file="$2" binary="$3"
    shift 3
    (
        export "$@"
        cd "$PROJECT_ROOT" || exit 1
        ulimit -n 60000 2>/dev/null || true
        exec nohup "$LOG_PYTHON" "$ROTATING_SERVICE" --log "$log_file" -- "$binary"
    ) </dev/null >/dev/null 2>&1 &
    DETACHED_PID=$!
}
