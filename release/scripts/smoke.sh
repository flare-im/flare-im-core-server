#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<'USAGE'
Usage: smoke.sh [options]

Run the Flare IM Core message-flow smoke test against a started release bundle.

Options:
  -h, --help   Show this help.

Environment overrides:
  SMOKE_MESSAGE_INGEST_ENDPOINT  Default: 127.0.0.1:50182
  SMOKE_STORAGE_READER_ENDPOINT  Default: 127.0.0.1:60083
  SMOKE_POSTGRES_URL             Default: postgres://flare:flare123@localhost:25432/flare2
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        *)
            flare_die "unknown option: $1"
            ;;
    esac
done

runner="$APP_HOME/scripts/smoke_message_flow.sh"
if [ ! -x "$runner" ] && [ -f "$runner" ]; then
    chmod +x "$runner"
fi

if [ ! -x "$runner" ]; then
    source_runner="$APP_HOME/../scripts/smoke_message_flow.sh"
    [ -x "$source_runner" ] || flare_die "missing smoke runner: $runner"
    runner="$source_runner"
fi

if [ -d "$APP_HOME/proto/flare-grpc-proto/proto" ] && [ -d "$APP_HOME/proto/flare-proto/proto" ]; then
    export SMOKE_PROTO_ROOT="${SMOKE_PROTO_ROOT:-$APP_HOME/proto}"
fi

export SMOKE_POSTGRES_URL="${SMOKE_POSTGRES_URL:-postgres://flare:flare123@localhost:25432/flare2}"
"$runner"
