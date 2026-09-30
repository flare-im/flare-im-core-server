#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"

APP_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<'USAGE'
Usage: smoke.sh [options]

Run the Flare IM message-flow smoke test. It only uses the public Edge
(Social login + Core HTTP API), so the deployment needs Edge and Social in
front of this Core; a Core-only bundle cannot pass it.

Options:
  -h, --help   Show this help.

Required environment (see scripts/smoke_message_flow.sh --help for all):
  SMOKE_BASE_URL       Public Edge origin, e.g. https://im.example.test
  SMOKE_TENANT_ID      Isolated fixture tenant e2e-<20 lowercase hex>
  SMOKE_PG_CONTAINER   PostgreSQL container for the read-only check
                       (or SMOKE_SKIP_DB_CHECK=1)
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

"$runner"
