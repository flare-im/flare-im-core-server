#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---dry-run}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ "${MODE}" != "--dry-run" && "${MODE}" != "--execute" ]]; then
  echo "usage: $0 [--dry-run|--execute]" >&2
  exit 2
fi

required_files=(
  "deploy/docker-compose.yml"
  "deploy/prometheus.yml"
  "deploy/alert_rules.yml"
  "scripts/start_server_core.sh"
  "scripts/smoke_message_flow.sh"
  "tools/flare-dlq-replay/Cargo.toml"
)

for file in "${required_files[@]}"; do
  if [[ ! -f "${ROOT_DIR}/${file}" ]]; then
    echo "missing chaos prerequisite: ${file}" >&2
    exit 1
  fi
done

drills=(
  "broker-outage:pause nats/jetstream producer path and verify WAL/retry recovery"
  "storage-outage:pause postgres and verify writer retry/DLQ plus no false persisted ACK"
  "redis-seq-outage:pause redis-backed seq/idempotency dependency and verify explicit failure/no duplicate seq"
  "gateway-reconnect-storm:restart signaling-gateway while SDK clients reconnect"
  "duplicate-send-retry:replay fixed clientMsgId and verify ingest/storage idempotency"
  "plugin-unavailable:disable SFU/plugin capability and verify typed unavailable events"
)

echo "Flare IM release chaos drill plan (${MODE})"
for drill in "${drills[@]}"; do
  echo "- ${drill}"
done

if [[ "${MODE}" == "--dry-run" ]]; then
  echo "dry-run OK: prerequisites and drill catalog are valid"
  exit 0
fi

cat >&2 <<'EOF'
--execute is intentionally a guarded hook.
Run the concrete fault injection commands from docs/06-testing-performance-and-operations.md
in a disposable staging environment, then attach the run artifacts to the release.
EOF
exit 3
