#!/usr/bin/env bash
# Core only. Connection is explicit via DATABASE_URL or libpq PG* environment.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -eq 0 ]]; then set -- run; fi
exec python3 "$repo_root/tools/flare-migrate/flare_migrate.py" "$@"
