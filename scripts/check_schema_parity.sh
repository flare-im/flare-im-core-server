#!/usr/bin/env bash
# Read-only: compare the target to migration output, not destructive init.sql.
# No implicit target/container, unrelated Social SQL, temporary DB deletion or SKIP.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$repo_root/tools/flare-migrate/flare_migrate.py" verify "$@"
