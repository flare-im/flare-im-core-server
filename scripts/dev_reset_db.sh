#!/usr/bin/env bash
# Deliberately destructive, development-only. Never called by migration/installation.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${FLARE_ALLOW_DEV_RESET:-}" != 1 ]]; then
  echo 'FAIL: dev-reset deletes Core data; set FLARE_ALLOW_DEV_RESET=1 only for a disposable development database' >&2
  exit 1
fi
if [[ -n "${DATABASE_URL:-}" ]]; then
  export PGDATABASE="$DATABASE_URL"
fi
if [[ -z "${PGDATABASE:-}" ]]; then
  echo 'FAIL: explicit DATABASE_URL or PGDATABASE is required' >&2
  exit 1
fi
exec "${PSQL:-psql}" -X -v ON_ERROR_STOP=1 -f "$repo_root/deploy/init.sql"
