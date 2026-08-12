#!/usr/bin/env bash
# 校验「全新部署」与「已迁移的运行库」schema 一致。
#
# 这个仓反复踩同一个坑：加列时只写了 migrations（或只在运行库上手动 ALTER），
# 忘了同步 init.sql。表现是**老库一切正常，新部署直接起不来**——而没人会在
# 日常开发里重建库，所以这类问题往往拖到别人第一次克隆部署才炸。
#
# 做法：用 init.sql 建一个临时库，与目标库逐列比对，有差异即失败。
#
# 用法：
#   ./scripts/check_schema_parity.sh                 # 与 flare2 比对
#   TARGET_DB=mydb ./scripts/check_schema_parity.sh
#
# 前置：本机能连到 dev Postgres（默认 flare-im-core/deploy 起的那个）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$(cd "$ROOT/.." && pwd)"

PG_CONTAINER="${PG_CONTAINER:-flare-postgres}"
PG_USER="${PG_USER:-flare}"
TARGET_DB="${TARGET_DB:-flare2}"
TMP_DB="${TMP_DB:-flare_schema_parity_check}"

# 容器运行时：优先 podman，回退 docker。
#
# 用**真实连一次库**来探测，而不是 `ps | grep 容器名`：后者会因为一次瞬时抖动
# （podman 偶发返回空列表）让门禁静默 SKIP——一个会悄悄跳过的门禁比没有更危险。
RUNNER=""
for candidate in podman docker; do
  command -v "$candidate" >/dev/null 2>&1 || continue
  if "$candidate" exec -i "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -c 'SELECT 1' >/dev/null 2>&1; then
    RUNNER=$candidate
    break
  fi
done
if [ -z "$RUNNER" ]; then
  echo "SKIP: 连不上 $PG_CONTAINER（先起 flare-im-core/deploy）"
  exit 0
fi

# 压掉 NOTICE：init.sql 的 DROP IF EXISTS 在空库上会刷出几十行「does not exist, skipping」，
# 把真正的失败埋掉。
psql_db() {
  "$RUNNER" exec -i -e PGOPTIONS='-c client_min_messages=warning' \
    "$PG_CONTAINER" psql -U "$PG_USER" -d "$1" "${@:2}"
}

cleanup() {
  psql_db postgres -c "DROP DATABASE IF EXISTS $TMP_DB;" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== 用 init.sql 建临时库 $TMP_DB =="
cleanup
psql_db postgres -c "CREATE DATABASE $TMP_DB;" >/dev/null
psql_db "$TMP_DB" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" >/dev/null 2>&1 || true

apply() {
  local file=$1
  [ -f "$file" ] || return 0
  if ! psql_db "$TMP_DB" -v ON_ERROR_STOP=1 -q < "$file" >/dev/null; then
    echo "FAIL: $file 在全新库上执行失败——新部署会直接建不起来"
    exit 1
  fi
  echo "  ✓ $(basename "$(dirname "$(dirname "$file")")")/$(basename "$file")"
}

apply "$ROOT/deploy/init.sql"
# flare-social 是同级仓，缺席时只校验 IM 核部分。
apply "$WORKSPACE/flare-social/deploy/db/init.sql"

COLUMNS_SQL="SELECT table_name||'.'||column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY 1;"

fresh=$(psql_db "$TMP_DB" -t -A -c "$COLUMNS_SQL" | sort)
if ! target=$(psql_db "$TARGET_DB" -t -A -c "$COLUMNS_SQL" 2>/dev/null | sort); then
  echo "SKIP: 目标库 $TARGET_DB 不可用，仅验证了 init.sql 可执行"
  exit 0
fi

missing=$(comm -23 <(echo "$target") <(echo "$fresh") || true)
extra=$(comm -13 <(echo "$target") <(echo "$fresh") || true)

if [ -z "$missing" ] && [ -z "$extra" ]; then
  echo "PASS: 全新部署与 $TARGET_DB 的 schema 一致（$(echo "$fresh" | wc -l | tr -d ' ') 列）"
  exit 0
fi

echo ""
if [ -n "$missing" ]; then
  echo "FAIL: 运行库有、init.sql 没有 —— **新部署会缺这些列**，多半是加列时漏改 init.sql："
  echo "$missing" | sed 's/^/    /'
fi
if [ -n "$extra" ]; then
  echo ""
  echo "FAIL: init.sql 有、运行库没有 —— 已部署的库缺对应 migration："
  echo "$extra" | sed 's/^/    /'
fi
exit 1
