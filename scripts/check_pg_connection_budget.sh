#!/usr/bin/env bash
# PostgreSQL 连接预算门禁。
#
# base.toml 里每个连接池都有自己的 max_connections，各服务进程各开一份；
# compose 里 postgres 的 max_connections 是全局硬上限。两个数字分别维护、
# 谁也不知道对方，一旦对不上，故障形态是**发消息静默失败**：
# seq 已经分配、日志里 outcome="ok"，只有 postgres 侧一句
# "sorry, too many clients already"。线上真出过。
#
# 这里只做一个下界检查：所有声明的池上限之和 + 业务栈预留，必须放得进 compose 的上限。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_CONFIG="$ROOT/config/base.toml"
COMPOSE="$ROOT/release/docker-compose.infra.yml"

# flare-social 七个服务各自的池，不在本仓库声明，按实测取整预留。
BUSINESS_STACK_RESERVE=${BUSINESS_STACK_RESERVE:-60}

fail() { echo "✗ $*" >&2; exit 1; }

[ -f "$BASE_CONFIG" ] || fail "找不到 $BASE_CONFIG"
[ -f "$COMPOSE" ] || fail "找不到 $COMPOSE"

pool_total=$(awk '
  /^\[postgres\./ { in_pg = 1; next }
  /^\[/           { in_pg = 0 }
  in_pg && /^max_connections[[:space:]]*=/ {
    gsub(/[^0-9]/, "", $0); sum += $0
  }
  END { print sum + 0 }
' "$BASE_CONFIG")

[ "$pool_total" -gt 0 ] || fail "没能从 $BASE_CONFIG 解析出任何 [postgres.*] 池上限"

# max_connections=${POSTGRES_MAX_CONNECTIONS:-300} → 取默认值 300
compose_limit=$(sed -n 's/.*max_connections=\${POSTGRES_MAX_CONNECTIONS:-\([0-9]*\)}.*/\1/p' "$COMPOSE" | head -1)
[ -n "$compose_limit" ] && [ "$compose_limit" -gt 0 ] \
  || fail "没能从 $COMPOSE 解析出 postgres max_connections 默认值"

need=$((pool_total + BUSINESS_STACK_RESERVE))

echo "  连接池声明合计: $pool_total"
echo "  业务栈预留:     $BUSINESS_STACK_RESERVE"
echo "  需要:           $need"
echo "  compose 上限:   $compose_limit"

if [ "$need" -gt "$compose_limit" ]; then
  fail "连接预算超了：需要 $need，postgres 只给 $compose_limit。
     要么调小 config/base.toml 里的池上限，要么调大 release/docker-compose.infra.yml
     的 POSTGRES_MAX_CONNECTIONS 默认值（同时记得跟着调 mem_limit）。"
fi

echo "✓ 连接预算够用（余量 $((compose_limit - need))）"
