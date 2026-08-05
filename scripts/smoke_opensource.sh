#!/usr/bin/env bash
# 开源栈冒烟：证明**不依赖任何商业组件**，通信侧端到端可用。
#
#   ./scripts/start_server.sh          # 先起开源栈
#   ./scripts/smoke_opensource.sh      # 再跑这个
#
# 退出码 0 = 全部通过。
#
# 为什么需要它：此前想确认「开源部分能不能用」，只能开两个终端手动敲字聊天。
# 对评估者是门槛，对 CI 等于没有覆盖 —— 通信链路是这个项目对外的第一承诺，
# 却没有一条命令能证明它还活着。
#
# 覆盖的是**跨进程的真实链路**（客户端 SDK → 网关验签 → 信令 → 存储 → 同步），
# 不是单元测试：
#   - 登录与连接协商（自签 token，即「自带身份」模式）
#   - send + local persist
#   - 事件总线：连接/同步/消息事件的观察
#   - unread regression
#   - RTC 房间加入（media-control 链路）
#   - 端到端加密：服务端只见密文，仅持钥方可还原

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

# 用例住在客户端 SDK 仓（flare-im-core-sdk）。默认按同级目录找，可用
# FLARE_SDK_ROOT 覆盖 —— CI 里两个仓不一定并排放。
#
# 这里不用 `cd ... && pwd`：目录不存在时 cd 失败，SDK_ROOT 会悄悄变成当前目录，
# 然后 cargo 报一句风马牛不相及的错。宁可当场说清楚缺的是什么。
SDK_ROOT="${FLARE_SDK_ROOT:-$CORE_ROOT/../flare-im-core-sdk}"
if [ ! -f "$SDK_ROOT/Cargo.toml" ]; then
  echo -e "${RED}✗ client SDK repo not found: $SDK_ROOT${NC}" >&2
  echo "  The cases live in flare-im-core-sdk. Clone it next to this repo," >&2
  echo "  or point FLARE_SDK_ROOT=/path/to/flare-im-core-sdk at it." >&2
  exit 1
fi
SDK_ROOT="$(cd "$SDK_ROOT" && pwd)"

if [ ! -s "$CORE_ROOT/logs/.dev-token-secret" ]; then
  echo -e "${RED}✗ logs/.dev-token-secret not found — run ./scripts/start_server.sh${NC}" >&2
  exit 1
fi

CASES=(
  "e2e_message_ops:send + local persist"
  "e2e_event_observer:event bus (connect/sync)"
  "e2e_full_event_observer:full event surface + RTC room join"
  "e2e_full_event_ops:full operation surface"
  "e2e_unread_regression:unread regression"
)

# E2EE 演示要额外的 e2ee feature，单列
E2EE_CASE="e2ee_demo:end-to-end encryption (server sees ciphertext only)"

pass=0; fail=0
echo -e "${YELLOW}Open-source smoke ($(( ${#CASES[@]} + 1 )) cases)${NC}"
cd "$SDK_ROOT" || exit 1

for entry in "${CASES[@]}"; do
  ex="${entry%%:*}"; desc="${entry#*:}"
  if cargo run -q --example "$ex" --features lifecycle-sqlite >/tmp/flare-smoke-$ex.log 2>&1; then
    pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $desc"
  else
    fail=$((fail+1)); echo -e "  ${RED}✗${NC} $desc  — see /tmp/flare-smoke-$ex.log"
    tail -3 "/tmp/flare-smoke-$ex.log" | sed 's/^/      /'
  fi
done

ex="${E2EE_CASE%%:*}"; desc="${E2EE_CASE#*:}"
if cargo run -q --example "$ex" --features "lifecycle-sqlite e2ee" >/tmp/flare-smoke-$ex.log 2>&1; then
  pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $desc"
else
  fail=$((fail+1)); echo -e "  ${RED}✗${NC} $desc  — see /tmp/flare-smoke-$ex.log"
  tail -3 "/tmp/flare-smoke-$ex.log" | sed 's/^/      /'
fi

echo
if [ "$fail" -eq 0 ]; then
  echo -e "${GREEN}✅ Open-source stack is self-sufficient: $pass/$(( ${#CASES[@]} + 1 )) passed (no commercial components involved)${NC}"
else
  echo -e "${RED}❌ $fail failed of $(( ${#CASES[@]} + 1 )) ${NC}"
fi
exit "$fail"
