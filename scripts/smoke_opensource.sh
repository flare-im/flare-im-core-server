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

# 每项的超时上限（秒）。可用 FLARE_SMOKE_TIMEOUT 覆盖。
#
# 没有它的时候，任何一项连不上都会**永久挂住**：客户端在等 ACK，而 CI 只会在
# 90 分钟的作业超时后被杀掉——既烧满额度，又不告诉你是哪一项卡的。
# 实测正常情况下单项都在几十秒内结束，300 秒是留足余量的上限。
CASE_TIMEOUT="${FLARE_SMOKE_TIMEOUT:-300}"

# macOS 默认没有 timeout(1)（coreutils 里叫 gtimeout）。都没有时退化为不限时，
# 并明确告知——静默地不设超时才是最坏的情况。
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD=(timeout "$CASE_TIMEOUT")
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD=(gtimeout "$CASE_TIMEOUT")
else
  TIMEOUT_CMD=()
  echo -e "${YELLOW}   注意：未找到 timeout/gtimeout，单项不设超时上限（brew install coreutils 可启用）${NC}"
fi

# 超时的退出码不统一：GNU coreutils 的 timeout 返回 124，busybox（Alpine 等）
# 返回 143（128+SIGTERM）。两个都要认，否则在某些镜像里超时会被当成普通失败，
# 报出「见日志」而日志里什么都没有。（这是在容器里实测出来的。）
is_timeout_rc() { [ "$1" -eq 124 ] || [ "$1" -eq 143 ]; }
run_case() {
  local ex="$1"; shift
  # `${arr[@]}` 在 set -u 下展开空数组会报 unbound variable（bash 3.2，即 macOS 自带版本）。
  # 用 ${arr[@]+"${arr[@]}"} 让空数组展开成「什么都没有」而不是报错。
  ${TIMEOUT_CMD[@]+"${TIMEOUT_CMD[@]}"} cargo run -q --example "$ex" "$@" \
    >"/tmp/flare-smoke-$ex.log" 2>&1
}

pass=0; fail=0
echo -e "${YELLOW}Open-source smoke ($(( ${#CASES[@]} + 1 )) cases)${NC}"
cd "$SDK_ROOT" || exit 1

for entry in "${CASES[@]}"; do
  ex="${entry%%:*}"; desc="${entry#*:}"
  if run_case "$ex" --features lifecycle-sqlite; then
    pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $desc"
  else
    rc=$?
    fail=$((fail+1))
    if is_timeout_rc "$rc"; then
      echo -e "  ${RED}✗${NC} $desc  — 超过 ${CASE_TIMEOUT}s 未结束（多半是连不上服务端，客户端在死等）"
    elif grep -q "flare-strom-sfu" "/tmp/flare-smoke-$ex.log" 2>/dev/null; then
      # RTC 走的是能力插件，不在核心服务里。插件没起时报出来的是一句
      # 「discover media-control service ... timeout」，看不出该去做什么。
      echo -e "  ${RED}✗${NC} $desc  — SFU 能力插件没在跑"
      echo -e "      RTC 由插件提供，start_server.sh 只在 ../flare-plugin/flare-strom-sfu"
      echo -e "      存在时才起它。核心链路是否正常看其余几项。"
    else
      echo -e "  ${RED}✗${NC} $desc  — see /tmp/flare-smoke-$ex.log"
    fi
    tail -3 "/tmp/flare-smoke-$ex.log" | sed 's/^/      /'
  fi
done

ex="${E2EE_CASE%%:*}"; desc="${E2EE_CASE#*:}"
if run_case "$ex" --features "lifecycle-sqlite e2ee"; then
  pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $desc"
else
  rc=$?
  fail=$((fail+1))
  if is_timeout_rc "$rc"; then
    echo -e "  ${RED}✗${NC} $desc  — 超过 ${CASE_TIMEOUT}s 未结束（多半是连不上服务端，客户端在死等）"
  else
    echo -e "  ${RED}✗${NC} $desc  — see /tmp/flare-smoke-$ex.log"
  fi
  tail -3 "/tmp/flare-smoke-$ex.log" | sed 's/^/      /'
fi

echo
if [ "$fail" -eq 0 ]; then
  echo -e "${GREEN}✅ Open-source stack is self-sufficient: $pass/$(( ${#CASES[@]} + 1 )) passed (no commercial components involved)${NC}"
else
  echo -e "${RED}❌ $fail failed of $(( ${#CASES[@]} + 1 )) ${NC}"
fi
exit "$fail"
