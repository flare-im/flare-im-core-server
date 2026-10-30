#!/usr/bin/env bash
# 能力插件生命周期冒烟：登记 → 未授权被拒 → 控制面下发授权投影 → 调用 → 撤销后被拒 →
# 租约到期后被拒 → 退出即注销。
#
# 为什么需要它：插件「装上就能用」这句话此前只有人工验证过一次。登记、授权投影、调用票据、
# Dispatch 转发任意一处坏掉，都不会有任何信号——单测覆盖不到跨进程的这条链路，而它正是
# 能力插件对外的全部承诺。
#
# 核心的远程插件调用只认控制面下发、平台签名、带租约的插件授权投影（TenantProjection.
# UpsertPlugins）；插件登记要平台签发、绑定请求体的注册凭据；每次调用核心都签一张调用票据，
# 插件验过才执行。开源栈没有控制面，本脚本用 examples/capability_dev_control 在本机扮演它：
# --prepare 现生成一把平台密钥与一把调用票据密钥，Core 启动时只信任这把平台公钥
# （FLARE_PLATFORM_PUBLIC_KEY），用这把密钥签调用票据（FLARE_PLUGIN_CALL_KEY_ID /
# FLARE_PLUGIN_CALL_SIGNING_KEY）。这些都是 Core 已有的配置项：没有开发开关，也不跳过任何校验。
#
# 用法：
#   ./scripts/smoke_plugin_lifecycle.sh --prepare DIR        # 在空目录 DIR 里现生成夹具密钥
#   set -a; . DIR/core.env; set +a; ./scripts/start_server.sh
#   PLUGIN_SMOKE_DIR=DIR ./scripts/smoke_plugin_lifecycle.sh
#
# 只需要带库运行的 capability 服务（默认 :50110）。调用方身份用 start_server.sh 的 Cell 私钥
# 签（FLARE_INTERNAL_CTX_KEY_DIR，缺省 logs/.dev-internal-context），与网关、插件宿主签的相同。
# 每次运行用一个新租户，不受上一次留下的登记与投影影响。
#
# 退出码 0 = 全部通过；缺前提即失败，不跳过。
set -uo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CAPABILITY_ADDR="${CAPABILITY_ADDR:-127.0.0.1:50110}"
PLUGIN_ADDR="${PLUGIN_ADDR:-127.0.0.1:7803}"
BROKER_ADDR="${PLUGIN_BROKER_ADDR:-127.0.0.1:7811}"
USER_ID="${PLUGIN_USER:-smoke-alice}"
STEP_TIMEOUT="${PLUGIN_SMOKE_TIMEOUT:-60}"
CAPABILITY_ID="link.preview.v1"
PLUGIN_ID="flare-link-preview"
DEPLOYMENT_ID="link-preview-smoke"
CALL_KEY_ID="plugin-smoke"
MANIFEST="$ROOT/examples/capability_link_preview.manifest.json"
LOG_DIR="$ROOT/logs/plugin-smoke"
TOTAL=7

# ---- --prepare：启动 Core 之前现生成夹具密钥 --------------------------------

if [ "${1:-}" = "--prepare" ]; then
  dir="${2:-}"
  if [ -z "$dir" ]; then
    echo "用法：$0 --prepare DIR" >&2
    exit 2
  fi
  command -v openssl >/dev/null 2>&1 || { echo "需要 openssl" >&2; exit 2; }
  mkdir -p "$dir" && chmod 700 "$dir" || exit 2
  # 已有密钥说明某个正在运行的 Core 可能信任着它们：换掉会让那边对不上，宁可报错。
  if [ -e "$dir/platform.pem" ] || [ -e "$dir/call.pem" ] || [ -e "$dir/core.env" ]; then
    echo "$dir 里已有夹具密钥，换一个空目录" >&2
    exit 2
  fi
  (
    umask 077
    openssl genpkey -algorithm Ed25519 -out "$dir/platform.pem" 2>/dev/null &&
      openssl genpkey -algorithm Ed25519 -out "$dir/call.pem" 2>/dev/null &&
      openssl rand -hex 32 > "$dir/workload.token"
  ) || { echo "生成夹具密钥失败" >&2; exit 2; }
  # Ed25519 的 PKCS#8 DER 固定 48 字节（16 字节前缀 + 32 字节种子），下面按这个取种子。
  if [ "$(openssl pkey -in "$dir/call.pem" -outform DER 2>/dev/null | wc -c | tr -d ' ')" != 48 ]; then
    echo "openssl 输出的不是 48 字节的 Ed25519 PKCS#8 私钥" >&2
    exit 2
  fi
  platform_public="$(openssl pkey -in "$dir/platform.pem" -pubout 2>/dev/null)"
  # 调用票据的签名钥是 32 字节 Ed25519 种子（base64），即 PKCS#8 DER 的最后 32 字节。
  call_seed="$(openssl pkey -in "$dir/call.pem" -outform DER 2>/dev/null | tail -c 32 | openssl base64 -A)"
  if [ -z "$platform_public" ] || [ "${#call_seed}" -ne 44 ]; then
    echo "读取夹具密钥失败" >&2
    exit 2
  fi
  (
    umask 077
    {
      printf "FLARE_PLATFORM_PUBLIC_KEY='%s'\n" "$platform_public"
      printf "FLARE_PLUGIN_CALL_KEY_ID='%s'\n" "$CALL_KEY_ID"
      printf "FLARE_PLUGIN_CALL_SIGNING_KEY='%s'\n" "$call_seed"
    } > "$dir/core.env"
  )
  echo "夹具密钥已生成：$dir（启动 Core 前 set -a; . $dir/core.env; set +a）"
  exit 0
fi

pass=0; fail=0
plugin_pid=""; broker_pid=""

note_pass() { pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $1"; }
note_fail() { fail=$((fail+1)); echo -e "  ${RED}✗${NC} $1"; [ -n "${2:-}" ] && echo "      $2"; }
abort() { echo -e "${RED}✗ $1${NC}"; [ -n "${2:-}" ] && echo "  $2"; exit 1; }

cleanup() {
  for pid in $plugin_pid $broker_pid; do
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
  done
}
trap cleanup EXIT

# ---- 前置检查：缺什么就明说缺什么，不静默跳过 ------------------------------

DIR="${PLUGIN_SMOKE_DIR:-}"
if [ -z "$DIR" ] || [ ! -f "$DIR/platform.pem" ] || [ ! -f "$DIR/call.pem" ] || [ ! -f "$DIR/workload.token" ]; then
  abort "缺少夹具目录（PLUGIN_SMOKE_DIR）" \
    "先 $0 --prepare DIR，按 DIR/core.env 启动 Core，再 PLUGIN_SMOKE_DIR=DIR $0"
fi
DIR="$(cd "$DIR" && pwd)"
CELL_KEY="${FLARE_INTERNAL_CTX_KEY_DIR:-$ROOT/logs/.dev-internal-context}/private.pem"
[ -f "$CELL_KEY" ] || abort "找不到 Cell 私钥 $CELL_KEY" "它由 ./scripts/start_server.sh 生成"
for tool in openssl nc python3 cargo; do
  command -v "$tool" >/dev/null 2>&1 || abort "缺少 $tool"
done
if ! nc -z "${CAPABILITY_ADDR%%:*}" "${CAPABILITY_ADDR##*:}" 2>/dev/null; then
  abort "capability 服务未在 $CAPABILITY_ADDR 监听" "先起 Core：./scripts/start_server.sh"
fi

# 与 start_server.sh 一样按 cargo 的实际产物目录找二进制（上层 .cargo/config.toml 可能改了它）。
target_dir="$(cd "$ROOT" && cargo metadata --format-version 1 --no-deps 2>/dev/null |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])' 2>/dev/null)"
[ -n "$target_dir" ] || abort "cargo metadata 读不出产物目录"
PLUGIN_BIN="$target_dir/debug/examples/capability_link_preview"
CONTROL_BIN="$target_dir/debug/examples/capability_dev_control"
if [ ! -x "$PLUGIN_BIN" ] || [ ! -x "$CONTROL_BIN" ]; then
  echo -e "${YELLOW}   未找到预编译的样例，先构建（首次可能要几分钟）${NC}"
  cargo build -q --manifest-path "$ROOT/examples/Cargo.toml" \
    --example capability_link_preview --example capability_dev_control ||
    abort "样例构建失败"
fi

TENANT="plugin-smoke-$(openssl rand -hex 6)"
CAPABILITY_URL="http://$CAPABILITY_ADDR"
mkdir -p "$LOG_DIR"

control() { "$CONTROL_BIN" "$@" 2>&1; }
project() {
  local key="$1"; shift
  control project --capability "$CAPABILITY_URL" --platform-key "$key" --tenant "$TENANT" \
    --manifest "$MANIFEST" --deployment-id "$DEPLOYMENT_ID" --authority "http://$PLUGIN_ADDR" "$@"
}
dispatch() {
  control dispatch --capability "$CAPABILITY_URL" --cell-key "$CELL_KEY" --tenant "$TENANT" \
    --user "$USER_ID" --operation "$CAPABILITY_ID" --payload '{"url":"https://example.com"}'
}
listed() {
  control list --capability "$CAPABILITY_URL" --cell-key "$CELL_KEY" --tenant "$TENANT" \
    --plugin-id "$PLUGIN_ID"
}
# 投影缺席、撤销与租约到期，核心都按平台错误码拒绝，命令不到插件。
refused_as_unusable() {
  echo "$1" | grep -qE '^refused code=PermissionDenied reason=PLUGIN_(NOT_INSTALLED|DISABLED) '
}
dispatched_by_plugin() {
  echo "$1" | grep -q "^dispatched success=true plugin_id=$PLUGIN_ID "
}

echo -e "${YELLOW}能力插件生命周期冒烟（$TOTAL 步，租户 $TENANT）${NC}"

# ---- 0. 注册凭据交换（平台的一部分，由夹具扮演） -------------------------------

"$CONTROL_BIN" broker --listen "$BROKER_ADDR" --platform-key "$DIR/platform.pem" \
  --workload-token-file "$DIR/workload.token" --deployment-id "$DEPLOYMENT_ID" \
  --plugin-id "$PLUGIN_ID" --tenant "$TENANT" >"$LOG_DIR/broker.log" 2>&1 &
broker_pid=$!
for _ in $(seq 1 20); do
  grep -q "^broker listening=" "$LOG_DIR/broker.log" 2>/dev/null && break
  kill -0 "$broker_pid" 2>/dev/null || break
  sleep 0.5
done
grep -q "^broker listening=" "$LOG_DIR/broker.log" 2>/dev/null ||
  abort "注册凭据交换没有起来" "$(tail -3 "$LOG_DIR/broker.log" 2>/dev/null)"

# ---- 1. 起插件，它应当带平台签的注册凭据自登记 ------------------------------------

call_public="$(openssl pkey -in "$DIR/call.pem" -pubout -outform DER 2>/dev/null | tail -c 32 | openssl base64 -A)"
LINK_PREVIEW_ADDR="$PLUGIN_ADDR" \
CAPABILITY_CORE_ADDR="$CAPABILITY_URL" \
LINK_PREVIEW_TENANT="$TENANT" \
FLARE_PLUGIN_CALL_PUBLIC_KEYS="[{\"key_id\":\"$CALL_KEY_ID\",\"public_key_base64\":\"$call_public\"}]" \
FLARE_PLUGIN_BROKER_URL="http://$BROKER_ADDR/api/v1/internal/plugins/registration-tokens" \
FLARE_PLUGIN_WORKLOAD_TOKEN_FILE="$DIR/workload.token" \
FLARE_PLUGIN_DEPLOYMENT_ID="$DEPLOYMENT_ID" \
  "$PLUGIN_BIN" >"$LOG_DIR/plugin.log" 2>&1 &
plugin_pid=$!

# 轮询而不是固定 sleep：慢机器上固定等待要么不够要么浪费。
registered=""
for _ in $(seq 1 "$STEP_TIMEOUT"); do
  registered="$(listed)"
  echo "$registered" | grep -q "^listed registered=true" && break
  kill -0 "$plugin_pid" 2>/dev/null || break
  sleep 1
done
if echo "$registered" | grep -q "^listed registered=true"; then
  note_pass "插件带平台签的注册凭据自登记到核心"
else
  note_fail "插件未能在 ${STEP_TIMEOUT}s 内登记" \
    "$(echo "$registered" | tail -1)；插件日志：$(tail -2 "$LOG_DIR/plugin.log" 2>/dev/null)"
  echo -e "\n${RED}❌ 后续步骤依赖插件在线，中止${NC}"
  exit 1
fi

# ---- 2. 没有授权投影时应被拒 -----------------------------------------------------
#
# 反向断言：登记不授予调用权。少了它，就算授权判断被误删，这个冒烟也照样全绿。

denied="$(dispatch)"
if refused_as_unusable "$denied"; then
  note_pass "控制面未下发授权投影时调用被拒（登记不授予调用权）"
else
  note_fail "没有授权投影却调通了——授权校验可能失效" "$denied"
fi

# ---- 3. 控制面下发授权投影 -----------------------------------------------------------
#
# 先用一把别的密钥签：核心只信任配置给它的那把平台公钥，这一份必须被拒。

openssl genpkey -algorithm Ed25519 -out "$LOG_DIR/stranger.pem" 2>/dev/null
forged="$(project "$LOG_DIR/stranger.pem" --version 1)"
rm -f "$LOG_DIR/stranger.pem"
granted="$(project "$DIR/platform.pem" --version 1)"
if ! echo "$forged" | grep -q "^refused code=Unauthenticated"; then
  note_fail "别的密钥签的授权投影没有被拒——核心可能没在验平台签名" "$forged"
elif echo "$granted" | grep -q "^projected accepted=true current_version=1"; then
  note_pass "平台签名、带租约的授权投影被接受（别的密钥签的被拒）"
else
  note_fail "授权投影没有被接受" "$granted"
fi

# ---- 4. 经核心 Dispatch，结果应来自插件 -----------------------------------------------
#
# 核心按投影找到插件、签一张调用票据；插件验过票据才执行，验不过核心拿不到结果。

result="$(dispatch)"
if dispatched_by_plugin "$result"; then
  note_pass "经核心 Dispatch 调通插件（插件验过调用票据）"
else
  note_fail "Dispatch 未拿到插件的结果" "$result；插件日志：$(tail -2 "$LOG_DIR/plugin.log" 2>/dev/null)"
fi

# ---- 5. 撤销后应被拒 ---------------------------------------------------------------

revoked="$(project "$DIR/platform.pem" --version 2 --revoked)"
after_revoke="$(dispatch)"
if ! echo "$revoked" | grep -q "^projected accepted=true current_version=2"; then
  note_fail "撤销投影没有被接受" "$revoked"
elif refused_as_unusable "$after_revoke"; then
  note_pass "控制面撤销授权后调用被拒"
else
  note_fail "撤销后仍能调用" "$after_revoke"
fi

# ---- 6. 租约到期后应被拒 -----------------------------------------------------------
#
# 重新授权、只给 5 秒租约：到期前调得通，到期后控制面没续约，核心不再认这份投影。

leased="$(project "$DIR/platform.pem" --version 3 --lease-secs 5)"
before_expiry="$(dispatch)"
sleep 6
after_expiry="$(dispatch)"
if ! echo "$leased" | grep -q "^projected accepted=true current_version=3"; then
  note_fail "短租约投影没有被接受" "$leased"
elif ! dispatched_by_plugin "$before_expiry"; then
  note_fail "重新授权后、租约到期前没有调通" "$before_expiry"
elif refused_as_unusable "$after_expiry"; then
  note_pass "授权租约到期后调用被拒（到期前调得通）"
else
  note_fail "租约到期后仍能调用" "$after_expiry"
fi

# ---- 7. 插件退出应自动注销 ---------------------------------------------------------

kill -INT "$plugin_pid" 2>/dev/null
for _ in $(seq 1 15); do
  kill -0 "$plugin_pid" 2>/dev/null || break
  sleep 1
done
plugin_pid=""

gone="$(listed)"
if echo "$gone" | grep -q "^listed registered=false"; then
  note_pass "插件退出后已从路由簿摘除"
else
  note_fail "插件退出后仍留在路由簿" "$gone；插件日志：$(tail -2 "$LOG_DIR/plugin.log" 2>/dev/null)"
fi

echo ""
if [ "$fail" -eq 0 ] && [ "$pass" -eq "$TOTAL" ]; then
  echo -e "${GREEN}✅ 能力插件生命周期完整：$pass/$TOTAL 通过${NC}"
  exit 0
fi
echo -e "${RED}❌ $fail 项失败（共 $TOTAL 项）${NC}"
echo "   日志：$LOG_DIR"
exit 1
