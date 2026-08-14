#!/usr/bin/env bash
# 能力插件生命周期冒烟：注册 → 授权 → 调用 → 注销。
#
# 为什么需要它：插件「装上就能用」这句话此前只有人工验证过一次。
# 路由簿、权限校验、Dispatch 转发这三处任意一处坏掉，都不会有任何信号——
# 单测覆盖不到跨进程的这条链路，而它正是能力插件对外的全部承诺。
#
# 用法：
#   ./scripts/start_server.sh                    # 或至少起 postgres + consul + capability
#   ./scripts/smoke_plugin_lifecycle.sh
#
# 只需要 capability 服务（默认 :50110），不需要完整 IM 栈。
#
# 退出码 0 = 全部通过。
set -uo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$(cd "$ROOT/.." && pwd)"
CAPABILITY_ADDR="${CAPABILITY_ADDR:-127.0.0.1:50110}"
PLUGIN_ADDR="${PLUGIN_ADDR:-127.0.0.1:7803}"
TENANT="${PLUGIN_TENANT:-0}"
USER_ID="${PLUGIN_USER:-smoke-alice}"
CAPABILITY_ID="link.preview.v1"
PLUGIN_ID="flare-link-preview"
STEP_TIMEOUT="${PLUGIN_SMOKE_TIMEOUT:-60}"

PROTO_DIRS=(-import-path "$WORKSPACE/flare-grpc-proto/proto" -import-path "$WORKSPACE/flare-proto/proto")

pass=0; fail=0
plugin_pid=""

note_pass() { pass=$((pass+1)); echo -e "  ${GREEN}✓${NC} $1"; }
note_fail() { fail=$((fail+1)); echo -e "  ${RED}✗${NC} $1"; [ -n "${2:-}" ] && echo "      $2"; }

cleanup() {
  [ -n "$plugin_pid" ] && kill "$plugin_pid" 2>/dev/null
  wait "$plugin_pid" 2>/dev/null
}
trap cleanup EXIT

# ---- 前置检查：缺什么就明说缺什么，不静默跳过 ----------------------------

missing=""
command -v grpcurl >/dev/null 2>&1 || missing="$missing grpcurl"
[ -d "$WORKSPACE/flare-grpc-proto/proto" ] || missing="$missing flare-grpc-proto"
[ -d "$WORKSPACE/flare-proto/proto" ] || missing="$missing flare-proto"

if [ -n "$missing" ]; then
  echo -e "${YELLOW}SKIP: 缺少$missing${NC}"
  echo "      grpcurl: brew install grpcurl"
  echo "      同级仓：与本仓同级克隆 flare-grpc-proto / flare-proto"
  exit 0
fi

if ! nc -z "${CAPABILITY_ADDR%%:*}" "${CAPABILITY_ADDR##*:}" 2>/dev/null; then
  echo -e "${YELLOW}SKIP: capability 服务未在 $CAPABILITY_ADDR 监听${NC}"
  echo "      先起它：./scripts/start_server.sh"
  echo "      （只跑本脚本的话，postgres + consul + flare-capability 即可）"
  exit 0
fi

call() {
  local method="$1" data="$2"
  grpcurl -plaintext "${PROTO_DIRS[@]}" -proto capability_service.proto \
    -d "$data" "$CAPABILITY_ADDR" "flare.capability.v1.CapabilityService/$method" 2>&1
}

# 授权是**持久化**的：上一次跑完留下的授权会让「未授权应被拒」这一步反转。
# 冒烟必须自带干净起点，否则第二次跑的结果与第一次不同——那样的门禁不可信。
call RevokeUserCapability \
  "{\"tenant_id\":\"$TENANT\",\"user_id\":\"$USER_ID\",\"capability_id\":\"$CAPABILITY_ID\"}" >/dev/null 2>&1

echo -e "${YELLOW}能力插件生命周期冒烟（5 步）${NC}"

# ---- 1. 起插件，它应当自注册 --------------------------------------------

# 优先用已编译好的二进制：CI 与本地都可能刚跑完构建，再走一次 cargo run
# 会为了同一个产物重新解析依赖，慢且占盘。找不到才回退到 cargo run。
PLUGIN_BIN="$WORKSPACE/target/debug/examples/capability_link_preview"
if [ -x "$PLUGIN_BIN" ]; then
  LINK_PREVIEW_ADDR="$PLUGIN_ADDR" \
  CAPABILITY_CORE_ADDR="http://$CAPABILITY_ADDR" \
  LINK_PREVIEW_TENANT="$TENANT" \
    "$PLUGIN_BIN" >/tmp/flare-plugin-smoke.log 2>&1 &
else
  echo -e "${YELLOW}   注意：未找到预编译的插件二进制，回退到 cargo run${NC}"
  echo "         首次编译可能超过 ${STEP_TIMEOUT}s，届时会误报「插件未注册」。"
  echo "         预先构建：cargo build --manifest-path examples/Cargo.toml --example capability_link_preview"
  LINK_PREVIEW_ADDR="$PLUGIN_ADDR" \
  CAPABILITY_CORE_ADDR="http://$CAPABILITY_ADDR" \
  LINK_PREVIEW_TENANT="$TENANT" \
    cargo run -q --manifest-path "$ROOT/examples/Cargo.toml" \
    --example capability_link_preview >/tmp/flare-plugin-smoke.log 2>&1 &
fi
plugin_pid=$!

# 等它注册完成。轮询而不是固定 sleep：慢机器上固定等待要么不够要么浪费。
registered=0
for _ in $(seq 1 "$STEP_TIMEOUT"); do
  if call ListRegisteredPlugins "{\"tenant_id\":\"$TENANT\"}" | grep -q "$PLUGIN_ID"; then
    registered=1; break
  fi
  sleep 1
done

if [ "$registered" -eq 1 ]; then
  note_pass "插件启动后自注册到核心"
else
  # 日志为空是有意义的信号：说明插件进程压根没起来（多半是回退到 cargo run
  # 且还在编译），而不是起来了但注册失败。两种情况的排查方向完全不同。
  plugin_log="$(tail -3 /tmp/flare-plugin-smoke.log 2>/dev/null)"
  if [ -z "$plugin_log" ]; then
    plugin_log="插件日志为空——进程可能还没起来。若走的是 cargo run 回退路径，多半是仍在编译。"
  fi
  note_fail "插件未能在 ${STEP_TIMEOUT}s 内注册" "$plugin_log"
  echo -e "\n${RED}❌ 后续步骤依赖注册成功，中止${NC}"
  exit 1
fi

# ---- 2. 未授权时应被拒 ---------------------------------------------------
#
# 这一步是**反向断言**：它证明权限校验真的在起作用。
# 少了它，就算权限判断被误删，这个冒烟也照样全绿。

# request_id 每次都要不同：核心按它做幂等，复用会让第二次请求命中第一次的
# 在途/已决结果——实测复用同一个 id 时，授权后那次 Dispatch 会一直等到超时。
dispatch() {
  call Dispatch "{\"capability_id\":\"$CAPABILITY_ID\",\"tenant_id\":\"$TENANT\",\"user_id\":\"$USER_ID\",\"payload_json\":\"{\\\"url\\\":\\\"https://example.com\\\"}\",\"request_id\":\"smoke-$1-$$\"}"
}

denied="$(dispatch denied)"
if echo "$denied" | grep -q "PermissionDenied"; then
  note_pass "未授权时被拒（权限校验生效）"
else
  note_fail "未授权却调通了——权限校验可能失效" "$(echo "$denied" | head -2)"
fi

# ---- 3. 授权 -------------------------------------------------------------

granted="$(call GrantUserCapability "{\"tenant_id\":\"$TENANT\",\"user_id\":\"$USER_ID\",\"capability_id\":\"$CAPABILITY_ID\"}")"
if echo "$granted" | grep -q "granted"; then
  note_pass "能力已授予用户"
else
  note_fail "授权失败" "$(echo "$granted" | head -2)"
fi

# ---- 4. 经核心 Dispatch，结果应来自插件 -----------------------------------

result="$(dispatch granted)"
if echo "$result" | grep -q '"success": true' && echo "$result" | grep -q "$PLUGIN_ID"; then
  note_pass "经核心 Dispatch 调通插件并拿到结果"
else
  note_fail "Dispatch 未拿到预期结果" "$(echo "$result" | head -4)"
fi

# ---- 5. 插件退出应自动注销 -----------------------------------------------

kill -INT "$plugin_pid" 2>/dev/null
for _ in $(seq 1 15); do
  kill -0 "$plugin_pid" 2>/dev/null || break
  sleep 1
done
plugin_pid=""

if call ListRegisteredPlugins "{\"tenant_id\":\"$TENANT\"}" | grep -q "$PLUGIN_ID"; then
  note_fail "插件退出后仍留在路由簿" "核心会继续把请求发到一个死地址"
else
  note_pass "插件退出后已从路由簿摘除"
fi

echo ""
if [ "$fail" -eq 0 ]; then
  echo -e "${GREEN}✅ 能力插件生命周期完整：$pass/5 通过${NC}"
else
  echo -e "${RED}❌ $fail 项失败（共 5 项）${NC}"
  echo "   插件日志：/tmp/flare-plugin-smoke.log"
fi
exit "$fail"
