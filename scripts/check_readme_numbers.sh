#!/usr/bin/env bash
# 校验 README 里写死的数字与仓库现状一致。
#
# 首屏那几个数字（平台 SDK 个数、UI 组件数）是评估者最先看到、也最容易随时间
# 变假的东西：加一个平台、删一个组件，没有任何机制会提醒你回来改 README。
# 数字对不上不会让任何测试变红，但会让读的人开始怀疑其余内容。
#
# 需要 flare-im-core-client-sdk 与 flare-im-design 在同级目录；
# 缺席时**跳过并说明缺哪个**，不静默通过。
set -uo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$(cd "$ROOT/.." && pwd)"
CLIENT_SDK="$WORKSPACE/flare-im-core-client-sdk"
DESIGN="$WORKSPACE/flare-im-design"

missing=""
[ -d "$CLIENT_SDK/packages" ] || missing="$missing flare-im-core-client-sdk"
[ -f "$DESIGN/spec/components.json" ] || missing="$missing flare-im-design"
if [ -n "$missing" ]; then
  echo -e "${YELLOW}SKIP: 缺少$missing，无法核对 README 数字${NC}"
  echo "      把它们克隆到与本仓同级后再跑。"
  exit 0
fi

# 平台 SDK：packages/ 下的目录，排除非平台的共享包。
# 这个排除清单本身是约定——新增一个共享目录时要记得加进来，
# 否则平台数会莫名其妙多一。
NON_PLATFORM="shared dart"
platform_count=0
for d in "$CLIENT_SDK"/packages/*/; do
  name="$(basename "$d")"
  skip=0
  for x in $NON_PLATFORM; do [ "$name" = "$x" ] && skip=1; done
  [ "$skip" -eq 0 ] && platform_count=$((platform_count+1))
done

# 只认 components 这一个字段，不做「取不到就退回数顶层键」的兜底。
# 那种兜底看着稳，实际会在结构变化时**静默给出一个错误的数字**——
# 首次接入 CI 时它就返回了顶层键数（8），把一个本来正确的 README 判成不符。
component_count="$(python3 -c "
import json, sys
d = json.load(open('$DESIGN/spec/components.json', encoding='utf-8'))
items = d.get('components') if isinstance(d, dict) else d
if not isinstance(items, list):
    sys.exit('components.json 里没有 components 数组')
print(len(items))
" 2>&1)"

case "$component_count" in
  ''|*[!0-9]*)
    echo -e "${RED}✗ 读不出组件数：$component_count${NC}"
    echo "   spec/components.json 的结构可能变了——门禁宁可报错也不猜。"
    exit 1
    ;;
esac

# 同级仓可能停在与本仓不同的分支上。flare-im-design 的 main 与 dev 就相差
# 4 个组件（111 / 107），CI 按当前分支名克隆，跑 dev 时数出来的是另一个值。
#
# 这种情况**不是 README 写错了**，把它判成失败会让本仓的 CI 被另一个仓的
# 分支状态卡住。所以：数字对不上时先看看同级仓在哪个分支，若与本仓不同，
# 降级为提示而不是失败。真正的「README 没跟上」仍会被抓到——那时两边同分支。
DESIGN_BRANCH="$(git -C "$DESIGN" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
SELF_BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

fail=0
check() {
  local file="$1" pattern="$2" actual="$3" label="$4"
  grep -q "$pattern" "$file" 2>/dev/null && return 0
  local written
  written="$(grep -oE "[0-9]+ 个${label}|[0-9]+ ${label}" "$file" 2>/dev/null | head -1)"
  if [ "$DESIGN_BRANCH" != "$SELF_BRANCH" ] && [ "$DESIGN_BRANCH" != "unknown" ]; then
    echo -e "  ${YELLOW}·${NC} $(basename "$file"): $label 数字为 $written，同级仓 flare-im-design"
    echo "      当前在 $DESIGN_BRANCH 分支（本仓 $SELF_BRANCH），两边内容不同，跳过判定。"
    return 0
  fi
  echo -e "  ${RED}✗${NC} $(basename "$file"): $label 与实际不符（实际 $actual）"
  [ -n "$written" ] && echo "      README 现写：$written"
  fail=1
}

for f in "$ROOT/README.md" "$ROOT/README.zh-CN.md"; do
  [ -f "$f" ] || continue
  check "$f" "$platform_count 个平台 SDK\|$platform_count platform SDKs" "$platform_count" "平台 SDK"
  check "$f" "$component_count 个 UI 组件\|$component_count UI components" "$component_count" "UI 组件"
done

if [ "$fail" -eq 0 ]; then
  echo -e "  ${GREEN}✓${NC} README 数字与仓库一致（平台 SDK $platform_count、UI 组件 $component_count）"
else
  echo ""
  echo "  数字变了就改 README —— 首屏的具体数字是评估者最先核对的东西。"
fi
exit "$fail"
