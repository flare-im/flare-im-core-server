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

fail=0
check() {
  local file="$1" pattern="$2" actual="$3" label="$4"
  grep -q "$pattern" "$file" 2>/dev/null && return 0
  echo -e "  ${RED}✗${NC} $(basename "$file"): $label 与实际不符（实际 $actual）"
  grep -oE "[0-9]+ 个${label}|[0-9]+ ${label}" "$file" 2>/dev/null | head -1 | sed 's/^/      README 现写：/'
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
