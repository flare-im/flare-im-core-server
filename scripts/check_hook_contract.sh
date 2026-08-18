#!/usr/bin/env bash
# 校验 docs/HOOK-PLUGIN-CONTRACT.md 里列的 operation 与 gRPC 适配器实际派发的一致。
#
# 这份文档是给**外部** hook 作者看的：他们照着实现一个 operation，如果那个
# operation 其实从来不会被派发过来，插件跑起来一切正常、就是永远收不到调用 ——
# 没有任何报错，只有「怎么没反应」。
#
# 反过来也一样：适配器新增一个 operation 而文档没跟上，外部作者压根不知道
# 有这个扩展点。两个方向都要挡。
#
# 内部 HookKind 枚举比这多得多（push / presence / login 等），那些是**进程内**
# 的，远程插件够不到。判据取自适配器里真实的 hook_plugin_call 调用点，
# 不是枚举 —— 曾经有文档按枚举写，把只在进程内可达的扩展点写给了外部作者。
set -uo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOC="$ROOT/docs/HOOK-PLUGIN-CONTRACT.md"
ADAPTER="$ROOT/crates/flare-im-hooks/src/hooks/adapters/grpc.rs"

for f in "$DOC" "$ADAPTER"; do
  if [ ! -f "$f" ]; then
    echo -e "${YELLOW}SKIP${NC}: 找不到 $f"
    exit 0
  fi
done

# 适配器里真实派发的 operation
in_code="$(grep -oE '"flare\.hook\.v1\.[a-z_]+"' "$ADAPTER" | tr -d '"' | sort -u)"
# 文档表格里列出的 operation
in_doc="$(grep -oE '`flare\.hook\.v1\.[a-z_]+`' "$DOC" | tr -d '`' | sort -u)"

if [ -z "$in_code" ]; then
  echo -e "${RED}FAIL${NC}: 适配器里一个 flare.hook.v1.* 都没找到 —— 判据取法可能已失效"
  exit 1
fi

only_code="$(comm -23 <(echo "$in_code") <(echo "$in_doc"))"
only_doc="$(comm -13 <(echo "$in_code") <(echo "$in_doc"))"

fail=0
if [ -n "$only_code" ]; then
  echo -e "${RED}FAIL${NC}: 适配器会派发但文档没写 —— 外部作者不知道有这个扩展点："
  echo "$only_code" | sed 's/^/    /'
  fail=1
fi
if [ -n "$only_doc" ]; then
  echo -e "${RED}FAIL${NC}: 文档写了但适配器不会派发 —— 照做的人永远收不到调用："
  echo "$only_doc" | sed 's/^/    /'
  fail=1
fi
[ $fail -eq 1 ] && exit 1

# type_url 也要对得上：载荷是 protobuf 强类型，写错了对方解不出来
for t in $(grep -oE 'type\.googleapis\.com/flare\.capability\.v1\.[A-Za-z]+' "$ADAPTER" | sed 's|^type\.googleapis\.com/||' | sort -u); do
  if ! grep -qF "$t" "$DOC"; then
    echo -e "${RED}FAIL${NC}: 适配器用的 type_url $t 在文档里找不到"
    exit 1
  fi
done

echo -e "${GREEN}✓${NC} Hook 契约文档与 gRPC 适配器一致（$(echo "$in_code" | wc -l | tr -d ' ') 个 operation）"
