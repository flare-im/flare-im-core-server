#!/usr/bin/env bash
# 校验文档里的 `cargo run --example X` 指向真实存在的 example，
# 且当它属于同级仓时，文档在附近说清楚了这一点。
#
# 这类错误已经出现两次，都在新人最先读的文档里：
#   - QUICKSTART 的 `mint_token` 在 flare-server-core
#   - README / INTEGRATION 的 `e2ee_demo` 在 flare-im-core-sdk
# 两者都被写成本仓可直接执行的样子，照做只会得到「找不到该 example」——
# 而这恰好是评估者遇到的头几条命令，第一印象就成了「文档不可信」。
#
# 判据刻意不要求字面的 `cd`：说明所在仓可以是 `cd ../flare-xxx`，也可以是
# 「（在同级仓 flare-xxx 里跑）」这样的散文；读者已经在那个目录时更不该硬加 cd。
# 所以只要求**前文一定范围内出现过所属仓名**。
#
# 用法：./scripts/check_doc_commands.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$(cd "$ROOT/.." && pwd)"
CONTEXT_LINES=12

# example 名 -> 所在仓
EXAMPLES="$(
  find "$WORKSPACE" -maxdepth 3 -type d -name examples \
       -not -path "*/target/*" -not -path "*/node_modules/*" 2>/dev/null |
  while read -r dir; do
    repo="$(basename "$(dirname "$dir")")"
    find "$dir" -maxdepth 1 -name "*.rs" 2>/dev/null |
      while read -r f; do echo "$(basename "$f" .rs) $repo"; done
  done
)"

problems=""

for doc in "$ROOT"/*.md; do
  [ -f "$doc" ] || continue
  while IFS=: read -r lineno _rest; do
    name="$(sed -n "${lineno}p" "$doc" |
            grep -o -- "--example[[:space:]]\+[A-Za-z0-9_]\+" | awk '{print $2}')"
    [ -n "$name" ] || continue
    [ -f "$ROOT/examples/$name.rs" ] && continue

    owner="$(echo "$EXAMPLES" | awk -v n="$name" '$1==n {print $2; exit}')"
    if [ -z "$owner" ]; then
      # 分清两种情况：真的没有这个 example，还是它所在的同级仓没被检出。
      # 后者在 CI 里很常见，报成前者会把人引向完全错误的方向（我自己被误导过一次）。
      siblings="$(find "$WORKSPACE" -maxdepth 1 -mindepth 1 -type d -name 'flare-*' \
                    -not -path "$ROOT" 2>/dev/null | wc -l | tr -d ' ')"
      problems+="  ✗ $(basename "$doc"):$lineno  --example $name —— 在已检出的仓里找不到"$'\n'
      problems+="      当前工作区有 $siblings 个同级仓。若它属于一个未检出的仓，"$'\n'
      problems+="      请先克隆该仓再跑本门禁（CI 里就是在工作流里补一条 clone）。"$'\n'
      continue
    fi

    start=$(( lineno > CONTEXT_LINES ? lineno - CONTEXT_LINES : 1 ))
    if ! sed -n "${start},${lineno}p" "$doc" | grep -q "$owner"; then
      problems+="  ✗ $(basename "$doc"):$lineno  --example $name 在 $owner，但前文没提到它"$'\n'
      problems+="      读者会在本仓执行，得到「找不到该 example」"$'\n'
    fi
  done < <(grep -n -- "--example[[:space:]]\+[A-Za-z0-9_]\+" "$doc")
done

if [ -n "$problems" ]; then
  echo "文档里的命令指向不存在（或未说明所在仓）的 example："
  printf '%s' "$problems"
  echo ""
  echo "要么把 example 补进本仓，要么在命令前文点明它在哪个同级仓。"
  exit 1
fi

echo "  ✓ 文档里的 --example 引用都能对上"
