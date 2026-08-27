#!/usr/bin/env bash
# 重复索引门禁：同一张表上不能有两个列定义完全相同的索引。
#
# 为什么会出现：init.sql（全新部署）与 migrations/（已部署增量）各写了一份同一个索引，
# 但用了**不同的名字**，于是 `IF NOT EXISTS` 去重不掉。跑过迁移的库再跑 init.sql
# 就会建出两份完全相同的索引——查询用不上第二份，写入却要维护两遍。
# 线上实测撞到两处：events(tenant_id,conversation_id,seq) 与
# flare_moments_visibility_rules(tenant_id,peer_id,rule_kind)。
#
# 纯静态检查，只读 SQL 文件，不需要跑起来的数据库。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_FILES=()
while IFS= read -r -d '' f; do SQL_FILES+=("$f"); done < <(
  find "$ROOT/deploy" -name '*.sql' -not -path '*/dist/*' -print0 2>/dev/null
)
[ ${#SQL_FILES[@]} -gt 0 ] || { echo "没找到 SQL 文件，跳过"; exit 0; }

python3 - "${SQL_FILES[@]}" <<'PY'
import re, sys, collections

# (表, 归一化列清单) -> [(索引名, 文件)]
seen = collections.defaultdict(list)

# 必须把 WHERE 一起捕获：部分索引与全量索引即使列相同也是两个不同的索引，
# 各自服务不同查询。漏掉 WHERE 会把它们误判成重复——门禁误报比没有门禁更糟，
# 因为大家会直接把它关掉。（实测 message_write_ledger 上就有这么一对。）
idx_re = re.compile(
    r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?'
    r'(\w+)\s+ON\s+(?:ONLY\s+)?["\w.]*?(\w+)\s*\(([^;]*?)\)\s*((?:WHERE[^;]*)?);',
    re.I | re.S)
pk_re = re.compile(r'CREATE\s+TABLE[^;]*?PRIMARY\s+KEY\s*\(([^)]*)\)', re.I | re.S)
tbl_re = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\w.]*?(\w+)', re.I)

def norm(cols):
    return ",".join(c.strip().lower().split()[0].strip('"') for c in cols.split(",") if c.strip())

for path in sys.argv[1:]:
    text = open(path, encoding="utf-8", errors="replace").read()
    for m in idx_re.finditer(text):
        where = " ".join((m.group(4) or "").lower().split())
        key = (m.group(2).lower(), norm(m.group(3)), where)
        seen[key].append((m.group(1), path.split("/")[-1]))
    # 主键也是索引，必须一起比
    for stmt in re.split(r';\s*', text):
        if not re.search(r'CREATE\s+TABLE', stmt, re.I): continue
        t = tbl_re.search(stmt); k = pk_re.search(stmt + ";")
        if t and k:
            seen[(t.group(1).lower(), norm(k.group(1)), "")].append(("<PRIMARY KEY>", path.split("/")[-1]))

dups = {k: v for k, v in seen.items() if len({n for n, _ in v}) > 1}
if not dups:
    print(f"✓ 无重复索引（检查了 {len(sys.argv)-1} 个 SQL 文件、{len(seen)} 个索引定义）")
    raise SystemExit(0)

print("✗ 发现列定义完全相同的重复索引：", file=sys.stderr)
for (tbl, cols, where), items in dups.items():
    suffix = f" {where}" if where else ""
    print(f"  表 {tbl} 的 ({cols}){suffix} 上有 {len(items)} 个索引：", file=sys.stderr)
    for name, f in items:
        print(f"      {name}   —— {f}", file=sys.stderr)
print("\n  查询只会用其中一个，写入却要维护全部。", file=sys.stderr)
print("  修法：两处用**同一个索引名**，让 IF NOT EXISTS 生效；", file=sys.stderr)
print("  若其中一个就是主键，直接把多余的那条 CREATE INDEX 删掉。", file=sys.stderr)
raise SystemExit(1)
PY
