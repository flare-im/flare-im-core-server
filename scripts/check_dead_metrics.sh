#!/usr/bin/env bash
# 死指标门禁：注册进 Prometheus 却没有任何写入路径的指标字段。
#
# 为什么要管：这类指标不是"少了点观测"，而是**主动误导**。
# 字段一旦注册，/metrics 里就会出现且**永远是 0**。运维在故障时看到
# `access_gateway_push_success_total 0` 会直接断定"推送全挂"，
# 而实际推送好好的——比压根没有这个指标更糟。
#
# 本仓实测撞到两处同类：
#   - AccessGatewayMetrics 九个字段只有两个被写过（整个推送投递观测面是死的）
#   - TimelineMetadata.dispatched_ts 只被读和序列化，从没有任何地方赋值
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" <<'PY'
import re, subprocess, sys, pathlib

root = pathlib.Path(sys.argv[1])
mod = root / "crates/flare-im-core-service-kit/src/metrics/mod.rs"
if not mod.exists():
    print("找不到 metrics 模块，跳过"); raise SystemExit(0)

text = mod.read_text()
field_types = dict(re.findall(
    r'^    pub ([a-z_]+): ((?:HistogramVec|Histogram|IntCounterVec|IntCounter|IntGaugeVec|IntGauge))\b',
    text, re.M))
fields = sorted(field_types)

# 仓库里除 metrics 模块外的全部 rs 源码，一次读完，避免逐字段起 grep 进程
others = []
for p in root.rglob("*.rs"):
    sp = str(p)
    if "/target/" in sp or "/src/metrics/" in sp:
        continue
    try:
        others.append(p.read_text(errors="replace"))
    except OSError:
        pass
blob = "\n".join(others)

# 写入经辅助函数转一手的也算：`export_builds(&self.connections_by_build, ..)` 这种
# 把字段按引用传进一个函数、由函数体对形参调 `.with_label_values` / `.set` / `.inc` 的写法。
# 只认「形参类型与字段类型相同、函数体里真对形参写了」的函数，单纯被传来传去读一下不算。
WRITE_CALL = r'(?:with_label_values|set|inc|inc_by|dec|dec_by|add|sub|observe)'
FN_RE = re.compile(r'\bfn\s+([a-z_][a-z0-9_]*)\s*(?:<[^>]*>)?\s*\(([^)]*)\)[^{;]*\{', re.S)

def writer_params(source):
    """{(函数名, 类型): 是否写了该类型的形参} —— 只收会写指标形参的函数。"""
    found = set()
    for m in FN_RE.finditer(source):
        name, params = m.group(1), m.group(2)
        # 函数体：从左花括号起按括号配平截到对应的右花括号
        depth, i = 0, m.end() - 1
        while i < len(source):
            c = source[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    break
            i += 1
        body = source[m.end():i]
        for pm in re.finditer(r'([a-z_][a-z0-9_]*)\s*:\s*&\s*([A-Za-z]+)\b', params):
            param, ty = pm.group(1), pm.group(2)
            if re.search(r'\b' + re.escape(param) + r'\s*\.\s*' + WRITE_CALL + r'\s*\(', body):
                found.add((name, ty))
    return found

writers = writer_params(text) | writer_params(blob)

def written_via_helper(field, source):
    ty = field_types[field]
    for m in re.finditer(r'\b([a-z_][a-z0-9_]*)\s*\(\s*([^;]*?)&\s*(?:self|[a-z_][a-z0-9_]*)\.' + re.escape(field) + r'\s*[,)]', source, re.S):
        if (m.group(1), ty) in writers:
            return True
    return False

dead, checked = [], 0
for f in fields:
    # 只看真正注册过的：没注册的不会出现在 /metrics，不会误导任何人
    if f"REGISTRY.register(Box::new({f}.clone()))" not in text:
        continue
    checked += 1
    # ⚠️ 必须容忍换行：rustfmt 会把 `self.x.with_label_values(..)` 拆成
    #    `self.x\n    .with_label_values(..)`，要求紧跟一个点会把写过的字段
    #    全判成死指标（第一版就是这么误报了 16 个）。门禁误报比没有门禁更糟。
    use_re = re.compile(r'\.' + re.escape(f) + r'\s*\.')
    written_in_mod = bool(use_re.search(text))
    written_outside = bool(use_re.search(blob))
    if not written_in_mod and not written_outside:
        if written_via_helper(f, text) or written_via_helper(f, blob):
            continue
        dead.append(f)

if dead:
    for f in dead:
        print(f"  ✗ {f} —— 已注册但没有任何写入路径，/metrics 里会永远显示 0", file=sys.stderr)
    print("", file=sys.stderr)
    print("已注册的指标必须有人写。要么补上记录路径，要么把字段和注册一起删掉——", file=sys.stderr)
    print("留一个恒为 0 的指标比没有更糟：它会在故障时把人引向错误结论。", file=sys.stderr)
    raise SystemExit(1)

print(f"✓ 已注册的 {checked} 个指标都有写入路径")
PY
