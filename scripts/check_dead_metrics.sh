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
mod = root / "crates/flare-im-service-kit/src/metrics/mod.rs"
if not mod.exists():
    print("找不到 metrics 模块，跳过"); raise SystemExit(0)

text = mod.read_text()
fields = sorted(set(re.findall(
    r'^    pub ([a-z_]+): (?:HistogramVec|Histogram|IntCounterVec|IntCounter|IntGauge)',
    text, re.M)))

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
