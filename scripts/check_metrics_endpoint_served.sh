#!/usr/bin/env bash
# 指标出口门禁：构造了 metrics 结构的服务，必须真的把 /metrics 暴露出来。
#
# 网关踩过这个坑：AccessGatewayMetrics 九个字段都注册进了全局 REGISTRY，
# wire.rs 还打了一句 "Prometheus metrics initialized" 的日志，
# 但**从没调用 serve_prometheus_metrics** —— 指标有人写、有人注册，
# 就是没有任何出口，整个网关侧观测面无处可读，而且日志看着像一切正常。
#
# 判据：某个服务 crate 里出现 `XxxMetrics::new()`，就必须能找到
# `serve_prometheus_metrics`。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" <<'PY'
import pathlib, re, sys

root = pathlib.Path(sys.argv[1])
bad, checked = [], 0

# 按 Cargo.toml 找出全部 crate，不要靠猜目录层级——
# 第一版只扫顶层 */src，把 flare-signaling/gateway 这类两层目录全漏了，
# 结果"只检查了 1 个服务"却报绿。门禁扫不到东西比报错更危险。
for manifest in sorted(root.rglob("Cargo.toml")):
    if "/target/" in str(manifest):
        continue
    src = manifest.parent / "src"
    if not src.is_dir():
        continue

    files = [p for p in src.rglob("*.rs") if "/target/" not in str(p)]
    if not files:
        continue
    # ⚠️ 必须剥掉注释再判：注释里提到 serve_prometheus_metrics 就能把门禁骗过去
    #    （我自己写的那句"没有像 ingest 那样起 serve_prometheus_metrics"就干过这事，
    #    移除了真实调用后门禁照样报绿）。判据只认真实代码。
    def strip_comments(text):
        out = []
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            out.append(line.split("//")[0] if "//" in line else line)
        return "\n".join(out)

    blob = "\n".join(strip_comments(p.read_text(errors="replace")) for p in files)

    # 只看真正构造了 metrics 结构的服务
    if not re.search(r'\b\w*Metrics::new\(\)', blob):
        continue
    checked += 1
    name = str(manifest.parent.relative_to(root))
    if "serve_prometheus_metrics" not in blob:
        bad.append(name)

if bad:
    for c in bad:
        print(f"  ✗ {c} —— 构造了 metrics 却没有调用 serve_prometheus_metrics", file=sys.stderr)
    print("", file=sys.stderr)
    print("指标注册了但没有出口，等于没有观测——而且日志看着像一切正常。", file=sys.stderr)
    print("照 ingest/orchestrator 的写法，在启动任务里加 serve_prometheus_metrics。", file=sys.stderr)
    raise SystemExit(1)

print(f"✓ 构造 metrics 的 {checked} 个服务都暴露了 /metrics")
PY
