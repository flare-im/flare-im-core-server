#!/usr/bin/env bash
# 指标出口门禁：构造了 metrics 结构的服务，必须真的把 /metrics 暴露出来。
#
# 网关踩过这个坑：AccessGatewayMetrics 九个字段都注册进了全局 REGISTRY，
# wire.rs 还打了一句 "Prometheus metrics initialized" 的日志，
# 但**从没调用 serve_prometheus_metrics** —— 指标有人写、有人注册，
# 就是没有任何出口，整个网关侧观测面无处可读，而且日志看着像一切正常。
#
# 判据一：某个服务 crate 里出现 `XxxMetrics::new()`，就必须能找到出口——
# 直接调用 `serve_prometheus_metrics`，或经共享运行时 `attach_metrics_endpoint` /
# `ImServiceRuntimePlan { metrics: .. }` 挂载。
# 判据二：config/services 下每个服务都声明了指标端口（专属 metrics_port 或
# [services.<name>.metrics] 段；接入网关用代码默认 19183），端口互不冲突，
# 且 deploy/prometheus.yml 恰好为每个端口配置了抓取目标——端口表漂移同样会让面板无数。
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
    exits = ("serve_prometheus_metrics", "attach_metrics_endpoint")
    # 显式传 disabled() 等于没有出口，不能因为字段存在就放行。
    plan_metrics = re.findall(r'ImServiceRuntimePlan\s*\{[^}]*\bmetrics\s*:\s*([^,}]+)', blob)
    via_plan = any("disabled" not in value for value in plan_metrics)
    if not any(marker in blob for marker in exits) and not via_plan:
        bad.append(name)

if bad:
    for c in bad:
        print(f"  ✗ {c} —— 构造了 metrics 却没有调用 serve_prometheus_metrics", file=sys.stderr)
    print("", file=sys.stderr)
    print("指标注册了但没有出口，等于没有观测——而且日志看着像一切正常。", file=sys.stderr)
    print("经共享运行时 attach_metrics_endpoint 挂载，或在启动任务里调用 serve_prometheus_metrics。", file=sys.stderr)
    raise SystemExit(1)

print(f"✓ 构造 metrics 的 {checked} 个服务都暴露了 /metrics")

# ── 判据二：端口表 ──
# 服务专属字段或共享段之外、由代码给默认端口的服务。
CODE_DEFAULT_PORTS = {"access-gateway": 19183}
ports, missing = {}, []
for toml in sorted((root / "config/services").glob("*.toml")):
    text = toml.read_text(errors="replace")
    service = toml.stem
    found = None
    section = re.search(r'^\[services\.[a-z_]+\.metrics\]\s*\n((?:[^\[\n][^\n]*\n?)*)', text, re.M)
    if section:
        port = re.search(r'^port\s*=\s*(\d+)', section.group(1), re.M)
        found = int(port.group(1)) if port else None
    if found is None:
        legacy = re.search(r'^metrics_port\s*=\s*(\d+)', text, re.M)
        found = int(legacy.group(1)) if legacy else CODE_DEFAULT_PORTS.get(service)
    if found is None:
        missing.append(service)
    else:
        ports.setdefault(found, []).append(service)

problems = [f"{name} 没有声明指标端口" for name in missing]
problems += [f"端口 {port} 被 {', '.join(names)} 同时使用" for port, names in ports.items() if len(names) > 1]
scrape = (root / "deploy/prometheus.yml").read_text(errors="replace")
scraped = {int(port) for port in re.findall(r"targets:\s*\['[a-z0-9-]+:(\d+)'\]", scrape)}
problems += [f"deploy/prometheus.yml 缺少 {', '.join(ports[port])} 的抓取目标 :{port}"
             for port in sorted(set(ports) - scraped)]
problems += [f"deploy/prometheus.yml 抓取了没有服务声明的端口 :{port}"
             for port in sorted(scraped - set(ports)) if 19180 <= port < 19300]
if problems:
    for problem in problems:
        print(f"  ✗ {problem}", file=sys.stderr)
    raise SystemExit(1)
print(f"✓ {sum(len(v) for v in ports.values())} 个服务的指标端口唯一且都在 prometheus.yml 中抓取")
PY
