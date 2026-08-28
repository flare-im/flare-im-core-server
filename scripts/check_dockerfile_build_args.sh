#!/usr/bin/env bash
# 构建参数一致性门禁：文档里教人传的 --build-arg，Dockerfile 必须声明。
#
# Docker 对**未声明**的 --build-arg 只会给一句警告然后**静默忽略**。
# 于是照着文档传了 CARGO_BUILD_JOBS 却毫无作用——构建仍按核数满并发跑，
# 在小内存构建机上照样 OOM，而你以为自己已经限流了。
# flare-social 的 Dockerfile 就缺这两个参数，实测传了等于没传。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

python3 - "$ROOT" <<'PY'
import pathlib, re, sys

root = pathlib.Path(sys.argv[1])
# 这几个是构建机环境相关的开关，缺了会有真实后果（卡网络 / OOM）
REQUIRED = ["APT_MIRROR", "CARGO_REGISTRY_MIRROR", "CARGO_BUILD_JOBS"]

targets = []
for df in sorted(root.rglob("Dockerfile")):
    sp = str(df)
    if "/target/" in sp or "/dist/" in sp or "/node_modules/" in sp:
        continue
    text = df.read_text(errors="replace")
    # 只管从源码编译 Rust 的镜像；纯运行时镜像不需要这些
    if "cargo build" not in text:
        continue
    targets.append((df, text))

if not targets:
    print("没有从源码编译的 Dockerfile，跳过"); raise SystemExit(0)

bad = []
for df, text in targets:
    declared = set(re.findall(r'^ARG\s+([A-Z_]+)', text, re.M))
    missing = [a for a in REQUIRED if a not in declared]
    if missing:
        bad.append((df.relative_to(root), missing))

if bad:
    for path, missing in bad:
        print(f"  ✗ {path} —— 缺少 {', '.join(missing)}", file=sys.stderr)
    print("", file=sys.stderr)
    print("Docker 对未声明的 --build-arg 只警告不报错，会**静默忽略**——", file=sys.stderr)
    print("照文档传了参数却毫无作用，比没有这个参数更难排查。", file=sys.stderr)
    raise SystemExit(1)

print(f"✓ {len(targets)} 个源码构建 Dockerfile 都声明了构建机相关参数")
PY
