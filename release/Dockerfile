# syntax=docker/dockerfile:1
#
# Runtime image for Flare IM Core.
#
# Why this exists: evaluating the project used to require Docker *plus* compiling the
# whole Rust workspace — the first run took well over ten minutes, which makes "five
# minute start" a promise the repo could not keep. This image ships prebuilt binaries so
# `docker compose up` is the only step.
#
# One image, all services. The services differ only by which binary runs, so shipping
# twelve near-identical images would multiply registry size and version skew for nothing;
# the compose file picks the binary per container.
#
#   docker build -f release/Dockerfile -t flare/im-core:dev .
#
# Note this is NOT release/Dockerfile.bundle — that one is a *build environment* (toolchain
# only, no artifacts), used for reproducible compilation, not for running anything.

ARG RUST_IMAGE=rust:1.94.1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM ${RUST_IMAGE} AS builder

ENV DEBIAN_FRONTEND=noninteractive

# apt 镜像源可覆盖。默认走官方源；在网络到 deb.debian.org 慢的地区（实测某次部署
# 卡在下载阶段十几分钟零进展）用 --build-arg APT_MIRROR=mirrors.aliyun.com 之类换掉。
ARG APT_MIRROR=deb.debian.org
RUN if [ "$APT_MIRROR" != "deb.debian.org" ]; then \
        for f in /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list; do \
            [ -f "$f" ] && sed -i "s|deb.debian.org|$APT_MIRROR|g" "$f"; \
        done; \
        # ⚠️ 这行不能删：它不只是校验，还兜住了退出码。
        # 上面循环最后一条是 `[ -f "$f" ] && sed`，而 sources.list 在 rust 镜像里
        # 并不存在 → 该行返回 1 → 整个 if 块退出码就是 1 → 后面的
        # `&& apt-get update` 被短路，RUN 失败且报错完全看不出原因。
        # （实测：照抄这段却漏掉本行的 Dockerfile.partial 就是这么挂的。）
        grep -rq "$APT_MIRROR" /etc/apt/ || { echo "APT_MIRROR 未生效，源文件格式可能变了"; exit 1; }; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential clang cmake pkg-config \
        libssl-dev libprotobuf-dev protobuf-compiler zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC=/usr/bin/protoc

# 跨架构模拟构建（arm64 开发机 → amd64 服务器）时 rustc 会在 QEMU 下栈溢出：
#   qemu: uncaught target signal 11 (Segmentation fault) - core dumped
# rustc 自己会提示 RUST_MIN_STACK。原生构建不受影响，设着无害。
# flare-social 的 Dockerfile 早就有这行，这里一直缺——而"在开发机上构建
# 再推镜像"恰恰是这台 40G 生产机唯一可行的交付方式，缺了它这条路直接走不通。
ENV RUST_MIN_STACK=16777216

# crates.io 镜像源。和上面的 APT_MIRROR 是同一类问题：在到 crates.io 慢的地区，
# 构建会卡在 "Updating crates.io index" 反复报 spurious network error
# （实测某次十分钟零进展）。默认走官方源，不影响其它地区。
#   docker build --build-arg CARGO_REGISTRY_MIRROR=https://mirrors.aliyun.com/crates.io-index/ ...
ARG CARGO_REGISTRY_MIRROR=
RUN if [ -n "$CARGO_REGISTRY_MIRROR" ]; then \
        mkdir -p "${CARGO_HOME:-/usr/local/cargo}" && \
        printf '[source.crates-io]\nreplace-with = "mirror"\n\n[source.mirror]\nregistry = "sparse+%s"\n' \
            "$CARGO_REGISTRY_MIRROR" > "${CARGO_HOME:-/usr/local/cargo}/config.toml" && \
        echo "cargo 镜像源已设为 $CARGO_REGISTRY_MIRROR"; \
    fi

WORKDIR /build

# Sibling crates are path dependencies of this workspace, so the build context is the
# parent directory. Copy them first — they change far less often than flare-im-core, so
# this layer stays cached across most rebuilds.
COPY flare-proto ./flare-proto
COPY flare-grpc-proto ./flare-grpc-proto
COPY flare-core ./flare-core
COPY flare-server-core ./flare-server-core

COPY flare-im-core ./flare-im-core

WORKDIR /build/flare-im-core

# 编译并发度。默认跟随 CPU 核数；但 rustc 在 release 下每个并行任务都很吃内存，
# 小内存机器上并行 4 个足以 OOM——而这台机器往往同时跑着整套服务，
# 一次 OOM 就会波及线上容器。release/scripts/build_release_bundle.sh 因此用 --jobs 1；
# 镜像构建以前没有对应开关，这里补上。
#   docker build --build-arg CARGO_BUILD_JOBS=1 ...
#
# 默认值必须是 `default` 而不是留空：ENV 会把未传的 ARG 设成空串，而 cargo
# 拒绝解析空串（error: could not parse ``），于是不传这个 build-arg 时构建
# 100% 失败——报错还指向 cargo build 那一大行，看着像是编译错误。
ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/flare-im-core/target \
    cargo build --release \
        -p flare-api-gateway \
        -p flare-admin-gateway \
        -p flare-signaling-gateway \
        -p flare-signaling-online \
        -p flare-signaling-route \
        -p flare-message-ingest \
        -p flare-orchestrator \
        -p flare-conversation \
        -p flare-sync-orchestrator \
        -p flare-storage-writer \
        -p flare-storage-reader \
        -p flare-push-server \
        -p flare-push-worker \
        -p flare-capability \
        -p flare-media \
    # The cache mount disappears with the layer, so copy the binaries out while it's
    # still mounted. Forgetting this yields an image with an empty /out and a build
    # that "succeeded".
    && mkdir -p /out \
    && find target/release -maxdepth 1 -type f -perm -111 \
         ! -name '*.d' ! -name '*.so' -exec cp {} /out/ \;

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM ${RUNTIME_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive
# 运行时阶段同样换源：只换 builder 的话这一步照样会去够不着的官方源。
ARG APT_MIRROR=deb.debian.org
RUN if [ "$APT_MIRROR" != "deb.debian.org" ]; then \
        for f in /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list; do \
            [ -f "$f" ] && sed -i "s|deb.debian.org|$APT_MIRROR|g" "$f"; \
        done; \
        grep -rq "$APT_MIRROR" /etc/apt/ || { echo "APT_MIRROR 未生效，源文件格式可能变了"; exit 1; }; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m flare

COPY --from=builder /out/ /usr/local/bin/
COPY flare-im-core/config /etc/flare/config

USER flare
WORKDIR /home/flare

# 变量名必须是 FLARE_CONFIG_PATH —— 加载器（service-kit runtime.rs）读的是它。
# 这里曾经写成 FLARE_CONFIG_DIR，而那个名字**没有任何代码读**：服务会回退到
# 「相对 CWD 的 config/」，容器里那个目录不存在，于是每个需要库的服务都以
# 「postgres config is required」启动失败。镜像从未发布过，所以一直没人撞上。
ENV FLARE_CONFIG_PATH=/etc/flare/config \
    RUST_LOG=info

# No default CMD: this image carries every service and the compose file names the one to
# run. A default would silently start the wrong process when someone forgets `command:`.
