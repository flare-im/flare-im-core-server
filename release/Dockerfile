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
        grep -rq "$APT_MIRROR" /etc/apt/ || { echo "APT_MIRROR 未生效，源文件格式可能变了"; exit 1; }; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential clang cmake pkg-config \
        libssl-dev libprotobuf-dev protobuf-compiler zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC=/usr/bin/protoc

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
