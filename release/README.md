# Flare IM Core 4C4G 云服务器发布包

这个目录用于生成并远程管理单台 4 核 4G 云服务器上的 Flare IM Core 快速部署测试包。目标是验证业务中立 IM Core 的核心链路，而不是把本地开发全家桶原样搬到小机器上。

日常运维入口只有一个：

```bash
cp release/deploy.env.example release/deploy.env
$EDITOR release/deploy.env
./release/flarectl.sh deploy --smoke
```

它会读取 `release/deploy.env`，在本机生成发布包、上传到服务器、切换 `current` 版本、启动服务，并可选运行 smoke 测试。服务器上的内部配置文件仍然存在，但不作为常规操作界面。

## 架构取舍

保留必需组件：

- `consul`: 服务注册发现
- `redis`: 在线状态、token、WAL/cache
- `postgres/timescaledb`: 消息、账本、会话、媒体元数据主存储
- `nats/jetstream`: 默认 MQ 后端
- `rustfs`: 必需对象存储
- Flare IM Core release 二进制：消息摄入、编排、存储读写、同步、推送、媒体、API/access gateway 等

默认不启动：

- Kafka：4G 机器上不作为快速测试默认 MQ
- Prometheus/Loki/Tempo/Grafana：快速发布包先用本地日志和 smoke 脚本做门禁
- Strom SFU 插件：RTC 能力后续作为独立插件部署

## 生成发布包

通常不需要手动生成发布包，`flarectl.sh deploy` 会自动生成。需要离线包时，在构建机器上执行，推荐不要在 4G 云服务器上现场编译：

```bash
cd /path/to/flare-im/flare-im-core
./release/scripts/build_linux_bundle_docker.sh --jobs 1
```

`flarectl.sh deploy` 默认使用 `--build-mode auto`：Linux 构建机直接本机打包；macOS 等非 Linux 构建机会通过 Docker 生成 `linux/amd64` 发布包，避免把 Mach-O/Darwin 二进制上传到 Ubuntu 服务器。也可以显式指定：

```bash
./release/flarectl.sh deploy --build-mode docker --smoke
./release/flarectl.sh deploy --build-mode host --smoke
```

Docker 打包需要本机 Docker。国内网络拉 crate 较慢时可配置 Cargo sparse 镜像：

```bash
export FLARE_DOCKER_CARGO_REGISTRY_MIRROR='sparse+https://mirrors.ustc.edu.cn/crates.io-index/'
./release/scripts/build_linux_bundle_docker.sh --jobs 1
```

生成目录默认位于：

```bash
release/dist/flare-im-core-cloud-4c4g-YYYYMMDDHHMMSS
```

目录结构：

```text
bin/                         # 编译后的 release 二进制
config/                      # 运行时 TOML 配置
data/                        # 中间件数据目录
docker-compose.infra.yml     # 4C4G 轻量基础设施
logs/                        # Flare 进程日志
nats/nats.conf               # JetStream 轻量配置
proto/                       # grpcurl smoke 所需 proto
run/                         # pid 文件
scripts/                     # 启动、停止、状态、烟测脚本
sql/init.sql                 # 首次初始化数据库 schema
.env.example                 # 环境变量模板
```

发布包中的 `config/` 是内部运行时配置，打包时只包含最小运行集：`base.toml`、`hooks.core.toml`、`services/cloud-4c4g.toml` 和环境覆盖文件；不会把示例 hook、配置文档和开发说明复制到服务器包。

配置治理原则：

- 源码仓库的 `config/services/*.toml` 继续按服务拆分，方便开发和代码所有权维护。
- 发布包生成时合并为 `config/services/cloud-4c4g.toml`，避免服务器上出现一堆散配置。
- 运维入口只使用 `release/deploy.env` 和远端 `shared/.env`；TOML 视作运行时内部文件。
- 单机 4C4G 默认 `FLARE_MQ_DEFAULT_BACKEND=nats`，Kafka、Grafana、Loki、Tempo 不进入快速发布链路。

## 一键远程操作

### 推荐：env 驱动

把服务器信息写入 `release/deploy.env`：

```bash
cp release/deploy.env.example release/deploy.env
```

最小内容：

```bash
FLARE_DEPLOY_HOST=203.0.113.10
FLARE_DEPLOY_USER=root
FLARE_DEPLOY_PASSWORD=your-password
FLARE_DEPLOY_REMOTE_DIR=/opt/flare-im-core
FLARE_DEPLOY_SMOKE=1
```

之后常用命令可以简化为：

```bash
./release/flarectl.sh deploy
./release/flarectl.sh update
./release/flarectl.sh restart
./release/flarectl.sh status
./release/flarectl.sh logs
./release/flarectl.sh smoke
```

### 临时覆盖

不想落本地 env 文件时，也可以临时传参数：

```bash
./release/flarectl.sh deploy \
  --host 203.0.113.10 \
  --user root \
  --password 'your-password' \
  --smoke
```

更新/升级到新发布包：

```bash
./release/flarectl.sh update --host 203.0.113.10 --user root --password 'your-password' --smoke
```

重启：

```bash
./release/flarectl.sh restart --host 203.0.113.10 --user root --password 'your-password'
```

状态检查：

```bash
./release/flarectl.sh status --host 203.0.113.10 --user root --password 'your-password'
```

查看日志：

```bash
./release/flarectl.sh logs --host 203.0.113.10 --user root --password 'your-password'
```

停止 core 进程：

```bash
./release/flarectl.sh stop --host 203.0.113.10 --user root --password 'your-password'
```

密码也可以通过环境变量传入，避免进入 shell history：

```bash
export FLARE_DEPLOY_PASSWORD='your-password'
./release/flarectl.sh deploy --host 203.0.113.10 --user root --smoke
```

本机密码登录依赖 `sshpass`；不想安装 `sshpass` 时，用 `--identity-file ~/.ssh/id_ed25519` 或 SSH agent。

云服务器依赖：

- Linux x86_64，glibc 版本与构建机器兼容
- Docker + Docker Compose
- `grpcurl` 和 `psql` 用于 smoke 测试
- 建议 4G swap，避免首次启动和峰值抖动触发 OOM

## 服务器目录结构

`flarectl.sh` 默认使用 `/opt/flare-im-core`：

```text
/opt/flare-im-core/
  current -> releases/release-YYYYMMDDHHMMSS
  releases/
  shared/
    data/                  # 中间件持久化数据
    logs/                  # Flare 进程日志
    run/                   # pid 文件
    .env                   # 可选覆盖配置，默认不存在
```

每次 deploy/update 只新增一个 `releases/*` 并切换 `current`；`shared/data` 不会被覆盖。

默认不会把 `.env.example` 自动复制成 `.env`，避免示例 secret 被误用。没有 `.env` 时，启动脚本会在 `shared/data/.dev-token-secret` 自动生成本机 token secret。需要固定生产 secret 时，再手工创建 `/opt/flare-im-core/shared/.env`。

## 服务器本地启动

如果已经在服务器上，可以进入当前版本本地操作：

```bash
cd /opt/flare-im-core/current
./scripts/start.sh
```

## 验证

```bash
./scripts/status.sh
./scripts/smoke.sh
```

`smoke.sh` 会发送一条单聊文本消息，并验证：

- `flare-message-ingest` 返回成功
- ACK durability 至少达到 `SEND_ACK_DURABILITY_BROKER_ACCEPTED`
- PostgreSQL `messages` 有持久化记录
- `message_write_ledger.write_state` 至少达到 `archive_persisted`
- `flare-storage-reader` 可按 seq 读回刚发送的消息

也可以启动后自动跑 smoke：

```bash
./scripts/start.sh --smoke
```

## 停止

只停止 Flare Core 进程：

```bash
./scripts/stop.sh
```

停止 Flare Core 并关闭基础设施容器：

```bash
./scripts/stop.sh --infra
```

## 端口

基础设施默认只绑定 `127.0.0.1`：

- Consul: `28500`
- Redis: `26379`
- PostgreSQL: `25432`
- NATS: `24222`, monitoring `28222`
- RustFS: `29000`, console `29001`

Flare Core 进程监听本机服务端口：

- `50061`: signaling-online
- `50062`: signaling-route
- `50090`: conversation
- `50181`: message-orchestrator
- `50182`: message-ingest
- `50050`: api-gateway
- `60051`: access-gateway WebSocket
- `60060`: access-gateway gRPC
- `60081`: media
- `60083`: storage-reader
- `60084`: sync-orchestrator

生产公网暴露请只开放必要 gateway/API 端口，并用安全组限制来源。
