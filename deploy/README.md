# Flare IM 通信核心层部署指南

> **版本**: 0.1.0
> **用途**: 提供中间件和服务的 Docker Compose 部署配置

---

## 概述

本目录只保留 Flare IM Core 本地开发/压测所需的中间件编排，覆盖注册发现、缓存、主存储、消息队列、对象存储和 Grafana 可观测性栈。数据库 schema 以根目录 `init.sql` 为唯一入口。

---

## 快速开始

### 1. 启动所有中间件

```bash
cd deploy
docker compose up -d
```

### 2. 检查服务状态

```bash
docker compose ps
```

### 3. 查看日志

```bash
docker compose logs -f [service_name]
```

---

## 包含的服务

### 1. Consul (服务注册 / 配置中心)

- **端口**: 28500 (HTTP/UI), 28600/udp (DNS)
- **用途**: 服务注册、发现和本地配置中心
- **访问**: http://localhost:28500
- **数据目录**: `./data/consul`

### 2. Redis / Dragonfly (缓存)

- **端口**: 26379
- **用途**: 缓存、在线状态存储（默认使用 Redis 单节点，亦可替换为 DragonflyDB）
- **访问**: redis://localhost:26379
- **数据目录**: `./data/redis`

### 3. PostgreSQL + TimescaleDB (时序数据库)

- **端口**: 25432
- **数据库**: flare2
- **用户**: flare
- **密码**: flare123
- **访问**: postgresql://flare:flare123@localhost:25432/flare2
- **数据目录**: `./data/postgres`
- **初始化脚本**: `./init.sql`
- **特性**:
  - TimescaleDB 扩展已启用
  - 消息表使用超表（Hypertable）按 `created_at` 分区
  - 支持消息、会话、媒体、Hook、Capability、ACK 审计等核心表

### 4. 消息队列（NATS JetStream + Apache Kafka）

> 二者同时拉起；测试时在应用配置中切换 `mq.default_backend`（或各服务引用的 profile）即可分别走 NATS / Kafka。

**NATS JetStream**

- **客户端端口**: 24222（映射容器内 4222）
- **监控 HTTP**: http://localhost:28222
- **数据目录**: `./data/nats`

**Apache Kafka**（KRaft，无 ZooKeeper）

- **宿主机 bootstrap**: `127.0.0.1:29092`（与仓库 `flare-im-core/config/base.toml` 中 `[kafka.*].brokers` 默认一致）
- **Compose 网络内**: `kafka:9092`（服务 `hostname` 为 `kafka`）
- **数据目录**: `./data/kafka`

### 5. RustFS (对象存储)

- **API端口**: 29000
- **控制台端口**: 29001
- **默认用户**: rustfsadmin
- **默认密码**: rustfsadmin
- **访问**:
  - API: http://localhost:29000
  - 控制台: http://localhost:29001
- **数据目录**: 命名卷 `rustfs-data`（不是 bind mount —— 容器内以 rustfs 用户运行，
  宿主机建的目录属于另一个 UID，Linux 上会直接 `Permission denied` 起不来）

### 6. Loki (日志聚合)

- **端口**: 3100
- **用途**: 日志聚合与查询（Grafana 数据源）
- **访问**: http://localhost:3100
- **数据目录**: `./data/loki`
- **配置**: `./loki-config.yml`（默认启用本地文件存储）

### 7. Prometheus (监控)

- **端口**: 29090
- **用途**: 指标收集和监控
- **访问**: http://localhost:29090
- **数据目录**: `./data/prometheus`

### 8. Grafana (可视化 & 告警)

- **端口**: 23000
- **用途**: 监控、日志、追踪统一可视化，以及告警管理
- **访问**: http://localhost:23000
- **默认用户**: admin / admin
- **数据目录**: `./data/grafana`

### 9. Tempo (分布式追踪)

- **端口**: 3200 (HTTP), 4317 (OTLP gRPC), 4318 (OTLP HTTP)
- **用途**: Trace 采集与查询
- **数据目录**: `./data/tempo`

---

## 配置说明

### 环境变量

可以通过环境变量配置各个服务：

```bash
# Redis
REDIS_PASSWORD=your_password

# PostgreSQL
POSTGRES_USER=flare
POSTGRES_PASSWORD=flare123
POSTGRES_DB=flare2

# RustFS
RUSTFS_ACCESS_KEY=rustfsadmin
RUSTFS_SECRET_KEY=rustfsadmin
```

### 存储配置

数据持久化存储在本地目录 `./data/` 下：

- `./data/consul`: Consul 数据
- `./data/redis`: Redis 数据
- `./data/postgres`: PostgreSQL 数据
- `./data/nats`: NATS JetStream 数据（配置文件是 `./nats.conf`，**不在 data/ 下** ——
  `/deploy/data/` 整个被 .gitignore 忽略，放进去等于全新检出时缺文件）
- `./data/kafka`: Kafka（KRaft）数据
- `./data/loki`: Loki 数据
- `./data/prometheus`: Prometheus 数据
- `./data/grafana`: Grafana 数据
- `./data/tempo`: Tempo 数据

>  **提示**: 首次启动前，建议创建数据目录：
> ```bash
> mkdir -p data/{consul,redis,postgres,nats,kafka,loki,prometheus,grafana,tempo}
> ```

---

## 使用示例

### 1. 启动服务

```bash
# 启动所有服务
docker compose up -d

# 启动特定服务
docker compose up -d redis postgres
```

### 2. 停止服务

```bash
# 停止所有服务
docker compose down

# 停止并删除数据
docker compose down -v
```

### 3. 查看日志

```bash
# 查看所有日志
docker compose logs -f

# 查看特定服务日志
docker compose logs -f nats kafka
```

### 5. 访问 RustFS 控制台

1. 打开浏览器访问 http://localhost:29001
2. 使用默认账号登录：rustfsadmin / rustfsadmin
3. 按需创建 bucket：flare-media

---

## 服务连接信息

### 开发环境连接

```toml
# Consul
consul_endpoint = "http://localhost:28500"

# Redis
redis_url = "redis://localhost:26379"

# PostgreSQL + TimescaleDB
postgres_url = "postgresql://flare:flare123@localhost:25432/flare2"
# TimescaleDB 扩展已自动启用，消息表已按 created_at 转换为超表（Hypertable）

# NATS JetStream（与 config/base.toml [jetstream.*] 一致）
# url = "nats://127.0.0.1:24222"

# Kafka（与 config/base.toml [kafka.*] 一致；mq.default_backend = "kafka" 时使用）
# brokers = ["127.0.0.1:29092"]

# RustFS / S3 compatible
s3_endpoint = "http://localhost:29000"
s3_access_key = "rustfsadmin"
s3_secret_key = "rustfsadmin"
s3_bucket = "flare-media"

# Loki
loki_url = "http://localhost:3100"

# Prometheus
prometheus_url = "http://localhost:29090"

# Grafana
grafana_url = "http://localhost:23000"
```

---

## 相关文档

- [测试、性能与运维](../docs/06-testing-performance-and-operations.md)
- [架构总览](../docs/01-architecture-overview.md)
- [第三方接入](../docs/04-third-party-integration.md)
- [TimescaleDB 使用指南](./TIMESCALEDB_GUIDE.md) - TimescaleDB 详细配置和使用

---

**文档维护**: Flare IM Architecture Team
**最后更新**: 2025-01-XX
**版本**: 0.1.0
