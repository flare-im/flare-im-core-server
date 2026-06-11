# Flare IM Core 脚本使用指南

本文档介绍 Flare IM Core 项目中所有脚本的使用方法和命令。

## 📋 目录

- [快速开始](#快速开始)
- [脚本说明](#脚本说明)
- [服务启动](#服务启动)
- [多网关部署](#多网关部署)
- [客户端连接](#客户端连接)
- [服务检查](#服务检查)
- [数据库迁移](#数据库迁移)
- [常见问题](#常见问题)

---

## 快速开始

### 1. 启动基础设施服务

```bash
cd deploy
docker compose up -d
```

这将启动以下基础设施服务：
- Redis (端口 26379)
- PostgreSQL (端口 25432)
- JetStream (端口 29092)
- Consul (端口 28500)

### 2. 启动 IM Core（二选一）

**业务中立（默认推荐，无业务系统 Hook）：**

```bash
./scripts/start_server_core.sh
```

**集成业务系统 gRPC PreSend Hook：**

```bash
# 先启动业务系统 gRPC Hook 服务，并确保它可通过服务发现或固定 endpoint 访问

# 再启动 IM Core（业务系统 Hook 模式）
cd ../flare-im-core && ./scripts/start_server.sh
```

Hook 配置文件：

| 文件 | 说明 |
|------|------|
| `config/hooks.core.toml` | 空 Hook，纯 IM 链路 |
| 业务系统 Hook 配置 | 业务系统 gRPC PreSend → `discovery://business-policy-hook` |
| `config/hooks.toml` | 运行时 symlink 目标（由启动脚本写入） |

### 3. 启动客户端（示例）

```bash
# 启动第一个客户端
cargo run --example chatroom_client -- user1

# 启动第二个客户端（新终端）
cargo run --example chatroom_client -- user2
```

### 4. 验证消息收发读链路

启动 Core 后，用烟测脚本发送一条单聊文本消息，确认 `messages` 与 `message_write_ledger` 已收敛，并通过 storage-reader 读回该消息：

```bash
./scripts/smoke_message_flow.sh
```

脚本默认检查：
- `flare-message-ingest` `MessageSendService/SendMessage`：`127.0.0.1:50182`
- `flare-storage-reader` `StorageReaderService/QueryMessagesBySeq`：`127.0.0.1:60083`
- PostgreSQL / TimescaleDB：`postgres://flare:flare123@localhost:25432/flare2`
- 可靠性边界：`SEND_ACK_DURABILITY_BROKER_ACCEPTED`
- 持久化账本终态：`message_write_ledger.write_state = ack_published`
- 读模型返回刚发送的 `server_msg_id`

---

## 脚本说明

### 核心启动脚本

| 脚本 | 说明 | 用途 |
|------|------|------|
| `start_server.sh` | 启动所有核心服务 | 通用入口；可配合 `FLARE_HOOKS_PROFILE` |
| `start_server_core.sh` | 业务中立启动 | `hooks.core.toml`，**不**注册业务系统 Hook |
| `stop_server.sh` | 停止所有服务 | 停止全栈 |

### 辅助脚本

| 脚本 | 说明 | 用途 |
|------|------|------|
| `check_services.sh` | 检查服务状态 | 验证服务是否正常运行 |
| `start_client.sh` | 启动客户端 | 快速启动聊天客户端 |
| `migrate_db.sh` | 数据库迁移 | 初始化数据库表结构 |
| `smoke_message_flow.sh` | 消息流烟测 | 发送一条消息，检查落库、ledger，并通过读侧读回 |

---

## 服务启动

### 启动所有核心服务

```bash
./scripts/start_multi_gateway.sh
```

**功能**：
- 检查基础设施服务状态（Redis、PostgreSQL、JetStream、Consul）
- 清理旧进程（防止端口冲突）
- 按依赖顺序启动所有核心服务
- 启动多地区 Access Gateway 实例（北京、上海）
- 检查服务启动状态

**服务启动顺序**：
1. `signaling-online` - 在线状态服务（端口 50061）
2. `signaling-route` - 路由目录服务（端口 50062）
3. `capability`（`flare-capability`）- Hook 与能力插件（gRPC 端口见应用 `config`）
4. `conversation` - 会话管理服务（端口 50090）
5. `message-ingest` - 消息摄入服务（端口 50182）
6. `message-orchestrator` - 消息编排服务（端口 50181）
7. `storage-writer` - 消息持久化服务（JetStream 消费者，不注册到服务注册中心）
8. `storage-reader` - 消息读模型服务（端口 60083）
9. `sync-orchestrator` - 同步编排服务（端口 60084）
10. `push-server` - 消息推送服务（JetStream 消费者，不注册到服务注册中心）
11. `push-worker` - 推送 worker（JetStream 消费者，不注册到服务注册中心）
12. `media` - 媒体服务（端口 60081）
13. `access-gateway` - 客户端接入网关（默认实例，端口 60051）
14. `core-gateway` - 业务系统统一入口（端口 50050）
15. `access-gateway-beijing-1` - 北京网关实例（端口 60051）
16. `access-gateway-shanghai-1` - 上海网关实例（端口 60052）

**日志位置**：
- 所有服务日志保存在 `/tmp/flare-<service-name>.log`
- 多网关实例日志：`/tmp/flare-access-gateway-<gateway-key>.log`
- 查看日志：`tail -f /tmp/flare-<service-name>.log`

**停止服务**：
```bash
# 使用停止脚本（推荐）
./scripts/stop_multi_gateway.sh

# 或手动停止（查找并杀死进程）
pkill -f "flare-"
```

---

## 多网关部署

### 使用统一启动脚本（推荐）

```bash
# 启动所有核心服务 + 多地区网关
./scripts/start_multi_gateway.sh
```

**功能**：
- 检查基础设施服务状态
- 启动所有核心服务
- 自动启动两个不同地区的 Access Gateway 实例：
  - 北京网关：`gateway-beijing-1`（端口 60051）
  - 上海网关：`gateway-shanghai-1`（端口 60052）

**停止所有服务**：
```bash
./scripts/stop_multi_gateway.sh
```

### 手动启动多个网关（高级用法）

如果需要单独启动额外的网关实例，可以使用环境变量：

```bash
# 北京网关（终端1）
GATEWAY_ID=gateway-beijing-1 \
GATEWAY_REGION=beijing \
PORT=60051 \
cargo run -p flare-signaling-gateway --bin flare-signaling-gateway

# 上海网关（终端2）
GATEWAY_ID=gateway-shanghai-1 \
GATEWAY_REGION=shanghai \
PORT=60052 \
cargo run -p flare-signaling-gateway --bin flare-signaling-gateway
```

**环境变量说明**：
- `GATEWAY_ID` - 网关唯一标识（如：`gateway-beijing-1`）
- `GATEWAY_REGION` - 网关所在地区（如：`beijing`、`shanghai`）
- `PORT` - 网关监听端口（默认 60051）

**注意事项**：
- 多网关部署需要先启动核心服务（signaling-online、message-orchestrator、push-server）
- 每个网关需要不同的端口
- 网关ID和地区信息会注册到 Signaling Online，用于跨地区路由

---

## 客户端连接

### 启动聊天客户端

```bash
# 方式1：使用启动脚本
./scripts/start_client.sh user1

# 方式2：直接使用 cargo
cargo run --example chatroom_client -- user1
```

### 连接到指定网关

```bash
# 连接到北京网关
NEGOTIATION_HOST=localhost:60051 \
cargo run --example chatroom_client -- user1

# 连接到上海网关
NEGOTIATION_HOST=localhost:60052 \
cargo run --example chatroom_client -- user2
```

### 业务系统推送消息

```bash
# 通过 Core Gateway 推送消息给所有在线用户
cargo run --example business_push_client

# 推送给指定用户
USER_IDS=user1,user2 \
cargo run --example business_push_client

# 自定义消息内容
MESSAGE_CONTENT="Hello from business system" \
cargo run --example business_push_client
```

**环境变量说明**：
- `NEGOTIATION_HOST` - 网关地址（格式：`host:port`）
- `GATEWAY_ENDPOINT` - Core Gateway 地址（默认：`http://localhost:50050`）
- `MESSAGE_CONTENT` - 消息内容
- `USER_IDS` - 目标用户ID列表（逗号分隔，为空则推送给所有在线用户）
- `TOKEN_SECRET` - JWT Token 密钥；必须显式提供并与网关签名密钥一致，或直接提供 `TOKEN`
- `TENANT_ID` - 租户ID（默认：`0`）
- `BUSINESS_USER_ID` - 业务系统用户ID（默认：`business-system`）

---

## 服务检查

### 检查服务状态

```bash
./scripts/check_services.sh
```

**功能**：
- 检查基础设施服务（Redis、PostgreSQL、JetStream）
- 检查核心服务进程状态
- 显示服务日志位置

### 手动检查服务

```bash
# 检查进程是否运行
ps aux | grep flare-

# 检查端口是否监听
lsof -i :50061  # signaling-online
lsof -i :50062  # signaling-route
lsof -i :50110  # flare-capability（若配置为该端口）
lsof -i :50090  # conversation
lsof -i :50182  # message-ingest
lsof -i :50181  # message-orchestrator
lsof -i :60083  # storage-reader
lsof -i :60084  # sync-orchestrator
lsof -i :60081  # media
lsof -i :50091  # push-server
lsof -i :60051  # access-gateway
lsof -i :50050  # core-gateway

# 检查 Redis 连接
redis-cli -h localhost -p 26379 ping

# 检查 PostgreSQL 连接
psql -h localhost -p 25432 -U flare -d flare2 -c "SELECT 1;"

# 检查 JetStream
jetstream-broker-api-versions --bootstrap-server localhost:29092
```

### 消息流烟测

```bash
./scripts/smoke_message_flow.sh
```

可覆盖默认 endpoint：

```bash
SMOKE_MESSAGE_INGEST_ENDPOINT=127.0.0.1:50182 \
SMOKE_STORAGE_READER_ENDPOINT=127.0.0.1:60083 \
SMOKE_POSTGRES_URL=postgres://flare:flare123@localhost:25432/flare2 \
./scripts/smoke_message_flow.sh
```

---

## 数据库迁移

### 初始化数据库表结构

```bash
./scripts/migrate_db.sh
```

**功能**：
- 运行数据库迁移脚本
- 创建必要的表结构
- 初始化默认数据

**手动迁移**：
```bash
# 使用 sqlx-cli
sqlx migrate run --database-url "postgresql://flare:flare123@localhost:25432/flare2"
```

---

## 常见问题

### 1. 端口被占用

**问题**：服务启动失败，提示端口被占用

**解决方案**：
```bash
# 查找占用端口的进程
lsof -i :<port>

# 停止旧进程
pkill -f "flare-<service-name>"

# 或使用启动脚本自动清理
./scripts/start_multi_gateway.sh
```

### 2. 基础设施服务未启动

**问题**：核心服务无法连接 Redis、PostgreSQL 或 JetStream

**解决方案**：
```bash
# 检查基础设施服务状态
./scripts/check_services.sh

# 启动基础设施服务
cd deploy && docker compose up -d

# 等待服务就绪（约10秒）
sleep 10
```

### 3. 多网关部署时用户无法收到消息

**问题**：用户连接到不同网关，但无法收到对方的消息

**解决方案**：
1. 确保 Signaling Online 服务正常运行
2. 检查网关是否正确注册到 Signaling Online：
   ```bash
   # 查看 Redis 中的在线状态
   redis-cli -h localhost -p 26379 KEYS "conversation:*"
   ```
3. 检查 Push Server 日志：
   ```bash
   tail -f /tmp/flare-push-server.log | grep -E "(gateway|routing)"
   ```
4. 确保 Push Server 配置了正确的 `signaling_endpoint` 和 `gateway_endpoints`

### 4. Flare Capability 无法连接数据库 / 缺表 / 缺授权

**问题**：`flare-capability` 启动失败，或报 `capability_service_settings` 不存在、或 `user capability grant missing`（`PostgresCapabilityPolicy`）

**说明**：数据库初始化与种子数据均在 **`deploy/init.sql`**（开发阶段可删库后整文件重跑，或按需执行其中 Hook+Capability 节）。

**排查**：
```bash
# 检查 PostgreSQL 是否运行
docker ps | grep postgres

# 检查数据库连接
psql -h localhost -p 25432 -U flare -d flare2

# 设置数据库URL（如果使用环境变量）
export DATABASE_URL="postgresql://flare:flare123@localhost:25432/flare2"
```

### 5. 客户端无法连接到网关

**问题**：客户端连接失败，提示连接超时

**解决方案**：
1. 检查 Access Gateway 是否运行：
   ```bash
   ps aux | grep flare-access-gateway
   tail -f /tmp/flare-access-gateway.log
   ```
2. 检查网关端口是否正确：
   ```bash
   lsof -i :60051  # 默认端口
   ```
3. 检查防火墙设置
4. 使用正确的 `NEGOTIATION_HOST` 环境变量

---

## 服务端口列表

| 服务 | 端口 | 说明 |
|------|------|------|
| signaling-online | 50051 | 在线状态服务 gRPC |
| signaling-route | 50062 | 路由目录服务 gRPC |
| capability（flare-capability） | 见 `config` | Hook 扩展与能力插件 gRPC |
| conversation | 50090 | 会话管理服务 gRPC |
| message-orchestrator | 50181 | 消息编排服务 gRPC |
| push-server | 50091 | 消息推送服务 gRPC |
| access-gateway | 60051 | 客户端接入网关（WebSocket/QUIC） |
| access-gateway-grpc | 60053 | 客户端接入网关 gRPC（port + 2） |
| core-gateway | 50050 | 业务系统统一入口 gRPC |
| Redis | 26379 | Redis 服务 |
| PostgreSQL | 25432 | PostgreSQL 服务 |
| JetStream | 29092 | JetStream 服务（外部端口） |
| Consul | 28500 | 服务注册 / 配置中心 |

---

## 日志查看

### 查看所有服务日志

```bash
# 实时查看服务日志
tail -f /tmp/flare-*.log

# 查看特定服务日志
tail -f /tmp/flare-signaling-online.log
tail -f /tmp/flare-message-orchestrator.log
tail -f /tmp/flare-push-server.log
tail -f /tmp/flare-access-gateway.log
tail -f /tmp/flare-api-gateway.log
```

### 过滤日志内容

```bash
# 查看 Push Server 的路由日志
tail -f /tmp/flare-push-server.log | grep -E "(gateway|routing|Found.*online)"

# 查看 Message Orchestrator 的消息处理日志
tail -f /tmp/flare-message-orchestrator.log | grep -E "(message|jetstream|hook)"

# 查看 Access Gateway 的连接日志
tail -f /tmp/flare-access-gateway.log | grep -E "(connect|disconnect|login)"
```

---

## 环境变量参考

### 核心服务环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `GATEWAY_ID` | Access Gateway 唯一标识 | 自动生成 |
| `GATEWAY_REGION` | Access Gateway 所在地区 | 无 |
| `PORT` | Access Gateway 监听端口 | 60051 |
| `DATABASE_URL` | PostgreSQL 连接URL | 从配置文件读取 |
| `REDIS_URL` | Redis 连接URL | 从配置文件读取 |
| `JETSTREAM_BOOTSTRAP_SERVERS` | JetStream 连接地址 | 从配置文件读取 |

### 客户端环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `NEGOTIATION_HOST` | 网关地址（格式：`host:port`） | `localhost:60051` |
| `GATEWAY_ENDPOINT` | Core Gateway 地址 | `http://localhost:50050` |
| `MESSAGE_CONTENT` | 消息内容 | `Hello from business system` |
| `USER_IDS` | 目标用户ID列表（逗号分隔） | 空（推送给所有在线用户） |
| `TOKEN_SECRET` | JWT Token 密钥 | 必须显式提供，或直接提供 `TOKEN` |
| `TENANT_ID` | 租户ID | `0` |
| `BUSINESS_USER_ID` | 业务系统用户ID | `business-system` |

---

## 完整示例

### 示例1：单地区部署

```bash
# 1. 启动基础设施
cd deploy && docker compose up -d

# 2. 启动所有核心服务（包括多网关实例）
cd ../flare-im-core
./scripts/start_multi_gateway.sh

# 3. 等待服务启动（约15秒）
sleep 15

# 4. 启动客户端（连接到默认网关）
cargo run --example chatroom_client -- user1

# 5. 在另一个终端启动第二个客户端
cargo run --example chatroom_client -- user2
```

### 示例2：多地区部署

```bash
# 1. 启动基础设施
cd deploy && docker compose up -d

# 2. 启动所有核心服务（包括多网关实例）
cd ../flare-im-core
./scripts/start_multi_gateway.sh

# 3. 等待服务启动（约15秒）
sleep 15

# 5. 连接到北京网关
NEGOTIATION_HOST=localhost:60051 \
cargo run --example chatroom_client -- user1

# 6. 连接到上海网关（新终端）
NEGOTIATION_HOST=localhost:60052 \
cargo run --example chatroom_client -- user2

# 7. 业务系统推送消息（新终端）
cargo run --example business_push_client
```

### 示例3：业务系统集成

```bash
# 1. 确保所有服务已启动
./scripts/check_services.sh

# 2. 通过 Core Gateway 推送消息给所有在线用户
cargo run --example business_push_client

# 3. 推送给指定用户
USER_IDS=user1,user2 \
MESSAGE_CONTENT="Custom message" \
cargo run --example business_push_client

# 4. 使用自定义 Core Gateway 地址
GATEWAY_ENDPOINT=http://localhost:50050 \
cargo run --example business_push_client
```

---

## 相关文档

- [架构设计文档](../doc/消息流程架构设计.md)
- [跨地区网关路由设计](../doc/跨地区网关路由设计.md)
- [跨地区网关路由使用指南](../doc/跨地区网关路由使用指南.md)
- [消息上下行流程设计](../doc/消息上下行流程设计.md)

---

**最后更新**：2025-11-17
**维护者**：Flare IM Core Team

## 测试脚本
```
# 默认（非调试）
./scripts/start_server.sh single

# 调试模式，所有服务打 trace 日志（含 SQL）
./scripts/start_server.sh single debug

# 自定义 RUST_LOG 再开调试（例如只看 sqlx）
RUST_LOG=info,sqlx=trace ./scripts/start_server.sh single debug
```
