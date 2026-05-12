# 消息事件完整流程图

## 规范：消息上下行链路（约定表述）

- **上行**：`Client → Gateway → Router → Orchestrator → JetStream`
- **下行**：`JetStream → Push Server → Gateway → Client`

上行由客户端发起，经网关注册、路由与编排后写入 JetStream；下行由 JetStream 消费端（Push Server）调度，经同一 Gateway 实例下发给对端客户端。文档与代码中的流程描述均以此为准。

---

以下是消息、撤回、编辑等事件的完整流程示意图，适用于文档和架构说明：

```
                        ┌────────────────────┐
                        │      Client A      │
                        └─────────┬──────────┘
                                  │ WebSocket
                                  ▼
                        ┌────────────────────┐
                        │   Signaling Gateway │
                        └─────────┬──────────┘
                                  │
                                  ▼
                        ┌────────────────────┐
                        │   Signaling Route   │  ← 顺序保证 / 流控 / 权限校验
                        └─────────┬──────────┘
                                  │
                                  ▼
                        ┌────────────────────┐
                        │ Message Orchestrator│  ← 事件归一化 & 入 JetStream
                        │  • 普通消息         │
                        │  • 撤回消息         │
                        │  • 编辑消息         │
                        │  • 自定义事件       │
                        └─────────┬──────────┘
                                  │
                                  ▼
                              ┌────────┐
                              │ JetStream  │  ← 事件总线，保证幂等与顺序
                              └──┬───┬─┘
                                 │   │
                ┌────────────────┘   └────────────────┐
                ▼                                     ▼
      ┌──────────────────┐                ┌──────────────────┐
      │ Storage Writer    │                │   Push Server     │
      │ (持久化消费者)    │                │ (在线推送调度)    │
      └─────────┬────────┘                └─────────┬────────┘
                │ DB 写入                           │
                │                                   │
                │                                   ▼
                │                         ┌────────────────────┐
                │                         │ Signaling Online   │  ← 查接收方在线 & gateway_id
                │                         └─────────┬──────────┘
                │                                   │
                │                                   ▼
                │                         ┌────────────────────┐
                │                         │  Gateway Router     │  ← 按 gateway_id 调 Access Gateway
                │                         └─────────┬──────────┘
                ▼                                   ▼
        ┌──────────────┐                    ┌────────────────────┐
        │  Database     │                    │ Access Gateway      │
        │ • 保存消息     │                    │ (User B 所在网关)   │
        │ • 更新撤回状态 │                    └─────────┬──────────┘
        │ • 更新编辑内容 │                              │
        └──────────────┘                              ▼
                                                     ┌────────────────────┐
                                                     │      Client B      │
                                                     └────────────────────┘
```

## 流程说明

1. **消息发送（普通消息）**
   - **上行**：Client A → Gateway → Router → Orchestrator → JetStream（Storage + Push 两条流）
   - **下行**：JetStream → Push Server → Gateway → Client B（在线推送）；离线由 Push Worker / 拉取补充

2. **消息撤回**
   - **上行**：Client A → Gateway → Router → Orchestrator → JetStream（撤回事件）
   - **下行**：JetStream → Push Server → Gateway → Client B（推送撤回事件）；Storage Writer 更新 DB `deleted = true`

3. **消息编辑**
   - **上行**：Client A → Gateway → Router → Orchestrator → JetStream（编辑事件）
   - **下行**：JetStream → Push Server → Gateway → Client B（推送编辑事件）；Storage Writer 更新 DB `content = new_content`

4. **统一事件总线（JetStream）**
   - 所有事件（消息、撤回、编辑、其他自定义事件）统一入 JetStream
   - 消费者可按事件类型分发给 Storage 或 Push

5. **Signaling Route（Router）作用**
   - 保证单个会话顺序
   - 流控，避免瞬时高并发压力
   - 权限和合法性校验

6. **Push Server**
   - 在线用户即时收到事件
   - 消费 topic 需与 Orchestrator 的 push topic 一致（如 `flare.im.push.tasks`），部署时注意配置对齐
   - 离线用户通过离线队列或 DB 查询补充消息

7. **Online 服务**
   - Gateway 登录/心跳时向 Online 注册（user_id、gateway_id）；Push Server 推送前调用 Online 批量查询接收方在线状态与所在 gateway_id。
   - Route 的 `select_push_targets` 用于上行或管理侧按需查询；下行在线推送由 Push Server 直接查 Online，再经 Gateway Router 调 Access Gateway。

8. **Push Worker / Push Proxy**
   - Worker：消费离线推送 topic（如 `flare.im.push.offline`），处理离线或重试任务。
   - Proxy：提供入队推送的 gRPC/HTTP 入口，写入 JetStream 后由 Server/Worker 消费。

## 各服务与 flow 对应

| 链路 | 环节 | 服务/模块 | 职责 |
|------|------|-----------|------|
| 上行 | 入口 | flare-signaling/gateway | 接收 WebSocket，解析 ClientPacket，转发 SendMessage / SendEvent / Sync* |
| 上行 | 路由 | flare-signaling/route | 顺序保证、流控、权限校验，转发至 Orchestrator |
| 上行 | 编排 | flare-orchestrator | 普通消息 → StoreMessage + 事件；操作 → ExecuteEvent → Event，写入 JetStream |
| — | 连接 | flare-signaling/online | 连接注册、设备路由、在线状态；下行时 Push Server 查在线与 gateway_id |
| — | 事件总线 | JetStream | 普通消息 topic、推送 topic（flare.im.push.tasks）、操作事件 topic，保证幂等与顺序 |
| — | 持久化 | flare-storage/writer | 消费 JetStream，普通消息落库，操作事件更新 DB（撤回/编辑/已读等） |
| 下行 | 调度 | flare-push/server | 消费 JetStream 推送 task，经 Online 查在线与 gateway_id、Gateway Router 调 Access Gateway → **Gateway** → Client |
| 下行 | 离线 | flare-push/worker | 消费离线推送 topic（如 flare.im.push.offline），处理离线/重试推送任务 |
| — | 推送入口 | flare-orchestrator（待实现 PushService） | 对外 gRPC：入队推送消息/通知/ACK，写入 JetStream，由 Push Server/Worker 消费 |

---

## 下行消息整流程（端到端）

规范表述：**下行 = JetStream → Push Server → Gateway → Client**。以下展开实现细节（Online 查在线、Gateway Router 调 Access Gateway 等），用于排查「收不到对方消息」或配置不一致问题。

### 1. Orchestrator：写入推送队列

| 项目 | 说明 |
|------|------|
| 模块 | `flare-orchestrator` |
| 入口 | 普通消息：`message_domain_service` 提交后 `publish_both(storage, push)` 或 `publish_push(push)`；撤回/编辑等：`message_event_publisher` / `message_operation_service` 内 `publish_push(push_req)` |
| 格式 | `PushMessageRequest`（protobuf），含 `user_ids`、`message`（common.Message）、`options` |
| 写入 | JetStream topic = **jetstream_push_topic**（配置：`push_topic` / `PUSH_TOPIC`），默认 `flare.im.push.tasks` |
| 配置 | `config/services/message_orchestrator.toml` → `push_topic`；或环境变量 |

单聊时 `user_ids` 由 `build_push_request` 设为 `[receiver_id]`，必须非空。

### 2. Push Server：消费并调度

| 项目 | 说明 |
|------|------|
| 模块 | `flare-push/server` |
| 消费 | JetStream **task_topic** 需与 Orchestrator 的 push topic **一致**（如 `flare.im.push.tasks`） |
| 解码 | `PushMessageRequest::decode(payload)`，失败则跳过并 commit offset |
| 逻辑 | `dispatch_push_message` → `convert_message_request_to_tasks` → `process_tasks`：<br>① 批量查在线：`online_repo.batch_get_online_status(user_ids)`（实际调用 **Signaling Online**）<br>② 按 `gateway_id` 分组在线用户，离线入 `offline_tasks`<br>③ 按 gateway 并发调用 `push_to_gateway_batch`（内部**按用户**构造 `PushMessageRequest`，避免推错人）<br>④ 离线用户走 `handle_offline_tasks`（可写 `flare.im.push.offline`） |
| 配置 | `config/services/push_server.toml` → `task_topic`；或 `PUSH_SERVER_TASK_TOPIC` |

Online 返回的 `gateway_id` 来自 Gateway 登录时注册的 `server_id`，若为空则视为离线。

### 3. Signaling Online：在线与网关解析

| 项目 | 说明 |
|------|------|
| 模块 | `flare-signaling/online` |
| 写入 | Gateway 连接建立后调用 `login(LoginRequest)`，请求中含 `server_id`（即该实例的 **gateway_id**） |
| 查询 | Push Server 通过 `SignalingOnlineClient.batch_get_online_status(user_ids)` → gRPC `GetOnlineStatus`，返回每用户的 `online` 与 **gateway_id** |
| 约定 | 同一 Gateway 实例上所有连接共享同一 `gateway_id`；多实例时 Push Server 按 `gateway_id` 选实例 |

### 4. Gateway Router：按 gateway_id 调 Access Gateway

| 项目 | 说明 |
|------|------|
| 模块 | `flare-im-core/src/gateway/router.rs`（被 Push Server / Push Worker 使用） |
| 输入 | `gateway_id`、`PushMessageRequest`（access_gateway 协议：`target_user_ids`、`message`） |
| 行为 | 服务发现按 **instance_id == gateway_id** 解析目标实例，建立/复用连接，调用 Access Gateway 的 `PushMessage` |
| 配置 | Push Server wire 中 `access_gateway_service`、`local_gateway_id`、`deployment_mode` 等 |

### 5. Access Gateway（Signaling Gateway）：下发给连接

| 项目 | 说明 |
|------|------|
| 模块 | `flare-signaling/gateway`，实现 Access Gateway gRPC（含 `PushMessage`） |
| 处理 | `handle_push_message`：将 `message` 封装为 **EventEnvelope**（EventMessage），encode 后对每个 `target_user_id` 调用 `process_single_user` |
| 本地状态 | `check_user_online` / `get_filtered_connections` 查询**本实例** ConnectionManager（user_id → connection_id），仅本实例连接的用户能收到 |
| 发送 | `push_to_connections` → `push_message_to_connection(connection_id, message_bytes)`，通过 Flare 的 `send_to` 下发 Frame（MessageCommand，payload = EventEnvelope 序列化） |

客户端协商的压缩（如 Gzip）在连接层处理；payload 为未压缩的 EventEnvelope 或由连接层压缩后传输。

### 6. 配置对齐检查清单

| 配置项 | Orchestrator | Push Server | 说明 |
|--------|--------------|-------------|------|
| Push Topic | `push_topic` / `jetstream_push_topic` = `flare.im.push.tasks` | `task_topic` = `flare.im.push.tasks` | 必须一致，否则 Push Server 收不到；两处默认均为 `flare.im.push.tasks`，可通过配置或环境变量覆盖 |
| Gateway 注册 | — | — | Gateway 启动时向 Online 注册的 `gateway_id` 需与实例一致；Push Server 服务发现能按该 id 解析到对应实例 |
| 单实例部署 | — | `deployment_mode` = `single_region` 或 `local_gateway_id` 与唯一实例 id 一致 | 确保路由到唯一 Gateway |

### 7. 常见问题

- **收不到对方消息**：① 确认两端连接的是同一 Gateway 或 Push Server 能解析到接收方所在 Gateway；② 确认 Orchestrator 已发 push、Push Server 消费无解码错误；③ 客户端对下行 payload 若为压缩需先解压再解析（如 EventEnvelope / ServerPacket）。
- **user_ids 为空**：单聊时 Orchestrator 的 `build_push_request` 必须设置 `receiver_id`，从而 `user_ids = [receiver_id]`，否则 Push Server 会直接报错。
- **Online 返回无 gateway_id**：检查 Gateway 登录是否成功、LoginRequest 的 `server_id`（gateway_id）是否与配置一致，以及 Online 是否持久化/返回该字段。