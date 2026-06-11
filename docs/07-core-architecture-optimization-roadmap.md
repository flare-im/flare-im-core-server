# Core 架构优化落地方案

本文记录针对 `flare-im-core` 网关边界、message ingest、存储选型和统一鉴权的优化方案。目标不是兼容旧本地原型，而是把当前已经拆出的生产主链写硬、写清楚，并继续删除 MongoDB 与旧双写心智。

## 0. 当前落地状态

截至 2026-06-11，已完成第一轮无兼容清理：

- `flare-signaling/gateway` 已使用 `flare-server-core::auth::TokenValidator` 作为长连接认证边界，并校验 token device_id 与连接 device_id。
- `AccessGatewayServiceConfig` 已增加一等 `auth` provider 配置，支持 `core_jwt` 与 `http_hook`；`http_hook` 模式不再要求 Core JWT secret。
- `conversation.ensure` 已改为 protobuf `MqEnvelope(EventCustom)` 投递，conversation consumer 不再解析旧 JSON `EventEnvelope` 或 direct protobuf `Event` payload。
- send path 已增加架构契约测试，要求 conversation 不承载 send/seq，api-gateway 与 signaling route 的消息发送入口收敛到 `flare-message-ingest`。
- `flare-storage` 文档已明确 PostgreSQL / TimescaleDB 是唯一 durable storage database family；MongoDB 不再作为 Core 存储路径。
- `flare-storage/writer` 已抽出 MQ backend failure policy：Kafka 使用 retry topic + DLQ + retry-forwarder，NATS/JetStream 使用 broker-native retry/NACK + DLQ，并由 lib 测试覆盖 ledger ACK failure/retry。

## 1. 架构方向

目标架构采用一条客户端长连接、一个消息摄入真源、一个持久化数据库族和一个统一鉴权抽象：

```mermaid
flowchart LR
    Client["Client / IM SDK"] --> SGW["flare-signaling/gateway<br/>single long connection"]
    Business["Business / third-party"] --> CGW["flare-api-gateway<br/>HTTP facade"]
    Admin["Admin / ops"] --> AGW["flare-admin-gateway<br/>admin facade"]

    SGW --> Route["flare-signaling/route"]
    Route --> Ingest["flare-message-ingest<br/>send command + seq + WAL"]
    CGW --> Ingest
    CGW --> Conversation["flare-conversation<br/>metadata/read model"]
    AGW --> Reader["flare-storage/reader<br/>audit/query"]

    Ingest --> MQMain["flare.im.message.main"]
    MQMain --> Orchestrator["flare-orchestrator<br/>fanout"]
    Orchestrator --> MQStorage["storage/events topics"]
    Orchestrator --> MQPush["push topics"]
    MQStorage --> Writer["flare-storage/writer"]
    Writer --> PG[("PostgreSQL / TimescaleDB")]
    Writer --> Redis[("Redis hot cache / WAL / idempotency")]
    MQPush --> Push["flare-push/server"]
    Push --> SGW
```

核心结论：

- 客户端长连接统一由 `flare-signaling/gateway` 承载，消息、ACK、sync、custom data、call signaling 在连接内逻辑多路复用。
- `flare-api-gateway` 是业务系统和三方 HTTP facade，不再作为客户端 WebSocket 入口。
- `flare-conversation` 不再承担消息发送、seq 分配或通话生命周期，只维护会话元数据、成员、游标、未读和读模型。
- `flare-call` 拥有通话会话业务 FSM；`flare-signaling/gateway` 只处理连接侧信令路由，`flare-capability` / 插件只处理 RTC/SFU 能力编排。
- `flare-message-ingest` 是上行消息写入真源，负责校验、Hook、seq、WAL、conversation ensure 和主消息流发布。
- `flare-storage/writer` 异步消费 MQ，使用 PostgreSQL / TimescaleDB 持久化消息、事件和 ledger。
- MongoDB 不再出现在 Core 持久化路径中；Redis 不是主存储，只做热状态和短期恢复辅助。
- 鉴权统一由 `flare-server-core::auth::TokenValidator` 抽象负责，HTTP 和长连接网关只做传输适配。

## 2. 取舍分析

### 一条连接还是多条连接

推荐客户端一条主长连接。

优点：

- 客户端生命周期简单，断线重连、设备绑定、心跳、限流和 backpressure 只维护一套。
- 消息、ACK、sync、call signaling 可在同一连接中按 frame 类型隔离，避免两个 WebSocket 入口竞争认证、路由和在线状态。
- 下行推送只需通过 online/route 找到一个 connection owner。

保留例外：

- 音视频媒体流不进入 IM 长连接，走 SFU/WebRTC 自己的媒体通道。
- 业务服务和管理后台继续走 HTTP/gRPC facade，不复用客户端长连接。

### message-ingest 是否独立

推荐保持独立，并把所有 send path 收敛到它。

理由：

- seq 分配、WAL、broker accepted ACK 和幂等属于高吞吐写入链路，不应绑定 conversation metadata 事务。
- conversation 的扩缩容维度是读模型、成员和会话状态，不应被大群消息写入 QPS 拉爆。
- 后续可对 `flare-message-ingest` 独立做限流、队列延迟观测、Redis seq 分片、WAL replay 和压测。

### PostgreSQL + TimescaleDB 是否替代 MongoDB

推荐完全替代。

理由：

- 消息、事件、ledger、审计查询之间有强关系索引需求，PostgreSQL 更适合租户、会话、seq、状态和审计维度。
- TimescaleDB 能处理按时间分区、压缩和冷热历史消息查询。
- 删除 MongoDB 可避免同一份消息写两处导致的分布式事务、补偿和排障复杂度。

边界：

- JSONB 可以承载业务扩展快照，但稳定协议语义必须是 typed proto / enum / columns。
- 大媒体对象仍走 S3 兼容对象存储，数据库只保存元数据和引用关系。

### auth sidecar 还是共享 auth provider

推荐先用共享 auth provider，sidecar 作为部署形态保留。

理由：

- `flare-server-core::auth::TokenValidator` 已经是传输无关抽象，可覆盖 Core JWT、trusted issuer 和 HTTP Hook。
- HTTP gateway 与长连接 gateway 可以复用同一套 validator，不需要先增加独立进程和网络跳转。
- 如果未来需要 Envoy/ext_authz、OPA、SSO 或零信任网格，可以把同一套 provider 包装成 `flare-auth-sidecar`，但 gateway 仍依赖同一个 principal contract。

## 3. 推荐方案

### 服务职责

| 服务 | 保留职责 | 明确禁止 |
|------|----------|----------|
| `flare-signaling/gateway` | 客户端长连接、AUTH frame、心跳、frame 多路复用、连接 metadata、下行投递 | 写消息状态、直接落库、暴露业务 HTTP API |
| `flare-api-gateway` | 业务/三方 HTTP typed facade、认证上下文、错误映射、下游 gRPC 调用 | 客户端 WebSocket、消息持久化、会话事务 |
| `flare-admin-gateway` | 管理面、审计、运维查询、管理员权限 | 客户端接入、业务主链写入 |
| `flare-message-ingest` | send command、幂等、校验、Hook、seq、WAL、conversation ensure、MQ main publish | 会话读模型、存储事务、推送执行 |
| `flare-conversation` | 会话元数据、成员、游标、未读、presence 聚合、conversation ensure 消费 | send message、seq 分配、消息体落库、通话生命周期 FSM |
| `flare-call` | 通话会话生命周期、业务 FSM、通话命令处理 | 长连接路由、媒体/SFU 控制、会话读模型 |
| `flare-im-capability-core` | 能力/插件共享契约、dispatch DTO、guard/resolver/RTC 端口 | 服务运行时、gRPC server、路由登记簿、持久化 |
| `flare-orchestrator` | MQ main 消费、storage/push fanout、操作事件执行、capability enrich | 客户端协议适配、conversation metadata 事务 |
| `flare-storage/writer` | 消息归档、事件流、ledger、热缓存、ACK、retry/DLQ | 路由、实时推送、业务权限 |
| `flare-storage/reader` | 历史消息、事件、ledger、审计查询 | 写入主链、状态变更 |

### 消息写入边界

```text
Client / Gateway
  -> flare-message-ingest
  -> Redis WAL when persistent
  -> MQ main with stable idempotency key
  -> broker accepted ACK
  -> flare-orchestrator fanout
  -> storage topic / push topic
  -> storage-writer PostgreSQL transaction
  -> ledger + ACK + sync convergence
```

ACK 语义：

- `TRANSIENT_ACCEPTED`：临时消息只进入实时投递，不承诺存储和离线恢复。
- `BROKER_ACCEPTED`：持久消息默认成功边界，表示 MQ 已接受，存储和推送异步收敛。
- `PERSISTED`：只给确实需要同步落库确认的内部场景，不作为默认发送路径。

### 存储边界

PostgreSQL / TimescaleDB：

- `messages`：消息归档主表，按 `created_at` hypertable。
- `events`：durable event stream，以 `tenant_id + conversation_id + seq` 幂等。
- `message_write_ledger`：写入阶段诊断和最终幂等。
- conversation tables：会话、成员、设置、游标等关系元数据。

Redis：

- seq allocator backend。
- sending/writer WAL。
- hot cache。
- idempotency window。
- presence and short-lived connection state。

MQ：

- main、storage、events、push、retry、DLQ topics。
- 消费者失败通过 nack/retry/DLQ 处理，不回退到同步双写。

## 4. 实施形态

### Phase 0: 边界固化

- 更新文档，删除 MongoDB 存储路径描述。
- 在架构文档写明一条客户端长连接策略。
- 写明 conversation 不承载 send/seq。
- 把 signaling/gateway 认证迁移到 `TokenValidator` 适配层。

### Phase 1: 鉴权配置统一

- 给 `AccessGatewayServiceConfig` 增加与 core/admin 一致的 auth provider 配置。
- 支持 `ACCESS_GATEWAY_AUTH_MODE=core_jwt|http_hook`。
- 长连接 AUTH frame 构造 `TokenValidationRequest`，携带 connection_id、trace_id、device_id、tenant hint。
- 对 token device_id 与连接 device_id 做一致性校验；不一致直接认证失败。

### Phase 2: send path 收敛验证

- 已增加架构契约测试，禁止 conversation 新增 send/seq 职责。
- api-gateway `message/send` 与 signaling route `forward_message` 已通过契约测试守住 `flare-message-ingest` 边界。
- 后续业务 typed gRPC 若新增发送入口，必须复用 `message_ingest_send` / MessageIngest route，不得直连 conversation 或 orchestrator send path。

### Phase 3: PostgreSQL / TimescaleDB 强化

- 确认 `deploy/init.sql` 覆盖 messages、events、ledger、conversation metadata 的索引和 hypertable。
- storage-reader 查询只依赖 PostgreSQL / TimescaleDB 和 Redis cache，不引入第二主存储。
- 写入失败路径标准化为 backend-specific retry/DLQ + ledger error；Kafka 使用 retry topic + DLQ + retry-forwarder，NATS/JetStream 使用 broker-native retry/NACK + DLQ。
- storage-writer retry/DLQ policy 已契约化；ledger ACK failure/retry 测试覆盖。

### Phase 4: 旧 payload 路径清理

- conversation event consumer 已移除旧 JSON `EventEnvelope` 和 direct `Event` payload 分支，只接受 protobuf `MqEnvelope`。
- 删除旧文档、旧配置名和历史 prototype 描述。
- 对 SDK 和平台包确认 ACK durability、sync convergence、pending state 的语义一致。

## 5. 扩展性

- MQ 后端保持 JetStream/Kafka 二选一，同一环境不要双跑同一链路。
- seq allocator 当前可以基于 Redis，未来可按 tenant/conversation shard 拆分。
- auth provider 可以从库内 `TokenValidator` 扩展为 sidecar/mesh ext-authz，但 gateway principal contract 不变。
- capability、RTC/SFU、审核、风控、机器人只通过 Hook/Capability 接入，不成为 Core 必需依赖。
- TimescaleDB 可按租户规模引入 retention/compression policy 和归档导出 worker。

## 6. 瓶颈与风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| Redis seq 热点 | 超大群或单会话高 QPS 下 Redis key 热 | 分 conversation shard、批量号段、观测 allocator latency。 |
| MQ lag | broker accepted 后存储延迟变大 | 暴露 per topic lag、storage writer consumer lag、ledger stuck rows。 |
| storage writer DB 压力 | 批量写入和索引维护导致尾延迟 | 批量 insert、连接池调优、Timescale compression、冷热分层。 |
| Hook 慢或不可用 | 发送链路尾延迟和失败率上升 | 主链 Hook 短超时、fail-fast、旁路 Hook async retry。 |
| auth provider 不统一 | SSO/JWT 改造需要多处改代码 | 所有 gateway 只依赖 `TokenValidator` 和 principal contract。 |
| 旧 payload 分支重新出现 | 消费者逻辑复杂、误解析、排障困难 | contract/test 固化 protobuf `MqEnvelope` 入口，禁止恢复 JSON/direct payload 分支。 |
