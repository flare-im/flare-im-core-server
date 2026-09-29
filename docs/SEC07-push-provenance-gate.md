# SEC-07 Push 事件来源门禁与接线顺序

状态：**阻断 / 候选设计**。本文件是对当前代码的信任边界审计与实施验收清单，**不是 Push 已可启用的声明**。截至本文检查时，`SignedPushRecordV1` 和 `flare-im-service-kit::push_provenance` 只是可复用的签名/验证原语；没有生产者、消费者、Broker ACL、持久去重或旧队列切换接线。`PUSH_PROXY_EVENT_PROVENANCE_MODE`、`PUSH_SERVER_EVENT_PROVENANCE_MODE`、`PUSH_WORKER_EVENT_PROVENANCE_MODE` 的 `signed-v1` 必须继续拒绝启动/入队。

## 1. 当前数据流和不能跨越的信任断点

| 阶段 | 代码位置 / 函数 | 当前事实 | 接线前置条件 |
| --- | --- | --- | --- |
| 已认证消息入口 | `flare-message-ingest/src/interface/grpc/send_handler.rs` 的 `verified_send_ctx`、`authorize_message_claims` | gRPC 入口验签并核对签名 tenant、actor 与消息属性；这是直接发送路径的可信起点。 | 签名生产者只能接受已经授权的 `Ctx`，不能只因持有服务进程私钥就为任意调用者签 Push。 |
| 直接 Push 生产 | `crates/flare-im-message-pipeline/src/messaging/push_repository.rs` 的 `push_only_message`、`push_only_event`、`push_only_message_ping`、`push_only_message_inline_event`、`publish_push_envelope` | 往旧版 `flare.im.push.messages/events/envelope` 发送裸 `MqEnvelope`/`PushEnvelope`；`build_headers_from_ctx` 对缺租户可回退 `0`。`publish_push_envelope` 还把传入的 envelope headers 覆盖到从 `Ctx` 得到的 headers。共享仓储同时被 Ingest 与 Orchestrator 使用，不能在这个通用方法中无条件给当前 `Ctx` 背书。 | 各调用点分别证明来源；签名前强制 canonical tenant、目标和 body/header 租户一致，不得默认补 `0`、不得信任用户可控 header。 |
| 持久消息主队列 | 同上 `publish_message` → `flare.im.message.main`；`flare-orchestrator/src/interface/mq/storage_consumer.rs` 的 `handle` | `MqEnvelope` 没有独立 tenant 字段。Orchestrator 合并外层 MQ header 与内层 `MqEnvelope.headers`，且**内层覆盖外层**，然后 `context_from_mq_metadata` 重建普通 `Ctx`。这一步没有验证 Main 消息来源或租户。 | 先给主队列建立经过认证的来源（独立签名的版本化 Main record，或等价的可信持久化证明），并在 Orchestrator 解码/扇出前验证；不能在 Orchestrator 仅凭该 `Ctx` 签派生 Push。 |
| Orchestrator 派生 Push | `flare-orchestrator/src/application/handlers/storage_handler.rs::handle_envelope`、`domain/service/message_fanout_service.rs::persist_and_push_with_recipients`、`crates/flare-im-message-pipeline/src/messaging/push_repository.rs` | 目前从上述重建的 `Ctx` 生成 Push event。若直接在此签 `SignedPushRecordV1`，会把未经认证的 `x-tenant-id` **洗白为已签名租户**。 | Main record 认证、body/tenant/会话关系校验、已验证来源 ID 向下传递后，才允许派生记录签名。 |
| Push Proxy | `flare-push/proxy/src/interface/grpc/push_handler.rs` 的 `require_verified_actor`、`require_tenant_id`、`require_delivery_provenance_ready`；`infrastructure/mq_publisher.rs` | gRPC 要求带签名的 Service actor，入口已暂停；publisher 仍发裸 `PushEnvelope`，`publish_push_message` 则用 `MqEventBus` 写 `push_request_topic`，与 Push Server 当前注册的 `push.messages/events/envelope` 消费路径不完全同构。 | 明确 `push_request` 的真实消费者或删除未用路径；验证每个 RPC 的 actor 权限与目标 tenant 后才签，且在 Server/Worker 可安全消费前继续拒绝新入队。 |
| Push Server 入站 | `flare-push/server/src/service/wire.rs::initialize` 与 `interface/messaging/{message_consumer,event_consumer,push_consumer}.rs::handle` | 三个旧 topic 注册裸载荷处理器，`PushRouterHandler::request_tenant_id` 只验证 MQ `Ctx` 中 tenant 的格式，并不证明来源；各错误路径可能 DLQ 后 ACK。启动门禁目前在 `service/bootstrap.rs` 且拒绝 `signed-v1`。 | 只订阅专用 `.v1` topic；在**任何**解析业务载荷、读 Redis、路由、DLQ+ACK 前，用实际 delivery topic/key 验签；再比较 signed tenant、body tenant（`PushEnvelope`）及已证明的请求上下文。对无内嵌 tenant 的 `MqEnvelope`，来源 tenant 必须由已验证 record 注入下游 `Ctx`，不能复用 header `Ctx`。 |
| Server → Worker | `flare-push/server/src/application/handlers/push_router_handler.rs` 的 `publish_targeted_tasks_inner`、`publish_event_broadcast_task`；`infrastructure/mq/publisher.rs` | Server 把多用户 Push 拆成在线分片、单用户离线任务，发裸 `PushTaskEnvelope` 到旧版 `.online/.offline`。任务本身有 tenant 但无来源证明。 | Server 用独立 producer key 为每一条派生任务签名；`source_event_id` 指向已验证入站事件，`event_id` 为稳定且区分 online chunk/offline user 的派生 ID；payload、tenant、topic、key 均绑定。 |
| Worker → Gateway / 设备供应商 | `flare-push/worker/src/interface/messaging/{online_consumer,offline_consumer}.rs` 的 `handle`/`handle_batch`，`application/handlers/online_gateway_delivery.rs` | 当前直接解裸 task。在线 batch 用首条 `message.context.ctx` 调度，虽然分组包含 tenant，却未检查 MQ header tenant 与 task tenant；离线可写 Redis outbox 或调用 APNs/FCM/个推。启动门禁目前在 `service/bootstrap.rs` 且拒绝 `signed-v1`。 | 每条任务在 batch 分组前验 Server key、来源 ID、body tenant、目标/会话和 payload 内目标的一致性；从已验证 record 构建 tenant `Ctx`；持久 replay 状态在首次外部副作用前占位，成功后完成，失败按可重试/终态分类。不得宣称外部推送 exactly-once。 |
| WAL replay | `flare-message-ingest/src/application/handlers/wal_replay_handler.rs::replay_once`、`replay_ctx` | 从 Redis WAL 行取 tenant，缺失时回退默认 tenant，再构造无 `VerifiedContext` 的 `Ctx` 重发。 | 单独审计 WAL 写入时的已授权 tenant 与行完整性；旧行缺 tenant 不可自动签作 `0`，需要隔离/修复；稳定 replay ID 必须复用原消息身份。 |

### Broker 元数据不是身份

`flare-server-core/flare-core-messaging/src/mq/kafka/consumer.rs::decode_message` 的 topic/key 来自 Kafka record（key 缺失时仍会回退 header），而 NATS `mq/nats/consumer.rs::decode_message` 的 subject 来自 broker、`MessageContext.key` **仅由生产者可写的 `x-message-key` header 重建**。因此 NATS 上不能把 `MessageContext.key` 描述成 broker 强制的分区键；签名可以覆盖该 header 的值并验证一致性，但真正的发布者限制仍必须依赖 NATS 账户/subject ACL 和生产者私钥隔离。Kafka v1 记录必须拒绝真实 key 缺失，不能借 header fallback 冒充 broker key；需要在 MQ 适配层暴露可区分的 broker key 或专用已核验投递元数据。

`flare-server-core/flare-core-messaging/src/mq/nats/config.rs::default_stream_specs` 的 Push subject 是 `flare.im.push.*`，**不匹配** `flare.im.push.messages.v1` 等五段 subject；`flare-im-core/deploy/kafka-topics.sh` 也只创建旧 topic。`.v1` 命名、stream/topic、retention、容量和 ACL 尚未落地。现有 `flare.im.push.*` 与 `.v1` 不能混用同一 consumer，也不得给裸 backlog 事后补签。

## 2. 建议实施顺序（每一步均保持 Push 暂停）

1. **冻结契约和拥有者**：列出所有入站 producer（Ingest 直接 Push、Orchestrator 派生、Push Proxy、WAL replay）和 Server 派生 producer；为每个 `producer_id` 定义只允许发布的精确 `.v1` topic、独立 Ed25519 key、公开验证 key/kid 窗口、payload schema、最大有效期和来源 ID。`SignedPushRecordV1` 的应用层验证不能只比较 tenant：必须核对业务 payload 的 target user/device、conversation、event/message ID 与来源事件。留出旧签名 key 验证窗口直到所有合法 backlog 与 replay 状态过期；紧急撤销有独立操作。
2. **先认证上游**：直接 gRPC 发送用已验证 `Ctx`；Main path 在 Ingest/WAL 产生可验证的版本化记录、Orchestrator 消费时验签并从记录重建 tenant `Ctx`。WAL 缺失/冲突 tenant 的历史行隔离，不用默认 `0`。Orchestrator 不能从 MQ headers 或未经校验的 protobuf map 推断可信 tenant。`MqPushRepository` 的共享方法需要显式接收已验证来源能力/不可伪造的验证结果，避免普通 `Ctx` 即可签名。
3. **准备 Broker 隔离**：为 `messages.v1`、`events.v1`、`envelope.v1`、`online.v1`、`offline.v1` 及 protected quarantine/DLQ 定义独立精确 topic/subject；NATS stream pattern 必须实际覆盖，Kafka topic 必须预建并验证既有配置（`--if-not-exists` 不修改旧 topic）。按 producer/consumer 身份部署最小 publish/subscribe ACL，验证拒绝跨 producer/topic 读写。内网可达不等于 ACL。规划峰值 backlog、`MAX_PUSH_RECORD_TTL_MS` 24h 与 broker retention 的一致性：超过签名有效期的记录不得重签继续普通投递。
4. **接生产者，但不双写旧 topic**：可信来源校验后签原始 raw protobuf bytes，绑定实际 publish topic/key、tenant、schema、稳定 event ID；`Push Proxy` 保持“消费者未就绪不可接受任务”。避免因重试产生新的 event ID；派生 task ID 包含来源 ID、目标 user/chunk、在线/离线路径及版本，防止两个目标互相去重。
5. **接 Server/Worker 验证与持久去重**：两级消费者都先验签、schema、topic/key、issued/expires/key window，再解 body；每条记录将 `PushReplayKey` 与 payload digest 存于可靠、tenant-scoped 的持久状态，冲突 digest 必须告警而非视为成功重复。只有经验证的 tenant `Ctx` 可触达在线索引、Gateway 或离线供应商。处理中的租约、崩溃恢复、成功完成、可重试 NACK、终态 quarantine+ACK 的次序要覆盖 batch 和单条；Kafka 与 NATS 分别验证 ACK/offset 语义。
6. **隔离旧 backlog 后切换**：先在隔离环境采集旧 topic/stream 的开始/结束 offset 或 stream sequence、数量、年龄、缺 tenant/无法归属数量；旧裸记录不能自动搬运为可信 v1。安排停发、旧消费者处置/人工授权迁移、已签 v1 开始位置与回滚位置，保持 v1 consumer group 与旧组隔离。先用生产镜像验证旧/新 payload 永不共 topic，按租户小流量试运行，观察 lag、quarantine、重复率、密钥验证失败率和推送到达率；只有两级均验收且回滚手册可执行，才考虑改变三个 `signed-v1` gate。期间禁止通过改环境变量绕过 gate。

## 3. 可执行验收清单

以下每项需要留下测试命令、通过结果、运行环境、topic/subject ACL 导出和可复现的证据；勾选前 Push 仍视为不可用。

- [ ] 单元：`cargo test -p flare-im-service-kit push_provenance --lib` 覆盖签名篡改、topic/key/schema/tenant 不一致、未知/撤销 key、过期/未来时间、重复 ID 的 digest 冲突；`cargo check -p flare-push-proxy -p flare-push-server -p flare-push-worker --all-targets`、`cargo fmt --all -- --check` 通过。当前工作区磁盘不足时这些命令**未执行**，不可把静态检查等同于 Rust 回归。
- [ ] 集成：伪造 Main MQ header、内层 header 覆盖、缺 tenant WAL、跨 tenant `PushEnvelope` 和跨 topic 重放均**不能**产出 signed v1 Push；可信 Ingest 与 WAL 重放产生相同稳定业务事件身份。
- [ ] 集成：每种 v1 入站原始 schema 只被其允许的 producer 签；Server 解码前拒绝未签/错签；派生 task 使用不同 producer key 且 `source_event_id` 可追溯至已验入站记录；Worker 在单条与 batch 前拒绝跨 tenant、跨 target、错 conversation、重复但 digest 冲突任务。
- [ ] 故障注入：Redis 去重存储、Kafka/NATS、quarantine/DLQ、Online/Gateway、APNs/FCM 任一不可用时，不得在外部副作用未确认或 quarantine 未持久写入时 ACK/提交 offset；NATS redelivery 与 Kafka retry/commit 分别覆盖崩溃窗口。外部供应商超时需承认可能重复，并用供应商幂等 key/业务去重降低风险。
- [ ] Broker：不同凭据的 publish/subscribe ACL 实测；NATS `.v1` subjects 解析到正确 stream；Kafka v1 topics 的 partition、RF、`min.insync.replicas`、retention 实际值检查；真实 broker topic/key 与签名 frame 一致。确认 24h 过期策略不会把仍需投递的有效 backlog 变为不可处理，并证明容量足以承受计划内暂停时间。
- [ ] 切换：记录旧 backlog 快照和逐类处置结果，完成 staging/生产等价环境的 offset/sequence 演练、灰度、回滚；监控 `invalid_signature`、`unknown_key`、`wrong_tenant`、`replay_conflict`、`quarantine_write_failed`、lag 和端到端到达率。安全/运维负责人审阅后才解除启动和入队门禁。

相关现有背景见 [`SEC07-tenant-cache-keys.md`](SEC07-tenant-cache-keys.md)。
