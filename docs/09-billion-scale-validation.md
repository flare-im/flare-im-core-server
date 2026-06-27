# 亿级落地验证标准

本文是 [08-billion-scale-evolution-plan.md](08-billion-scale-evolution-plan.md) 的验证配套，定义 Flare IM Core 在统一读扩散内核、低延迟、0 可观测丢失和大群降本方向上的测试门禁。

## 验证目标

| 目标 | 本地/CI 可验证项 | 生产压测/演练项 |
|------|------------------|-----------------|
| 统一投递原语 | `EventEnvelope` / `PushEventRequest` typed 字段、inline event、pure ping、arch-tests | 客户端连续性校验、inline 关闭后 sync 收敛 |
| 大群 notify+pull | recipient-less ping、Push Server pre-pagination coalescing、participant pagination、online-only task、ping debounce、pull rate limit | 10 万人大群单消息成本 O(在线成员)，高频连发不会按消息数重复扫描成员 |
| 0 可观测丢失 | WAL fail-closed、storage idempotency、DLQ replay CLI、retry/DLQ arch-tests | 杀 Redis/MQ/PostgreSQL/任一服务实例后的最终必达演练 |
| 读扩散性能 | user_version index、hot tail cache、Redis online backend | 热缓存命中率 >95%、冷启动 100 会话 <=2s |
| 大群未读降本 | `large_conversation_precise_unread_threshold` 阈值守卫 | 大群未读显示近似化，@提及精确索引独立验收 |

## 本地确定性门禁

每次修改投递、同步、未读、在线过滤、DLQ 或 proto 合同时，至少运行：

```bash
cargo fmt --all -- --check
cargo check -p flare-orchestrator -p flare-im-service-kit -p flare-im-message-pipeline -p flare-push-server -p flare-push-worker -p flare-signaling-gateway -p flare-conversation -p flare-dlq-replay -p flare-im-contracts
cargo test -p flare-orchestrator -p flare-push-server -p flare-push-worker -p flare-signaling-gateway -p flare-conversation -p flare-im-message-pipeline -p flare-dlq-replay
cargo test -p flare-im-arch-tests
```

当前本地门禁覆盖：

- 小会话 persistent push 走 `EVENT_MESSAGE + PING_WITH_INLINE`，携带完整 `Message` inline payload。
- 大群 persistent push 走 recipient-less `EVENT_MESSAGE + PING`，push topic 不携带物化收件人列表。
- Push Server 在解析成员前按 `(tenant_id, conversation_id)` 合并 recipient-less ping，窗口内只保留最高 `max_conversation_seq`。
- `MESSAGE_ORCHESTRATOR_INLINE_MESSAGE_PUSH_ENABLED=false` 时所有 persistent realtime push 退化为 pure ping。
- Push Server 对 recipient-less ping 通过 ConversationReadService 分页解析成员，并只对在线用户发任务。
- Push Server 可直读 Redis `session:{user_id}` 做在线过滤，gRPC online status 为显式 fallback。
- Access Gateway sync pull limiter 不影响 ACK / cursor update。
- Push Worker ping debounce 对 tenant/user/conversation 合并重复 ping。
- Conversation 大群 unread 写扩散被成员数阈值保护。
- `flare-dlq-replay` 支持 dry-run、Kafka/NATS producer、payload size guard 和 replay headers。

## CI 等价门禁

合并前建议在完整 CI profile 运行：

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

如果 CI 环境没有 Kafka/NATS/Redis/PostgreSQL 实例，应保持所有外部依赖测试可显式 ignored 或使用 fake/in-memory boundary；不要让非确定性网络环境决定单元测试成败。

## 压测门禁

亿级标准不能只靠本地单元测试证明，必须在可观测压测环境补齐以下场景：

| 场景 | 验收 |
|------|------|
| 单聊乒乓 | P99 broker accepted ACK <= 50ms，端到端 inline 命中 <=150ms |
| 10 万人大群单消息 | push topic payload 不随成员数线性膨胀；下行 ping task 仅针对在线成员 |
| 10 万人大群连发 100 条 | `event_ping_coalesce_window_ms=200` 下 Push Server 成员分页扫描次数不随消息数线性增长，trailing ping 携带最高 conversation seq，reader 热缓存命中率 >95% |
| 登录风暴 | 10 万设备冷同步不打穿 reader，tenant/user pull limiter 生效 |
| inline 泄压 | 关闭 `MESSAGE_ORCHESTRATOR_INLINE_MESSAGE_PUSH_ENABLED` 后吞吐平稳，客户端通过 sync 收敛 |
| Redis online backend | `online_status_backend=redis` 与 `grpc` fallback 在相同数据下结果一致 |

## 混沌与恢复门禁

| 故障 | 期望 |
|------|------|
| Redis WAL 不可用 | 持久消息 fail-closed，不返回 broker accepted |
| MQ 暂时不可用 | WAL 保留，重试或 replay 后 storage 幂等收敛 |
| Storage Writer 崩溃 | MQ redelivery，ledger 不进入错误终态或可恢复 |
| Push Server 崩溃 | push topic redelivery；offline/ping 由 sync 兜底 |
| DLQ 注入 | `flare-dlq-replay --dry-run` 可解析，受控重放后 ledger/event stream 幂等 |
| 网络分区 | 客户端按 `SyncRecoveryHint` 或 user_version 差异重新拉取 |

## 文档与发布要求

- 任何稳定语义必须写入 typed proto/model/config，不能只靠 `metadata` 或 MQ header。
- 修改投递原语时同步更新 01、02、06、08、09 文档和 arch-tests。
- 删除 `TOPIC_PUSH_MESSAGES` 旧路径前，必须完成客户端能力协商、灰度两周以上、非持久 push-only 入口迁移和回滚演练。
- @提及/回复精确索引必须作为 typed 服务端索引落地后，才能声明 Phase 2.3 全部完成。
