# 11 超大群与可靠性整改方案

日期：2026-06-12

本文是 [08-billion-scale-evolution-plan.md](08-billion-scale-evolution-plan.md) / [10-100k-large-group-test-report.md](archive/10-100k-large-group-test-report.md) 的后续：08 给方向，10 验证了 100k 分页 pure-ping 的**功能正确性**，本文针对一次面向 100k 会话的代码级复检中发现的、**功能测试覆盖不到的规模与可靠性问题**，逐条给出落地方案。

核心结论一句话：读扩散改造已正确消灭了**推送 payload** 的 O(成员) 放大，但一条消息在 100k 群里穿过的成员成正比基底有四层，目前只砍掉了最表层一层；其余三层（ingest 物化、user_sync per-member 写、presence per-user 读）仍是 O(成员)，且 user_sync 的当前模型与"大群不物化成员"的终极形态在数学上互斥。

## 问题总表

| ID | 严重度 | 问题 | 现状落点 | 状态 |
|----|--------|------|----------|------|
| P1 | 🔴 阻断 | user_sync 大群 eager per-member 模型与"不物化成员"互斥（设计死结） | `event_domain_service.rs:330` | 待改造 |
| P2 | 🔴 高 | user_sync 写入是 **per-user 串行 EVAL**（100k 群 = 10万次串行 Redis 往返） | `redis_user_sync_index.rs:125` | 待改造 |
| P3 | 🔴 高 | presence `fetch_statuses` **per-user 串行 GET**（非 MGET/pipeline） | `online/.../redis/repository.rs:206` | 待修复 |
| P4 | 🔴 高 | 无 per-conversation 在线成员索引：查大群在线 = 扫全员 O(成员) | 缺失 | 待新增 |
| P5 | 🟠 中 | ingest 大群仍全量物化成员（无 cap），MQ 信封承载 100k user_id | `message_ingest_service.rs:269`、`topic_envelope.proto:20` | 待改造 |
| P6 | 🔴 高 | user_sync 写入在投递关键路径上（`.await?`），Redis 抖动→大群 fanout 全停 | `event_domain_service.rs:330` | 待解耦 |
| P7 | 🟠 中 | 单 Redis 故障域 = seq+WAL+presence+user_sync+cache 五合一 | 部署/config | 待拆分 |
| P8 | 🟠 中 | WAL fail-closed 契约未落地（Redis 不可用时行为未定义） | ingest WAL 分支 | 待补 |
| P9 | 🟡 低 | DLQ 有重放 CLI 但无 depth 告警 | `tools/flare-dlq-replay` | 待补 |
| P10 | 🟠 中 | 大群成员解析在 push 消费内同步 200 页，阻塞 consumer | `push_router_handler.rs:592` | 待解耦 |
| P11 | 🟡 低 | ping coalesce 状态在进程内存，多实例不跨实例合并 | `ConversationPingCoalescer` | 待下沉 |
| P12 | 🟡 低 | MQ 主题分区键 conversation_id，10万人热群=单分区热点 | topic 设计 | 待分片 |
| P13 | 🟢 治理 | `doc/` 与 `docs/` 并存 | 仓库根 | 待归档 |
| P14 | 🟢 治理 | service-kit 6.6K 未拆（08 W4 列了未排期） | `crates/flare-im-service-kit` | 待排期 |
| P15 | 🟢 治理 | gateway 8041 行最大，call_signal 子模块待收敛 | `flare-signaling/gateway` | 待收敛 |

> 核查更正：`logs/` 与 `data/` 经 `git ls-files` 确认**未被跟踪**，无需处理。上一轮报告中此项作废。

---

## A. 10 万人会话：解开"伪读扩散"四层基底

### P1（阻断）user_sync 模型重构：大群从"写时 per-member 推"改为"读时 per-conversation 拉"

**根因**：`record_conversation_change(user_ids, conversation_id, …)` 依赖 ingest 物化的**全量成员列表**作为输入，对每个成员写一份 user 版本。这意味着：要优化掉 P5（大群不物化成员），就抽走了 user_sync 的数据来源 → 大群离线成员 version 不再 bump → sync 兜底静默失效 → **漏消息（破坏 0 丢失）**。eager 模型与"不物化成员"互斥。

**方案**——按会话规模二分，引入**会话版本**作为大群的一级索引：

```text
小群（成员 ≤ user_sync_eager_threshold，初始 500）：
  维持现状 eager：per-member 写 user changes（延迟最优，成本可忽略）。

大群（成员 > 阈值）：
  orchestrator 只 bump 一个会话版本（O(1)，不碰成员）：
    INCR conv:version:{tenant}:{conversation_id}
    HSET conv:state:{tenant}:{conversation_id} max_seq … updated_at …
  不再 per-member ZADD。

客户端拉取改为以"我的会话列表"为驱动（O(我的会话数)，非 O(群成员数)）：
  上线/心跳：带上本地各会话的已知 conv_version（或仅大群子集）
  服务端用 user 自己的会话成员关系批量比对 conv:version
  返回"版本落后的 conversation_id 列表" → 走现有会话级 seq 增量
```

**契约扩展**（向后兼容）：`SyncSessionHints` 增 `conversation_versions: map<conversation_id, uint64>` 下行；`GetSyncCursorSync` 已有会话维度，复用。小群路径协议不变。

**新增 trait 方法**（`UserSyncIndexRepository`）：

```rust
/// 大群：只递增会话版本，不展开成员（O(1)）。
async fn record_conversation_version_bump(
    &self, ctx: &Ctx, conversation_id: &str,
    max_conversation_seq: u64, occurred_at_ms: i64,
) -> Result<u64>;

/// 拉取侧：批量比对用户已知版本，返回落后的会话。
async fn diff_changed_conversations(
    &self, ctx: &Ctx, known: &[(String /*conv*/, u64 /*ver*/)],
) -> Result<Vec<ConversationChange>>;
```

**fanout 接线**（`event_domain_service.rs` / `message_fanout_service.rs`）：在 `should_use_notify_pull_ping` 同一阈值判定处分流——小群走 `record_conversation_change`，大群走 `record_conversation_version_bump`。**阈值与 push 分流阈值共用同一配置**，保证"谁不物化成员，谁就用会话版本"。

**成本/收益**：1-2 周（架构级）。100k 群 user_sync 写从 10万次/条 → **1 次/条**；同时解开与 P5 的死结，使 P5 可安全实施。

**验收**：新增契约 `user_sync_large_group_uses_conversation_version`（大群 fanout 路径禁止调用 per-member `record_conversation_change`）；e2e：100k 群离线成员上线后能通过会话版本 diff 拉到漏掉的消息。

---

### P2（高）user_sync 写入串行 → 批量 pipeline

**根因**：`redis_user_sync_index.rs:125` `for user_id in user_ids { Script::invoke_async().await? }`，每个成员一次往返。

**方案**：
- 大群路径被 P1 消除（O(1)），不再适用。
- 小群 eager 路径仍可能有数百成员：把 per-user 串行循环改为**单次 `pipe.atomic()` 批量** EVAL，或一个接收 user_ids 列表、循环内嵌的 Lua（一次往返处理整批）。优先后者（减少 RTT 到 1）。

**成本**：0.5 天。**收益**：500 人小群 user_sync 从 500 RTT → 1 RTT。

---

### P3（高）presence 串行 GET → MGET/pipeline

**根因**：`online/.../redis/repository.rs:206` `for user_id in user_ids { load_user_records }`，逐个查。push 侧"批量 is_online"在协议层是数组，实现层退化成串行——100k 群过滤一次 = 10万次串行 Redis 读。10 号功能测试 mock 了 OnlineReader，未暴露。

**方案**：`fetch_statuses` 内部改批量。presence 记录是 per-user 多设备（`load_user_records` 读 user→connections），用 **pipeline 一次提交全部 user 的查询**；若 key 结构是 `online:{tenant}:{user}` 的 set/hash，用 pipeline 批 `SMEMBERS`/`HGETALL`。保持 `fetch_statuses(&[String]) -> HashMap` 签名不变，仅换内部实现。

**成本**：0.5-1 天（纯实现修复，无接口变更）。**收益**：在线过滤 RTT 从 O(成员) → O(页数)。**这是性价比最高的单点修复，建议第一个做。**

---

### P4（高）新增 per-conversation 在线成员索引

**根因**：即便 P3 批量化，"找 100k 群的在线成员"仍需扫全部 100k（200 页 × 批量查），因为没有"谁在线"的倒排。本质成本是 O(总成员) 而非 O(在线成员)。

**方案**——presence 维护会话→在线成员 Set，随连接生命周期增删：

```text
用户上线（gateway 连接建立 → online 服务）：
  对该用户所属的大群集合，SADD conv:online:{tenant}:{conv} {user_id}
  （大群成员关系由 conversation 服务提供；只对大群维护，小群不需要）
用户下线 / 心跳超时：
  SREM conv:online:{tenant}:{conv} {user_id}
大群 ping 解析在线成员：
  SMEMBERS conv:online:{tenant}:{conv}   → O(在线成员)，直接得到在线集
```

push 侧 `handle_conversation_ping_without_recipients` 改为：大群优先查 `conv:online` 倒排（命中即 O(在线)），不再分页扫全员 + 逐页过滤。全员分页仅作为倒排未建立时的回退。

**一致性**：倒排是软状态，可由心跳重建；漏增漏删导致的偏差由 user_version/sync 兜底（最坏是该用户靠拉取收敛，不丢）。

**成本**：1 周。**收益**：100k 群在线解析从 O(10万) → O(在线数，通常数百~数千)。配合 P1，大群单条消息的服务端成本全面降到与"在线规模"成正比，与"注册成员规模"解耦。

---

### P5（中）ingest 大群停止全量物化

**根因**：`get_message_recipients` 无 cap，大群拉全量成员，`MqEnvelope.recipient_user_ids` 序列化 100k user_id（数 MB 单消息）。

**方案**（依赖 P1 先完成，解开死结后才安全）：
- ingest 查成员前先取 `member_count`（conversation 服务已维护）；超过 `ingest_materialize_threshold`（与 P1/push 阈值共用）则**不查全量、不物化**，`recipient_user_ids` 留空，仅在 `MqEnvelope.large_conversation` typed 字段标记为 `true`。
- orchestrator 见大群标记：走 `record_conversation_version_bump`（P1）+ recipient-less pure ping（已有），成员解析下沉 push 侧的 `conv:online` 倒排（P4）。
- 单聊/小群维持现状物化（延迟最优）。

**契约**：`mq_envelope_boundary` 增断言——大群 MqEnvelope 不得携带 `recipient_user_ids`。

**成本**：3-5 天。**收益**：消灭 MQ 信封 O(成员) 体积与 ingest 成员查询；三层基底（P1/P4/P5）合并收口。

---

## B. 0 丢失：解除 Redis 关键路径耦合

### P6（高）user_sync 写入移出投递关键路径

**根因**：`event_domain_service.rs:330` `record_conversation_change(...).await?` 的 `?` 让 user_sync Redis 故障直接令 fanout 返回 Err → 消息 Nack。正确性不丢（幂等+重投），但**可用性**：user_sync Redis 宕 = 所有（含大群）消息 fanout 全停。

**方案**：user_sync 不是投递真源（sync 拉取才是），降级为**尽力而为 + 补偿**：
- fanout 中 user_sync 写入失败**不阻断**主链：记 metric、写本地补偿 outbox（复用离线 outbox 同款 Redis Stream 或内存有界队列），后台 worker 重放。
- 主链继续 push（实时性不受 user_sync 影响）；user_sync 最终一致即可，因为它只服务"离线/重连用户的拉取提示"，而拉取本身有会话级 seq 兜底。

```rust
if let Some(idx) = &self.user_sync_index {
    if let Err(e) = idx.record_conversation_version_bump(ctx, &conv, seq, ts).await {
        self.user_sync_compensation.enqueue(conv, seq, ts); // 不 ? 传播
        tracing::warn!(error=%e, "user_sync bump deferred to compensation");
    }
}
```

**成本**：2-3 天。**收益**：user_sync Redis 故障从"发送停摆"降级为"同步提示延迟"，故障半径回到 0 丢失定义假设的"单组件"。

---

### P7（中）Redis 按职责拆故障域

**根因**：seq（强一致真源）、WAL（不丢关键）、presence（可重建）、user_sync（可重建）、cache（可丢）全压一个 Redis，故障半径过大。

**方案**：config 先逻辑命名 profile，部署时物理隔离：

```toml
[redis.seq]       # 强一致，独占，开 AOF everysec，不与他人争内存
[redis.wal]       # 关键，AOF everysec
[redis.presence]  # 可重建，可用 cluster
[redis.sync]      # user_version/会话版本，可重建
[redis.cache]     # 会话尾部热缓存，可丢，可用 LRU maxmemory
```

各 crate 从对应 profile 取连接（service-kit 已有 `redis_profile(name)`，按用途传 name 即可）。

**成本**：1 周（含部署）。**收益**：cache 打满不影响 seq；presence 抖动不拖垮发送。

---

### P8（中）WAL fail-closed 契约落地

**根因**：WAL 介质 Redis，但 Redis 不可用时 ingest 行为未定义——继续发（破坏 0 丢失承诺）还是拒发（需明确降级语义）未在代码/契约固化。

**方案**：在 ingest 写 WAL 分支显式区分：
- **持久消息** + WAL 写失败 → **fail-closed 拒发**，返回错误让客户端重试（绝不返回 BROKER_ACCEPTED）。
- **临时消息**（push_only）→ 降级为 `TRANSIENT_ACCEPTED` 放行（本就不承诺存储恢复）。
- 部署确认 Redis-WAL 开 **AOF everysec**（这是 0 丢失定义里 ≤1s 窗口的数学来源）。

新增契约 `wal_fail_closed_for_durable`（断言持久消息路径在 WAL 错误时不产出 BROKER_ACCEPTED）。

**成本**：1-2 天。

---

### P9（低）DLQ depth 告警

**根因**：`tools/flare-dlq-replay` 能重放（✅），但无人知道 DLQ 在涨。

**方案**：push/orchestrator 暴露 `*_dlq_depth` gauge（消费组 lag 或 stream 长度）；Prometheus 告警规则 depth > 0 持续 N 分钟。重放 runbook 引用 CLI。

**成本**：0.5 天。

---

## C. 亿级横向扩展

### P10（中）大群成员解析与投递解耦

**根因**：`handle_conversation_ping_without_recipients` 在 MQ 消费回调内同步串行 200 页解析，单个热群首 ping 阻塞整个 consumer。

**方案**：P4 的 `conv:online` 倒排命中后，解析成本本身降为 O(在线)；对仍需全员回退的场景，把"成员分页解析"从消费回调中剥离为**独立的有界并发任务**（解析与 publish 解耦，consumer 快速返回），或对大群成员做 TTL 快照缓存避免每窗口重扫。

**成本**：与 P4 合并，约 3 天增量。

---

### P11（低）ping coalesce 跨实例

**根因**：`ConversationPingCoalescer` 是进程内 Map，多 push-server 实例各自 coalesce，同一大群的 ping 散落多实例无法合并。

**方案**：大群消息按 `conversation_id` hash 路由到固定 push-server 实例（MQ 分区键对齐），使同群 ping 落同一实例，进程内 coalesce 即生效；或将 coalesce 窗口状态下沉 Redis（`SET conv:ping:pending NX PX window`）。优先前者（零额外依赖）。

**成本**：2-3 天。

---

### P12（低）超大群 MQ 分区热点

**根因**：主题分区键 conversation_id，10万人活跃群=单分区热点，吃满单分区吞吐。

**方案**：仅对识别为超大群的会话启用**子分片键**（`conversation_id#shardN`），消费侧按会话聚合；保持普通会话单分区有序。需与有序性保证协调（同会话内仍需 seq 有序，子分片要求消费侧按 seq 重排或接受分片内有序）。列为容量触顶后的专项，不前置。

**成本**：1-2 周（设计 + 实现），按需启用。

---

## D. 目录与架构治理

### P13（治理）doc/ 归档
`doc/`（旧设计）与 `docs/`（现行）并存是认知负担。方案：`doc/` 整体移入 `docs/legacy/` 或归档分支，README 注明。成本 0.5 天。

### P14（治理）service-kit 拆分排期
6.6K 行 7 关切，全员扇入。按已验证的 capability-core 模式拆 `flare-im-grpc-clients` / `flare-im-observability` / `flare-im-gateway-auth`，config+runtime-plan 留本体；拆后加依赖方向契约。补进 08 W4 排期。成本 1-2 周。

### P15（治理）gateway call_signal 收敛
gateway 8041 行（全仓最大），call_signal 子模块随 flare-call 接线完成后应收敛：gateway 只留连接侧信令路由，FSM 归 flare-call（`call_boundary` 契约已锁 conversation，补锁 gateway 不得持有 CallSession）。成本随 flare-call 接线。

---

## 实施序列（按"100k 解锁度 × 成本"）

```text
第 1 步（本周，止血+最高 ROI）
  P3 presence MGET 批量          (0.5-1d) ── 纯实现修复，立即可验证
  P9 DLQ depth 告警              (0.5d)
  P13 doc/ 归档                  (0.5d)

第 2 步（解死结，架构级，2-3 周）
  P1 user_sync 会话版本模型       (1-2w) ── 阻断项，先行
  P2 user_sync 小群批量          (0.5d，并入 P1)
  P6 user_sync 移出关键路径       (2-3d)
  P8 WAL fail-closed 契约         (1-2d)

第 3 步（成员基底收口，1-2 周）
  P4 per-conv 在线倒排            (1w)
  P5 ingest 大群不物化            (3-5d，依赖 P1)
  P10 成员解析解耦                (并入 P4)

第 4 步（横扩与治理，按需）
  P7 Redis 故障域拆分            (1w)
  P11 coalesce 跨实例 / P12 分区分片 / P14 service-kit 拆分 / P15 gateway 收敛
```

**关键路径**：P1 是唯一阻断项——它解开 user_sync 与"不物化成员"的死结，使 P5 安全、P2 失效（被消除）、P4 价值最大化。**P3 是独立的最高 ROI 单点**，可与 P1 并行立即落地。

## 验收门（每步必过）

- **契约门**：`user_sync_large_group_uses_conversation_version`、`mq_envelope_no_recipients_for_large_group`、`wal_fail_closed_for_durable` 三个新断言进 arch-tests。
- **压测门**：100k 全在线群单消息端到端 P99；100k 群 10 msg/s 持续下 Redis user_sync / presence QPS 曲线（验证从 O(成员) 降到 O(在线)）；user_sync Redis 注入故障，验证 fanout 不停摆（P6）。
- **混沌门**：按 08 §0 形式化定义，逐一杀 redis-seq / redis-sync / redis-presence，验证已 BROKER_ACCEPTED 必达、发送不因 sync/presence 故障停摆。

---

**收束**：10 万人会话真正的成本不在 payload，在它下面成员成正比的三层基底（ingest 物化、user_sync 写、presence 读），而 user_sync 的 eager per-member 模型恰与"大群不物化成员"的终极形态数学互斥。**先做 P1 把大群 user_version 从"写时推"改成"读时拉"，它同时解锁 100k 吞吐墙、0 丢失兜底链与成员基底收口——是整个亿级演进里唯一的架构级关键路径；其余皆为沿途的性能修复与治理。**
