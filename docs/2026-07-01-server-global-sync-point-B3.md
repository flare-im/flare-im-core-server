# B3 服务端全局同步点（user-version 增量索引）

> 客户端收敛重设计（`flare-im-core-sdk/docs/superpowers/plans/2026-07-01-sync-convergence-redesign.md`）已完成并认证（436 测试/0 warning/4 次 app 验证）。客户端多路复用路由(E1/E3)+changed-only 收敛(B4)已就位，目前走**每会话 diff 降级路径**。本计划补服务端，让 catch-up 真正 O(增量)。

## Goal
让断线重连/冷启 catch-up 从 O(会话数) 降到 **O(变化数)**：客户端一次拿到"自上次 user_version 以来**哪些**会话变了 + 各自 max_seq"，再一次 `MultiConversationSync` 批量拉这些会话。可检查：
1. 服务端**真实填充** `SyncSessionHints.{server_user_version, changed_conversation_ids, conversation_versions}`（当前恒空/0）。
2. 给定 `client user_version` → 返回该版本后变化的 conversation 集合（有界、分页 `user_version_index_truncated` 语义已在 proto）。
3. 客户端用返回集合走 `MultiConversationSync`（已存在）批量拉取，替换每会话 diff 降级路径。
4. 大账号(1000+会话)重连：请求数从 O(会话) 降到 ~2（hints + 批量），端到端延迟显著下降。

## 现状锚点（已勘察）
- Sync 协议 = 单 RPC `SyncService.ExecuteSync(common.v1.Sync) → SyncRes`（`flare-grpc-proto/proto/sync_service.proto`）。`Sync` 是 oneof 信封（`flare-proto/proto/sync.proto:168`），含 kinds：`single_conversation / multi_conversation / conversations_incremental / conversations_all / query_events / sync_snapshot / conversation_max_seq / conversations / conversation_participants / ...`。
- **已有但未填充的 B3 底座**（`sync.proto` `SyncSessionHints`）：`server_user_version(7)`、`changed_conversation_ids(8)`、`user_version_index_truncated(9)`、`conversation_versions(10) ConversationVersion{conversation_id,version,max_conversation_seq}`、`server_max_conversation_seq(5)`。**proto 字段齐，服务端无填充逻辑**（rust 中 `server_user_version`/`changed_conversation_ids` 仅出现在 client-sdk，服务端 grep 无）。
- **已有可复用批量拉取**：`MultiConversationSync{conversation_ids, last_conversation_seq_per_conversation, limit_per_conversation}` → 一次拉多会话增量。
- Sync 分发/查询在 **`flare-storage/reader`**（`application/handlers/query_handler.rs`、`infrastructure/persistence/optimized_postgres_store.rs`、`domain/repository/message_storage.rs`）。
- seq 基础设施 `crates/flare-im-seq`：**per-conversation** 租约分段发号（`LeasedSegmentAllocator`），无 user-level 全局 seq。
- sync-orchestrator (`flare-sync-orchestrator`) 已建模 `SyncIntent{InitialBootstrap/OfflineCatchUp/Incremental}` + `cursor_policy`。

## 设计方向：user-version 增量索引（非新增全局 seq）
不引入设备级全局 seq（与 per-conversation 租约发号冲突大）。而是维护**每用户的"变更索引"**：`(user_id, conversation_id) → version/max_conversation_seq/updated_at`，`server_user_version` = 该用户索引的单调水位。
- 会话有新消息/关键事件 → 更新该用户所有成员的索引行 + 抬升 user_version 水位。
- 客户端带 `client user_version` 请求 hints → 服务端返回 `> client_version` 的 `changed_conversation_ids` + `conversation_versions`（有界；超限置 `user_version_index_truncated=true` 让客户端回退全量摘要）。
- 客户端据此 `MultiConversationSync` 批量拉。

## Constraints & decisions
- **复用 proto，不加新 kind**（字段已在 `SyncSessionHints`；批量拉用 `MultiConversationSync`）——最小侵入。
- **降级安全**：`user_version_index_truncated` / 索引缺失 → 客户端回退现有全量摘要 + 每会话 diff（B4 已实现，零风险回退）。
- **写放大权衡**：user-version 索引在消息写路径更新"该会话所有成员的索引行"——大群写放大。缓解：大群用 fan-out-on-read（与 F4 频道读扩散同源，见客户端计划 Phase F），或索引仅记 conversation 级 version + 成员按需 join。**这是本计划最大的设计决策点，需定**。
- 遵循 flare-im-core 分层（DDD/CQRS）：索引读写归 storage，编排归 sync-orchestrator。

## Status: CLIENT BATCH PATH DONE（server hints optional）
Current focus: B3-1 决策完成，服务端 `MultiConversationSync` handler 已存在；客户端 B3-2/B3-3 已在 core-sdk 接线并验证。剩余 B3-4 是 server hints 填充优化，用于免去客户端摘要 diff；非 correctness 必需。

## Steps
- [x] B3-1 定索引模型 — **决策：方案 B（会话级 version + 成员按需 join on read）**。不建每成员索引行 → **无写放大**；changed set 在读时由"客户端已知版本 vs 会话当前 version"diff 得出（会话摘要增量已携带各会话 max_conversation_seq）。真正省的是把每会话 catch-up(N 请求)合并为一次 `MultiConversationSync`(1 请求)。
> **重大 de-risk 发现**：方案 B 下服务端**基本已就绪**——`MultiConversationSync` handler **已完整实现**（`flare-sync-orchestrator/src/application/handlers/sync_orchestration_handler.rs:520-567`：按 conversation_ids 批量返回 slices + max_seq_per_conversation + has_more）；`ConversationVersionIndexPort` 已存在（会话级 version 底座）。→ **B3 收敛为一处客户端 batch 接线 + 至多小幅 server hints 填充，非多微服务大工程**。changed set 客户端已能从会话摘要增量(各会话 max_conversation_seq) diff 得出(B4 已做)，无需先造 user-version 索引。

- [x] B3-2 客户端 batch 拉取能力 — `sync_request_use_case` / adapter 已走 `request_multi_conversation(ids, last_seq_per_conv, limit)`；`SyncResPayload::MultiConversation` 每个 slice 转单会话 page 形状并复用既有应用逻辑落库/发布/推游标。verify: `sync_protocol_adapter` 6 passed。
- [x] B3-3 接线收敛路径 — `MessagesSyncTask` changed set 已改为 attention ordered batch：最高优先会话单独先拉，其余按有界批量 `sync_multi_conversations_with_context` 拉取；single_conversation 仍保留给单会话打开/补拉路径。verify: `sync_task::messages` 3 passed；web Playwright E2E messaging flow 1 passed。
- [optional(server)] B3-4（可选增强）server hints — 填充 `SyncSessionHints.{server_user_version, changed_conversation_ids}` 让客户端免摘要全量 diff（进一步省）；依赖会话 version 索引，方案 B 读时 join。**非必需**（B3-2/3 已达 O(1请求) catch-up）。
- [~] B3-5 验证：单会话打开路径不变、WASM 重建、web app 消息补齐无回归已验证；1000+ 会话大账号请求数压测仍需真实数据集/服务端环境。

## Notes / open questions
- **写放大是核心权衡**（B3-1 必须先定）——直接决定大群可行性；与客户端 Phase F(频道读扩散) 强相关，宜合并考虑。
- 索引漏标 = 客户端欠拉（correctness 风险）→ 需"索引 vs 真实 max_seq"审计/自愈（周期校验 `conversation_max_seq`）。
- 客户端 B4 降级路径保留作 truncated/索引缺失兜底，**不删**（此处非 pre-release 无兼容——是有意的双路径降级，correctness）。
- 可与 F4(频道读扩散)、G6(媒体) 合并为一份 flare-im-core 服务端全栈计划分阶段推进。
