# IM 初始化与重连同步设计（工业级）

> 本文档是 flare-storage 初始化/重连同步的**唯一**权威设计，合并了此前重叠的
> `im_sync_design.md`（早期草案）与 `im_sync_best_practice.md`（取代前者的最佳实践）。

## 一、目标

- 秒开（低延迟）
- 不丢消息（强一致）
- 多端一致
- 可续传、客户端幂等
- 可扩展（支持万级数据）

## 二、核心思想

> 初始化同步 = Snapshot（状态） + Event（变化）

并对 Event 做“分级处理”，而非全量回放。核心不变量：**seq 连续**、**事件完整
（message + delete + update）**、**支持断线续传**、**客户端幂等**。

## 三、事件分级策略

### 1. 必须回放（强一致）

- EVENT_MESSAGE_RECALL
- EVENT_MESSAGE_EDIT
- EVENT_MESSAGE_DELETE
- EVENT_CONVERSATION_UPDATE
- EVENT_CONVERSATION_DELETE
- EVENT_PIN / UNPIN
- EVENT_MARK / UNMARK

说明：影响最终状态，必须参与 seq 补齐。

### 2. 可聚合（优化性能）

- EVENT_READ_RECEIPT
- EVENT_REACTION

策略：不逐条回放，服务端聚合为最终结果。

```json
{ "msgId": "xxx", "readCount": 10 }
```

### 3. 不回放（瞬时事件）

- EVENT_TYPING
- EVENT_PRESENCE
- EVENT_CALL_SIGNAL

策略：登录后重新订阅，不参与历史同步。

## 四、初始化同步流程（第一次登录）

### Step 1：Snapshot（轻量）

客户端请求 `GET /sync/init`，服务端返回当前可见数据的快照（不返回历史全量、
不含已删除消息）：

```json
{
  "conversations": ["...前 50 条"],
  "messages": ["...每个会话最新 20 条"],
  "snapshotSeq": 10000
}
```

### Step 2：关键事件回放

客户端请求 `GET /sync/events?fromSeq=10000`，只回放删除 / 编辑 / 撤回 / 会话变更：

```json
[
  { "seq": 10001, "type": "delete" },
  { "seq": 10002, "type": "edit" }
]
```

### Step 3：实时订阅

WebSocket 推送所有后续事件。

### Step 4：后台分页加载

历史消息按需加载，用户无感知。

## 五、断线重连 / 再登录

请求：`GET /sync?fromSeq=lastSeq`

- 返回后续事件；若 `seq` 过期 → 返回错误，客户端重新 snapshot。

服务端优化：

1. 事件压缩：多次 edit → 保留最后一次；delete 覆盖 edit。
2. 会话聚合：

```json
{ "convId": "xxx", "lastMsg": "...", "unread": 10 }
```

## 六、客户端处理

1. 应用 snapshot：`applySnapshot(data)`
2. 回放事件（幂等）：`if (event.seq <= lastSeq) return`
3. 更新 seq：`lastSeq = event.seq`
4. ACK：`ack(lastSeq)`
5. 分批更新 UI：`batchApply(events, 50)`

## 七、删除设计

删除必须作为事件存在，不可物理删除、必须占用 seq：

```json
{ "seq": 10002, "type": "delete", "msgId": "xxx" }
```

## 八、服务端存储

- **Message 表**（当前状态）：`msg_id` / `seq` / `content` / `status`（normal / deleted）
- **Event 表 / Event Log**（完整事件流）：`seq` / `type` / `target_id`
- **Snapshot** 定期生成

## 九、最佳实践总结

- Snapshot 控制体积（只给可见数据）
- Event 分级处理（不是全回放）
- seq 保证一致性
- ACK 保证可靠性
- WebSocket 保证实时性

> 一句话：IM 同步的本质不是“拉数据”，而是“恢复状态 + 回放变化”。
