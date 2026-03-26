# Flare IM 存储设计（对齐 flare-proto）

> 依据 `flare-proto/IM_PROTO_DESIGN.md` 与 common proto（message / event / sync / conversation），定义 PostgreSQL + TimescaleDB 的写模型与读模型，支撑 CQRS 与按会话同步。

---

## 1. Bounded Context 与表归属

| BC | 职责 | 存储表 / 视图 |
|----|------|----------------|
| **Message** | 消息聚合根、FSM、会话内全序 | `messages`（写模型）、旁路表见下 |
| **Event** | 单一 Command 路径、事件流 | `events`（原 conversation_events，与 EventType 对齐） |
| **Session** | 会话元数据、参与者、策略 | `conversations`、`conversation_participants` |
| **Sync** | 按会话 last_seq 拉取、游标 | `user_sync_cursor`、读模型依赖 events + messages |
| **读模型** | 会话列表/详情/预览 | `conversation_participants` 的 unread/max_seq/last_read_seq、可建视图对齐 ConversationLight/Summary/Detail |

租户/媒体/Hook 不归属单一 BC，作为支撑表保留。

---

## 2. Message 聚合根与 FSM（对齐 proto MessageStatus）

- **身份**：`(tenant_id, conversation_id, seq)` 会话内全序；`server_id` 全局唯一。
- **FSM**（与 `common/message.proto` 一致）：
  - `MESSAGE_STATUS_CREATED` → `MESSAGE_STATUS_SENT` → `MESSAGE_STATUS_DELIVERED` → `MESSAGE_STATUS_READ`
  - 终态：`MESSAGE_STATUS_RECALLED`、`MESSAGE_STATUS_DELETED_SOFT`、`MESSAGE_STATUS_DELETED_HARD`
- **存储**：`messages` 表，主键含时间列以满足 TimescaleDB hypertable；唯一约束 `(tenant_id, server_id)`；`status` 使用与 proto 一致的枚举字符串（CREATED/SENT/DELIVERED/READ/RECALLED/DELETED_SOFT/DELETED_HARD）。
- **编辑**：不改变主状态（仍 SENT），仅 `current_edit_version`、`last_edited_at` 变更；完整编辑历史在旁路表 `message_edit_history`。

---

## 3. 事件流（对齐 proto Event / EventType）

- **单一写入口**：SendMessage → 写 `Event(EVENT_MESSAGE, message)`；ExecuteEvent → 写对应 `Event(type, payload)`。
- **存储**：`events` 表（或保留名 `conversation_events`），字段对齐 `common/event.proto`：
  - `tenant_id`, `conversation_id`, `seq`（会话内序）, `event_type`（与 EventType 枚举一致）, `created_at`, `operator_id`, `request_id`
  - `payload` 使用 JSONB 或 BYTEA 存储 oneof 序列化结果；或拆列为可选字段（message_id, payload_message, payload_recall, payload_edit, …）。
- **EventType 取值**（与 proto 一致）：EVENT_MESSAGE, EVENT_MESSAGE_RECALL, EVENT_MESSAGE_EDIT, EVENT_MESSAGE_DELETE, EVENT_READ_RECEIPT, EVENT_TYPING, EVENT_CONVERSATION_UPDATE, EVENT_CONVERSATION_DELETE, EVENT_PRESENCE, EVENT_CALL_SIGNAL, EVENT_REACTION, EVENT_PIN, EVENT_UNPIN, EVENT_MARK, EVENT_UNMARK, EVENT_CUSTOM。

Sync 按会话拉取：`WHERE conversation_id = ? AND seq > last_seq ORDER BY seq LIMIT n`，返回 EventEnvelope(events, max_seq, has_more)。

---

## 4. 旁路表（按需 Query，对齐 models.proto / event payload）

| 表名 | 用途 | Proto 对应 |
|------|------|------------|
| `message_edit_history` | 编辑历史，GetMessageEditHistory | EditHistory |
| `message_read_records` | 已读记录，GetMessageReadReceipts | MessageReadRecord |
| `message_visibility` | 用户维度软删/隐藏 | User-Message FSM |
| `message_reactions` | 反应汇总，GetMessageReactions | Reaction |
| `pinned_messages` | 置顶，PinEvent/UnpinEvent | PinnedMessageInfo |
| `marked_messages` | 标记，MarkEvent/UnmarkEvent | MarkedMessageInfo |

以上不随 Message 主模型同步下发，由 Query 按需拉取。

---

## 5. 会话与 Sync 读模型

- **写模型**：`conversations`（会话元数据）、`conversation_participants`（参与者、last_read_seq、unread_count、is_muted、is_pinned 等）。
- **Sync 游标**：`user_sync_cursor(user_id, conversation_id, last_synced_seq)` 或直接用 `conversation_participants.last_sync_msg_seq`。
- **读模型**：
  - **ConversationLight**：轻量列表项，可由 `conversation_participants` + 会话最后一条消息预览构成（或物化视图）。
  - **ConversationSummary**：在 Light 基础上加 display_name、avatar_url、last_message 等。
  - **ConversationDetail**：由 `conversations` + `conversation_participants` + 策略/公告等拼装。

未读数：由 ReadReceiptEvent 与消费事件流更新 `conversation_participants.last_read_msg_seq` / `unread_count`；或通过 `max_seq - last_read_seq` 计算。

---

## 6. 文件与执行顺序

| 文件 | 内容 |
|------|------|
| `00_extensions.sql` | TimescaleDB 扩展 |
| `01_tenant_media.sql` | 租户、告警、媒体资产与引用 |
| `02_message_aggregate.sql` | messages（Message 聚合根 + Hypertable） |
| `03_events.sql` | events（事件流，EventType 对齐） |
| `04_message_side_tables.sql` | 编辑/已读/可见性/反应/置顶/标记 |
| `05_conversation.sql` | conversations、conversation_participants |
| `06_sync_and_read_model.sql` | user_sync_cursor、可选视图 |
| `07_hook.sql` | hook_configs、hook_executions |
| `08_timescale_policies.sql` | 压缩、连续聚合、保留策略（可选） |
| `09_triggers.sql` | updated_at 等触发器 |

入口：`init.sql` 按顺序 `\i` 上述文件（或由应用/迁移工具顺序执行）。

---

## 7. 与 proto 的字段对齐说明

- **Message**：server_id, conversation_id, client_msg_id, seq, timestamp, sender_id, receiver_id, source, conversation_type, message_type, business_type, content(BYTEA), content_type, quote(JSONB), attributes, extra, status, recalled_at, recall_reason, is_burn_after_read, burn_after_seconds, current_edit_version, last_edited_at, tags, offline_push_info(JSONB), tenant_id。与 `common/message.proto` 一致。
- **Event**：tenant_id, conversation_id, seq, event_type, created_at, operator_id, request_id；payload 存 JSONB 或 BYTEA，类型由 event_type 决定。
- **ConversationParticipant**：与 conversation.ConversationParticipant 及 Session 读模型所需字段一致（last_read_seq、unread_count、is_muted、is_pinned 等）。

本设计可直接支撑 SyncRequest(conversation_id, last_seq, limit) → EventEnvelope，以及 QueryMessages / GetConversationDetail / 会话列表增量拉取。
