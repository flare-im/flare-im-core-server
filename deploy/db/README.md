# deploy/db — 数据库初始化（对齐 flare-proto）

本目录包含按 **flare-proto** 最新设计（DDD + CQRS、Message 聚合根、Event 流、Sync 按会话）整理的 PostgreSQL + TimescaleDB 初始化脚本与存储设计说明。

## 设计依据

- **flare-proto/IM_PROTO_DESIGN.md**：Bounded Context、CQRS、Message FSM、Sync 模型
- **common/message.proto**：Message 聚合根、MessageStatus
- **common/event.proto**：EventType、Event、EventEnvelope
- **common/sync.proto**：SyncRequest/Response、ConversationPatch
- **common/conversation.proto**：ConversationLight/Summary/Detail、读模型

详见 **STORAGE_DESIGN.md**。

## 文件说明

| 文件 | 说明 |
|------|------|
| **STORAGE_DESIGN.md** | 存储设计文档（BC、表归属、与 proto 对齐） |
| **init.sql** | **单文件合并版**：完整建表 + 所有表/列 COMMENT，可直接执行 |
| **00_extensions.sql** ～ **09_triggers.sql** | 按模块拆分的脚本（可选，便于分段执行或对比） |

## 执行方式

```bash
cd flare-im-core/deploy/db
psql -U flare -d flare -f init.sql
```

**init.sql** 已合并扩展、租户/媒体、Message 聚合根、events、旁路表、会话、Sync 游标、Hook、TimescaleDB 策略与触发器，并包含所有 `COMMENT ON TABLE` / `COMMENT ON COLUMN`。

## 与旧版 deploy/init.sql 的差异

- **Message 状态**：与 proto `MessageStatus` 一致（CREATED/SENT/DELIVERED/READ/RECALLED/DELETED_SOFT/DELETED_HARD），不再使用 INIT/EDITED。
- **事件表**：表名为 `events`，`event_type` 与 `common/event.proto` 的 `EventType` 枚举一致。
- **列注释**：所有表均有表注释与列注释，便于维护与文档生成。
- **读模型**：会话列表/详情依赖 `conversation_participants` 与 `conversations`，Sync 依赖 `events` + `user_sync_cursor`。

原 `deploy/init.sql` 仍可单独使用；本目录为“对齐 proto 的推荐 schema”，后续新环境建议以 **deploy/db/init.sql** 为准。
