# 会话生成（Session/Conversation Ensure）设计：同步 vs 异步

## 背景

消息落库前需保证会话（Conversation）存在，否则 Storage Writer 更新会话 last_msg / 未读等时可能依赖的会话记录不存在。  
Orchestrator 在编排存储流程中需要完成「如会话不存在则创建」的处理。

## 方案对比

### 同步创建（当前默认）

- **做法**：在 `orchestrate_message_storage` 内，在 WAL 写入之后、Kafka 发布之前，同步调用 Conversation 服务的 `CreateConversation`（ensure 语义：存在则返回，不存在则创建）。
- **优点**
  - 强一致：消息发出时会话记录已存在，Storage Writer / 未读数等不依赖兜底。
  - 实现简单，无需新 Topic、消费者与幂等 DB。
  - 易排查：发消息路径上即可确认会话是否创建成功。
- **缺点**
  - 增加发送路径延迟（一次 gRPC + 会话服务可能 DB 写入，约 5–50ms）。
  - Orchestrator 与 Conversation 在写路径上耦合；Conversation 不可用时需降级（当前：超时 2s 后继续，Storage Writer 侧有 UPSERT 兜底）。

### 异步创建（事件驱动）

- **做法**：Orchestrator 不直接调 gRPC，而是发布「会话需创建」事件（如 `conversation.ensure`）到专用 Topic；Conversation 服务消费后幂等创建会话。
- **优点**
  - 发消息路径更短，延迟更低，符合 EDA（事件驱动）。
  - Orchestrator 不依赖 Conversation 可用性；Conversation 可独立扩展、重试。
- **缺点**
  - 最终一致：存在短暂「消息已写、会话尚未创建」窗口，需 Storage Writer / 会话读模型能容忍或兜底（如 UPSERT）。
  - 需新增 Topic、消费者，且会话表需幂等写入（如 `INSERT ... ON CONFLICT DO NOTHING`），否则并发创建同一会话会报错。

## 选型建议

- **默认推荐：同步创建**  
  保证语义简单、一致性好，且当前 Conversation 表为普通 INSERT，无幂等约束，同步更稳妥。
- **可选：异步创建**  
  在需要极致发消息延迟、且已实现「会话表幂等创建 + Conversation 消费 conversation.ensure」时，通过配置 `session_creation_mode: async` 启用。

## 配置

- `session_creation_mode`: `sync` | `async`，默认 `sync`。
- 同步模式：依赖现有 gRPC Conversation 服务；超时 2s，失败/超时后继续发消息（Storage Writer 兜底）。
- 异步模式：发布到 `flare.im.conversation.ensure`（或配置的 topic），由 Conversation 服务消费并幂等创建。

## 实现要点

1. **同步路径**：保持现有 `ensure_conversation` 调用，带超时与降级。
2. **异步路径**：发布 `TopicEventEnvelope`，`event_type = "conversation.ensure"`，payload 含 `conversation_id`、`conversation_type`、`business_type`、`participants`（如通过 CustomEvent JSON）。
3. **Conversation 服务**：若支持异步，需消费 ensure 事件并调用现有 `create_conversation`；会话表需 `ON CONFLICT DO NOTHING` 或等效幂等写入。
