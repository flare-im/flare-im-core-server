# 消息操作 Storage 层实现方案

> **📦 已归档（历史规划文档）**：本文是 2025-01 的一次性实现规划，其 P0/P1/P2
> 计划大部分已落地。文中 API 命名（`is_operation_message` / `update_message_status`
> / `ProcessMessageOperationCommand` / `MessageOperationDomainService`）已与当前实现
> 漂移——真实写模型走 `flare-storage/writer` 的 `EventApplicationService` +
> `event_handlers::dispatch` + `ArchiveStoreRepository`（`update_message_fsm_state`
> / `update_message_content` / `update_message_visibility` 等）。仅作历史参考，勿当现状。

> **作者**: IM 架构专家  
> **日期**: 2025-01-XX  
> **版本**: 1.0  
> **参考**: 微信、飞书、Discord、Telegram 等主流 IM 系统

---

## 📋 目录

1. [架构设计](#架构设计)
2. [操作分类与处理路径](#操作分类与处理路径)
3. [Writer 实现方案](#writer-实现方案)
4. [Reader 实现方案](#reader-实现方案)
5. [实现细节](#实现细节)

---

## 🏗️ 架构设计

### 核心原则

1. **需要 JetStream 的操作**：走 Writer（从 JetStream 消费并更新数据库）
2. **不需要 JetStream 的操作**：走 Reader（直接 gRPC 调用更新数据库）

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    Message Orchestrator                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ 操作消息
                            ▼
        ┌───────────────────┴───────────────────┐
        │                                         │
        │ 需要 JetStream?                             │ 不需要 JetStream?
        │                                         │
        ▼                                         ▼
┌──────────────┐                          ┌──────────────┐
│   JetStream      │                          │  Reader      │
│ (操作消息)    │                          │ (gRPC)       │
└──────────────┘                          └──────────────┘
        │                                         │
        │ 消费                                     │ 直接更新
        ▼                                         ▼
┌──────────────┐                          ┌──────────────┐
│   Writer     │                          │  PostgreSQL  │
│ (更新数据库)  │                          │  (数据库)     │
└──────────────┘                          └──────────────┘
        │                                         │
        └───────────────────┬───────────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │  PostgreSQL  │
                    │  (数据库)     │
                    └──────────────┘
```

---

## 📊 操作分类与处理路径

### 完整操作分类表

| 操作类型 | JetStream 策略 | 处理路径 | 实现位置 |
|---------|-----------|---------|---------|
| **撤回消息（全局）** | ✔️ 必须 | Orchestrator → JetStream → Writer → DB | Writer 消费操作消息 |
| **撤回消息（仅自己）** | ❌ 不需要 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新 |
| **编辑消息** | ✔️ 必须 | Orchestrator → JetStream → Writer → DB | Writer 消费操作消息 |
| **删除消息（硬删除）** | ✔️ 必须 | Orchestrator → JetStream → Writer → DB | Writer 消费操作消息 |
| **删除消息（软删除，仅自己）** | ❌ 不需要 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新 |
| **已读回执** | ⚠️ 条件 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新（需要推送） |
| **反应操作** | ✔️ 必须 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新（需要推送） |
| **置顶操作** | ✔️ 必须 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新（需要推送） |
| **收藏/标记** | ❌ 不需要 | Orchestrator → Reader (gRPC) → DB | Reader 直接更新 |

---

## 🔧 Writer 实现方案

### 1. 操作消息识别

Writer 从 JetStream 消费 `StoreMessageRequest`，需要识别是否为操作消息：

```rust
// 在 prepare_message 或消费时识别
fn is_operation_message(message: &Message) -> bool {
    message.message_type == MessageType::Notification as i32
        && message.content.as_ref()
            .and_then(|c| c.content.as_ref())
            .and_then(|c| match c {
                Content::Notification(notif) => Some(notif.r#type == "message_operation"),
                _ => None,
            })
            .unwrap_or(false)
}
```

### 2. 操作消息处理

创建新的命令处理器处理操作消息：

```rust
// application/commands/process_message_operation.rs
pub struct ProcessMessageOperationCommand {
    pub operation: MessageOperation,
    pub message: Message,  // 原始消息（包含操作信息）
    pub context: RequestContext,
    pub tenant: TenantContext,
}

// domain/service/message_operation_domain_service.rs
pub struct MessageOperationDomainService {
    archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>>,
    // ... 其他依赖
}

impl MessageOperationDomainService {
    /// 处理消息操作（撤回、编辑、删除等）
    pub async fn process_operation(
        &self,
        operation: MessageOperation,
        message: &Message,
    ) -> Result<()> {
        match OperationType::try_from(operation.operation_type) {
            Ok(OperationType::Recall) => {
                self.handle_recall_operation(&operation, message).await
            }
            Ok(OperationType::Edit) => {
                self.handle_edit_operation(&operation, message).await
            }
            Ok(OperationType::Delete) => {
                self.handle_delete_operation(&operation, message).await
            }
            _ => Err(anyhow!("Unsupported operation type")),
        }
    }
    
    /// 处理撤回操作
    async fn handle_recall_operation(
        &self,
        operation: &MessageOperation,
        _message: &Message,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        
        // 更新数据库：设置 is_recalled = true, status = Recalled
        if let Some(repo) = &self.archive_repo {
            repo.update_message_status(
                message_id,
                MessageStatus::Recalled,
                Some(true),  // is_recalled
                Some(operation.timestamp.clone()),  // recalled_at
            ).await?;
        }
        
        Ok(())
    }
    
    /// 处理编辑操作
    async fn handle_edit_operation(
        &self,
        operation: &MessageOperation,
        _message: &Message,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        
        // 从 operation_data 中提取编辑后的内容
        if let Some(OperationData::Edit(edit_data)) = &operation.operation_data {
            if let Some(repo) = &self.archive_repo {
                repo.update_message_content(
                    message_id,
                    &edit_data.new_content,
                    edit_data.edit_version,
                ).await?;
            }
        }
        
        Ok(())
    }
    
    /// 处理删除操作（硬删除）
    async fn handle_delete_operation(
        &self,
        operation: &MessageOperation,
        _message: &Message,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        
        // 硬删除：更新 visibility 为 DELETED（全局）
        if let Some(repo) = &self.archive_repo {
            repo.update_message_visibility(
                message_id,
                None,  // user_id = None 表示全局删除
                VisibilityStatus::Deleted,
            ).await?;
        }
        
        Ok(())
    }
}
```

### 3. Consumer 扩展

在 `StorageWriterConsumer` 中添加操作消息处理：

```rust
async fn process_store_message(
    &self,
    request: StoreMessageRequest,
) -> AnyhowResult<PersistenceResult> {
    // 检查是否是操作消息
    if let Some(message) = &request.message {
        if is_operation_message(message) {
            // 提取 MessageOperation
            if let Some(operation) = extract_operation_from_message(message)? {
                // 处理操作消息
                return self.command_handler
                    .handle_operation(ProcessMessageOperationCommand {
                        operation,
                        message: message.clone(),
                        context: request.context,
                        tenant: request.tenant,
                    })
                    .await;
            }
        }
    }
    
    // 普通消息处理
    self.command_handler
        .handle(ProcessStoreMessageCommand { request })
        .await
}
```

---

## 🔧 Reader 实现方案

### 1. 完善现有操作实现

Reader 已经有撤回、删除等操作的 gRPC 接口，需要确保实现完整：

#### 撤回消息（已实现 ✅）

```rust
// domain/service/message_storage_domain_service.rs
pub async fn recall_message(
    &self,
    message_id: &str,
    recall_time_limit_seconds: i64,
) -> Result<Option<Timestamp>> {
    // ✅ 已实现：检查时间限制、更新状态、记录操作
}
```

#### 编辑消息（待实现 ⏳）

需要在 Reader 中添加编辑消息的接口：

```rust
// domain/service/message_storage_domain_service.rs
pub async fn edit_message(
    &self,
    message_id: &str,
    new_content: MessageContent,
    edit_version: i32,
) -> Result<()> {
    // 获取消息
    let message = self.get_message(message_id).await?
        .ok_or_else(|| anyhow!("message not found"))?;
    
    // 验证编辑权限（只有发送者可以编辑）
    // 验证编辑版本号（必须递增）
    if edit_version <= message.extra.get("edit_version")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0) {
        return Err(anyhow!("Edit version must be greater than current version"));
    }
    
    // 更新消息内容
    let update = MessageUpdate {
        // ... 其他字段
        attributes: Some({
            let mut attrs = message.attributes.clone();
            attrs.insert("edit_version".to_string(), edit_version.to_string());
            attrs.insert("edited_at".to_string(), Utc::now().timestamp().to_string());
            attrs
        }),
        // 注意：content 更新需要通过特殊方法处理
    };
    
    // 更新数据库
    self.storage.update_message_content(message_id, new_content, edit_version).await?;
    
    Ok(())
}
```

#### 删除消息（已实现 ✅）

```rust
// domain/service/message_storage_domain_service.rs
pub async fn delete_messages(&self, message_ids: &[String]) -> Result<usize> {
    // ✅ 已实现：批量更新 visibility 为 DELETED
}

pub async fn delete_message_for_user(
    &self,
    message_id: &str,
    user_id: &str,
    permanent: bool,
) -> Result<usize> {
    // ✅ 已实现：更新用户维度的 visibility
}
```

### 2. 添加编辑消息接口

在 `storage.proto` 中添加编辑消息的 RPC：

```protobuf
// 编辑消息请求
message EditMessageRequest {
  string message_id = 1;
  string operator_id = 2;
  flare.common.v1.MessageContent new_content = 3;
  int32 edit_version = 4;
  string reason = 5;
  bool show_edited_mark = 6;
  flare.common.v1.RequestContext context = 7;
  flare.common.v1.TenantContext tenant = 8;
}

// 编辑消息响应
message EditMessageResponse {
  bool success = 1;
  string error_message = 2;
  flare.common.v1.RpcStatus status = 3;
}

// 在 StorageReaderService 中添加
rpc EditMessage(EditMessageRequest) returns (EditMessageResponse);
```

### 3. 完善操作记录

所有操作都应该记录到 `Message.operations` 数组中：

```rust
// 在操作处理时，追加操作记录
pub async fn append_operation(
    &self,
    message_id: &str,
    operation: MessageOperation,
) -> Result<()> {
    let message = self.get_message(message_id).await?
        .ok_or_else(|| anyhow!("message not found"))?;
    
    let mut operations = message.operations.clone();
    operations.push(operation);
    
    let update = MessageUpdate {
        operations: Some(operations),
        // ... 其他字段
    };
    
    self.storage.update_message(message_id, update).await?;
    Ok(())
}
```

---

## 🔧 实现细节

### 1. Writer 操作消息处理流程

```
JetStream Consumer
    ↓
process_store_message()
    ↓
检查：is_operation_message()?
    ↓ Yes
提取：extract_operation_from_message()
    ↓
MessageOperationDomainService.process_operation()
    ↓
根据操作类型处理：
  - Recall → update_message_status()
  - Edit → update_message_content()
  - Delete → update_message_visibility()
    ↓
更新数据库（PostgreSQL）
    ↓
发布 ACK（可选）
```

### 2. Reader 操作处理流程

```
gRPC Handler
    ↓
Command Handler
    ↓
Domain Service
    ↓
根据操作类型：
  - Recall → recall_message()
  - Edit → edit_message()
  - Delete → delete_message() / delete_message_for_user()
  - Attributes → set_message_attributes()
    ↓
Storage.update_message() / update_message_visibility()
    ↓
更新数据库（PostgreSQL）
```

### 3. 操作记录持久化

所有操作都应该记录到 `Message.operations` 数组：

```rust
// 在操作处理时
let operation = MessageOperation {
    operation_type: OperationType::Recall as i32,
    target_message_id: message_id.to_string(),
    operator_id: operator_id.to_string(),
    timestamp: Some(Utc::now().into()),
    // ... 其他字段
};

// 追加到 operations 数组
self.append_operation(message_id, operation).await?;
```

---

## 📝 实现优先级

### P0（核心功能，必须实现）

1. ✅ **撤回消息** - Reader 已实现，Writer 需要支持
2. ✅ **删除消息** - Reader 已实现（软删除），Writer 需要支持硬删除
3. ⏳ **编辑消息** - Reader 和 Writer 都需要实现

### P1（重要功能，优先实现）

1. ⏳ **Writer 操作消息识别** - 识别并处理操作消息
2. ⏳ **Writer 操作处理** - 实现撤回、编辑、删除的处理逻辑
3. ⏳ **Reader 编辑接口** - 添加 EditMessage RPC

### P2（增强功能，后续实现）

1. ⏳ **操作记录持久化** - 确保所有操作都记录到 operations 数组
2. ⏳ **操作审计** - 记录操作日志和审计信息

---

## 🎯 关键实现点

### 1. Writer 操作消息识别

- 检查 `message_type == Notification`
- 检查 `content.content == NotificationContent`
- 检查 `NotificationContent.type == "message_operation"`
- 提取并反序列化 `MessageOperation`

### 2. Writer 操作处理

- 撤回：更新 `is_recalled`、`recalled_at`、`status`
- 编辑：更新 `content`、`edit_version`、`attributes`
- 删除：更新 `visibility`（全局删除）

### 3. Reader 操作完善

- 编辑消息：添加 `EditMessage` RPC 和实现
- 操作记录：确保所有操作都追加到 `operations` 数组
- 权限验证：编辑操作需要验证发送者权限

---

**最后更新**: 2025-01-XX  
**维护者**: IM 架构团队
