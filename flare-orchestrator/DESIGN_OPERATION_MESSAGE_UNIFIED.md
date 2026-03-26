# 消息操作统一处理方案（命令转消息）

## 设计改进：直接使用 MessageOperation

**重要改进**：`MessageContent` 直接使用 `MessageOperation`，而不是通过 `OperationContent` 包装。

### 为什么直接使用 MessageOperation？

1. **类型安全**：`MessageOperation` 使用枚举类型（`OperationType`）和 `oneof operation_data`，而不是字符串和 JSON
2. **结构完整**：`MessageOperation` 包含完整的操作信息（`timestamp`、`target_user_id`、`extensions` 等）
3. **避免冗余**：不需要 `OperationContent` 这一层包装，减少序列化/反序列化开销
4. **简化代码**：直接访问 `MessageOperation`，不需要从 `NotificationContent.data` 中 base64 解码

### Proto 定义

```proto
// message_content.proto
message MessageContent {
  oneof content {
    // ... 其他内容类型
    MessageOperation operation = 302; // 直接使用 MessageOperation
  }
}
```

**注意**：`OperationContent` 已被移除，直接使用 `MessageOperation`。

## 一、设计理念

**核心思想**：在 IM 系统中，所有操作最终都以消息的形式推送给用户。因此，应该将接口操作转换为操作消息，统一通过消息处理流程。

### 为什么选择"命令转消息"？

1. **符合 IM 领域模型**：操作本身就是一种消息（操作消息）
2. **统一处理流程**：所有操作都通过 `SendMessage` → 统一消息处理流程
3. **可追溯性**：所有操作都有对应的消息记录，便于审计和问题排查
4. **架构简化**：不需要维护两套处理路径（命令路径和消息路径）
5. **推送一致性**：所有操作都通过统一的消息推送机制

## 二、架构设计

### 2.1 统一架构

```
┌─────────────────────────────────────────────────────────────┐
│                     接口层 (Handler)                          │
├─────────────────────────────────────────────────────────────┤
│  RecallMessage()  │  EditMessage()  │  SendMessage()        │
│       ↓                ↓                    ↓                │
│  构建操作消息     构建操作消息     直接处理                    │
│  (Message with    (Message with                             │
│   MessageOperation)  MessageOperation)                      │
│       ↓                ↓                    ↓                │
│              SendMessage() ← 统一入口                        │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                 应用层 (CommandHandler)                       │
├─────────────────────────────────────────────────────────────┤
│  handle_send_message() ← 统一消息处理入口                    │
│       ↓                                                       │
│  识别消息类别 (Temporary/Operation/Normal)                   │
│       ↓                                                       │
│  Operation? → 提取 MessageOperation → 执行操作              │
│       ↓                                                       │
│  生成操作结果消息 → 推送                                     │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                 领域层 (OperationService)                    │
├─────────────────────────────────────────────────────────────┤
│  handle_recall()  │  handle_edit()  │  handle_delete()       │
│       ↓                                                       │
│  执行操作逻辑 → 生成操作结果消息 → 推送                      │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 处理流程对比

#### 当前架构（两套路径）
```
接口操作 → 命令 → 领域服务 → 推送消息
操作消息 → 提取操作 → 命令 → 领域服务 → 推送消息
```

#### 新架构（统一路径）
```
接口操作 → 构建操作消息 → SendMessage → 统一处理 → 推送消息
操作消息 → SendMessage → 统一处理 → 推送消息
```

## 三、核心组件设计

### 3.1 操作消息构建器（Operation Message Builder）

**位置**：`application/utils/operation_message_builder.rs`

**职责**：
- 将操作请求（`RecallMessageRequest`、`EditMessageRequest` 等）转换为操作消息
- 构建包含 `MessageOperation` 的 `NotificationContent`
- 设置消息类型、会话ID、发送者等字段

**接口**：
```rust
pub struct OperationMessageBuilder;

impl OperationMessageBuilder {
    /// 构建撤回操作消息
    pub fn build_recall_message(
        req: &RecallMessageRequest,
        conversation_id: &str,
    ) -> Result<flare_proto::Message> {
        // 构建 MessageOperation（直接使用，不需要 base64 编码）
        let operation = MessageOperation {
            operation_type: OperationType::Recall as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: chrono::Utc::now().timestamp_subsec_nanos() as i32,
            }),
            show_notice: true,
            notice_text: format!("{} 撤回了消息", req.operator_id),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Recall(RecallOperationData {
                reason: req.reason.clone().unwrap_or_default(),
                time_limit_seconds: req.recall_time_limit_seconds.unwrap_or(0),
                allow_admin_recall: false,
            })),
            metadata: std::collections::HashMap::new(),
            extensions: vec![],
        };
        
        // 构建 Message（直接使用 MessageOperation）
        let mut message = flare_proto::Message {
            server_id: format!("op_{}", uuid::Uuid::new_v4()),
            conversation_id: conversation_id.to_string(),
            sender_id: req.operator_id.clone(),
            message_type: flare_proto::MessageType::Operation as i32,
            content: Some(MessageContent {
                content: Some(Content::Operation(operation)), // 直接使用 MessageOperation
            }),
            // ...
        };
        
        Ok(message)
    }
    
    /// 构建编辑操作消息
    pub fn build_edit_message(req: &EditMessageRequest, ...) -> Result<Message> { ... }
    
    /// 构建删除操作消息
    pub fn build_delete_message(req: &DeleteMessageRequest, ...) -> Result<Message> { ... }
    
    // ... 其他操作类型
}
```

### 3.2 操作消息处理器（Operation Message Handler）

**位置**：`application/handlers/command_handler.rs`

**职责**：
- 在 `handle_send_message()` 中识别操作消息
- 提取 `MessageOperation` 并执行操作
- 生成操作结果消息并推送

**处理流程**：
```rust
pub async fn handle_send_message(
    &self,
    cmd: SendMessageCommand,
) -> Result<(String, u64)> {
    use crate::domain::model::message_kind::MessageProfile;
    use crate::application::utils::operation_extractor::extract_operation_from_message;

    let mut message = cmd.message.clone();
    let profile = MessageProfile::ensure(&mut message);
    let category = profile.category();

    match category {
        MessageCategory::Temporary => {
            // 临时消息处理
            // ...
        }
        MessageCategory::Operation => {
            // 操作消息：提取并执行操作
            if let Some(operation) = extract_operation_from_message(&message)? {
                // 执行操作
                self.execute_operation(&operation, &message, &cmd).await?;
                
                // 操作消息返回目标消息ID和seq=0（操作不产生新消息）
                // 但操作结果会通过推送消息通知用户
                Ok((operation.target_message_id, 0))
            } else {
                // 无法提取操作，降级为普通消息
                self.handle_normal_message(cmd).await
            }
        }
        _ => {
            // 普通消息处理
            self.handle_normal_message(cmd).await
        }
    }
}

/// 执行消息操作
/// 
/// 注意：operation 参数直接是 MessageOperation，不需要从 NotificationContent 中提取
async fn execute_operation(
    &self,
    operation: &flare_proto::common::MessageOperation,
    message: &flare_proto::Message,
    cmd: &SendMessageCommand,
) -> Result<()> {
    use flare_proto::common::{OperationType, message_operation::OperationData};
    
    let tenant_id = cmd.tenant.as_ref()
        .map(|t| t.tenant_id.as_str())
        .unwrap_or("default");
    
    match OperationType::try_from(operation.operation_type) {
        Ok(OperationType::Recall) => {
            let recall_data = match &operation.operation_data {
                Some(OperationData::Recall(data)) => data,
                _ => return Err(anyhow::anyhow!("Recall operation requires RecallOperationData")),
            };
            
            let recall_cmd = RecallMessageCommand {
                base: MessageOperationCommand {
                    message_id: operation.target_message_id.clone(),
                    operator_id: operation.operator_id.clone(),
                    timestamp: operation.timestamp.as_ref()
                        .map(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                        .flatten()
                        .unwrap_or_else(|| chrono::Utc::now()),
                    tenant_id: tenant_id.to_string(),
                    conversation_id: message.conversation_id.clone(),
                },
                reason: if recall_data.reason.is_empty() {
                    None
                } else {
                    Some(recall_data.reason.clone())
                },
                time_limit_seconds: if recall_data.time_limit_seconds == 0 {
                    None
                } else {
                    Some(recall_data.time_limit_seconds)
                },
            };
            
            // 执行撤回操作（会生成操作结果消息并推送）
            self.handle_recall_message(recall_cmd).await
        }
        // ... 其他操作类型
        _ => Err(anyhow::anyhow!("Unsupported operation type: {}", operation.operation_type)),
    }
}
```

### 3.3 接口层改造

**改造前**：
```rust
async fn recall_message(
    &self,
    request: Request<MessageRecallMessageRequest>,
) -> Result<Response<MessageRecallMessageResponse>, Status> {
    let req = request.into_inner();
    
    // 构建命令
    let cmd = RecallMessageCommand { ... };
    
    // 调用命令处理器
    self.command_handler.handle_recall_message(cmd).await?;
    
    // 返回响应
    Ok(Response::new(MessageRecallMessageResponse { ... }))
}
```

**改造后**：
```rust
async fn recall_message(
    &self,
    request: Request<MessageRecallMessageRequest>,
) -> Result<Response<MessageRecallMessageResponse>, Status> {
    let req = request.into_inner();
    
    // 从请求中获取会话ID（需要查询原消息获取）
    // 或者从请求中直接提供
    let conversation_id = self.get_conversation_id_for_message(&req.message_id).await?;
    
    // 构建操作消息
    let operation_message = OperationMessageBuilder::build_recall_message(
        &req,
        &conversation_id,
    )?;
    
    // 构建 SendMessageRequest
    let send_req = SendMessageRequest {
        conversation_id: conversation_id.clone(),
        message: Some(operation_message),
        sync: req.sync.unwrap_or(false),
        context: req.context.clone(),
        tenant: req.tenant.clone(),
    };
    
    // 调用 SendMessage（统一处理）
    let send_resp = self.send_message(Request::new(send_req)).await?;
    let send_inner = send_resp.into_inner();
    
    // 转换为 RecallMessageResponse
    Ok(Response::new(MessageRecallMessageResponse {
        success: send_inner.success,
        error_message: if send_inner.success {
            String::new()
        } else {
            send_inner.status.as_ref()
                .map(|s| s.message.clone())
                .unwrap_or_default()
        },
        recalled_at: send_inner.sent_at,
        status: send_inner.status,
    }))
}
```

## 四、优势分析

### 4.1 架构优势

1. **统一处理流程**：所有操作都通过 `SendMessage` → 统一消息处理流程
2. **消息可追溯**：所有操作都有对应的消息记录，便于审计
3. **推送一致性**：所有操作都通过统一的消息推送机制
4. **架构简化**：不需要维护两套处理路径

### 4.2 业务优势

1. **符合 IM 领域模型**：操作本身就是消息
2. **支持操作历史**：可以查询操作历史消息
3. **支持操作回放**：可以通过消息重放操作
4. **支持操作同步**：多端可以通过消息同步操作状态

### 4.3 技术优势

1. **代码复用**：操作消息和普通消息共享处理逻辑
2. **易于测试**：操作消息构建器可以独立测试
3. **易于扩展**：新增操作类型只需添加构建方法

## 五、实施步骤

### 步骤 1：创建操作消息构建器
- 创建 `application/utils/operation_message_builder.rs`
- 实现各种操作的消息构建方法

### 步骤 2：创建操作提取器
- 创建 `application/utils/operation_extractor.rs`
- 实现从消息中提取 `MessageOperation` 的方法

### 步骤 3：改造 handle_send_message
- 在 `handle_send_message()` 中添加操作消息处理逻辑
- 实现 `execute_operation()` 方法

### 步骤 4：改造接口层
- 将所有操作接口（`RecallMessage`、`EditMessage` 等）改为构建操作消息并调用 `SendMessage`
- 需要查询原消息获取 `conversation_id`（或从请求中提供）

### 步骤 5：测试和验证
- 单元测试：操作消息构建器和提取器
- 集成测试：接口操作转换为操作消息的流程
- 端到端测试：操作消息的完整处理流程

## 六、注意事项

### 6.1 会话ID获取

**问题**：接口操作请求中可能没有 `conversation_id`，需要从原消息中获取。

**解决方案**：
1. **方案A**：在接口层查询原消息获取 `conversation_id`
   ```rust
   let original_message = self.query_handler.query_message(QueryMessageQuery {
       message_id: req.message_id.clone(),
       conversation_id: String::new(),
   }).await?;
   let conversation_id = original_message.conversation_id;
   ```

2. **方案B**：在请求中要求提供 `conversation_id`（推荐）
   - 修改 proto 定义，在操作请求中添加 `conversation_id` 字段
   - 客户端在调用操作接口时提供 `conversation_id`

### 6.2 操作消息的响应

**问题**：操作消息不产生新消息，但需要返回操作结果。

**解决方案**：
- 返回目标消息ID和seq=0
- 操作结果通过推送消息通知用户
- 响应中包含操作状态（成功/失败）

### 6.3 操作消息的持久化

**问题**：操作消息是否需要持久化？

**解决方案**：
- **需要持久化**：操作消息应该持久化，用于审计和历史查询
- **设置 persistent = true**：在 `NotificationContent` 中设置 `persistent = true`

### 6.4 操作消息的推送

**问题**：操作消息是否需要推送给所有用户？

**解决方案**：
- **根据操作类型决定**：
  - 撤回、编辑、删除：推送给会话所有成员
  - 已读：不推送（仅个人状态）
  - 反应、置顶：推送给会话所有成员
  - 标记：不推送（仅个人状态）

## 七、总结

本方案通过"命令转消息"的方式，实现了接口操作和操作消息的统一处理。核心思想是：

1. **统一入口**：所有操作都通过 `SendMessage` 统一处理
2. **消息化**：接口操作转换为操作消息，统一消息处理流程
3. **可追溯**：所有操作都有对应的消息记录
4. **一致性**：所有操作都通过统一的消息推送机制

这样既保持了架构的简洁性，又实现了业务逻辑的统一和可追溯性。

