# Utils 工具函数模块

本模块提供了一系列通用工具函数，用于消息处理、时间戳转换、seq 操作、未读数计算等。

## 模块结构

- 服务启动辅助函数已移至 `flare-im-service-kit::ServiceHelper`
- 会话ID生成和验证功能已移至 `flare-core`，请使用 `flare_core::common::conversation_id`
- `mod.rs` - 核心工具函数（时间戳、时间线、seq、未读数）

## 核心功能

### 1. 时间戳转换

```rust
use flare_im_contracts::utils::{timestamp_to_millis, millis_to_timestamp, timestamp_to_datetime, datetime_to_timestamp};

// 时间戳转毫秒
let ts = prost_types::Timestamp { seconds: 1000, nanos: 0 };
let millis = timestamp_to_millis(&ts); // Some(1000000)

// 毫秒转时间戳
let ts = millis_to_timestamp(1000000); // Some(Timestamp { seconds: 1000, nanos: 0 })

// 时间戳转 DateTime
let dt = timestamp_to_datetime(&ts); // Some(DateTime<Utc>)

// DateTime 转时间戳
let ts = datetime_to_timestamp(Utc::now());
```

### 2. Seq 操作

```rust
use flare_im_contracts::utils::{extract_seq_from_message, embed_seq_in_message, extract_seq_from_extra, embed_seq_in_extra};
use flare_proto::common::Message;

// 从消息中提取 seq
let mut message = Message::default();
message.extra.insert("seq".to_string(), "100".to_string());
let seq = extract_seq_from_message(&message); // Some(100)

// 将 seq 嵌入到消息中
embed_seq_in_message(&mut message, 200);
assert_eq!(message.extra.get("seq"), Some(&"200".to_string()));

// 从 extra HashMap 中提取 seq
use std::collections::HashMap;
let mut extra = HashMap::new();
extra.insert("seq".to_string(), "100".to_string());
let seq = extract_seq_from_extra(&extra); // Some(100)

// 将 seq 嵌入到 extra 中
embed_seq_in_extra(&mut extra, 200);
assert_eq!(extra.get("seq"), Some(&"200".to_string()));
```

### 3. 未读数计算

```rust
use flare_im_contracts::utils::calculate_unread_count;

// 正常情况
let unread_count = calculate_unread_count(Some(100), 80); // 20

// last_message_seq 为 None
let unread_count = calculate_unread_count(None, 80); // 0

// last_read_msg_seq 大于 last_message_seq（不会返回负数）
let unread_count = calculate_unread_count(Some(50), 80); // 0
```

### 4. 时间线元数据

```rust
use flare_im_contracts::utils::{extract_timeline_from_extra, embed_timeline_in_extra, TimelineMetadata};
use std::collections::HashMap;

// 从消息的 extra 字段中提取时间线元数据
let mut extra = HashMap::new();
extra.insert("timeline".to_string(), r#"{"ingestion_ts":"1000","persisted_ts":"1001"}"#.to_string());
let timeline = extract_timeline_from_extra(&extra, 0);

// 将时间线元数据嵌入到消息中
let mut message = Message::default();
let timeline = TimelineMetadata {
    ingestion_ts: 1000,
    persisted_ts: Some(1001),
    ..Default::default()
};
embed_timeline_in_extra(&mut message, &timeline);
```

### 5. 会话ID生成

**注意**：会话ID生成功能已移至 `flare-core`，客户端和服务端SDK都可以使用。

```rust
use flare_core::common::conversation_id::*;

// 生成单聊会话ID
let conversation_id = generate_single_chat_conversation_id("user1", "user2");
// 格式：1-<hash>

// 生成群聊会话ID
let conversation_id = generate_group_conversation_id("group123");
// 格式：2-group123

// 生成AI助手会话ID
let conversation_id = generate_ai_conversation_id("user1", "ai_service");
// 格式：3-user1-ai_service

// 验证会话ID
let conversation_type = validate_conversation_id(&conversation_id)?;
// 返回 ConversationType

// 提取会话类型
let conversation_type = extract_conversation_type(&conversation_id);
// 返回 Option<ConversationType>
```

## 使用建议

1. **Seq 操作**：统一使用 `extract_seq_from_message` 和 `embed_seq_in_message`，避免手动解析字符串
2. **未读数计算**：使用 `calculate_unread_count` 确保计算逻辑一致，避免返回负数
3. **时间戳转换**：使用工具函数确保转换逻辑正确，避免时区问题
4. **会话ID生成**：使用 `flare_core::common::conversation_id` 模块的函数，确保格式一致（客户端和服务端SDK通用）

## 测试

运行单元测试：

```bash
cargo test --package flare-im-core --lib utils
```

## 参考文档

- [消息关系模型落地优化方案](../../doc/plan/04-消息关系模型落地优化方案.md)
- [消息关系设计](../../doc/消息关系设计.md)
