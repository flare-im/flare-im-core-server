//! 与 deploy/init.sql、common/message.proto 对齐的持久化辅助

use chrono::{DateTime, Utc};
use flare_proto::common::Message;
use flare_server_core::error::Result;
use serde_json::{Map, Value};

/// 编码消息内容为字节数组（proto Message.content）
pub fn encode_message_content(message: &Message) -> Vec<u8> {
    flare_proto::encode_message_content(message).unwrap_or_default()
}

/// 从领域 extra 构建 JSONB 列值（init_v2 存 JSONB）
pub fn build_extra_value(
    extra: &std::collections::HashMap<String, String>,
) -> Result<Map<String, Value>> {
    let mut extra_value = Map::new();
    for (k, v) in extra {
        extra_value.insert(k.clone(), Value::String(v.clone()));
    }
    Ok(extra_value)
}

/// 从 proto Message 取时间戳，用于 created_at / timestamp 列。
pub fn get_message_timestamp(message: &Message) -> DateTime<Utc> {
    if message.created_at > 0 {
        DateTime::<Utc>::from_timestamp_millis(message.created_at).unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
}
