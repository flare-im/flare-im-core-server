//! 与 init_v2.sql、common/message.proto 对齐的持久化辅助

use anyhow::Result;
use chrono::{DateTime, Utc};
use flare_im_core::utils::timestamp_to_datetime;
use flare_proto::common::Message;
use prost::Message as _;
use serde_json::{to_value, Map, Value};

/// 编码消息内容为字节数组（proto Message.content）
pub fn encode_message_content(message: &Message) -> Vec<u8> {
    flare_proto::encode_message_content(message)
}

/// 从 proto Message 构建 extra JSON（init_v2 存 JSONB）
pub fn build_extra_value(message: &Message) -> Result<Map<String, Value>> {
    let mut extra_value = Map::new();
    if let Ok(existing) = serde_json::from_value::<Map<String, Value>>(to_value(&message.extra)?) {
        for (k, v) in existing {
            extra_value.insert(k, v);
        }
    }
    Ok(extra_value)
}

/// 从 proto Message 取时间戳，用于 created_at / timestamp 列（优先 timestamp，否则 created_at）
pub fn get_message_timestamp(message: &Message) -> DateTime<Utc> {
    message
        .timestamp
        .as_ref()
        .and_then(timestamp_to_datetime)
        .unwrap_or_else(Utc::now)
}

