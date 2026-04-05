//! 工具函数模块
//!
//! 提供错误类型与映射、时间戳转换、时间线提取、seq 操作、未读数计算、字符串/上下文等通用工具。

pub mod error;
pub mod helpers;

/// 非空会话 ID 作为 resolve 的 fallback（trim 后非空返回 `Some`），供无 StorageReader 时解析使用。
#[inline]
pub fn optional_fallback_conversation(conversation_id: &str) -> Option<&str> {
    let s = conversation_id.trim();
    if s.is_empty() { None } else { Some(s) }
}

pub use helpers::ServiceHelper;

// 重新导出 flare-server-core 的 context 工具函数
pub use flare_server_core::utils::{
    ctx_to_map as context_to_mq_metadata,
    map_to_ctx as context_from_mq_metadata,
};

// 从 flare_server_core 根导入 gRPC 相关函数
pub use flare_server_core::{
    require_ctx_from_request as require_context,
    extract_ctx_from_request_opt as extract_context_opt,
};

// 从 flare_core_base 导入 context 相关函数
pub use flare_core_base::context::{
    Ctx, Context,
};

// 提供便捷函数
pub fn extract_session_id_from_context(ctx: &Ctx) -> Option<String> {
    ctx.session_id().map(|s: &str| s.to_string())
}

pub fn require_request_id_from_context(ctx: &Ctx) -> Result<String, &'static str> {
    let request_id = ctx.request_id();
    if request_id.is_empty() {
        Err("Request ID is required in context")
    } else {
        Ok(request_id.to_string())
    }
}

pub fn require_tenant_id_from_context(ctx: &Ctx) -> Result<String, &'static str> {
    ctx.tenant_id()
        .filter(|s: &&str| !s.is_empty())
        .map(|s: &str| s.to_string())
        .ok_or("Tenant ID is required in context")
}

pub fn require_user_id_from_context(ctx: &Ctx) -> Result<String, &'static str> {
    ctx.user_id()
        .filter(|s: &&str| !s.is_empty())
        .map(|s: &str| s.to_string())
        .ok_or("User ID is required in context")
}

#[cfg(test)]
mod seq_utils_tests;

use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;
use serde_json;
use std::collections::HashMap;

/// 时间戳转换为毫秒数
///
/// # 参数
/// * `ts` - 时间戳
///
/// # 返回
/// * `Option<i64>` - 如果时间戳有效，返回毫秒数；否则返回 None
pub fn timestamp_to_millis(ts: &Timestamp) -> Option<i64> {
    // 提前返回：如果时间戳为零值，直接返回 None
    if ts.seconds == 0 && ts.nanos == 0 {
        return None;
    }
    Some(ts.seconds * 1000 + (ts.nanos as i64 / 1_000_000))
}

/// 毫秒数转换为时间戳
///
/// # 参数
/// * `ms` - 毫秒数
///
/// # 返回
/// * `Option<Timestamp>` - 返回对应的时间戳
pub fn millis_to_timestamp(ms: i64) -> Option<Timestamp> {
    let seconds = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as i32;
    Some(Timestamp { seconds, nanos })
}

/// 时间戳转换为 DateTime
///
/// # 参数
/// * `ts` - 时间戳
///
/// # 返回
/// * `Option<DateTime<Utc>>` - 如果时间戳有效，返回 DateTime；否则返回 None
pub fn timestamp_to_datetime(ts: &Timestamp) -> Option<DateTime<Utc>> {
    timestamp_to_millis(ts).and_then(|ms| Utc.timestamp_millis_opt(ms).single())
}

/// DateTime 转换为时间戳
///
/// # 参数
/// * `dt` - DateTime
///
/// # 返回
/// * `Timestamp` - 返回对应的时间戳
pub fn datetime_to_timestamp(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// 获取当前时间戳（毫秒）
///
/// # 返回
/// * `i64` - 当前时间戳（毫秒）
pub fn current_millis() -> i64 {
    Utc::now().timestamp_millis()
}

/// 时间线元数据
#[derive(Debug, Clone, Default)]
pub struct TimelineMetadata {
    pub emit_ts: Option<i64>,
    pub ingestion_ts: i64,
    pub persisted_ts: Option<i64>,
    pub dispatched_ts: Option<i64>,
    pub acked_ts: Option<i64>,
    pub read_ts: Option<i64>,
    pub deleted_ts: Option<i64>,
}

/// 从消息的 extra 字段中提取时间线元数据
///
/// # 参数
/// * `extra` - 消息的 extra 字段
/// * `default_ingestion_ts` - 默认的 ingestion 时间戳
///
/// # 返回
/// * `TimelineMetadata` - 时间线元数据
pub fn extract_timeline_from_extra(
    extra: &HashMap<String, String>,
    default_ingestion_ts: i64,
) -> TimelineMetadata {
    // 提前返回：如果 extra 中没有 timeline 字段，直接返回默认值
    let raw = match extra.get("timeline") {
        Some(raw) => raw,
        None => {
            return TimelineMetadata {
                ingestion_ts: default_ingestion_ts,
                ..TimelineMetadata::default()
            };
        }
    };

    // 解析 timeline JSON 字符串
    let map = match serde_json::from_str::<HashMap<String, String>>(raw) {
        Ok(map) => map,
        Err(_) => {
            return TimelineMetadata {
                ingestion_ts: default_ingestion_ts,
                ..TimelineMetadata::default()
            };
        }
    };

    TimelineMetadata {
        emit_ts: map.get("emit_ts").and_then(|v| parse_i64(v.as_str())),
        ingestion_ts: map
            .get("ingestion_ts")
            .and_then(|v| parse_i64(v.as_str()))
            .unwrap_or(default_ingestion_ts),
        persisted_ts: map.get("persisted_ts").and_then(|v| parse_i64(v.as_str())),
        dispatched_ts: map.get("dispatched_ts").and_then(|v| parse_i64(v.as_str())),
        acked_ts: map.get("acked_ts").and_then(|v| parse_i64(v.as_str())),
        read_ts: map.get("read_ts").and_then(|v| parse_i64(v.as_str())),
        deleted_ts: map.get("deleted_ts").and_then(|v| parse_i64(v.as_str())),
    }
}

/// 将时间线元数据嵌入到 `extra` Map 中（与具体消息类型解耦，domain 层可用）
pub fn embed_timeline_in_extra_map(
    extra: &mut HashMap<String, String>,
    timeline: &TimelineMetadata,
) {
    let mut timeline_map = HashMap::new();

    if let Some(value) = timeline.emit_ts {
        timeline_map.insert("emit_ts".to_string(), value.to_string());
    }

    timeline_map.insert(
        "ingestion_ts".to_string(),
        timeline.ingestion_ts.to_string(),
    );

    if let Some(value) = timeline.persisted_ts {
        timeline_map.insert("persisted_ts".to_string(), value.to_string());
    }

    if let Some(value) = timeline.dispatched_ts {
        timeline_map.insert("dispatched_ts".to_string(), value.to_string());
    }

    if let Some(value) = timeline.acked_ts {
        timeline_map.insert("acked_ts".to_string(), value.to_string());
    }

    if let Some(value) = timeline.read_ts {
        timeline_map.insert("read_ts".to_string(), value.to_string());
    }

    if let Some(value) = timeline.deleted_ts {
        timeline_map.insert("deleted_ts".to_string(), value.to_string());
    }

    let json = serde_json::to_string(&timeline_map).unwrap_or_default();
    extra.insert("timeline".to_string(), json);
}

/// 将时间线元数据嵌入到消息的 extra 字段中（proto Message）
pub fn embed_timeline_in_extra(
    message: &mut flare_proto::common::Message,
    timeline: &TimelineMetadata,
) {
    embed_timeline_in_extra_map(&mut message.extra, timeline);
}

/// 解析 i64 字符串
///
/// # 参数
/// * `value` - 字符串值
///
/// # 返回
/// * `Option<i64>` - 如果解析成功，返回 i64；否则返回 None
fn parse_i64(value: &str) -> Option<i64> {
    value.parse::<i64>().ok()
}

/// 从消息的 extra 字段中提取 seq
///
/// # 参数
/// * `message` - 消息对象
///
/// # 返回
/// * `Option<i64>` - 如果消息包含 seq，返回 seq 值；否则返回 None
///
/// # 示例
/// ```
/// use flare_im_core::utils::extract_seq_from_message;
/// use flare_proto::common::Message;
///
/// let mut message = Message::default();
/// message.extra.insert("seq".to_string(), "100".to_string());
///
/// let seq = extract_seq_from_message(&message);
/// assert_eq!(seq, Some(100));
/// ```
pub fn extract_seq_from_message(message: &flare_proto::common::Message) -> Option<i64> {
    message
        .extra
        .get("seq")
        .and_then(|seq_str| seq_str.parse::<i64>().ok())
}

/// 将 seq 嵌入到消息的 extra 字段中
///
/// # 参数
/// * `message` - 消息对象（可变引用）
/// * `seq` - 序列号
///
/// # 示例
/// ```
/// use flare_im_core::utils::embed_seq_in_message;
/// use flare_proto::common::Message;
///
/// let mut message = Message::default();
/// embed_seq_in_message(&mut message, 100);
///
/// assert_eq!(message.extra.get("seq"), Some(&"100".to_string()));
/// ```
pub fn embed_seq_in_message(message: &mut flare_proto::common::Message, seq: i64) {
    message.extra.insert("seq".to_string(), seq.to_string());
}

/// 从消息的 extra 字段中提取 seq（从 HashMap 直接提取）
///
/// # 参数
/// * `extra` - 消息的 extra 字段
///
/// # 返回
/// * `Option<i64>` - 如果 extra 包含 seq，返回 seq 值；否则返回 None
///
/// # 示例
/// ```
/// use std::collections::HashMap;
/// use flare_im_core::utils::extract_seq_from_extra;
///
/// let mut extra = HashMap::new();
/// extra.insert("seq".to_string(), "100".to_string());
///
/// let seq = extract_seq_from_extra(&extra);
/// assert_eq!(seq, Some(100));
/// ```
pub fn extract_seq_from_extra(extra: &HashMap<String, String>) -> Option<i64> {
    extra
        .get("seq")
        .and_then(|seq_str| seq_str.parse::<i64>().ok())
}

/// 将 seq 嵌入到 extra 字段中
///
/// # 参数
/// * `extra` - 消息的 extra 字段（可变引用）
/// * `seq` - 序列号
///
/// # 示例
/// ```
/// use std::collections::HashMap;
/// use flare_im_core::utils::embed_seq_in_extra;
///
/// let mut extra = HashMap::new();
/// embed_seq_in_extra(&mut extra, 100);
///
/// assert_eq!(extra.get("seq"), Some(&"100".to_string()));
/// ```
pub fn embed_seq_in_extra(extra: &mut HashMap<String, String>, seq: i64) {
    extra.insert("seq".to_string(), seq.to_string());
}

/// 未读数计算工具函数
///
/// 计算未读数：`unread_count = last_message_seq - last_read_msg_seq`
///
/// # 参数
/// * `last_message_seq` - 最后一条消息的 seq（可选）
/// * `last_read_msg_seq` - 最后已读消息的 seq
///
/// # 返回
/// * `i32` - 未读数（>= 0）
///
/// # 示例
/// ```
/// use flare_im_core::utils::calculate_unread_count;
///
/// let unread_count = calculate_unread_count(Some(100), 80);
/// assert_eq!(unread_count, 20);
///
/// let unread_count = calculate_unread_count(None, 80);
/// assert_eq!(unread_count, 0);
///
/// let unread_count = calculate_unread_count(Some(50), 80);
/// assert_eq!(unread_count, 0); // 不会返回负数
/// ```
pub fn calculate_unread_count(last_message_seq: Option<i64>, last_read_msg_seq: i64) -> i32 {
    // 使用 match 表达式替代 if let，提高可读性
    match last_message_seq {
        Some(last_seq) => (last_seq - last_read_msg_seq).max(0) as i32,
        None => 0,
    }
}
