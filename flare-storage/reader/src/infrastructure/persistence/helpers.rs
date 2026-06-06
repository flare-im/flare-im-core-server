//! 消息存储辅助：extra 解析等（init_v2 使用 INT 类型，不再使用字符串 message_type/status/content_type）。

use flare_proto::common::MessageSource;
use serde_json::{Value, from_value};
use std::collections::HashMap;

fn timestamp_json_to_millis(value: Option<&Value>) -> Option<i64> {
    value
        .and_then(|v| v.as_object())
        .and_then(|obj| {
            let seconds = obj.get("seconds")?.as_i64()?;
            let nanos = obj.get("nanos").and_then(|v| v.as_i64()).unwrap_or(0);
            Some(seconds.saturating_mul(1000) + nanos / 1_000_000)
        })
        .or_else(|| value.and_then(|v| v.as_i64()))
}

/// 从 extra 解析出的租户信息（与 metadata 解耦，仅读模型使用）
#[derive(Debug, Clone, Default)]
pub struct TenantInfo {
    pub tenant_id: String,
    pub business_type: String,
    pub environment: String,
    pub organization_id: String,
    pub labels: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
}

/// 从 extra JSONB 中提取 seq
pub fn extract_seq_from_extra(extra: &serde_json::Map<String, Value>) -> Option<i64> {
    extra.get("seq").and_then(|v| v.as_i64())
}

/// 从 extra JSONB 中解析租户信息
pub fn parse_tenant_from_extra(extra_map: &HashMap<String, String>) -> Option<TenantInfo> {
    extra_map.get("tenant_id").map(|tenant_id| {
        let mut labels = HashMap::new();
        if let Some(labels_str) = extra_map.get("labels")
            && let Ok(labels_obj) = serde_json::from_str::<HashMap<String, String>>(labels_str)
        {
            labels = labels_obj;
        }
        let mut tenant_attributes = HashMap::new();
        if let Some(attrs_str) = extra_map.get("tenant_attributes")
            && let Ok(attrs_obj) = serde_json::from_str::<HashMap<String, String>>(attrs_str)
        {
            tenant_attributes = attrs_obj;
        }
        TenantInfo {
            tenant_id: tenant_id.clone(),
            business_type: extra_map.get("business_type").cloned().unwrap_or_default(),
            environment: extra_map.get("environment").cloned().unwrap_or_default(),
            organization_id: extra_map
                .get("organization_id")
                .cloned()
                .unwrap_or_default(),
            labels,
            attributes: tenant_attributes,
        }
    })
}

/// 从 extra JSONB 中解析消息源
pub fn parse_message_source_from_extra(extra_map: &HashMap<String, String>) -> i32 {
    let source_str = extra_map.get("sender_type").cloned().unwrap_or_default();
    match source_str.as_str() {
        "user" => MessageSource::User as i32,
        "system" => MessageSource::System as i32,
        "bot" => MessageSource::Bot as i32,
        "admin" => MessageSource::Admin as i32,
        _ => MessageSource::Unspecified as i32,
    }
}

/// 从 extra JSONB 中解析标签
pub fn parse_tags_from_extra(extra_map: &HashMap<String, String>) -> Vec<String> {
    extra_map
        .get("tags")
        .and_then(|tags_str| serde_json::from_str::<Vec<String>>(tags_str).ok())
        .unwrap_or_default()
}

/// 从 extra JSONB 中解析属性（排除系统字段）
pub fn parse_attributes_from_extra(extra_map: &HashMap<String, String>) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    for (k, v) in extra_map {
        if !matches!(
            k.as_str(),
            "tenant_id"
                | "business_id"
                | "receiver_id"
                | "conversation_type"
                | "sender_type"
                | "tags"
                | "seq"
        ) {
            attributes.insert(k.clone(), v.clone());
        }
    }
    attributes
}

/// 从 JSONB 解析 MessageReadRecord 列表
pub fn parse_read_by_from_jsonb(
    read_by: Option<Value>,
) -> Vec<flare_proto::common::MessageReadRecord> {
    read_by
        .and_then(|v| {
            from_value::<Vec<serde_json::Value>>(v).ok().map(|records| {
                let mut result = Vec::new();
                for record in records {
                    if let (Some(user_id), read_at_opt, burned_at_opt) = (
                        record.get("user_id").and_then(|v| v.as_str()),
                        record.get("read_at"),
                        record.get("burned_at"),
                    ) {
                        let read_at = timestamp_json_to_millis(read_at_opt).unwrap_or_default();
                        let burned_at = timestamp_json_to_millis(burned_at_opt);
                        result.push(flare_proto::common::MessageReadRecord {
                            user_id: user_id.to_string(),
                            read_at,
                            retention_expired_at: burned_at,
                        });
                    }
                }
                result
            })
        })
        .unwrap_or_default()
}
