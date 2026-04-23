use crate::domain::model::ConversationType;
use crate::error::Result;
use chrono::Utc;
use flare_im_core::{
    ErrorCode,
    utils::{
        TimelineMetadata, current_millis, datetime_to_timestamp, embed_timeline_in_extra,
        timestamp_to_millis,
    },
};
use flare_proto::common::Message;
use flare_server_core::flare_err;
use uuid::Uuid;

use crate::domain::model::message_kind::MessageProfile;

#[derive(Clone, Debug)]
pub struct MessageDefaults {
    pub default_business_type: String,
    pub default_conversation_type: ConversationType,
    pub default_sender_type: String,
    pub default_tenant_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MessageSubmission {
    pub message: Message,
    pub message_id: String,
    pub timeline: TimelineMetadata,
}

impl MessageSubmission {
    /// 从 common.Message 准备提交
    ///
    /// # 处理逻辑
    /// 1. 填充默认值（server_id, client_msg_id, conversation_type, status, source）
    /// 2. 推断消息类型（MessageProfile）
    /// 3. 设置时间戳和 timeline 元数据
    /// 4. 设置 tenant_id 和 shard_key
    pub fn prepare(mut request: Message, defaults: &MessageDefaults) -> Result<Self> {
        // 1. 校验必填字段
        if request.conversation_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::BadRequest,
                "conversation_id is required"
            ));
        }
        if request.sender_id.is_empty() {
            return Err(flare_err!(ErrorCode::BadRequest, "sender_id is required"));
        }

        // 2. 生成或保留 server_id
        let client_provided_server_id = if !request.server_id.is_empty() {
            Some(request.server_id.clone())
        } else {
            None
        };
        request.server_id = Uuid::new_v4().to_string();

        // 保存原始 server_id（如果有）
        if let Some(old_server_id) = client_provided_server_id {
            request
                .extra
                .insert("original_server_id".to_string(), old_server_id);
        }

        // 3. 填充 client_msg_id
        if request.client_msg_id.is_empty() {
            request.client_msg_id = request.server_id.clone();
        }

        // 4. 填充默认值
        if request.source == 0 {
            request.source = match defaults.default_sender_type.as_str() {
                "user" => 1,
                "system" => 2,
                "bot" => 3,
                "admin" => 4,
                _ => 1,
            };
        }
        if request.conversation_type == 0 {
            request.conversation_type = defaults.default_conversation_type.as_int();
        }
        if request.status == 0 {
            request.status = 1;
        }

        // 5. 推断消息类型
        let profile = MessageProfile::ensure(&mut request);

        // 6. 设置时间戳
        if request.timestamp.is_none() {
            request.timestamp = Some(datetime_to_timestamp(Utc::now()));
        }

        // 7. 构建 timeline 元数据
        let ingestion_ts = current_millis();
        let emit_ts = request.timestamp.as_ref().and_then(timestamp_to_millis);
        let timeline = TimelineMetadata {
            emit_ts,
            ingestion_ts,
            ..TimelineMetadata::default()
        };
        embed_timeline_in_extra(&mut request, &timeline);

        // 8. 设置 tenant_id（优先级：extra > defaults > default）
        let tenant_id = request
            .extra
            .get("x-tenant-id")
            .or_else(|| request.extra.get("tenant_id"))
            .cloned()
            .or_else(|| defaults.default_tenant_id.clone())
            .unwrap_or_else(|| "default".to_string());
        request
            .extra
            .entry("tenant_id".to_string())
            .or_insert(tenant_id);

        // 9. 设置 shard_key（默认为 conversation_id）
        let shard_key = request
            .extra
            .get("shard_key")
            .cloned()
            .unwrap_or_else(|| request.conversation_id.clone());
        request
            .extra
            .entry("shard_key".to_string())
            .or_insert(shard_key);

        // 10. 设置 business_type（如果 extra 中没有）
        if request
            .extra
            .get("business_type")
            .map_or(true, |v| v.is_empty())
        {
            request.extra.insert(
                "business_type".to_string(),
                defaults.default_business_type.clone(),
            );
        }

        // 11. 设置 message_type_label（如果 extra 中没有）
        if request.extra.get("message_type").is_none() {
            request.extra.insert(
                "message_type".to_string(),
                profile.message_type_label().to_string(),
            );
        }

        let message_id = request.server_id.clone();
        Ok(Self {
            message: request,
            message_id,
            timeline,
        })
    }
}
