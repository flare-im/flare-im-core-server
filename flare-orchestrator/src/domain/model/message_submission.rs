use anyhow::{Result, anyhow};
use chrono::Utc;
use flare_im_core::utils::{
    TimelineMetadata, current_millis, datetime_to_timestamp, embed_timeline_in_extra,
    timestamp_to_millis,
};
use flare_proto::common::Message;
use uuid::Uuid;

use crate::domain::model::message_kind::MessageProfile;

#[derive(Clone, Debug)]
pub struct MessageDefaults {
    pub default_business_type: String,
    pub default_conversation_type: String,
    pub default_sender_type: String,
    pub default_tenant_id: Option<String>,
}

#[derive(Clone)]
pub struct MessageSubmission {
    pub kafka_payload: Message,
    pub message: Message,
    pub message_id: String,
    pub timeline: TimelineMetadata,
}

impl MessageSubmission {
    /// 从 common.Message 准备提交（envelope 在 extra：__sync、__tags、metadata）
    pub fn prepare(mut request: Message, defaults: &MessageDefaults) -> Result<Self> {
        if request.conversation_id.is_empty() {
            return Err(anyhow!("conversation_id is required"));
        }

        let client_provided_server_id = if !request.server_id.is_empty() {
            Some(request.server_id.clone())
        } else {
            None
        };
        request.server_id = Uuid::new_v4().to_string();
        if let Some(old_server_id) = client_provided_server_id {
            request.extra.insert("original_server_id".to_string(), old_server_id);
        }
        if request.client_msg_id.is_empty() {
            request.client_msg_id = request.server_id.clone();
        }
        if request.sender_id.is_empty() {
            return Err(anyhow!("sender_id is required"));
        }
        if request.source == 0 {
            request.source = match defaults.default_sender_type.as_str() {
                "user" => 1,
                "system" => 2,
                "bot" => 3,
                "admin" => 4,
                _ => 1,
            };
        }
        if request.extra.get("business_type").map_or(true, |v| v.is_empty()) {
            request.extra.insert("business_type".to_string(), defaults.default_business_type.clone());
        }
        if request.conversation_type == 0 {
            request.conversation_type = match defaults.default_conversation_type.as_str() {
                "single" => 1,
                "group" => 2,
                "channel" => 3,
                _ => 1,
            };
        }
        if request.status == 0 {
            request.status = 1;
        }
        let profile = MessageProfile::ensure(&mut request);
        if request.extra.get("message_type").is_none() {
            request.extra.insert(
                "message_type".into(),
                profile.message_type_label().to_string(),
            );
        }
        if request.timestamp.is_none() {
            request.timestamp = Some(datetime_to_timestamp(Utc::now()));
        }
        let ingestion_ts = current_millis();
        let emit_ts = request.timestamp.as_ref().and_then(timestamp_to_millis);
        let shard_key = request
            .extra
            .get("shard_key")
            .cloned()
            .unwrap_or_else(|| request.conversation_id.clone());
        request.extra.entry("shard_key".to_string()).or_insert(shard_key.clone());
        let tenant_id = request
            .extra
            .get("x-tenant-id")
            .or_else(|| request.extra.get("tenant_id"))
            .cloned()
            .or_else(|| defaults.default_tenant_id.clone())
            .unwrap_or_else(|| "default".to_string());
        request.extra.entry("tenant_id".to_string()).or_insert(tenant_id);
        let timeline = TimelineMetadata {
            emit_ts,
            ingestion_ts,
            ..TimelineMetadata::default()
        };
        embed_timeline_in_extra(&mut request, &timeline);
        request.client_msg_id =
            String::from_utf8_lossy(request.client_msg_id.as_bytes()).to_string();
        let message_id = request.server_id.clone();
        Ok(Self {
            kafka_payload: request.clone(),
            message: request,
            message_id,
            timeline,
        })
    }
}
