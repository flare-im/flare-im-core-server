use crate::domain::model::ConversationType;
use chrono::Utc;
use flare_im_contracts::utils::{TimelineMetadata, current_millis, normalize_tenant_id};
use flare_im_message_pipeline::SubmittedMessage;
use flare_proto::common::Message;
use flare_server_core::error::{ErrorCode, Result};
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
    pub fn prepare(mut request: Message, defaults: &MessageDefaults) -> Result<Self> {
        if request.conversation_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::BadRequest,
                "conversation_id is required"
            ));
        }
        if request.sender_id.is_empty() {
            return Err(flare_err!(ErrorCode::BadRequest, "sender_id is required"));
        }

        let client_provided_server_id = if !request.server_id.is_empty() {
            Some(request.server_id.clone())
        } else {
            None
        };
        request.server_id = Uuid::new_v4().to_string();

        if let Some(old_server_id) = client_provided_server_id {
            request
                .extensions
                .insert("original_server_id".to_string(), old_server_id.into_bytes());
        }

        if request.client_msg_id.is_empty() {
            request.client_msg_id = request.server_id.clone();
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
        if request.conversation_type == 0 {
            request.conversation_type = defaults.default_conversation_type.as_int();
        }
        if request.status == 0 {
            request.status = 1;
        }

        let _profile = MessageProfile::ensure(&mut request);

        if request.created_at <= 0 {
            request.created_at = Utc::now().timestamp_millis();
        }

        let ingestion_ts = current_millis();
        let emit_ts = (request.created_at > 0).then_some(request.created_at);
        let timeline = TimelineMetadata {
            emit_ts,
            ingestion_ts,
            ..TimelineMetadata::default()
        };

        let _ = defaults.default_tenant_id.as_ref().map(normalize_tenant_id);

        let message_id = request.server_id.clone();
        Ok(Self {
            message: request,
            message_id,
            timeline,
        })
    }
}

impl SubmittedMessage for MessageSubmission {
    fn message(&self) -> &Message {
        &self.message
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }
}
