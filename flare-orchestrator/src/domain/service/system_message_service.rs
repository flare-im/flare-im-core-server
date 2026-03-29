use flare_proto::common::Message;
use flare_server_core::context::Ctx;

use crate::error::{FlareError, Result};

/// 领域逻辑：构造并规范化系统消息。
pub struct SystemMessageService;

impl SystemMessageService {
    pub fn prepare(
        ctx: &Ctx,
        conversation_id: &str,
        mut message: Message,
        system_message_type: &str,
    ) -> Result<Message> {
        if conversation_id.is_empty() {
            return Err(FlareError::system("conversation_id is required"));
        }
        if system_message_type.is_empty() {
            return Err(FlareError::system("system_message_type is required"));
        }

        message.extra.insert(
            "system_message_type".to_string(),
            system_message_type.to_string(),
        );
        message
            .extra
            .insert("sender_type".to_string(), "system".to_string());
        message.extra.insert(
            flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC.to_string(),
            "false".to_string(),
        );

        let tags = std::collections::HashMap::from([
            (
                "system_message_type".to_string(),
                system_message_type.to_string(),
            ),
            ("is_system_message".to_string(), "true".to_string()),
        ]);
        if let Ok(tags_json) = serde_json::to_string(&tags) {
            message.extra.insert(
                flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS.to_string(),
                tags_json,
            );
        }
        if let Some(tenant_id) = ctx.tenant_id() {
            message
                .extra
                .insert("x-tenant-id".to_string(), tenant_id.to_string());
        }
        if message.conversation_id.is_empty() {
            message.conversation_id = conversation_id.to_string();
        }

        Ok(message)
    }
}
