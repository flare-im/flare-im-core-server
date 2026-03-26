//! # Hook 上下文数据
//!
//! 存储 Hook 特定的上下文信息，这些信息会被存储到 `flare_server_core::Context` 的自定义数据中

use std::collections::HashMap;
use std::time::SystemTime;

/// Hook 特定的上下文数据
///
/// 这些字段会被存储到 `flare_server_core::Context` 的自定义数据中
#[derive(Debug, Clone, Default)]
pub struct HookContextData {
    pub conversation_id: Option<String>,
    pub conversation_type: Option<String>,
    pub message_type: Option<String>,
    pub sender_id: Option<String>,
    pub tags: HashMap<String, String>,
    pub attributes: HashMap<String, String>,
    pub request_metadata: HashMap<String, String>,
    pub occurred_at: Option<SystemTime>,
}

impl HookContextData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_conversation_id(mut self, conversation_id: impl Into<String>) -> Self {
        self.conversation_id = Some(conversation_id.into());
        self
    }

    pub fn with_conversation_type(mut self, conversation_type: impl Into<String>) -> Self {
        self.conversation_type = Some(conversation_type.into());
        self
    }

    pub fn with_message_type(mut self, message_type: impl Into<String>) -> Self {
        self.message_type = Some(message_type.into());
        self
    }

    pub fn with_sender_id(mut self, sender_id: impl Into<String>) -> Self {
        self.sender_id = Some(sender_id.into());
        self
    }

    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn with_request_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.request_metadata = metadata;
        self
    }

    pub fn occurred_now(mut self) -> Self {
        self.occurred_at = Some(SystemTime::now());
        self
    }
}

/// 从 `flare_server_core::Context` 中提取 Hook 上下文数据
pub fn get_hook_context_data(ctx: &flare_server_core::context::Context) -> Option<&HookContextData> {
    ctx.get_data::<HookContextData>()
}

/// 将 Hook 上下文数据存储到 `flare_server_core::Context` 中
pub fn set_hook_context_data(
    ctx: flare_server_core::context::Context,
    data: HookContextData,
) -> flare_server_core::context::Context {
    ctx.insert_data(data)
}

