//! 存储消息负载（领域 DTO）
//!
//! 使用 common.Message 原形；envelope（sync/tags/metadata）通过 Message.extra 约定键传递。
//! Context 与 metadata 通过 [context_to_mq_metadata] / [context_from_mq_metadata] 双向编解码。

use crate::utils::{context_from_mq_metadata, context_to_mq_metadata};
use crate::Ctx;
use flare_proto::common::Message;
use std::collections::HashMap;

/// Message.extra 中用于 envelope 的约定键（与业务 extra 区分）
pub const EXTRA_KEY_SYNC: &str = "__sync";
/// tags 存为 JSON object 字符串
pub const EXTRA_KEY_TAGS: &str = "__tags";

/// 存储消息负载（领域侧类型）
///
/// 与 proto Message 互转：envelope 信息放在 message.extra（__sync、__tags 及 metadata 键）。
#[derive(Clone, Debug)]
pub struct StorageMessagePayload {
    pub conversation_id: String,
    pub message: Option<Message>,
    pub metadata: HashMap<String, String>,
    pub tags: HashMap<String, String>,
    pub sync: bool,
}

impl StorageMessagePayload {
    /// 将 Ctx 编码并合并到 metadata（x-request-id、x-tenant-id 等）。
    pub fn with_context(mut self, ctx: &Ctx) -> Self {
        self.metadata.extend(context_to_mq_metadata(ctx));
        self
    }

    /// 从当前 metadata 解码为 Ctx（消费端从 MQ 读取后可用此还原链路上下文）。
    pub fn context_from_metadata(&self) -> Ctx {
        context_from_mq_metadata(&self.metadata)
    }

    /// 转为 proto Message（供 Kafka 等序列化）。envelope 写入 message.extra。
    pub fn to_message(&self) -> Option<Message> {
        let mut msg = self.message.clone()?;
        if msg.conversation_id.is_empty() {
            msg.conversation_id = self.conversation_id.clone();
        }
        msg.extra
            .insert(EXTRA_KEY_SYNC.to_string(), self.sync.to_string());
        if !self.tags.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.tags) {
                msg.extra.insert(EXTRA_KEY_TAGS.to_string(), json);
            }
        }
        for (k, v) in &self.metadata {
            msg.extra.insert(k.clone(), v.clone());
        }
        Some(msg)
    }

    /// 从 proto Message 解析（从 extra 读取 __sync、__tags，其余为 metadata）。
    pub fn from_message(msg: Message) -> Self {
        let conversation_id = msg.conversation_id.clone();
        let sync = msg
            .extra
            .get(EXTRA_KEY_SYNC)
            .map(|s| s == "true")
            .unwrap_or(false);
        let tags = msg
            .extra
            .get(EXTRA_KEY_TAGS)
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let mut metadata = HashMap::new();
        for (k, v) in &msg.extra {
            if k.as_str() != EXTRA_KEY_SYNC && k.as_str() != EXTRA_KEY_TAGS {
                metadata.insert(k.clone(), v.clone());
            }
        }
        let mut message_extra = msg.extra.clone();
        message_extra.remove(EXTRA_KEY_SYNC);
        message_extra.remove(EXTRA_KEY_TAGS);
        let mut message = msg;
        message.extra = message_extra;
        Self {
            conversation_id,
            message: Some(message),
            metadata,
            tags,
            sync,
        }
    }
}

impl From<Message> for StorageMessagePayload {
    fn from(msg: Message) -> Self {
        Self::from_message(msg)
    }
}

impl From<&Message> for StorageMessagePayload {
    fn from(msg: &Message) -> Self {
        Self::from_message(msg.clone())
    }
}
