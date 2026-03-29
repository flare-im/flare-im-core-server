//! Kafka Topic 名称常量。

// --- Storage Writer / 消息与操作流 ---

pub const TOPIC_MESSAGE_MAIN: &str = "flare.im.message.main";
pub const TOPIC_MESSAGE_MAIN_DLQ: &str = "flare.im.message.main.dlq";
pub const TOPIC_MESSAGE_CREATED: &str = "flare.im.message.storage";
pub const TOPIC_MESSAGE_EVENTS: &str = "flare.im.message.events";

// --- 对话 / 编排 ---

pub const TOPIC_CONVERSATION_UPDATE: &str = "flare.im.conversation.update";
pub const TOPIC_CONVERSATION_ENSURE: &str = "flare.im.conversation.ensure";

// --- Push Proxy / Push Server 入站分 topic ---

pub const TOPIC_PUSH_MESSAGES: &str = "push-messages";
pub const TOPIC_PUSH_EVENTS: &str = "push-events";
pub const TOPIC_PUSH_NOTIFICATIONS: &str = "push-notifications";
pub const TOPIC_PUSH_ACKS: &str = "push-acks";
pub const TOPIC_PUSH_CUSTOM: &str = "push-custom";

// --- Push Server → Worker ---

pub const TOPIC_PUSH_ONLINE: &str = "push-online";
pub const TOPIC_PUSH_OFFLINE: &str = "push-offline";
pub const TOPIC_PUSH_DLQ: &str = "push-dlq";
