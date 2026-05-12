//! JetStream Topic 名称常量。

// --- Storage Writer / 消息与操作流 ---

pub const TOPIC_MESSAGE_MAIN: &str = "flare.im.message.main";
pub const TOPIC_MESSAGE_MAIN_DLQ: &str = "flare.im.message.main.dlq";
pub const TOPIC_MESSAGE_CREATED: &str = "flare.im.message.storage";
pub const TOPIC_MESSAGE_EVENTS: &str = "flare.im.message.events";

// --- 对话 / 编排 ---

pub const TOPIC_CONVERSATION_UPDATE: &str = "flare.im.conversation.update";
pub const TOPIC_CONVERSATION_ENSURE: &str = "flare.im.conversation.ensure";

// --- Push Proxy / Push Server 入站分 topic ---

pub const TOPIC_PUSH_MESSAGES: &str = "flare.im.push.messages";
pub const TOPIC_PUSH_EVENTS: &str = "flare.im.push.events";
pub const TOPIC_PUSH_NOTIFICATIONS: &str = "flare.im.push.notifications";
pub const TOPIC_PUSH_ACKS: &str = "flare.im.push.acks";
pub const TOPIC_PUSH_CUSTOM: &str = "flare.im.push.custom";
pub const TOPIC_PUSH_ENVELOPE: &str = "flare.im.push.envelope"; // 统一推送信封 Topic

// --- Push Server → Worker ---

pub const TOPIC_PUSH_ONLINE: &str = "flare.im.push.online";
pub const TOPIC_PUSH_OFFLINE: &str = "flare.im.push.offline";
pub const TOPIC_PUSH_DLQ: &str = "flare.im.push.dlq";
