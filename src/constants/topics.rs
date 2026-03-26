//! Kafka Topic 名称常量。
//!
//! 与 `flare-push`、`flare-storage/writer` 等服务的默认订阅/发布名对齐；环境变量可覆盖运行时配置，但默认值应与此处一致。

// --- Storage Writer / 消息与操作流 ---

pub const TOPIC_MESSAGE_CREATED: &str = "flare.im.message.created";
pub const TOPIC_MESSAGE_EVENTS: &str = "flare.im.message.events";

// --- 对话 / 编排 ---

pub const TOPIC_CONVERSATION_UPDATE: &str = "flare.im.conversation.update";
pub const TOPIC_CONVERSATION_ENSURE: &str = "flare.im.conversation.ensure";

// --- Push（任务总线，与编排侧约定）---

pub const TOPIC_PUSH_TASKS: &str = "flare.im.push.tasks";

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
