//! IM Core MQ topic/subject 名称常量。

// --- Storage Writer / 消息与操作流 ---

pub const TOPIC_MESSAGE_MAIN: &str = "flare.im.message.main";
pub const TOPIC_MESSAGE_MAIN_DLQ: &str = "flare.im.message.main.dlq";
pub const TOPIC_MESSAGE_RETRY_5S: &str = "flare.im.message.retry.5s";
pub const TOPIC_MESSAGE_RETRY_30S: &str = "flare.im.message.retry.30s";
pub const TOPIC_MESSAGE_RETRY_5M: &str = "flare.im.message.retry.5m";
pub const TOPIC_MESSAGE_MAIN_RETRY_5S: &str = "flare.im.message.main.retry.5s";
pub const TOPIC_MESSAGE_STORAGE_RETRY_5S: &str = "flare.im.message.storage.retry.5s";
pub const TOPIC_MESSAGE_CREATED: &str = "flare.im.message.storage";
pub const TOPIC_MESSAGE_EVENTS: &str = "flare.im.message.events";

// --- 对话 / 编排 ---

pub const TOPIC_CONVERSATION_UPDATE: &str = "flare.im.conversation.update";
pub const TOPIC_CONVERSATION_ENSURE: &str = "flare.im.conversation.ensure";

// --- Push Proxy / Push Server 入站 topic ---

pub const TOPIC_PUSH_MESSAGES: &str = "flare.im.push.messages";
pub const TOPIC_PUSH_EVENTS: &str = "flare.im.push.events";
pub const TOPIC_PUSH_ACKS: &str = "flare.im.push.acks";
pub const TOPIC_PUSH_ENVELOPE: &str = "flare.im.push.envelope"; // 统一推送信封 Topic
/// 设备交接：网关（送达确认超时、链路断开、客户端托管）与 Online 清扫器（网关失联）发布
/// `DeviceHandoff`，分区键为 user_id；push-server 据此补提示。
pub const TOPIC_PUSH_HANDOFF: &str = "flare.im.push.handoff";

// --- Push Server → Worker ---

pub const TOPIC_PUSH_ONLINE: &str = "flare.im.push.online";
/// 离线任务的三条优先级通道，各自一个消费者，批量积压不影响单聊。三者都在 `FLARE_PUSH` 流的
/// `flare.im.push.*` 之内（最后一段不含点）。
///
/// 单聊、紧急、点名与对已发通知的修正。
pub const TOPIC_PUSH_OFFLINE: &str = "flare.im.push.offline";
/// 群里的普通提示与尾部刷新。
pub const TOPIC_PUSH_OFFLINE_GROUP: &str = "flare.im.push.offline_group";
/// 大群波次、@所有人波次、摘要与补提示。
pub const TOPIC_PUSH_OFFLINE_BULK: &str = "flare.im.push.offline_bulk";
pub const TOPIC_PUSH_DLQ: &str = "flare.im.push.dlq";

#[cfg(test)]
mod tests {
    use super::*;

    /// 推送任务的主题必须落在 `FLARE_PUSH` 流的 `flare.im.push.*` 里：通配只匹配一段，
    /// 多一个点就没有流收它，发布直接失败。
    #[test]
    fn push_task_subjects_stay_inside_the_push_stream_wildcard() {
        for subject in [
            TOPIC_PUSH_ONLINE,
            TOPIC_PUSH_OFFLINE,
            TOPIC_PUSH_OFFLINE_GROUP,
            TOPIC_PUSH_OFFLINE_BULK,
            TOPIC_PUSH_DLQ,
            TOPIC_PUSH_HANDOFF,
        ] {
            let last = subject
                .strip_prefix("flare.im.push.")
                .unwrap_or_else(|| panic!("{subject} is outside flare.im.push."));
            assert!(!last.is_empty() && !last.contains('.'), "{subject}");
        }
    }
}
