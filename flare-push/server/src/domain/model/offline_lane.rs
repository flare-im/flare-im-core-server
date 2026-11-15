//! 离线任务的优先级通道。
//!
//! 三条通道各有自己的主题与消费者，批量积压不影响单聊：
//!
//! | 通道 | 内容 |
//! |---|---|
//! | `Direct` | 单聊、不属于会话的推送、紧急、点名，以及对已发通知的修正 |
//! | `Group` | 群里的普通提示、尾部刷新 |
//! | `Bulk` | 大群波次、@所有人波次、摘要、补提示 |
//!
//! 通道只决定哪个消费者投递、谁先谁后，不改变任务本身。

use flare_notify_rules::{ConversationKind, Tier};
use flare_proto::common::AlertClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OfflineLane {
    Direct,
    Group,
    Bulk,
}

impl OfflineLane {
    /// 一条消息给某个接收者的提示走哪条通道：单聊、点名与紧急走单聊通道，群里的普通提示走群通道。
    pub fn of_alert(kind: ConversationKind, class: AlertClass) -> Self {
        if kind == ConversationKind::Direct
            || matches!(class, AlertClass::Mention | AlertClass::Urgent)
        {
            Self::Direct
        } else {
            Self::Group
        }
    }

    /// 推迟到期的提示：按会话类型与到期后的档位，与它当初那条消息同一条通道。
    pub fn of_delayed(kind: ConversationKind, tier: Tier) -> Self {
        if kind == ConversationKind::Direct || tier == Tier::Priority {
            Self::Direct
        } else {
            Self::Group
        }
    }

    /// 指标标签。
    pub fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Group => "group",
            Self::Bulk => "bulk",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_chats_mentions_and_urgent_never_share_the_group_lane() {
        use ConversationKind::{Direct, Group};
        assert_eq!(
            OfflineLane::of_alert(Direct, AlertClass::Normal),
            OfflineLane::Direct
        );
        assert_eq!(
            OfflineLane::of_alert(Group, AlertClass::Mention),
            OfflineLane::Direct
        );
        assert_eq!(
            OfflineLane::of_alert(Group, AlertClass::Urgent),
            OfflineLane::Direct
        );
        assert_eq!(
            OfflineLane::of_alert(Group, AlertClass::Normal),
            OfflineLane::Group
        );
        assert_eq!(
            OfflineLane::of_alert(Group, AlertClass::Silent),
            OfflineLane::Group
        );
        assert_eq!(
            OfflineLane::of_delayed(Group, Tier::Priority),
            OfflineLane::Direct
        );
        assert_eq!(
            OfflineLane::of_delayed(Group, Tier::Normal),
            OfflineLane::Group
        );
        assert_eq!(
            OfflineLane::of_delayed(Direct, Tier::Quiet),
            OfflineLane::Direct
        );
    }
}
