//! 把协议与绑定里的数据换成提示规则的输入，以及把规则的输出换回协议取值。
//!
//! 规则 crate 不依赖协议（要单独编进 wasm），两边的对应关系只在这里写一次。

use chrono::{Offset, TimeZone, Utc};
use chrono_tz::Tz;
use flare_notify_rules::{
    AlertClass as RulesAlertClass, ConversationKind, Decision, MinuteOfWeek, MultiDevicePolicy,
    PreviewMode, QuietHours, UserPrefs,
};
use flare_proto::common::{
    AlertClass, ConversationType, MultiDeviceAlertPolicy, NotificationPreview,
    UserNotificationSettings,
};

/// 协议的提示等级换成规则的等级。未指定按不提示处理：路由在这之前已经挡掉了不提示的载荷。
pub fn rules_alert_class(class: AlertClass) -> RulesAlertClass {
    match class {
        AlertClass::Unspecified | AlertClass::None => RulesAlertClass::None,
        AlertClass::Silent => RulesAlertClass::Silent,
        AlertClass::Normal => RulesAlertClass::Normal,
        AlertClass::Mention => RulesAlertClass::Mention,
        AlertClass::Urgent => RulesAlertClass::Urgent,
    }
}

/// 这条消息计不计入角标：口径与客户端的未读数一致，不提示的不算。
pub fn counts_toward_badge(class: AlertClass) -> bool {
    !matches!(class, AlertClass::Unspecified | AlertClass::None)
}

/// 会话类型决定响铃阶梯。消息没带类型时按会话标识的首位数字（协议约定的前缀）判断；
/// 都认不出时按群聊：群聊的阶梯更长，拿不准时宁可少响。
pub fn conversation_kind(conversation_type: i32, conversation_id: &str) -> ConversationKind {
    let by_type = match ConversationType::try_from(conversation_type) {
        Ok(ConversationType::Group | ConversationType::Channel | ConversationType::Broadcast) => {
            Some(ConversationKind::Group)
        }
        Ok(
            ConversationType::Single
            | ConversationType::Ai
            | ConversationType::System
            | ConversationType::Customer
            | ConversationType::Temp,
        ) => Some(ConversationKind::Direct),
        Ok(ConversationType::Unspecified) | Err(_) => None,
    };
    by_type.unwrap_or_else(|| match conversation_id.trim().as_bytes().first() {
        Some(b'1' | b'3' | b'4' | b'5' | b'6') => ConversationKind::Direct,
        _ => ConversationKind::Group,
    })
}

/// 设备当地时间在一周中的位置。时区按绑定登记的 IANA 名换算（含夏令时）；没有登记或认不出的
/// 时区按 UTC 算。
pub fn local_minute_of_week(time_zone: &str, now_ms: i64) -> MinuteOfWeek {
    MinuteOfWeek::from_utc_ms(now_ms, utc_offset_minutes(time_zone, now_ms))
}

/// 这个时区在 `now_ms` 时的 UTC 偏移（分钟）。
fn utc_offset_minutes(time_zone: &str, now_ms: i64) -> i32 {
    let Ok(zone) = time_zone.trim().parse::<Tz>() else {
        return 0;
    };
    let Some(utc) = Utc.timestamp_millis_opt(now_ms).single() else {
        return 0;
    };
    let offset = zone.offset_from_utc_datetime(&utc.naive_utc());
    offset.fix().local_minus_utc() / 60
}

/// 协议的用户级通知设置换成规则的用户偏好。认不出的预览按隐藏（宁可少露），认不出的多端策略
/// 按延迟（缺省值）。
pub fn user_prefs(settings: &UserNotificationSettings) -> UserPrefs {
    let quiet_hours = settings.quiet_hours.as_ref().map(|quiet| QuietHours {
        start_minute: u16::try_from(quiet.start_minute).unwrap_or(u16::MAX),
        end_minute: u16::try_from(quiet.end_minute).unwrap_or(u16::MAX),
        weekdays: (quiet.weekdays & 0x7f) as u8,
        allow_mentions: quiet.allow_mentions,
    });
    let preview = match NotificationPreview::try_from(settings.preview) {
        Ok(NotificationPreview::Unspecified | NotificationPreview::Full) => PreviewMode::Full,
        Ok(NotificationPreview::SenderOnly) => PreviewMode::SenderOnly,
        Ok(NotificationPreview::Hidden) | Err(_) => PreviewMode::Hidden,
    };
    let multi_device = match MultiDeviceAlertPolicy::try_from(settings.multi_device) {
        Ok(MultiDeviceAlertPolicy::Immediate) => MultiDevicePolicy::Immediate,
        Ok(MultiDeviceAlertPolicy::BadgeOnly) => MultiDevicePolicy::BadgeOnly,
        Ok(MultiDeviceAlertPolicy::Unspecified | MultiDeviceAlertPolicy::Delay) | Err(_) => {
            MultiDevicePolicy::Delay
        }
    };
    UserPrefs {
        quiet_hours,
        preview,
        multi_device,
    }
}

/// 规则的预览换成离线任务里的协议取值。
pub fn preview_to_proto(preview: PreviewMode) -> NotificationPreview {
    match preview {
        PreviewMode::Full => NotificationPreview::Full,
        PreviewMode::SenderOnly => NotificationPreview::SenderOnly,
        PreviewMode::Hidden => NotificationPreview::Hidden,
    }
}

/// 决策在指标里的标签。
pub fn decision_label(decision: Decision) -> &'static str {
    match decision {
        Decision::Drop => "drop",
        Decision::Quiet => "quiet",
        Decision::Candidate { priority: false } => "candidate",
        Decision::Candidate { priority: true } => "priority",
        Decision::Delay { .. } => "delay",
        Decision::BadgeOnly => "badge_only",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::QuietHours as ProtoQuietHours;

    /// 2026-11-11（周三）00:30 UTC。
    const WEDNESDAY_0030_UTC: i64 = 1_794_357_000_000;

    #[test]
    fn local_time_follows_the_registered_zone_and_falls_back_to_utc() {
        let utc = local_minute_of_week("", WEDNESDAY_0030_UTC);
        assert_eq!((utc.weekday(), utc.minute_of_day()), (2, 30));
        assert_eq!(local_minute_of_week("Not/AZone", WEDNESDAY_0030_UTC), utc);
        // 东八区：同一时刻是周三 08:30。
        let shanghai = local_minute_of_week("Asia/Shanghai", WEDNESDAY_0030_UTC);
        assert_eq!(
            (shanghai.weekday(), shanghai.minute_of_day()),
            (2, 8 * 60 + 30)
        );
        // 西五区（十一月是冬令时）：还是周二 19:30。
        let new_york = local_minute_of_week("America/New_York", WEDNESDAY_0030_UTC);
        assert_eq!(
            (new_york.weekday(), new_york.minute_of_day()),
            (1, 19 * 60 + 30)
        );
    }

    #[test]
    fn user_settings_map_to_rule_prefs() {
        let defaults = user_prefs(&UserNotificationSettings::default());
        assert_eq!(defaults, UserPrefs::default());

        let settings = UserNotificationSettings {
            quiet_hours: Some(ProtoQuietHours {
                start_minute: 22 * 60,
                end_minute: 7 * 60,
                weekdays: 0b1_0001_1111,
                allow_mentions: true,
            }),
            preview: NotificationPreview::SenderOnly as i32,
            multi_device: MultiDeviceAlertPolicy::BadgeOnly as i32,
            settings_version: 3,
        };
        let prefs = user_prefs(&settings);
        let quiet = prefs.quiet_hours.expect("quiet hours");
        assert_eq!((quiet.start_minute, quiet.end_minute), (1320, 420));
        assert_eq!(quiet.weekdays, 0b001_1111, "高于周日的位丢掉");
        assert!(quiet.allow_mentions);
        assert_eq!(prefs.preview, PreviewMode::SenderOnly);
        assert_eq!(prefs.multi_device, MultiDevicePolicy::BadgeOnly);

        let unknown = UserNotificationSettings {
            preview: 99,
            multi_device: 99,
            ..Default::default()
        };
        let prefs = user_prefs(&unknown);
        assert_eq!(prefs.preview, PreviewMode::Hidden, "认不出的预览宁可少露");
        assert_eq!(prefs.multi_device, MultiDevicePolicy::Delay);
    }

    #[test]
    fn conversation_kind_prefers_the_message_type() {
        assert_eq!(
            conversation_kind(ConversationType::Single as i32, "2-abc"),
            ConversationKind::Direct
        );
        assert_eq!(
            conversation_kind(ConversationType::Channel as i32, "1-abc"),
            ConversationKind::Group
        );
        assert_eq!(conversation_kind(0, "1abc"), ConversationKind::Direct);
        assert_eq!(conversation_kind(0, "2abc"), ConversationKind::Group);
        assert_eq!(conversation_kind(0, ""), ConversationKind::Group);
    }

    #[test]
    fn only_alerting_classes_count_toward_the_badge() {
        assert!(!counts_toward_badge(AlertClass::None));
        assert!(!counts_toward_badge(AlertClass::Unspecified));
        assert!(counts_toward_badge(AlertClass::Silent));
        assert!(counts_toward_badge(AlertClass::Mention));
        assert_eq!(
            rules_alert_class(AlertClass::Urgent),
            RulesAlertClass::Urgent
        );
        assert_eq!(
            preview_to_proto(PreviewMode::Hidden),
            NotificationPreview::Hidden
        );
    }
}
