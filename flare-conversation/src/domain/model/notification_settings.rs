//! 用户级通知设置（勿扰时段、通知预览、多端提示策略）与大群「全部消息通知」订阅者的分页。
//!
//! 用户级设置每个（租户，用户）一份，跨会话、跨设备，核心只存取与校验，怎么用由推送判定（规则
//! crate）决定。没有存过的用户按默认值处理：没有勿扰时段、完整预览、多端延迟，版本为 0。

use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};

/// 一天的分钟数：勿扰的起止分钟取 `0..MINUTES_PER_DAY`。
pub const MINUTES_PER_DAY: u32 = 1440;

/// 星期掩码的有效位：bit0 周一 … bit6 周日；0 表示每天。
pub const WEEKDAYS_MASK: u32 = 0x7F;

/// 一次批量读用户级设置的用户数上限。
pub const MAX_NOTIFICATION_SETTINGS_BATCH: usize = 500;

/// 会话通知订阅者列表的总上限：超出的不列（设计里单群订阅者上限默认 5000）。
pub const MAX_NOTIFICATION_SUBSCRIBERS: usize = 5000;

/// 会话通知订阅者每页的上限，也是不指定页大小时的页大小。
pub const MAX_NOTIFICATION_SUBSCRIBER_PAGE: usize = 1000;

fn invalid(message: &'static str) -> FlareError {
    ErrorBuilder::new(ErrorCode::InvalidParameter, message).build_error()
}

/// 勿扰时段，按设备当地时间计算。起止相等表示全天，结束小于开始表示跨零点。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuietHours {
    pub start_minute: u32,
    pub end_minute: u32,
    /// 按位：bit0 周一 … bit6 周日；0 表示每天。
    pub weekdays: u32,
    /// 勿扰时段内点名到我的消息照常响铃。
    pub allow_mentions: bool,
}

impl QuietHours {
    /// 分钟在 `0..1440`，星期掩码不超过 `0x7F`。
    pub fn validate(&self) -> Result<()> {
        if self.start_minute >= MINUTES_PER_DAY || self.end_minute >= MINUTES_PER_DAY {
            return Err(invalid("quiet hours minutes must be within 0..1439"));
        }
        if self.weekdays & !WEEKDAYS_MASK != 0 {
            return Err(invalid("quiet hours weekdays mask must not exceed 0x7F"));
        }
        Ok(())
    }
}

/// 通知预览：离线推送与系统通知里显示多少消息内容。取值与线上的 `NotificationPreview` 相同。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NotificationPreview {
    /// 按完整预览处理。
    #[default]
    Unspecified,
    Full,
    SenderOnly,
    Hidden,
}

impl NotificationPreview {
    /// 线上与存储的取值；不认识的值返回 `None`。
    pub fn from_wire(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Full),
            2 => Some(Self::SenderOnly),
            3 => Some(Self::Hidden),
            _ => None,
        }
    }

    pub fn wire(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Full => 1,
            Self::SenderOnly => 2,
            Self::Hidden => 3,
        }
    }
}

/// 同一用户有别的设备正在前台使用时手机怎么提示。取值与线上的 `MultiDeviceAlertPolicy` 相同。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MultiDeviceAlertPolicy {
    /// 按延迟处理。
    #[default]
    Unspecified,
    Delay,
    Immediate,
    BadgeOnly,
}

impl MultiDeviceAlertPolicy {
    /// 线上与存储的取值；不认识的值返回 `None`。
    pub fn from_wire(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Delay),
            2 => Some(Self::Immediate),
            3 => Some(Self::BadgeOnly),
            _ => None,
        }
    }

    pub fn wire(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Delay => 1,
            Self::Immediate => 2,
            Self::BadgeOnly => 3,
        }
    }
}

/// 一个用户的用户级通知设置。`Default` 就是从没设置过的用户的设置。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserNotificationSettings {
    pub quiet_hours: Option<QuietHours>,
    pub preview: NotificationPreview,
    pub multi_device: MultiDeviceAlertPolicy,
    pub settings_version: u64,
}

/// 勿扰时段这一项怎么改。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QuietHoursChange {
    #[default]
    Keep,
    Set(QuietHours),
    Clear,
}

/// 用户级设置的部分更新：没带的项保持不变。只能由 [`Self::from_request`] 构造出合法值。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserNotificationSettingsPatch {
    pub quiet_hours: QuietHoursChange,
    pub preview: Option<NotificationPreview>,
    pub multi_device: Option<MultiDeviceAlertPolicy>,
    /// 客户端已知版本；0 表示不校验。
    pub base_settings_version: u64,
}

impl UserNotificationSettingsPatch {
    /// 由请求里的原始字段构造并校验：至少改一项；勿扰时段不能既设置又取消；分钟、星期掩码、
    /// 两个枚举都要在已知范围内。
    pub fn from_request(
        quiet_hours: Option<QuietHours>,
        clear_quiet_hours: bool,
        preview: Option<i32>,
        multi_device: Option<i32>,
        base_settings_version: u64,
    ) -> Result<Self> {
        let quiet_hours = match (quiet_hours, clear_quiet_hours) {
            (Some(_), true) => {
                return Err(invalid(
                    "quiet_hours and clear_quiet_hours are mutually exclusive",
                ));
            }
            (Some(quiet), false) => {
                quiet.validate()?;
                QuietHoursChange::Set(quiet)
            }
            (None, true) => QuietHoursChange::Clear,
            (None, false) => QuietHoursChange::Keep,
        };
        let preview = preview
            .map(|value| {
                NotificationPreview::from_wire(value)
                    .ok_or_else(|| invalid("unknown notification preview"))
            })
            .transpose()?;
        let multi_device = multi_device
            .map(|value| {
                MultiDeviceAlertPolicy::from_wire(value)
                    .ok_or_else(|| invalid("unknown multi-device alert policy"))
            })
            .transpose()?;
        let patch = Self {
            quiet_hours,
            preview,
            multi_device,
            base_settings_version,
        };
        if patch.is_empty() {
            return Err(invalid("at least one notification setting is required"));
        }
        Ok(patch)
    }

    /// 什么都不改。
    pub fn is_empty(&self) -> bool {
        self.quiet_hours == QuietHoursChange::Keep
            && self.preview.is_none()
            && self.multi_device.is_none()
    }

    /// 把补丁应用到 `current` 上（不动版本号）。存储层按同一语义写库，这里给出期望结果。
    pub fn apply_to(&self, current: &UserNotificationSettings) -> UserNotificationSettings {
        UserNotificationSettings {
            quiet_hours: match self.quiet_hours {
                QuietHoursChange::Keep => current.quiet_hours,
                QuietHoursChange::Set(quiet) => Some(quiet),
                QuietHoursChange::Clear => None,
            },
            preview: self.preview.unwrap_or(current.preview),
            multi_device: self.multi_device.unwrap_or(current.multi_device),
            settings_version: current.settings_version,
        }
    }
}

/// 会话通知订阅者分页的位置：已经列出了多少个、上一页最后一个 user_id。
///
/// 页令牌是 `"{已列出数}:{最后一个 user_id}"`，对调用方不透明；空令牌是第一页。总数封顶
/// [`MAX_NOTIFICATION_SUBSCRIBERS`]，到顶就不再给下一页的令牌，所以合法令牌的已列出数总小于上限。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriberCursor {
    pub served: usize,
    pub after_user_id: Option<String>,
}

impl SubscriberCursor {
    pub fn parse(token: &str) -> Result<Self> {
        if token.is_empty() {
            return Ok(Self::default());
        }
        let bad = || invalid("invalid notification subscriber page token");
        let (served, user_id) = token.split_once(':').ok_or_else(bad)?;
        let digits = !served.is_empty() && served.bytes().all(|b| b.is_ascii_digit());
        if !digits || user_id.is_empty() {
            return Err(bad());
        }
        let served: usize = served.parse().map_err(|_| bad())?;
        if served == 0 || served >= MAX_NOTIFICATION_SUBSCRIBERS {
            return Err(bad());
        }
        Ok(Self {
            served,
            after_user_id: Some(user_id.to_string()),
        })
    }

    pub fn encode(served: usize, last_user_id: &str) -> String {
        format!("{served}:{last_user_id}")
    }

    /// 本页最多还能列多少个：页大小与总上限剩余量取小。
    pub fn page_limit(&self, page_size: usize) -> usize {
        page_size.min(MAX_NOTIFICATION_SUBSCRIBERS.saturating_sub(self.served))
    }
}

/// 页大小：0 或负数取上限，超过上限按上限。
pub fn subscriber_page_size(requested: i32) -> usize {
    match usize::try_from(requested) {
        Ok(size) if (1..=MAX_NOTIFICATION_SUBSCRIBER_PAGE).contains(&size) => size,
        _ => MAX_NOTIFICATION_SUBSCRIBER_PAGE,
    }
}

/// 会话通知订阅者的一页。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationSubscriberPage {
    pub user_ids: Vec<String>,
    /// `None` 表示没有下一页。
    pub next_page_token: Option<String>,
    /// 到了总上限且后面还有订阅者。
    pub truncated: bool,
}

impl NotificationSubscriberPage {
    /// 由按 `limit + 1` 取回的行组出一页：多出来的那一行只用来判断后面还有没有。
    pub fn from_fetched(cursor: &SubscriberCursor, limit: usize, mut fetched: Vec<String>) -> Self {
        let has_more = fetched.len() > limit;
        fetched.truncate(limit);
        let served = cursor.served + fetched.len();
        let (next_page_token, truncated) = match fetched.last() {
            Some(last) if has_more && served < MAX_NOTIFICATION_SUBSCRIBERS => {
                (Some(SubscriberCursor::encode(served, last)), false)
            }
            _ => (None, has_more),
        };
        Self {
            user_ids: fetched,
            next_page_token,
            truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quiet(start: u32, end: u32, weekdays: u32) -> QuietHours {
        QuietHours {
            start_minute: start,
            end_minute: end,
            weekdays,
            allow_mentions: true,
        }
    }

    #[test]
    fn quiet_hours_bounds_are_enforced() {
        for ok in [
            quiet(0, 0, 0),
            quiet(1439, 0, 0x7F),
            quiet(22 * 60, 7 * 60, 0b001_1111),
        ] {
            assert!(ok.validate().is_ok(), "{ok:?}");
        }
        for bad in [quiet(1440, 0, 0), quiet(0, 1440, 0), quiet(0, 0, 0x80)] {
            let error = bad.validate().unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{bad:?}");
        }
    }

    #[test]
    fn enums_round_trip_and_unknown_values_are_rejected() {
        for value in 0..=3 {
            assert_eq!(NotificationPreview::from_wire(value).unwrap().wire(), value);
            assert_eq!(
                MultiDeviceAlertPolicy::from_wire(value).unwrap().wire(),
                value
            );
        }
        for value in [-1, 4, i32::MAX] {
            assert!(NotificationPreview::from_wire(value).is_none());
            assert!(MultiDeviceAlertPolicy::from_wire(value).is_none());
        }
    }

    #[test]
    fn a_patch_must_change_something() {
        let error =
            UserNotificationSettingsPatch::from_request(None, false, None, None, 3).unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn quiet_hours_cannot_be_set_and_cleared_at_once() {
        let error = UserNotificationSettingsPatch::from_request(
            Some(quiet(60, 120, 0)),
            true,
            None,
            None,
            0,
        )
        .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn invalid_fields_reject_the_whole_patch() {
        for (quiet_hours, preview, multi_device) in [
            (Some(quiet(1440, 0, 0)), None, None),
            (Some(quiet(0, 0, 0x100)), None, None),
            (None, Some(4), None),
            (None, None, Some(-1)),
        ] {
            let error = UserNotificationSettingsPatch::from_request(
                quiet_hours,
                false,
                preview,
                multi_device,
                0,
            )
            .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        }
    }

    #[test]
    fn a_patch_changes_only_what_it_names() {
        let current = UserNotificationSettings {
            quiet_hours: Some(quiet(22 * 60, 7 * 60, 0)),
            preview: NotificationPreview::Hidden,
            multi_device: MultiDeviceAlertPolicy::Immediate,
            settings_version: 4,
        };
        let only_preview =
            UserNotificationSettingsPatch::from_request(None, false, Some(2), None, 4).unwrap();
        assert_eq!(only_preview.base_settings_version, 4);
        assert_eq!(
            only_preview.apply_to(&current),
            UserNotificationSettings {
                preview: NotificationPreview::SenderOnly,
                ..current.clone()
            }
        );

        let clear = UserNotificationSettingsPatch::from_request(None, true, None, None, 0).unwrap();
        assert_eq!(clear.apply_to(&current).quiet_hours, None);
        assert_eq!(
            clear.apply_to(&current).preview,
            NotificationPreview::Hidden
        );

        let set = UserNotificationSettingsPatch::from_request(
            Some(quiet(0, 0, 0b100_0000)),
            false,
            None,
            Some(3),
            0,
        )
        .unwrap();
        let next = set.apply_to(&UserNotificationSettings::default());
        assert_eq!(next.quiet_hours, Some(quiet(0, 0, 0b100_0000)));
        assert_eq!(next.multi_device, MultiDeviceAlertPolicy::BadgeOnly);
        assert_eq!(next.preview, NotificationPreview::Unspecified);
    }

    #[test]
    fn defaults_are_no_quiet_hours_full_preview_and_delay() {
        let defaults = UserNotificationSettings::default();
        assert_eq!(defaults.quiet_hours, None);
        assert_eq!(defaults.preview.wire(), 0);
        assert_eq!(defaults.multi_device.wire(), 0);
        assert_eq!(defaults.settings_version, 0);
    }

    #[test]
    fn page_size_is_capped_by_the_server() {
        for (asked, used) in [
            (0, 1000),
            (-5, 1000),
            (1, 1),
            (250, 250),
            (1000, 1000),
            (1001, 1000),
            (i32::MAX, 1000),
        ] {
            assert_eq!(subscriber_page_size(asked), used, "{asked}");
        }
    }

    #[test]
    fn page_tokens_round_trip_and_garbage_is_rejected() {
        assert_eq!(
            SubscriberCursor::parse("").unwrap(),
            SubscriberCursor::default()
        );
        let token = SubscriberCursor::encode(1000, "user:with:colons");
        assert_eq!(
            SubscriberCursor::parse(&token).unwrap(),
            SubscriberCursor {
                served: 1000,
                after_user_id: Some("user:with:colons".into()),
            }
        );
        for bad in [
            "abc",
            ":u",
            "12:",
            "x1:u",
            "-1:u",
            "0:u",
            "5000:u",
            "99999999999999999999999:u",
        ] {
            let error = SubscriberCursor::parse(bad).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{bad}");
        }
    }

    fn users(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|i| format!("u{i:05}")).collect()
    }

    #[test]
    fn a_full_page_with_more_behind_it_hands_out_the_next_token() {
        let cursor = SubscriberCursor::default();
        let limit = cursor.page_limit(1000);
        assert_eq!(limit, 1000);
        let page = NotificationSubscriberPage::from_fetched(&cursor, limit, users(0..1001));
        assert_eq!(page.user_ids.len(), 1000);
        assert_eq!(page.next_page_token.as_deref(), Some("1000:u00999"));
        assert!(!page.truncated);
    }

    #[test]
    fn the_last_page_has_no_token() {
        let cursor = SubscriberCursor::parse("1000:u00999").unwrap();
        let page = NotificationSubscriberPage::from_fetched(&cursor, 1000, users(1000..1200));
        assert_eq!(page.user_ids.len(), 200);
        assert_eq!(page.next_page_token, None);
        assert!(!page.truncated);
    }

    #[test]
    fn the_list_stops_at_five_thousand() {
        // 已经列了 4800 个，再要 1000 个只给 200 个，后面还有就标记截断、不给令牌。
        let cursor = SubscriberCursor::parse("4800:u04799").unwrap();
        let limit = cursor.page_limit(1000);
        assert_eq!(limit, 200);
        let page = NotificationSubscriberPage::from_fetched(&cursor, limit, users(4800..5001));
        assert_eq!(page.user_ids.len(), 200);
        assert_eq!(page.next_page_token, None);
        assert!(page.truncated);

        // 恰好 5000 个：没有更多，不算截断。
        let page = NotificationSubscriberPage::from_fetched(&cursor, limit, users(4800..5000));
        assert_eq!(page.next_page_token, None);
        assert!(!page.truncated);
    }

    #[test]
    fn an_empty_conversation_is_one_empty_page() {
        let cursor = SubscriberCursor::default();
        let page = NotificationSubscriberPage::from_fetched(&cursor, 1000, vec![]);
        assert!(page.user_ids.is_empty());
        assert_eq!(page.next_page_token, None);
        assert!(!page.truncated);
    }
}
