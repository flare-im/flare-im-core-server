//! 厂商无关的投递计划与通知文案。
//!
//! 把 `PushTaskEnvelope` 解成「标题 + 正文」是所有厂商通道共用的一步。三家对「消息被撤回、
//! 内容不可见、接收者不想露出内容时该显示什么」的判断只在这里做一次，否则会各自漂移 ——
//! 而那正是最容易泄露内容的地方。
//!
//! 路由给每条离线任务定了处置（响铃、静默覆盖、清除、仅角标、撤回替换、摘要）与接收者的预览设置。
//! 「该不该响、该不该发、文案露出多少」在 [`push_plan`] 里判一次；文案用哪种语言要到投递时按
//! 目标绑定登记的语言定（[`PushAlert::text`]）。厂商适配器只负责把结论翻译成各自的字段。
//!
//! 文案的规则：
//! - 发送端的 `offline_push_info` 与从消息里取的文字（文本、富文本的纯文本、系统消息的展示文案、
//!   通知的标题正文）都先经 [`push_text`](crate::infrastructure::push_text) 清理与截断，
//!   清理后为空当作没有；
//! - 没有可用的正文时按内容类型用占位（`[图片]`），没有标题时用通用标题；
//! - 点名到接收者时正文前加点名标记，文案自己已经写了就不重复；
//! - 预览设置：完整（缺省）照常；只显示发送者时保留标题，正文换成「发来一条消息」，点名标记保留；
//!   隐藏时标题正文都换成通用文案。内容不可见、已清除或是 E2EE 占位时不论预览设置一律用通用文案；
//! - 一条通知代表多条未读时标题前带条数；摘要通知的文案只由条数与会话数生成。

use flare_grpc_proto::access_gateway::{
    PushEventRequest, PushMessageRequest, PushNotificationRequest,
};
use flare_im_contracts::domain::push_policy::{event_has_mobile_alert, message_has_mobile_alert};
// 文案模板与引擎共用一份，见 flare-notify-rules 的 text 模块。
use flare_notify_rules::text::{Lang, Phrase, count_prefix, digest_text, mentions_already, phrase};
use flare_proto::PushTaskEnvelope;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    AlertClass, ContentVisibility, ConversationType, MessageSource, NotificationPreview,
    PushDisposition, PushTaskPayloadKind,
};
use flare_server_core::error::{ErrorCode, FlareError};
use prost::Message as _;
use serde_json::{Map, Value};

use crate::domain::offline_delivery::task_disposition;
// E2EE 占位符判定与个推实现共用：内容加密时不能把密文当正文推出去。
use crate::infrastructure::getui_push::message_has_e2ee_placeholder;
use crate::infrastructure::push_text::{clean_body, clean_title, fit_body, fit_title};

/// 合并键要进 HTTP 头部（`apns-collapse-id`），APNs 的上限是 64 字节。
const COLLAPSE_KEY_MAX_BYTES: usize = 64;

/// 通知打断用户的程度，取值与 APNs 的 `interruption-level` 一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interruption {
    /// 紧急消息：穿透专注模式与定时摘要。
    TimeSensitive,
    /// 正常提示：亮屏、可响铃。
    Active,
    /// 只进通知中心：不亮屏、不响铃。
    Passive,
}

impl Interruption {
    pub fn apns_level(self) -> &'static str {
        match self {
            Self::TimeSensitive => "time-sensitive",
            Self::Active => "active",
            Self::Passive => "passive",
        }
    }
}

/// 一条要展示（或覆盖同一合并键）的通知，文案来源与提示方式都已定好。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushAlert {
    /// 还没定语言的文案，见 [`PushAlert::text`]。
    pub copy: AlertCopy,
    /// 处置允许响铃，且消息自身的展示设置要求响铃。
    pub play_sound: bool,
    pub show_badge: bool,
    /// 走厂商的高优先级投递（APNs 10、FCM high）；静默覆盖用普通优先级，由系统择机送达。
    pub high_priority: bool,
    pub interruption: Interruption,
    /// 这条通知代表谁发的、哪一类会话：端上键 `sender_id`、`sender_name`、`kind`。
    pub origin: Origin,
}

/// 通知所属会话的类别，端上键 `kind` 的取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatKind {
    Direct,
    Group,
    /// 系统通知、业务通知、不属于任何会话的推送。
    Notice,
}

impl ChatKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Group => "group",
            Self::Notice => "notice",
        }
    }

    /// 消息带了会话类型就按类型；没带时按会话标识的首位数字（协议约定的前缀，与 Push Server 的
    /// 判断一致）；没有会话的是通知。认不出的按群聊。
    fn of_conversation(conversation_type: i32, conversation_id: &str) -> Self {
        match ConversationType::try_from(conversation_type) {
            Ok(
                ConversationType::Group | ConversationType::Channel | ConversationType::Broadcast,
            ) => Self::Group,
            Ok(
                ConversationType::Single
                | ConversationType::Ai
                | ConversationType::Customer
                | ConversationType::Temp,
            ) => Self::Direct,
            Ok(ConversationType::System) => Self::Notice,
            Ok(ConversationType::Unspecified) | Err(_) => {
                match conversation_id.trim().as_bytes().first() {
                    None => Self::Notice,
                    Some(b'1' | b'3' | b'5' | b'6') => Self::Direct,
                    Some(b'4') => Self::Notice,
                    Some(_) => Self::Group,
                }
            }
        }
    }
}

/// 通知代表的发送者与会话类别。端上能运行代码时（iOS 通知扩展、Android 数据消息）据此把正文里的
/// 发送者名字换成本地的显示名（备注优先）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    /// 所代表的最新一条消息的发送者；摘要、通知类内容与系统消息为空。
    pub sender_id: String,
    /// 服务端渲染的正文里出现的发送者名字，原样（端上据此替换）；正文里没有发送者名字时为空。
    pub sender_name: String,
    pub kind: ChatKind,
}

impl Origin {
    /// 没有发送者的来源：只知道会话类别。
    fn anonymous(kind: ChatKind) -> Self {
        Self {
            sender_id: String::new(),
            sender_name: String::new(),
            kind,
        }
    }
}

impl PushAlert {
    /// 按目标绑定登记的语言渲染标题正文：`Lang::from_locale(&binding.locale)`。
    pub fn text(&self, lang: Lang) -> AlertText {
        self.copy.render(lang)
    }
}

/// 渲染好的标题与正文，都在 [`push_text`](crate::infrastructure::push_text) 的长度上限内。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertText {
    pub title: String,
    pub body: String,
}

/// 一条通知的文案，还没定语言。标题正文从哪来已经按处置、预览设置与内容可见性定好，
/// 用户给的文字已经清理截断；剩下的只是模板取哪种语言。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertCopy {
    title: TitleCopy,
    body: BodyCopy,
    /// 正文前加点名标记。
    mention: bool,
    /// 标题前带的条数；0 与 1 不带。
    count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TitleCopy {
    Text(String),
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyCopy {
    Text(String),
    /// 按内容类型的占位（`Phrase::Image` 到 `Phrase::Message`）。
    Placeholder(Phrase),
    /// 「发来一条消息」。
    SentMessage,
    Generic(CopyKind),
    Recalled,
    Digest {
        messages: u32,
        conversations: u32,
    },
}

/// 文案代表的是消息还是通知类内容（好友申请、业务通知）：两者拿不到内容时的通用正文不同，
/// 「发来一条消息」与点名标记只对消息有意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyKind {
    Message,
    Notice,
}

impl AlertCopy {
    /// 标题与正文都是通用文案：既不露发送者，也不露内容（预览设为隐藏、内容不可见或是 E2EE
    /// 占位、读不出可展示的内容）。厂商对这种「主副标题都模糊」的通知另有要求时，适配器据此
    /// 换一种说法（见个推适配器里的小米模板）。
    pub fn is_generic(&self) -> bool {
        matches!(self.title, TitleCopy::Generic) && matches!(self.body, BodyCopy::Generic(_))
    }

    /// 正文是否原样带着这个名字。只有来自发送端的文字可能带：模板文案里没有名字。
    fn body_names(&self, name: &str) -> bool {
        !name.is_empty() && matches!(&self.body, BodyCopy::Text(text) if text.contains(name))
    }

    /// 不露出任何内容的文案：通用标题与通用正文。
    fn generic(kind: CopyKind, count: u32) -> Self {
        Self {
            title: TitleCopy::Generic,
            body: BodyCopy::Generic(kind),
            mention: false,
            count,
        }
    }

    fn render(&self, lang: Lang) -> AlertText {
        let title = match &self.title {
            TitleCopy::Text(text) => text.as_str(),
            TitleCopy::Generic => phrase(lang, Phrase::GenericTitle),
        };
        let body = match &self.body {
            BodyCopy::Text(text) => text.clone(),
            BodyCopy::Placeholder(placeholder) => phrase(lang, *placeholder).to_string(),
            BodyCopy::SentMessage => phrase(lang, Phrase::SentMessage).to_string(),
            BodyCopy::Generic(CopyKind::Message) => phrase(lang, Phrase::GenericBody).to_string(),
            BodyCopy::Generic(CopyKind::Notice) => phrase(lang, Phrase::NoticeBody).to_string(),
            BodyCopy::Recalled => phrase(lang, Phrase::Recalled).to_string(),
            BodyCopy::Digest {
                messages,
                conversations,
            } => digest_text(lang, *conversations, *messages),
        };
        let body = if self.mention {
            format!("{} {body}", phrase(lang, Phrase::MentionMarker))
        } else {
            body
        };
        // 前缀与标记加在已经截过的文字前面，拼好后再截一次：上限对最终文案成立。
        let prefix = count_prefix(lang, self.count).unwrap_or_default();
        AlertText {
            title: fit_title(&format!("{prefix}{title}")),
            body: fit_body(&body),
        }
    }
}

/// 一条离线任务在目标设备上要做的事。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushPlan {
    Alert(PushAlert),
    /// 只把角标改成信封里的绝对值，不展示、不响铃。
    BadgeOnly,
    /// 清除同一合并键的通知；信封带了角标就一并更新。各通道的清除能力不同，由适配器取舍。
    Clear,
    /// 没有可做的事，任务直接确认。
    Nothing,
}

/// 下发给端上代码的值的形态。APNs 顶层的自定义键可以是数字；FCM 的 data 与个推透传
/// 只收字符串（FCM 对非字符串值回 400）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueShape {
    Native,
    Strings,
}

/// 标题与正文是否随结构化字段一起下发，下发时用哪种语言。端上代码自己渲染的通道（FCM 数据消息、
/// 个推透传）要带；APNs 的标题正文已经在 `aps.alert` 里，不重复一份占 4 KiB 的载荷上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientText {
    Included(Lang),
    Omitted,
}

/// 三家厂商下发给端上代码的同一套键（蛇形命名）：端上代码能运行时据此判断是不是当前账号的
/// 推送、覆盖同一会话的通知、丢弃过期的更新、点击后直达会话。
///
/// - `uid`、`tenant_id`：接收者，端上据此丢弃发给别的账号的推送。
/// - `conversation_id`、`message_id`、`seq`：所代表的最新一条消息；补提示读不到消息时
///   `message_id` 为空。
/// - `collapse_id`：信封里的合并键（摘要是路由给的设备级摘要键）。不叫 `collapse_key`：那是
///   FCM 的保留数据键，Android SDK 会把它从应用可见的 data 里剔掉。`thread_id` 是分组键。
/// - `disposition`、`alert_class`：小写蛇形的处置与提示等级。
/// - `count`：这条通知代表的未读条数，至少为 1；`badge` 只在知道角标绝对值时带。
/// - `title`、`body`：服务端渲染好的文案，见 [`ClientText`]。
/// - `sound`：`1` 响铃、`0` 不响。
/// - `sender_id`、`sender_name`、`kind`：见 [`Origin`]；值总是字符串，不知道时为空串
///   （`kind` 总有取值）。
pub fn client_payload(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    shape: ValueShape,
    text: ClientText,
) -> Map<String, Value> {
    let number = |value: u64| match shape {
        ValueShape::Native => Value::from(value),
        ValueShape::Strings => Value::from(value.to_string()),
    };
    let disposition =
        PushDisposition::try_from(envelope.disposition).unwrap_or(PushDisposition::Unspecified);
    let class = AlertClass::try_from(envelope.alert_class).unwrap_or(AlertClass::Unspecified);
    let alert = match plan {
        PushPlan::Alert(alert) => Some(alert),
        PushPlan::BadgeOnly | PushPlan::Clear | PushPlan::Nothing => None,
    };
    let mut payload = Map::new();
    for (key, value) in [
        ("uid", &envelope.user_id),
        ("tenant_id", &envelope.tenant_id),
        ("conversation_id", &envelope.conversation_id),
        ("message_id", &envelope.message_id),
        ("collapse_id", &envelope.collapse_key),
        ("thread_id", &envelope.thread_key),
    ] {
        payload.insert(key.into(), Value::from(value.as_str()));
    }
    payload.insert("seq".into(), number(envelope.conversation_seq));
    payload.insert("disposition".into(), disposition_name(disposition).into());
    payload.insert("alert_class".into(), alert_class_name(class).into());
    payload.insert("count".into(), number(u64::from(envelope.count.max(1))));
    if let Some(badge) = envelope.badge {
        payload.insert("badge".into(), number(u64::from(badge)));
    }
    if let (ClientText::Included(lang), Some(alert)) = (text, alert) {
        let AlertText { title, body } = alert.text(lang);
        payload.insert("title".into(), Value::from(title));
        payload.insert("body".into(), Value::from(body));
    }
    payload.insert(
        "sound".into(),
        number(u64::from(alert.is_some_and(|alert| alert.play_sound))),
    );
    let origin = alert.map_or_else(
        || Origin::anonymous(task_chat_kind(envelope)),
        |alert| alert.origin.clone(),
    );
    payload.insert("sender_id".into(), Value::from(origin.sender_id));
    payload.insert("sender_name".into(), Value::from(origin.sender_name));
    payload.insert("kind".into(), Value::from(origin.kind.as_str()));
    payload
}

/// 不展示内容的任务（仅角标、清除）只看任务本身：业务通知与自定义推送是通知，其余按会话判断。
fn task_chat_kind(envelope: &PushTaskEnvelope) -> ChatKind {
    match PushTaskPayloadKind::try_from(envelope.payload_kind) {
        Ok(PushTaskPayloadKind::Notification | PushTaskPayloadKind::Custom) => ChatKind::Notice,
        _ => ChatKind::of_conversation(
            ConversationType::Unspecified as i32,
            &envelope.conversation_id,
        ),
    }
}

fn disposition_name(disposition: PushDisposition) -> &'static str {
    match disposition {
        PushDisposition::Unspecified => "unspecified",
        PushDisposition::Loud => "loud",
        PushDisposition::Quiet => "quiet",
        PushDisposition::Clear => "clear",
        PushDisposition::BadgeOnly => "badge_only",
        PushDisposition::RecallReplace => "recall_replace",
        PushDisposition::Digest => "digest",
    }
}

fn alert_class_name(class: AlertClass) -> &'static str {
    match class {
        AlertClass::Unspecified => "unspecified",
        AlertClass::None => "none",
        AlertClass::Silent => "silent",
        AlertClass::Normal => "normal",
        AlertClass::Mention => "mention",
        AlertClass::Urgent => "urgent",
    }
}

/// 把路由给的处置落成厂商无关的投递计划。
///
/// 没有处置、或合并键进不了 HTTP 头部的任务返回终态错误：这类任务重试多少次都不会变好，
/// 该进死信而不是占着重投预算。
pub fn push_plan(envelope: &PushTaskEnvelope) -> Result<PushPlan, FlareError> {
    let Some(disposition) = task_disposition(envelope) else {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "offline push task has no disposition",
        ));
    };
    if envelope.collapse_key.len() > COLLAPSE_KEY_MAX_BYTES
        || !envelope.collapse_key.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "offline push collapse key rejected",
        ));
    }
    Ok(match disposition {
        // 仅角标却没带角标：没有任何可见效果，不值得占一次厂商调用。
        PushDisposition::BadgeOnly if envelope.badge.is_none() => PushPlan::Nothing,
        PushDisposition::BadgeOnly => PushPlan::BadgeOnly,
        PushDisposition::Clear => PushPlan::Clear,
        PushDisposition::RecallReplace => {
            // 被撤回的消息不再满足「可提示」的条件，这里不能套用下面的可提示性检查；
            // 标题沿用原通知的来源，正文换成撤回文案。
            let shown = shown_content(envelope);
            let copy = recall_copy(envelope, &shown);
            PushPlan::Alert(alert(envelope, disposition, copy, &shown))
        }
        // 摘要代表一批会话，文案只由条数与会话数生成，不看载荷里有没有可展示的内容；
        // 载荷能解出消息时，消息自己「不响铃」的设置照样生效。
        PushDisposition::Digest => {
            let shown = shown_content(envelope);
            let copy = digest_copy(envelope);
            PushPlan::Alert(alert(envelope, disposition, copy, &shown))
        }
        // `task_disposition` 已把未指明的处置挡在前面，这里只会是响铃与静默覆盖。
        PushDisposition::Loud | PushDisposition::Quiet | PushDisposition::Unspecified => {
            if mobile_alert_eligible(envelope) {
                let shown = shown_content(envelope);
                let copy = content_copy(envelope, &shown);
                PushPlan::Alert(alert(envelope, disposition, copy, &shown))
            } else {
                PushPlan::Nothing
            }
        }
    })
}

fn alert(
    envelope: &PushTaskEnvelope,
    disposition: PushDisposition,
    copy: AlertCopy,
    shown: &Shown,
) -> PushAlert {
    let loud = matches!(disposition, PushDisposition::Loud | PushDisposition::Digest);
    let urgent = envelope.alert_class == AlertClass::Urgent as i32;
    // 摘要跨会话、不代表某一个发送者；名字只在正文原样带着它时才给，端上替换的就是这一段。
    let origin = if disposition == PushDisposition::Digest {
        Origin::anonymous(shown.chat)
    } else {
        Origin {
            sender_id: shown.sender_id.clone(),
            sender_name: shown
                .sender_name
                .as_deref()
                .filter(|name| copy.body_names(name))
                .unwrap_or_default()
                .to_string(),
            kind: shown.chat,
        }
    };
    PushAlert {
        copy,
        play_sound: loud && shown.play_sound,
        show_badge: shown.show_badge,
        high_priority: loud,
        interruption: match disposition {
            // 摘要代表一批普通消息，即使其中夹着紧急消息也不升级。
            PushDisposition::Loud if urgent => Interruption::TimeSensitive,
            PushDisposition::Loud | PushDisposition::Digest => Interruption::Active,
            _ => Interruption::Passive,
        },
        origin,
    }
}

/// 接收者的预览设置能露出多少。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Preview {
    Full,
    SenderOnly,
    Hidden,
}

/// 未指明按完整处理；不认识的取值按隐藏：宁可少露，不可多露。
fn preview_of(envelope: &PushTaskEnvelope) -> Preview {
    match NotificationPreview::try_from(envelope.preview) {
        Ok(NotificationPreview::Unspecified | NotificationPreview::Full) => Preview::Full,
        Ok(NotificationPreview::SenderOnly) => Preview::SenderOnly,
        Ok(NotificationPreview::Hidden) | Err(_) => Preview::Hidden,
    }
}

/// 一条消息或通知的文案，按预览设置露出。
fn content_copy(envelope: &PushTaskEnvelope, shown: &Shown) -> AlertCopy {
    let count = envelope.count;
    if shown.withheld {
        return AlertCopy::generic(shown.kind, count);
    }
    let title = shown
        .title
        .clone()
        .map_or(TitleCopy::Generic, TitleCopy::Text);
    let mention = shown.kind == CopyKind::Message
        && envelope.alert_class == AlertClass::Mention as i32
        && !shown.says_mentioned();
    match preview_of(envelope) {
        Preview::Hidden => AlertCopy::generic(shown.kind, count),
        Preview::SenderOnly => AlertCopy {
            // 「发来一条消息」要接在发送者后面读；没有标题时只能说通用的那句。
            body: match (shown.kind, &shown.title) {
                (CopyKind::Message, Some(_)) => BodyCopy::SentMessage,
                (kind, _) => BodyCopy::Generic(kind),
            },
            title,
            mention,
            count,
        },
        Preview::Full => AlertCopy {
            title,
            body: shown.body.clone(),
            mention,
            count,
        },
    }
}

/// 撤回替换：正文换成撤回文案，标题按预览设置与内容可见性取原通知的来源或通用标题。
fn recall_copy(envelope: &PushTaskEnvelope, shown: &Shown) -> AlertCopy {
    let title = match (&shown.title, shown.withheld, preview_of(envelope)) {
        (Some(title), false, Preview::Full | Preview::SenderOnly) => TitleCopy::Text(title.clone()),
        _ => TitleCopy::Generic,
    };
    AlertCopy {
        title,
        body: BodyCopy::Recalled,
        mention: false,
        count: envelope.count,
    }
}

/// 摘要：通用标题，正文是「N 个会话有 M 条新消息」。条数已经写在正文里，标题不再带。
fn digest_copy(envelope: &PushTaskEnvelope) -> AlertCopy {
    AlertCopy {
        title: TitleCopy::Generic,
        body: BodyCopy::Digest {
            messages: envelope.count,
            conversations: envelope.digest_conversations,
        },
        mention: false,
        count: 0,
    }
}

/// Defense for queued tasks made before routing policy was corrected. No Social event names.
pub fn mobile_alert_eligible(envelope: &PushTaskEnvelope) -> bool {
    match PushTaskPayloadKind::try_from(envelope.payload_kind) {
        Ok(PushTaskPayloadKind::Message) => {
            PushMessageRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .is_some_and(|req| req.messages.iter().any(message_has_mobile_alert))
        }
        Ok(PushTaskPayloadKind::Event) => {
            PushEventRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .is_some_and(|req| req.events.iter().any(event_has_mobile_alert))
        }
        Ok(PushTaskPayloadKind::Notification | PushTaskPayloadKind::Custom) => true,
        _ => false,
    }
}

/// 载荷里能拿来展示的内容。文字都已清理截断。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shown {
    kind: CopyKind,
    title: Option<String>,
    /// 完整预览时的正文：文字、按内容类型的占位，或通用正文。
    body: BodyCopy,
    play_sound: bool,
    show_badge: bool,
    /// 内容不可见、已清除或是 E2EE 占位：不管预览设置，只能用通用文案。
    withheld: bool,
    /// 发送者；通知类内容与系统消息没有。
    sender_id: String,
    /// 消息带的发送者昵称（已清理），用来认出正文里的发送者名字。
    sender_name: Option<String>,
    chat: ChatKind,
}

impl Shown {
    fn generic(kind: CopyKind) -> Self {
        Self {
            kind,
            title: None,
            body: BodyCopy::Generic(kind),
            play_sound: true,
            show_badge: false,
            withheld: false,
            sender_id: String::new(),
            sender_name: None,
            chat: match kind {
                CopyKind::Message => ChatKind::Group,
                CopyKind::Notice => ChatKind::Notice,
            },
        }
    }

    /// 读不出消息时的展示：会话类别按任务的会话判断。
    fn generic_in(kind: CopyKind, envelope: &PushTaskEnvelope) -> Self {
        Self {
            chat: match kind {
                CopyKind::Message => task_chat_kind(envelope),
                CopyKind::Notice => ChatKind::Notice,
            },
            ..Self::generic(kind)
        }
    }

    fn says_mentioned(&self) -> bool {
        self.title.as_deref().is_some_and(mentions_already)
            || matches!(&self.body, BodyCopy::Text(text) if mentions_already(text))
    }
}

fn shown_content(envelope: &PushTaskEnvelope) -> Shown {
    match PushTaskPayloadKind::try_from(envelope.payload_kind)
        .unwrap_or(PushTaskPayloadKind::Unspecified)
    {
        PushTaskPayloadKind::Notification => {
            PushNotificationRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .and_then(|request| request.notification)
                .map(|notice| Shown {
                    title: clean_title(&notice.title),
                    body: clean_body(&notice.body)
                        .map_or(BodyCopy::Generic(CopyKind::Notice), BodyCopy::Text),
                    ..Shown::generic(CopyKind::Notice)
                })
                .unwrap_or_else(|| Shown::generic(CopyKind::Notice))
        }
        PushTaskPayloadKind::Message => {
            PushMessageRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .and_then(|request| request.messages.into_iter().find(message_has_mobile_alert))
                .map(|message| message_shown(&message, envelope))
                .unwrap_or_else(|| Shown::generic_in(CopyKind::Message, envelope))
        }
        PushTaskPayloadKind::Event => PushEventRequest::decode(envelope.push_payload.as_slice())
            .ok()
            .and_then(|request| {
                request
                    .events
                    .into_iter()
                    .filter(event_has_mobile_alert)
                    .find_map(|event| match event.payload {
                        Some(EventPayload::Message(message)) => Some(message),
                        _ => None,
                    })
            })
            .map(|message| message_shown(&message, envelope))
            .unwrap_or_else(|| Shown::generic_in(CopyKind::Message, envelope)),
        // 业务自定义的推送：服务端不知道它说的是什么，只能说「你收到一条通知」。
        PushTaskPayloadKind::Custom => Shown::generic(CopyKind::Notice),
        _ => Shown::generic_in(CopyKind::Message, envelope),
    }
}

fn message_shown(message: &flare_proto::common::Message, envelope: &PushTaskEnvelope) -> Shown {
    let content = message.content.as_ref().and_then(|c| c.content.as_ref());
    let kind = match content {
        Some(Content::Notification(_)) => CopyKind::Notice,
        _ => CopyKind::Message,
    };
    let chat = match kind {
        CopyKind::Notice => ChatKind::Notice,
        CopyKind::Message => {
            let conversation_id = if message.conversation_id.trim().is_empty() {
                envelope.conversation_id.as_str()
            } else {
                message.conversation_id.as_str()
            };
            ChatKind::of_conversation(message.conversation_type, conversation_id)
        }
    };
    // 系统消息与通知类内容不代表某个人说的话。
    let personal = kind == CopyKind::Message
        && !matches!(content, Some(Content::System(_)))
        && message.source != MessageSource::System as i32
        && chat != ChatKind::Notice;
    let (sender_id, sender_name) = if personal {
        (
            message.sender_id.trim().to_string(),
            clean_title(&message.sender_name),
        )
    } else {
        (String::new(), None)
    };
    // 发送端给了推送文案就用它（社交服务端可以在发送前钩子里盖写）；正文空着时退回消息自己的内容。
    let (title, body, play_sound, show_badge) = if let Some(info) = &message.offline_push_info {
        (
            clean_title(&info.title),
            clean_body(&info.body).map(BodyCopy::Text),
            !info.sound.trim().is_empty(),
            info.badge,
        )
    } else if let Some(Content::Notification(notice)) = content {
        (
            clean_title(&notice.title),
            None,
            notice.play_sound,
            notice.show_badge,
        )
    } else {
        (None, None, true, false)
    };
    Shown {
        kind,
        title,
        body: body.unwrap_or_else(|| content_body(content)),
        play_sound,
        show_badge,
        withheld: message_requires_generic_push_display(message),
        sender_id,
        sender_name,
        chat,
    }
}

/// 消息内容本身能给出的正文：带文字的类型取文字，其余按类型用占位。
fn content_body(content: Option<&Content>) -> BodyCopy {
    let text_or = |raw: &str, placeholder| {
        clean_body(raw).map_or(BodyCopy::Placeholder(placeholder), BodyCopy::Text)
    };
    match content {
        None => BodyCopy::Generic(CopyKind::Message),
        Some(Content::Text(text)) => text_or(&text.text, Phrase::Message),
        Some(Content::RichText(rich)) => text_or(&rich.plain_text, Phrase::RichText),
        // 系统消息的展示文案由服务端渲染。
        Some(Content::System(system)) => text_or(&system.body, Phrase::Message),
        Some(Content::Notification(notice)) => {
            clean_body(&notice.body).map_or(BodyCopy::Generic(CopyKind::Notice), BodyCopy::Text)
        }
        Some(Content::Image(_) | Content::ImageGroup(_)) => BodyCopy::Placeholder(Phrase::Image),
        Some(Content::Video(_)) => BodyCopy::Placeholder(Phrase::Video),
        Some(Content::Audio(_)) => BodyCopy::Placeholder(Phrase::Voice),
        Some(Content::File(_)) => BodyCopy::Placeholder(Phrase::File),
        Some(Content::Location(_)) => BodyCopy::Placeholder(Phrase::Location),
        Some(Content::Card(_)) => BodyCopy::Placeholder(Phrase::Contact),
        Some(Content::LinkCard(_)) => BodyCopy::Placeholder(Phrase::Link),
        Some(Content::Sticker(_)) => BodyCopy::Placeholder(Phrase::Sticker),
        Some(Content::Emoji(_)) => BodyCopy::Placeholder(Phrase::Emoji),
        // 引用回复说的是回复本身；协议禁止引用里再套引用，这里也只看一层。
        Some(Content::Quote(quote)) => {
            match quote
                .current_content
                .as_ref()
                .and_then(|c| c.content.as_ref())
            {
                None | Some(Content::Quote(_)) => BodyCopy::Placeholder(Phrase::Message),
                current => content_body(current),
            }
        }
        Some(_) => BodyCopy::Placeholder(Phrase::Message),
    }
}

fn message_requires_generic_push_display(message: &flare_proto::common::Message) -> bool {
    let content_hidden = message
        .retention_state
        .as_ref()
        .and_then(|state| ContentVisibility::try_from(state.content_visibility).ok())
        .is_some_and(|visibility| {
            matches!(
                visibility,
                ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged
            )
        });
    content_hidden || message_has_e2ee_placeholder(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{
        Event, ImageContent, Message, MessageContent, MessageRetentionState, NotificationContent,
        OfflinePushInfo, PlaceholderContent, QuoteContent, RichTextContent, TextContent,
    };

    fn event_task(message: Message) -> PushTaskEnvelope {
        let req = PushEventRequest {
            events: vec![Event {
                r#type: flare_proto::common::EventType::EventMessage as i32,
                payload: Some(EventPayload::Message(message)),
                ..Default::default()
            }],
            ..Default::default()
        };
        PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Event as i32,
            push_payload: req.encode_to_vec(),
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        }
    }

    fn message_with_push_info(title: &str, body: &str) -> Message {
        Message {
            offline_push_info: Some(OfflinePushInfo {
                title: title.to_string(),
                body: body.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn message_with_content(content: Content) -> Message {
        Message {
            content: Some(MessageContent {
                content: Some(content),
            }),
            ..Default::default()
        }
    }

    fn with_disposition(
        mut task: PushTaskEnvelope,
        disposition: PushDisposition,
    ) -> PushTaskEnvelope {
        task.disposition = disposition as i32;
        task
    }

    fn with_preview(mut task: PushTaskEnvelope, preview: NotificationPreview) -> PushTaskEnvelope {
        task.preview = preview as i32;
        task
    }

    /// 事件里只有一个空事件：没有任何可展示的内容。
    fn bare_event_task(disposition: PushDisposition) -> PushTaskEnvelope {
        let req = PushEventRequest {
            events: vec![Event::default()],
            ..Default::default()
        };
        PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Event as i32,
            push_payload: req.encode_to_vec(),
            disposition: disposition as i32,
            ..Default::default()
        }
    }

    fn alert_of(task: &PushTaskEnvelope) -> PushAlert {
        match push_plan(task).expect("plan") {
            PushPlan::Alert(alert) => alert,
            other => panic!("expected an alert, got {other:?}"),
        }
    }

    /// 中文渲染的 (标题, 正文)。
    fn zh(task: &PushTaskEnvelope) -> (String, String) {
        let text = alert_of(task).text(Lang::Zh);
        (text.title, text.body)
    }

    /// 英文渲染的 (标题, 正文)。
    fn en(task: &PushTaskEnvelope) -> (String, String) {
        let text = alert_of(task).text(Lang::En);
        (text.title, text.body)
    }

    fn pair(title: &str, body: &str) -> (String, String) {
        (title.to_string(), body.to_string())
    }

    /// 群消息走内联事件，事件里带着整条消息——离线推送该显示真实文案，
    /// 而不是每条都「你收到一条新消息」。
    #[test]
    fn inline_event_renders_the_message_push_info() {
        let task = event_task(message_with_push_info("项目群", "小明: 今天几点开会"));
        assert_eq!(zh(&task), pair("项目群", "小明: 今天几点开会"));
        // 用户给的文字不翻译，只有模板随语言变。
        assert_eq!(en(&task), pair("项目群", "小明: 今天几点开会"));
        assert!(!alert_of(&task).play_sound, "消息自己没要求响铃");
    }

    /// 发送端的文字进通知之前要清理：控制字符与双向覆盖字符去掉、空白收拢、超长截断。
    #[test]
    fn push_info_is_sanitized_and_truncated() {
        let task = event_task(message_with_push_info(
            &format!("项目群\u{202E}\n{}", "长".repeat(100)),
            &format!("小明:\r\n今天\u{0}几点开会{}", "啊".repeat(300)),
        ));
        let (title, body) = zh(&task);
        assert!(title.starts_with("项目群 长"), "{title}");
        assert!(!title.contains('\u{202E}') && !title.contains('\n'));
        assert_eq!(title.chars().count(), 64);
        assert!(title.ends_with('…'));
        assert!(body.starts_with("小明: 今天几点开会啊"), "{body}");
        assert_eq!(body.chars().count(), 180);
        assert!(body.ends_with('…'));

        // 清理后为空的标题与正文当作没有。
        let blank = event_task(message_with_push_info("\u{200B} \u{202E}", "\n\t"));
        assert_eq!(zh(&blank), pair("新消息", "你收到一条新消息"));
        assert_eq!(en(&blank), pair("New message", "You have a new message"));
    }

    /// 没有推送文案时正文取消息自己的内容：文字取文字，其余按类型占位，两种语言各一套。
    #[test]
    fn content_without_push_info_renders_text_or_a_typed_placeholder() {
        use flare_proto::common::{
            AudioContent, CardContent, EmojiContent, FileContent, ForwardContent,
            ImageGroupContent, LinkCardContent, LocationContent, StickerContent, SystemContent,
            VideoContent,
        };
        let text = |text: &str| {
            Content::Text(TextContent {
                text: text.into(),
                ..Default::default()
            })
        };
        let rich = |plain: &str| {
            Content::RichText(RichTextContent {
                plain_text: plain.into(),
                ..Default::default()
            })
        };
        let quote = |current: Content| {
            Content::Quote(Box::new(QuoteContent {
                current_content: Some(Box::new(MessageContent {
                    content: Some(current),
                })),
                ..Default::default()
            }))
        };
        for (content, zh_body, en_body) in [
            (text("明天见"), "明天见", "明天见"),
            (text(" \n "), "[消息]", "[Message]"),
            (rich("周报已发出"), "周报已发出", "周报已发出"),
            (rich(""), "[富文本]", "[Rich text]"),
            (Content::Image(ImageContent::default()), "[图片]", "[Image]"),
            (
                Content::ImageGroup(ImageGroupContent::default()),
                "[图片]",
                "[Image]",
            ),
            (Content::Video(VideoContent::default()), "[视频]", "[Video]"),
            (Content::Audio(AudioContent::default()), "[语音]", "[Voice]"),
            (Content::File(FileContent::default()), "[文件]", "[File]"),
            (
                Content::Location(LocationContent::default()),
                "[位置]",
                "[Location]",
            ),
            (Content::Card(CardContent::default()), "[名片]", "[Contact]"),
            (
                Content::LinkCard(LinkCardContent::default()),
                "[链接]",
                "[Link]",
            ),
            (
                Content::Sticker(StickerContent::default()),
                "[贴纸]",
                "[Sticker]",
            ),
            (Content::Emoji(EmojiContent::default()), "[表情]", "[Emoji]"),
            (
                Content::Forward(ForwardContent::default()),
                "[消息]",
                "[Message]",
            ),
            (
                Content::System(SystemContent {
                    body: "小明加入了群聊".into(),
                    ..Default::default()
                }),
                "小明加入了群聊",
                "小明加入了群聊",
            ),
            (quote(text("收到")), "收到", "收到"),
            (
                quote(Content::Image(ImageContent::default())),
                "[图片]",
                "[Image]",
            ),
            (quote(quote(text("套娃"))), "[消息]", "[Message]"),
        ] {
            let task = event_task(message_with_content(content.clone()));
            assert_eq!(zh(&task), pair("新消息", zh_body), "{content:?}");
            assert_eq!(en(&task), pair("New message", en_body), "{content:?}");
        }

        // 推送文案只给了标题时，正文同样退回内容。
        let mut message = message_with_content(Content::Image(ImageContent::default()));
        message.offline_push_info = Some(OfflinePushInfo {
            title: "小明".into(),
            ..Default::default()
        });
        assert_eq!(zh(&event_task(message)), pair("小明", "[图片]"));

        // 消息里什么都没有：通用文案。
        assert_eq!(
            zh(&event_task(Message::default())),
            pair("新消息", "你收到一条新消息")
        );
    }

    /// 内容不可见（撤回/清除）时必须退回通用文案：推送是内容最容易漏出去的地方。
    #[test]
    fn hidden_content_falls_back_to_generic_display() {
        let mut message = message_with_push_info("项目群", "不该出现的正文");
        message.retention_state = Some(MessageRetentionState {
            content_visibility: ContentVisibility::Hidden as i32,
            ..Default::default()
        });
        let task = event_task(message);
        assert_eq!(zh(&task), pair("新消息", "你收到一条新消息"));
        assert_eq!(en(&task), pair("New message", "You have a new message"));
    }

    /// 内容不可见、E2EE 占位的通用文案不受预览设置影响：设成完整预览也不露。
    #[test]
    fn withheld_content_stays_generic_under_every_preview() {
        let mut purged = message_with_push_info("项目群", "清除了的正文");
        purged.retention_state = Some(MessageRetentionState {
            content_visibility: ContentVisibility::Purged as i32,
            ..Default::default()
        });
        let mut e2ee = message_with_content(Content::Placeholder(PlaceholderContent {
            reason: "e2e_ciphertext".into(),
            payload: vec![1, 2, 3],
            fallback_text: "[加密消息]".into(),
            ..Default::default()
        }));
        e2ee.offline_push_info = Some(OfflinePushInfo {
            title: "密聊".into(),
            body: "密文旁边的明文".into(),
            ..Default::default()
        });
        for message in [purged, e2ee] {
            for preview in [
                NotificationPreview::Unspecified,
                NotificationPreview::Full,
                NotificationPreview::SenderOnly,
                NotificationPreview::Hidden,
            ] {
                let mut task = with_preview(event_task(message.clone()), preview);
                task.alert_class = AlertClass::Mention as i32;
                assert_eq!(zh(&task), pair("新消息", "你收到一条新消息"), "{preview:?}");
            }
        }
    }

    /// 预览设置决定露出多少：完整照常；只显示发送者保留标题、正文换成「发来一条消息」；
    /// 隐藏时标题正文都是通用文案。不认识的取值按隐藏处理。
    #[test]
    fn preview_setting_decides_how_much_is_shown() {
        let message = message_with_push_info("小明", "今晚的方案我改好了");
        for (preview, zh_text, en_text) in [
            (
                NotificationPreview::Unspecified,
                pair("小明", "今晚的方案我改好了"),
                pair("小明", "今晚的方案我改好了"),
            ),
            (
                NotificationPreview::Full,
                pair("小明", "今晚的方案我改好了"),
                pair("小明", "今晚的方案我改好了"),
            ),
            (
                NotificationPreview::SenderOnly,
                pair("小明", "发来一条消息"),
                pair("小明", "sent you a message"),
            ),
            (
                NotificationPreview::Hidden,
                pair("新消息", "你收到一条新消息"),
                pair("New message", "You have a new message"),
            ),
        ] {
            let task = with_preview(event_task(message.clone()), preview);
            assert_eq!(zh(&task), zh_text, "{preview:?}");
            assert_eq!(en(&task), en_text, "{preview:?}");
        }
        let mut unknown = event_task(message.clone());
        unknown.preview = 9_999;
        assert_eq!(zh(&unknown), pair("新消息", "你收到一条新消息"));

        // 只显示发送者却没有标题：「发来一条消息」没有主语，说通用的那句。
        let untitled = with_preview(
            event_task(message_with_content(Content::Text(TextContent {
                text: "内容".into(),
                ..Default::default()
            }))),
            NotificationPreview::SenderOnly,
        );
        assert_eq!(zh(&untitled), pair("新消息", "你收到一条新消息"));

        // 条数前缀在各种预览下都在：它不泄露内容。
        let mut counted = with_preview(event_task(message), NotificationPreview::Hidden);
        counted.count = 3;
        assert_eq!(zh(&counted), pair("[3条] 新消息", "你收到一条新消息"));
        assert_eq!(
            en(&counted),
            pair("[3 messages] New message", "You have a new message")
        );
    }

    /// 点名到接收者时正文前加标记；只显示发送者时保留，隐藏时不加；文案自己写了就不重复。
    #[test]
    fn mention_marker_is_prepended_once() {
        let mut task = event_task(message_with_push_info("项目群", "小明: @你 看下这个"));
        task.alert_class = AlertClass::Mention as i32;
        assert_eq!(zh(&task), pair("项目群", "[有人@你] 小明: @你 看下这个"));
        assert_eq!(
            en(&task),
            pair("项目群", "[Mentioned you] 小明: @你 看下这个")
        );

        let sender_only = with_preview(task.clone(), NotificationPreview::SenderOnly);
        assert_eq!(zh(&sender_only), pair("项目群", "[有人@你] 发来一条消息"));
        assert_eq!(
            en(&sender_only),
            pair("项目群", "[Mentioned you] sent you a message")
        );
        let hidden = with_preview(task.clone(), NotificationPreview::Hidden);
        assert_eq!(zh(&hidden), pair("新消息", "你收到一条新消息"));

        // 社交服务端盖写的文案已经带了标记。
        for already in ["[有人@你] 小明: 看下这个", "[Mentioned you] Alice: look"] {
            let mut said = event_task(message_with_push_info("项目群", already));
            said.alert_class = AlertClass::Mention as i32;
            assert_eq!(zh(&said), pair("项目群", already));
        }

        // 占位与通用正文前同样加标记；不是点名的等级不加。
        let mut image = event_task(message_with_content(
            Content::Image(ImageContent::default()),
        ));
        image.alert_class = AlertClass::Mention as i32;
        assert_eq!(en(&image), pair("New message", "[Mentioned you] [Image]"));
        image.alert_class = AlertClass::Urgent as i32;
        assert_eq!(en(&image), pair("New message", "[Image]"));

        // 正文被截到上限时标记仍在开头，总长仍在上限内。
        let mut long = event_task(message_with_push_info("项目群", &"长".repeat(400)));
        long.alert_class = AlertClass::Mention as i32;
        let (_, body) = zh(&long);
        assert!(body.starts_with("[有人@你] 长"));
        assert_eq!(body.chars().count(), 180);
    }

    /// 通知类内容：只显示发送者与隐藏时说「你收到一条通知」，不加点名标记。
    #[test]
    fn notices_use_the_notice_wording() {
        let notice = message_with_content(Content::Notification(NotificationContent {
            title: "好友申请".into(),
            body: "小明请求添加你为好友".into(),
            play_sound: true,
            ..Default::default()
        }));
        let mut task = event_task(notice);
        task.alert_class = AlertClass::Mention as i32;
        assert_eq!(zh(&task), pair("好友申请", "小明请求添加你为好友"));
        let sender_only = with_preview(task.clone(), NotificationPreview::SenderOnly);
        assert_eq!(zh(&sender_only), pair("好友申请", "你收到一条通知"));
        assert_eq!(
            en(&sender_only),
            pair("好友申请", "You have a new notification")
        );
        let hidden = with_preview(task, NotificationPreview::Hidden);
        assert_eq!(
            en(&hidden),
            pair("New message", "You have a new notification")
        );

        // 推送请求里的通知与业务自定义推送。
        let request = PushNotificationRequest {
            notification: Some(Default::default()),
            ..Default::default()
        };
        let notification_task = PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Notification as i32,
            push_payload: request.encode_to_vec(),
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        };
        assert_eq!(zh(&notification_task), pair("新消息", "你收到一条通知"));
        let custom = PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Custom as i32,
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        };
        assert_eq!(
            en(&custom),
            pair("New message", "You have a new notification")
        );
    }

    #[test]
    fn fact_only_notification_does_not_alert_or_mutate_its_client_carrier() {
        let mut task = event_task(message_with_content(Content::Notification(
            NotificationContent::default(),
        )));
        let original = task.push_payload.clone();
        assert!(!mobile_alert_eligible(&task));
        assert_eq!(task.push_payload, original);
        task.push_payload =
            event_task(message_with_push_info("Visible", "Optional notification")).push_payload;
        assert!(mobile_alert_eligible(&task));
    }

    /// 事件里没有消息（纯 ping）时不该崩，也不该显示空白。
    #[test]
    fn ping_without_message_uses_generic_display() {
        let task = bare_event_task(PushDisposition::Loud);
        let shown = shown_content(&task);
        assert_eq!(shown, Shown::generic_in(CopyKind::Message, &task));
        // 没有会话可判断时不当成某个人的聊天。
        assert_eq!(shown.chat, ChatKind::Notice);
    }

    /// 响铃与摘要走高优先级并响铃；静默覆盖与撤回替换是对已有通知的修正，永远不再打扰一次。
    #[test]
    fn disposition_decides_sound_priority_and_interruption() {
        let mut message = message_with_push_info("项目群", "小明: 今天几点开会");
        if let Some(info) = message.offline_push_info.as_mut() {
            info.sound = "default".into();
        }
        for (disposition, sound, high_priority, interruption) in [
            (PushDisposition::Loud, true, true, Interruption::Active),
            (PushDisposition::Digest, true, true, Interruption::Active),
            (PushDisposition::Quiet, false, false, Interruption::Passive),
            (
                PushDisposition::RecallReplace,
                false,
                false,
                Interruption::Passive,
            ),
        ] {
            let alert = alert_of(&with_disposition(event_task(message.clone()), disposition));
            assert_eq!(
                (alert.play_sound, alert.high_priority, alert.interruption),
                (sound, high_priority, interruption),
                "{disposition:?}"
            );
        }
    }

    /// 处置只能收紧响铃，不能放宽：消息自己声明不响的，响铃处置也不硬塞铃声。
    #[test]
    fn a_loud_task_still_honours_the_message_sound_setting() {
        let task = event_task(message_with_push_info("项目群", "小明: 今天几点开会"));
        let alert = alert_of(&task);
        assert!(!alert.play_sound);
        assert!(alert.high_priority);
        assert_eq!(alert.interruption, Interruption::Active);
    }

    #[test]
    fn only_an_urgent_loud_task_is_time_sensitive() {
        let mut task = event_task(message_with_push_info("值班群", "线上告警"));
        task.alert_class = AlertClass::Urgent as i32;
        assert_eq!(alert_of(&task).interruption, Interruption::TimeSensitive);
        task.alert_class = AlertClass::Mention as i32;
        assert_eq!(alert_of(&task).interruption, Interruption::Active);
        task.alert_class = AlertClass::Urgent as i32;
        for (disposition, interruption) in [
            (PushDisposition::Digest, Interruption::Active),
            (PushDisposition::Quiet, Interruption::Passive),
            (PushDisposition::RecallReplace, Interruption::Passive),
        ] {
            task.disposition = disposition as i32;
            assert_eq!(
                alert_of(&task).interruption,
                interruption,
                "{disposition:?}"
            );
        }
    }

    /// 撤回替换要把通知栏里那条正文盖掉。撤回之后消息本身不再可提示，
    /// 计划不能因此变成「什么都不做」，否则旧正文一直留在通知栏里。
    #[test]
    fn recall_replacement_keeps_the_title_and_replaces_the_body() {
        let task = with_disposition(
            event_task(message_with_push_info("项目群", "小明: 发错了的内容")),
            PushDisposition::RecallReplace,
        );
        assert_eq!(zh(&task), pair("项目群", "撤回了一条消息"));
        assert_eq!(en(&task), pair("项目群", "Message recalled"));
        // 只显示发送者时标题照旧；隐藏时标题也换成通用的。
        let sender_only = with_preview(task.clone(), NotificationPreview::SenderOnly);
        assert_eq!(zh(&sender_only), pair("项目群", "撤回了一条消息"));
        let hidden = with_preview(task.clone(), NotificationPreview::Hidden);
        assert_eq!(zh(&hidden), pair("新消息", "撤回了一条消息"));
        // 点名的消息被撤回：正文只是撤回文案，不带标记。
        let mut mention = task;
        mention.alert_class = AlertClass::Mention as i32;
        assert_eq!(zh(&mention), pair("项目群", "撤回了一条消息"));

        let bare = bare_event_task(PushDisposition::RecallReplace);
        assert!(!mobile_alert_eligible(&bare));
        let alert = alert_of(&bare);
        assert_eq!(
            alert.text(Lang::Zh),
            AlertText {
                title: "新消息".into(),
                body: "撤回了一条消息".into()
            }
        );
        assert!(!alert.play_sound);

        // 已清除的内容被撤回：标题也不露。
        let mut purged = message_with_push_info("项目群", "清除了的正文");
        purged.retention_state = Some(MessageRetentionState {
            content_visibility: ContentVisibility::Purged as i32,
            ..Default::default()
        });
        let recalled = with_disposition(event_task(purged), PushDisposition::RecallReplace);
        assert_eq!(zh(&recalled), pair("新消息", "撤回了一条消息"));
    }

    #[test]
    fn title_carries_the_unread_count_capped_at_99() {
        let mut task = with_disposition(
            event_task(message_with_push_info("项目群", "小明: 今天几点开会")),
            PushDisposition::Quiet,
        );
        for (count, zh_title, en_title) in [
            (0, "项目群", "项目群"),
            (1, "项目群", "项目群"),
            (2, "[2条] 项目群", "[2 messages] 项目群"),
            (99, "[99条] 项目群", "[99 messages] 项目群"),
            (100, "[99+条] 项目群", "[99+ messages] 项目群"),
            (u32::MAX, "[99+条] 项目群", "[99+ messages] 项目群"),
        ] {
            task.count = count;
            assert_eq!(zh(&task).0, zh_title);
            assert_eq!(en(&task).0, en_title);
        }
        // 前缀算在标题的上限里。
        task.count = 5;
        task.push_payload =
            event_task(message_with_push_info(&"长".repeat(100), "正文")).push_payload;
        let (title, _) = zh(&task);
        assert!(title.starts_with("[5条] 长"));
        assert_eq!(title.chars().count(), 64);
    }

    /// 摘要的文案只由条数与会话数生成：载荷为空也照常展示，标题不再带条数，合并键原样取自任务。
    #[test]
    fn digest_is_rendered_from_counts_alone() {
        let mut digest = PushTaskEnvelope {
            disposition: PushDisposition::Digest as i32,
            collapse_key: "d:device-digest".into(),
            thread_key: "d:device-digest".into(),
            count: 42,
            digest_conversations: 7,
            ..Default::default()
        };
        assert!(!mobile_alert_eligible(&digest));
        assert_eq!(zh(&digest), pair("新消息", "7 个会话有 42 条新消息"));
        assert_eq!(
            en(&digest),
            pair("New message", "42 new messages in 7 chats")
        );
        let alert = alert_of(&digest);
        assert!(alert.play_sound && alert.high_priority);
        let payload = client_payload(
            &digest,
            &PushPlan::Alert(alert),
            ValueShape::Strings,
            ClientText::Included(Lang::En),
        );
        assert_eq!(payload["collapse_id"], "d:device-digest");
        assert_eq!(payload["disposition"], "digest");
        assert_eq!(payload["count"], "42");
        assert_eq!(payload["body"], "42 new messages in 7 chats");

        // 预览设置与点名都不改摘要；会话数缺省时只说条数。
        digest.preview = NotificationPreview::Hidden as i32;
        digest.alert_class = AlertClass::Mention as i32;
        digest.digest_conversations = 0;
        assert_eq!(zh(&digest), pair("新消息", "42 条新消息"));

        // 载荷里的消息自己不响铃时摘要也不响。
        let mut quiet = with_disposition(
            event_task(message_with_push_info("项目群", "正文")),
            PushDisposition::Digest,
        );
        quiet.count = 3;
        quiet.digest_conversations = 2;
        assert!(!alert_of(&quiet).play_sound);
        assert_eq!(zh(&quiet), pair("新消息", "2 个会话有 3 条新消息"));
    }

    /// 仅角标与清除不展示任何内容，不能要求载荷可渲染。
    #[test]
    fn badge_and_clear_plans_do_not_depend_on_the_payload() {
        let mut task = PushTaskEnvelope {
            disposition: PushDisposition::BadgeOnly as i32,
            ..Default::default()
        };
        assert_eq!(push_plan(&task).unwrap(), PushPlan::Nothing);
        task.badge = Some(0);
        assert_eq!(push_plan(&task).unwrap(), PushPlan::BadgeOnly);
        task.disposition = PushDisposition::Clear as i32;
        assert_eq!(push_plan(&task).unwrap(), PushPlan::Clear);
        task.badge = None;
        assert_eq!(push_plan(&task).unwrap(), PushPlan::Clear);
    }

    #[test]
    fn a_task_without_displayable_content_plans_nothing() {
        for disposition in [PushDisposition::Loud, PushDisposition::Quiet] {
            assert_eq!(
                push_plan(&bare_event_task(disposition)).unwrap(),
                PushPlan::Nothing,
                "{disposition:?}"
            );
        }
    }

    /// 这两类任务重试多少次都不会变好，必须是终态错误才会进死信。
    #[test]
    fn a_missing_disposition_or_unusable_collapse_key_is_terminal() {
        let unspecified = with_disposition(
            event_task(message_with_push_info("项目群", "正文")),
            PushDisposition::Unspecified,
        );
        assert!(!push_plan(&unspecified).unwrap_err().is_retryable());
        let mut unknown = unspecified.clone();
        unknown.disposition = 9_999;
        assert!(!push_plan(&unknown).unwrap_err().is_retryable());

        let mut task = with_disposition(unspecified, PushDisposition::Loud);
        task.collapse_key = "c:0123456789abcdef0123456789abcdef".into();
        assert!(push_plan(&task).is_ok());
        for collapse_key in [
            format!("c:{}", "a".repeat(63)),
            "会话".to_string(),
            "a b".to_string(),
            "a\nb".to_string(),
        ] {
            task.collapse_key = collapse_key;
            assert!(!push_plan(&task).unwrap_err().is_retryable());
        }
    }

    /// 三家共用的那套键：蛇形命名、小写取值、条数至少为 1；值的形态按通道选数字或字符串，
    /// 文案按给定的语言渲染。
    #[test]
    fn client_payload_carries_the_shared_keys_in_either_shape() {
        let mut task = PushTaskEnvelope {
            user_id: "u1".into(),
            tenant_id: "t1".into(),
            conversation_id: "c1".into(),
            message_id: "m1".into(),
            collapse_key: "c:abc".into(),
            thread_key: "c:abc".into(),
            conversation_seq: 42,
            disposition: PushDisposition::BadgeOnly as i32,
            alert_class: AlertClass::Mention as i32,
            badge: Some(5),
            ..Default::default()
        };
        let plan = push_plan(&task).unwrap();
        assert_eq!(
            Value::Object(client_payload(
                &task,
                &plan,
                ValueShape::Native,
                ClientText::Included(Lang::Zh)
            )),
            serde_json::json!({
                "uid": "u1",
                "tenant_id": "t1",
                "conversation_id": "c1",
                "message_id": "m1",
                "seq": 42,
                "collapse_id": "c:abc",
                "thread_id": "c:abc",
                "disposition": "badge_only",
                "alert_class": "mention",
                "count": 1,
                "badge": 5,
                "sound": 0,
                // 不展示内容的计划没有发送者；会话标识不是协议前缀时按群聊。
                "sender_id": "",
                "sender_name": "",
                "kind": "group",
            }),
            "没有展示内容的计划不带标题正文"
        );
        task.count = 7;
        let strings = client_payload(
            &task,
            &plan,
            ValueShape::Strings,
            ClientText::Included(Lang::Zh),
        );
        assert_eq!(strings["count"], "7");
        assert_eq!(strings["seq"], "42");
        assert_eq!(strings["badge"], "5");
        assert_eq!(strings["sound"], "0");
        assert!(strings.values().all(Value::is_string), "{strings:?}");
        for legacy in ["collapse_key", "show_badge", "play_sound", "collapseId"] {
            assert!(!strings.contains_key(legacy), "{legacy}");
        }

        // 要展示的通知：端上渲染的通道带文案，响铃与否写成 1 / 0。
        let loud = event_task(message_with_push_info("项目群", "小明: 今天几点开会"));
        let plan = push_plan(&loud).unwrap();
        let rendered = client_payload(
            &loud,
            &plan,
            ValueShape::Strings,
            ClientText::Included(Lang::Zh),
        );
        assert_eq!(rendered["title"], "项目群");
        assert_eq!(rendered["body"], "小明: 今天几点开会");
        assert_eq!(rendered["sound"], "0", "消息自己没要求响铃");
        assert!(!rendered.contains_key("badge"));
        let mut sounding = message_with_push_info("项目群", "小明: 今天几点开会");
        if let Some(info) = sounding.offline_push_info.as_mut() {
            info.sound = "default".into();
        }
        let loud = event_task(sounding);
        let plan = push_plan(&loud).unwrap();
        let rendered = client_payload(
            &loud,
            &plan,
            ValueShape::Strings,
            ClientText::Included(Lang::Zh),
        );
        assert_eq!(rendered["sound"], "1");
        let apns = client_payload(&loud, &plan, ValueShape::Native, ClientText::Omitted);
        assert!(!apns.contains_key("title"));
        assert!(!apns.contains_key("body"));
        assert_eq!(apns["count"], 1);
        assert_eq!(apns["sound"], 1);

        // 模板随语言变：同一条没有文案的图片消息，中英文各是各的。
        let image = event_task(message_with_content(
            Content::Image(ImageContent::default()),
        ));
        let plan = push_plan(&image).unwrap();
        let english = client_payload(
            &image,
            &plan,
            ValueShape::Strings,
            ClientText::Included(Lang::En),
        );
        assert_eq!(english["title"], "New message");
        assert_eq!(english["body"], "[Image]");
        let chinese = client_payload(
            &image,
            &plan,
            ValueShape::Strings,
            ClientText::Included(Lang::Zh),
        );
        assert_eq!(chinese["title"], "新消息");
        assert_eq!(chinese["body"], "[图片]");

        assert_eq!(
            [
                PushDisposition::Loud,
                PushDisposition::Quiet,
                PushDisposition::Clear,
                PushDisposition::BadgeOnly,
                PushDisposition::RecallReplace,
                PushDisposition::Digest,
            ]
            .map(disposition_name),
            [
                "loud",
                "quiet",
                "clear",
                "badge_only",
                "recall_replace",
                "digest"
            ]
        );
        assert_eq!(
            [
                AlertClass::None,
                AlertClass::Silent,
                AlertClass::Normal,
                AlertClass::Mention,
                AlertClass::Urgent,
            ]
            .map(alert_class_name),
            ["none", "silent", "normal", "mention", "urgent"]
        );
    }

    /// 端上替换显示名用的三个键：发送者、正文里原样出现的发送者名字、会话类别。
    #[test]
    fn origin_keys_name_the_sender_only_where_the_body_does() {
        let group_message = |body: &str| Message {
            sender_id: "u-2".into(),
            sender_name: "小明".into(),
            conversation_type: ConversationType::Group as i32,
            ..message_with_push_info("项目群", body)
        };
        let origin = |task: &PushTaskEnvelope| match push_plan(task).unwrap() {
            PushPlan::Alert(alert) => alert.origin,
            other => panic!("{other:?}"),
        };

        let task = event_task(group_message("小明: 今天几点开会"));
        assert_eq!(
            origin(&task),
            Origin {
                sender_id: "u-2".into(),
                sender_name: "小明".into(),
                kind: ChatKind::Group,
            }
        );
        let payload = client_payload(
            &task,
            &push_plan(&task).unwrap(),
            ValueShape::Native,
            ClientText::Omitted,
        );
        assert_eq!(payload["sender_id"], "u-2");
        assert_eq!(payload["sender_name"], "小明");
        assert_eq!(payload["kind"], "group");

        // 正文里没有名字（或预览设置把正文换掉了）就不给名字，发送者照给。
        let task = event_task(group_message("今天几点开会"));
        assert_eq!(origin(&task).sender_name, "");
        assert_eq!(origin(&task).sender_id, "u-2");
        let hidden = with_preview(
            event_task(group_message("小明: 今天几点开会")),
            NotificationPreview::SenderOnly,
        );
        assert_eq!(origin(&hidden).sender_name, "");

        // 单聊、系统消息、通知与摘要。
        let direct = event_task(Message {
            conversation_type: ConversationType::Single as i32,
            ..group_message("在吗")
        });
        assert_eq!(origin(&direct).kind, ChatKind::Direct);
        let system = event_task(Message {
            sender_id: "u-2".into(),
            conversation_id: "2-group".into(),
            ..message_with_content(Content::System(Default::default()))
        });
        assert_eq!(origin(&system).sender_id, "");
        assert_eq!(origin(&system).kind, ChatKind::Group);
        let notice = event_task(Message {
            sender_id: "u-2".into(),
            ..message_with_content(Content::Notification(NotificationContent {
                title: "审批".into(),
                body: "你有一条待审批".into(),
                play_sound: true,
                ..Default::default()
            }))
        });
        assert_eq!(origin(&notice), Origin::anonymous(ChatKind::Notice));
        let digest = with_disposition(
            event_task(group_message("小明: 今天几点开会")),
            PushDisposition::Digest,
        );
        assert_eq!(origin(&digest).sender_id, "");
        assert_eq!(origin(&digest).sender_name, "");

        // 没有消息可读的补提示：类别按会话标识的前缀。
        assert_eq!(
            ChatKind::of_conversation(ConversationType::Unspecified as i32, "1-abc"),
            ChatKind::Direct
        );
        assert_eq!(
            ChatKind::of_conversation(ConversationType::Unspecified as i32, "4-abc"),
            ChatKind::Notice
        );
        assert_eq!(
            ChatKind::of_conversation(ConversationType::Unspecified as i32, ""),
            ChatKind::Notice
        );
        assert_eq!(
            ChatKind::of_conversation(ConversationType::System as i32, "2-abc"),
            ChatKind::Notice
        );
    }
}
