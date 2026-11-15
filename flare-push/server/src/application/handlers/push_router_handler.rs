use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_grpc_proto::access_gateway;
use flare_im_service_kit::metrics::PushServerMetrics;
use flare_im_service_kit::push_trace::PushTraceRecorder;
use flare_notify_rules::{ConversationKind, Mentioned};
use flare_proto::common::EventEnvelopeDeliveryMode;
use flare_proto::common::{
    Ack, AckPayload, AlertClass, CustomData, CustomPayload, EventType, NotificationKind,
    NotificationMessage, NotificationPayload, NotificationPreview, NotificationPriority, PushAck,
    PushDisposition, PushEnvelope, PushTaskEnvelope, PushTaskPayloadKind, SystemPayload, ack,
    notification_message, push_envelope,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use futures::stream::{self, StreamExt};
use prost::Message as _;
use tokio::sync::Mutex as AsyncMutex;

use crate::application::notify_gate::NotifyGate;
use crate::application::notify_wave::NotifyWaves;
use crate::domain::model::{OfflineLane, UserReachability};
use crate::domain::notify_inputs::conversation_kind;
use crate::domain::repository::{
    BoundDevices, DeviceBindingRepository, MessageLookupRepository, NotifyPolicyRepository,
    NotifyPreference, RecentConversationRepository,
};
use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::online::online_status_service::OnlineStatusService;

/// 这个等级要不要在设备上产生提示。不提示的载荷（已读回执、能力包、只供同步的事实）
/// 只走在线一路，永远不建离线任务。
fn class_alerts(class: AlertClass) -> bool {
    !matches!(class, AlertClass::Unspecified | AlertClass::None)
}

/// 一份推送载荷里与提示有关的事实，解码一次得出。
#[derive(Debug)]
struct AlertFacts {
    /// 对没被点名的普通接收者的提示等级。
    class: AlertClass,
    /// 载荷里那条要提示的消息提到了谁。
    mentions: MentionTargets,
    /// 所代表的最新消息的会话 seq；载荷不属于会话时为 0。
    conversation_seq: u64,
    /// 要提示的那条消息的服务端 ID；载荷里没有消息时为空。
    message_id: String,
    /// 要提示的那条消息的发送者；载荷里没有消息时为空。
    sender_id: String,
    /// 会话类型，决定闸门用哪一条响铃阶梯。
    conversation_kind: ConversationKind,
}

impl AlertFacts {
    /// 不提示的载荷：解不出来的、以及本来就只走在线一路的类型。
    fn silent() -> Self {
        Self {
            class: AlertClass::None,
            mentions: MentionTargets::unknown(),
            conversation_seq: 0,
            message_id: String::new(),
            sender_id: String::new(),
            conversation_kind: ConversationKind::Group,
        }
    }

    /// 由一条要提示的消息得出。`class` 是整份载荷的等级，`conversation_seq` 是它所在事件或
    /// 请求给出的 seq，为 0 时取消息自己的。
    fn of_message(
        class: AlertClass,
        message: &flare_proto::common::Message,
        conversation_seq: u64,
    ) -> Self {
        Self {
            class,
            mentions: mentions_of_message(message),
            conversation_seq: if conversation_seq == 0 {
                message.conversation_seq
            } else {
                conversation_seq
            },
            message_id: message.server_id.clone(),
            sender_id: message.sender_id.clone(),
            conversation_kind: conversation_kind(
                message.conversation_type,
                &message.conversation_id,
            ),
        }
    }
}

/// 解出一份载荷的提示事实。
///
/// 等级取载荷里各条消息的最高等级；提及、seq、消息标识与发送者取第一条要提示的消息——
/// 设备上最终展示的也是这一条。
fn alert_facts(payload_kind: i32, push_payload: &[u8]) -> AlertFacts {
    use flare_im_contracts::domain::push_policy::{event_alert_class, message_alert_class};
    match PushTaskPayloadKind::try_from(payload_kind).unwrap_or(PushTaskPayloadKind::Unspecified) {
        PushTaskPayloadKind::Message => {
            let Ok(request) = access_gateway::PushMessageRequest::decode(push_payload) else {
                return AlertFacts::silent();
            };
            let class = request
                .messages
                .iter()
                .map(message_alert_class)
                .fold(AlertClass::None, AlertClass::max);
            match request
                .messages
                .iter()
                .find(|message| class_alerts(message_alert_class(message)))
            {
                Some(message) => AlertFacts::of_message(class, message, 0),
                None => AlertFacts::silent(),
            }
        }
        PushTaskPayloadKind::Event => {
            let Ok(request) = access_gateway::PushEventRequest::decode(push_payload) else {
                return AlertFacts::silent();
            };
            let class = request
                .events
                .iter()
                .map(event_alert_class)
                .fold(AlertClass::None, AlertClass::max);
            let alerting = request
                .events
                .iter()
                .filter(|event| class_alerts(event_alert_class(event)))
                .find_map(|event| match &event.payload {
                    Some(flare_proto::common::event::Payload::Message(message)) => {
                        Some((event, message))
                    }
                    _ => None,
                });
            match alerting {
                Some((event, message)) => {
                    AlertFacts::of_message(class, message, event.conversation_seq)
                }
                None => AlertFacts::silent(),
            }
        }
        // 通知与自定义推送没有消息体：按普通消息提示，提及无从解析，也不属于任何会话。
        PushTaskPayloadKind::Notification | PushTaskPayloadKind::Custom => AlertFacts {
            class: AlertClass::Normal,
            ..AlertFacts::silent()
        },
        _ => AlertFacts::silent(),
    }
}

#[cfg(test)]
fn mobile_message_payload_fixture() -> Vec<u8> {
    access_gateway::PushMessageRequest {
        messages: vec![flare_proto::common::Message::default()],
        ..Default::default()
    }
    .encode_to_vec()
}

#[async_trait]
pub trait ConversationOnlineIndexReader: Send + Sync {
    async fn online_user_ids(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>>;
}

#[async_trait]
pub trait OnlineStatusReader: Send + Sync {
    /// 这批用户各自的设备可达视图。没有任何会话的用户可以不出现在结果里。
    async fn device_reachability(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>>;
    /// Retained for existing adapters; never used as a routing tenant fallback.
    fn default_tenant_id(&self) -> &str;
}

#[async_trait]
impl OnlineStatusReader for OnlineStatusService {
    async fn device_reachability(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        OnlineStatusService::device_reachability(self, ctx, user_ids).await
    }
    fn default_tenant_id(&self) -> &str {
        OnlineStatusService::default_tenant_id(self)
    }
}

#[async_trait]
impl ConversationOnlineIndexReader for OnlineStatusService {
    async fn online_user_ids(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        OnlineStatusService::conversation_online_user_ids(self, ctx, conversation_id).await
    }
}

#[async_trait]
pub trait PushTaskPublisher: Send + Sync {
    async fn publish_online_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()>;

    /// 离线任务发到它那条优先级通道的主题上。
    async fn publish_offline_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        lane: OfflineLane,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()>;
}

#[async_trait]
impl PushTaskPublisher for PushServerMqPublisher {
    async fn publish_online_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        PushServerMqPublisher::publish_online_task(self, ctx, key, payload).await
    }

    async fn publish_offline_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        lane: OfflineLane,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        PushServerMqPublisher::publish_offline_task(self, ctx, lane, key, payload).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConversationPingCoalesceKey {
    tenant_id: String,
    conversation_id: String,
}

impl ConversationPingCoalesceKey {
    fn new(tenant_id: impl Into<String>, conversation_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            conversation_id: conversation_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConversationPingCoalesceDecision {
    SendNow,
    ScheduleAfter(Duration),
    Suppressed,
}

struct ConversationPingCoalescer {
    window: Duration,
    /// 空闲条目的清理间隔。取窗口的 10 倍并兜底 30s：清理本身要遍历整张表，
    /// 每次 observe 都扫会把 O(1) 变成 O(n)。
    sweep_interval: Duration,
    state: AsyncMutex<ConversationPingCoalesceState>,
}

/// 合流状态：条目表 + 上次清理时刻。
///
/// 两者必须在同一把锁下，否则清理与 observe 之间会出现窗口，让刚写入的
/// pending 被当成空闲条目清掉。
struct ConversationPingCoalesceState {
    entries: HashMap<ConversationPingCoalesceKey, ConversationPingCoalesceEntry>,
    last_sweep: Instant,
}

struct ConversationPingCoalesceEntry {
    last_sent: Instant,
    scheduled: bool,
    pending: Option<access_gateway::PushEventRequest>,
}

impl ConversationPingCoalesceEntry {
    /// 条目是否已经和"不存在"等价。
    ///
    /// observe 对缺失的键返回 SendNow；对 elapsed >= window 且没有挂起 ping 的
    /// 条目同样返回 SendNow。两者决策完全一致，所以这种条目可以安全丢弃——
    /// 清理不会改变任何行为，只是不再无限占着内存。
    fn is_idle(&self, now: Instant, window: Duration) -> bool {
        !self.scheduled && self.pending.is_none() && now.duration_since(self.last_sent) >= window
    }
}

impl ConversationPingCoalescer {
    fn new(window: Duration) -> Self {
        Self {
            window,
            sweep_interval: (window * 10).max(Duration::from_secs(30)),
            state: AsyncMutex::new(ConversationPingCoalesceState {
                entries: HashMap::new(),
                last_sweep: Instant::now(),
            }),
        }
    }

    async fn observe(
        &self,
        key: ConversationPingCoalesceKey,
        pending: access_gateway::PushEventRequest,
    ) -> ConversationPingCoalesceDecision {
        if self.window.is_zero() {
            return ConversationPingCoalesceDecision::SendNow;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;

        // 摊销清理：条目只增不减的话，每个出现过的会话都会永久占一条，
        // 进程活多久就涨多久。空闲条目与缺失键决策相同，丢掉不改变行为。
        if now.duration_since(state.last_sweep) >= self.sweep_interval {
            let window = self.window;
            state.entries.retain(|_, entry| !entry.is_idle(now, window));
            state.last_sweep = now;
        }

        let Some(entry) = state.entries.get_mut(&key) else {
            state.entries.insert(
                key,
                ConversationPingCoalesceEntry {
                    last_sent: now,
                    scheduled: false,
                    pending: None,
                },
            );
            return ConversationPingCoalesceDecision::SendNow;
        };

        let elapsed = now.duration_since(entry.last_sent);
        if elapsed >= self.window {
            entry.last_sent = now;
            entry.scheduled = false;
            entry.pending = None;
            return ConversationPingCoalesceDecision::SendNow;
        }

        entry.pending = Some(match entry.pending.take() {
            Some(existing) => merge_pending_conversation_ping(existing, pending),
            None => pending,
        });
        if entry.scheduled {
            ConversationPingCoalesceDecision::Suppressed
        } else {
            entry.scheduled = true;
            ConversationPingCoalesceDecision::ScheduleAfter(self.window - elapsed)
        }
    }

    async fn take_pending(
        &self,
        key: &ConversationPingCoalesceKey,
    ) -> Option<access_gateway::PushEventRequest> {
        let mut state = self.state.lock().await;
        let entry = state.entries.get_mut(key)?;
        entry.scheduled = false;
        let pending = entry.pending.take();
        if pending.is_some() {
            entry.last_sent = Instant::now();
        }
        pending
    }
}

fn pending_conversation_ping_request(
    req: &access_gateway::PushEventRequest,
    conversation_id: &str,
    max_conversation_seq: u64,
) -> access_gateway::PushEventRequest {
    let mut pending = req.clone();
    pending.user_ids.clear();
    pending.events.clear();
    pending.conversation_id = conversation_id.to_string();
    pending.max_conversation_seq = max_conversation_seq;
    pending.delivery_mode = EventEnvelopeDeliveryMode::Ping as i32;
    pending.inline_events_truncated = true;
    pending
}

fn merge_pending_conversation_ping(
    mut existing: access_gateway::PushEventRequest,
    incoming: access_gateway::PushEventRequest,
) -> access_gateway::PushEventRequest {
    existing.max_conversation_seq = existing
        .max_conversation_seq
        .max(incoming.max_conversation_seq);
    existing.options = incoming.options.or(existing.options);
    existing.user_ids.clear();
    existing.events.clear();
    existing.delivery_mode = EventEnvelopeDeliveryMode::Ping as i32;
    existing.inline_events_truncated = true;
    existing
}

#[derive(Clone)]
pub struct PushRouterHandler {
    online_status: Arc<dyn OnlineStatusReader>,
    conversation_online_index: Arc<dyn ConversationOnlineIndexReader>,
    /// 设备推送绑定。离线任务按设备发，没有绑定就没有可推的设备。
    device_bindings: Arc<dyn DeviceBindingRepository>,
    publisher: Arc<dyn PushTaskPublisher>,
    conversation_ping_coalescer: Option<Arc<ConversationPingCoalescer>>,
    /// 免打扰读取。未接线时不做任何过滤——保持既有行为，而不是把推送全挡掉。
    notify_policy: Option<Arc<dyn NotifyPolicyRepository>>,
    /// 补提示时读回消息。未接线或读不到时补提示用通用文案，不因此不提示。
    message_lookup: Option<Arc<dyn MessageLookupRepository>>,
    /// 网关失联的交接靠它问出窗口内有新未读的会话。未接线时这类交接无从补提示，只记日志。
    recent_conversations: Option<Arc<dyn RecentConversationRepository>>,
    /// 路由指标。未接线时不计数。
    metrics: Option<Arc<PushServerMetrics>>,
    /// 防打扰闸门。接线后每台候选设备按提示规则决定响铃、静默更新、合并、推迟还是只更新角标，
    /// 并处理已读、撤回的修正；未接线时每条消息直接给每台设备发一条任务。
    notify_gate: Option<Arc<NotifyGate>>,
    /// 群会话的波次，与闸门一起接线。未接线时群里每条消息都当场枚举接收者。
    notify_waves: Option<Arc<NotifyWaves>>,
    /// 群会话的接收者（成员与偏好，或大群的订阅者），按会话在进程内缓存 15 秒：一个波次只查一次。
    audience_cache: waves::AudienceCache,
    /// 群的成员上限。不超过它的群，波次枚举全体成员；超过它的只枚举开启了「全部消息通知」的
    /// 订阅者，被点名的人另行按人推送。
    offline_fanout_member_cap: usize,
    /// 推送决策链的写入端。未接线时什么都不记。
    trace: PushTraceRecorder,
}

struct PushTaskTemplate<'a> {
    message_id: &'a str,
    conversation_id: &'a str,
    tenant_id: &'a str,
    priority: i32,
    expire_at: Option<i64>,
    push_payload: &'a [u8],
    headers: &'a HashMap<String, String>,
    payload_kind: i32,
}

/// 一条消息提到了谁。
///
/// 只用于「只接收@我」的推送判定：拿不准时一律按「提到了」处理（fail-open），
/// 宁可多响一声，也别把真正点名叫人的消息吞掉。
#[derive(Debug, Default)]
struct MentionTargets {
    all: bool,
    users: HashSet<String>,
    /// 解析不出内容（纯 ping、非文本、解码失败）——此时不做抑制。
    unknown: bool,
}

impl MentionTargets {
    fn unknown() -> Self {
        Self {
            unknown: true,
            ..Default::default()
        }
    }

    fn mentions(&self, user_id: &str) -> bool {
        self.unknown || self.all || self.names(user_id)
    }

    /// 确实点到了这个人（或 @所有人）。解析不出时不算：`mentions` 的放行是为了不吞推送，
    /// 而提示等级升到「点名」会绕过响铃节制，拿不准就不能升。
    fn names(&self, user_id: &str) -> bool {
        self.all || self.users.contains(user_id)
    }

    /// 交给提示规则的点名状态：点到了、没点到，或解析不出。
    fn mention_state(&self, user_id: &str) -> Mentioned {
        if self.names(user_id) {
            Mentioned::Yes
        } else if self.unknown {
            Mentioned::Unknown
        } else {
            Mentioned::No
        }
    }
}

fn collect_mentions(content: &flare_proto::common::MessageContent, out: &mut MentionTargets) {
    use flare_proto::common::message_content::Content;
    let mentions = match content.content.as_ref() {
        Some(Content::Text(text)) => &text.mentions,
        Some(Content::RichText(rich)) => {
            // 富文本的提及在结构化文档里，这里不解析——拿不准就不抑制，
            // 让它照常推送，而不是把一条可能点名叫人的消息静默吞掉。
            let _ = rich;
            out.unknown = true;
            return;
        }
        Some(Content::Quote(quote)) => {
            if let Some(inner) = quote.current_content.as_ref() {
                collect_mentions(inner, out);
            }
            return;
        }
        _ => return,
    };
    for mention in mentions {
        if mention.r#type == flare_proto::common::MentionType::All as i32 {
            out.all = true;
        }
        if !mention.user_id.is_empty() {
            out.users.insert(mention.user_id.clone());
        }
        out.users.extend(mention.user_ids.iter().cloned());
    }
}

/// 一条消息提到了谁。没有内容的消息按解析不出处理。
fn mentions_of_message(message: &flare_proto::common::Message) -> MentionTargets {
    let Some(content) = message.content.as_ref() else {
        return MentionTargets::unknown();
    };
    let mut out = MentionTargets::default();
    collect_mentions(content, &mut out);
    out
}

/// 会话广播之后给离线成员补推所需的最小信息。
struct OfflineFanoutTemplate<'a> {
    message_id: &'a str,
    priority: i32,
    expire_at: Option<i64>,
    push_payload: &'a [u8],
}

struct PendingPushTask {
    user_id: String,
    payload: Vec<u8>,
    online: bool,
    /// 离线任务的处置，只用于指标标签；在线任务没有处置。
    disposition: PushDisposition,
    /// 离线任务的优先级通道；在线任务不用。
    lane: OfflineLane,
    /// 发出后在决策链里记一条 `task`；在线任务与不属于任何一条消息的任务没有。
    trace: Option<trace::TaskTrace>,
}

/// 一条离线任务走哪条优先级通道：不属于会话的推送走单聊通道，其余按会话类型与这个接收者的提示等级。
fn route_lane(conversation_id: &str, kind: ConversationKind, class: AlertClass) -> OfflineLane {
    if conversation_id.trim().is_empty() {
        OfflineLane::Direct
    } else {
        OfflineLane::of_alert(kind, class)
    }
}

/// 在线任务发给谁。只看连接，不看通知偏好、提及或设备的应用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnlineLeg {
    /// 有会话的收件人。
    Connected,
    /// 全体收件人，没连上的由网关丢弃。用于本身就是实时投递载体的载荷：在线索引有滞后，
    /// 只发给「看起来在线」的人会让刚连上的设备当场丢消息。
    Everyone,
    /// 不发：在线一路已由会话广播承担，再发一遍在线成员会收到重复消息。
    AlreadyBroadcast,
}

/// 在线任务的收件人。
///
/// 单独成函数、参数里没有通知偏好：免打扰、「只接收@我」、设备的提示归链路还是归推送通道，
/// 这些过滤只作用于推送目标，从类型上就碰不到这份名单。曾经有一次改动让在线投递与离线推送二选一，
/// 被误判离线的设备当场丢了实时消息。
fn online_task_recipients(
    user_ids: &[String],
    reachability: &HashMap<String, UserReachability>,
    leg: OnlineLeg,
) -> Vec<String> {
    match leg {
        OnlineLeg::Everyone => user_ids.to_vec(),
        OnlineLeg::Connected => user_ids
            .iter()
            .filter(|user_id| {
                reachability
                    .get(*user_id)
                    .is_some_and(|reach| reach.has_session)
            })
            .cloned()
            .collect(),
        OnlineLeg::AlreadyBroadcast => Vec::new(),
    }
}

/// 一个用户要推送的设备：有推送绑定、且不是活跃设备。
///
/// 电脑在线、手机已托管的用户，手机在这里；连着、却在后台不能自己提示的手机也在这里，
/// 它的提示从路由这一刻起就归推送通道。没有任何绑定的用户这里是空的。
fn devices_to_push(bound: &BoundDevices, reach: Option<&UserReachability>) -> Vec<String> {
    bound
        .iter()
        .filter(|device_id| !reach.is_some_and(|reach| reach.is_live(device_id)))
        .cloned()
        .collect()
}

/// 通知偏好是否压掉这个人的离线推送，返回压掉的原因。
///
/// 只回答推不推送，不回答投不投递：调用方只有离线任务这一路。
fn push_suppressed_by_preference(
    preference: NotifyPreference,
    mentioned: bool,
) -> Option<&'static str> {
    if preference.muted {
        return Some("muted conversation");
    }
    if preference.mention_only && !mentioned {
        return Some("mention-only and this message does not mention the user");
    }
    None
}

/// 一次通知偏好查询的结果。
#[derive(Debug)]
enum PreferenceLookup {
    /// 查到了。表里没有的人确定不是该会话的成员。
    Resolved(HashMap<String, NotifyPreference>),
    /// 没接线、载荷不属于任何会话、或查询失败：偏好未知，一律放行（fail-open）。
    Unavailable,
}

impl PreferenceLookup {
    /// 这个人的偏好。未知或表里没有时是默认偏好，即不抑制。
    fn preference_of(&self, user_id: &str) -> NotifyPreference {
        match self {
            Self::Resolved(preferences) => preferences.get(user_id).copied().unwrap_or_default(),
            Self::Unavailable => NotifyPreference::default(),
        }
    }

    /// 确定不是成员：查询成功而表里没有这个人。查询失败时下不了这个结论。
    fn is_known_non_member(&self, user_id: &str) -> bool {
        matches!(self, Self::Resolved(preferences) if !preferences.contains_key(user_id))
    }
}

/// 接收者各自的提示等级：被点到名的升到「点名」，其余用载荷的等级。
fn recipient_alert_class(class: AlertClass, named: bool) -> AlertClass {
    if named {
        class.max(AlertClass::Mention)
    } else {
        class
    }
}

/// 提示等级对应的处置：静默等级只更新通知与角标，其余展示并响铃。
fn disposition_of(class: AlertClass) -> PushDisposition {
    if class == AlertClass::Silent {
        PushDisposition::Quiet
    } else {
        PushDisposition::Loud
    }
}

/// 上游没给过期时间时，要响铃的离线任务（响铃、摘要）活多久。离线的设备可能几小时后才回来，
/// 但隔天才弹出的「新消息」只是噪声。
const LOUD_TASK_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// 上游没给过期时间时，不响铃的离线任务（静默更新、清除、仅角标、撤回替换）活多久。
/// 它们修正的是设备上已有的那条通知或角标：十分钟还没送到，后面多半已有更新的一条盖过它，
/// 迟到的修正只会把通知改回旧样子。
const QUIET_TASK_TTL_MS: i64 = 10 * 60 * 1000;

/// 上游没给过期时间时，这种处置的离线任务活多久。
fn default_task_ttl_ms(disposition: PushDisposition) -> i64 {
    match disposition {
        PushDisposition::Quiet
        | PushDisposition::Clear
        | PushDisposition::BadgeOnly
        | PushDisposition::RecallReplace => QUIET_TASK_TTL_MS,
        // 没有处置的任务不该出现；按最长的算，宁可晚到，不可早丢。
        PushDisposition::Loud | PushDisposition::Digest | PushDisposition::Unspecified => {
            LOUD_TASK_TTL_MS
        }
    }
}

/// 处置在指标里的标签：固定的小写名字，不随 proto 枚举名的写法变。
fn disposition_label(disposition: PushDisposition) -> &'static str {
    match disposition {
        PushDisposition::Loud => "loud",
        PushDisposition::Quiet => "quiet",
        PushDisposition::Clear => "clear",
        PushDisposition::BadgeOnly => "badge_only",
        PushDisposition::RecallReplace => "recall_replace",
        PushDisposition::Digest => "digest",
        PushDisposition::Unspecified => "unspecified",
    }
}

/// 离线任务的幂等键：同一台设备上同一条消息的同一种处置只有一个键。
///
/// 它同时是 broker 的消息标识。同一条提示可能被算出不止一次——源记录被重投、
/// 正常路由与设备交接各产出一次——靠它在落盘前去重，设备上才不会为同一条消息响两声。
/// 各段之间用 0 字节分隔，("a", "bc") 与 ("ab", "c") 不会撞；处置与 seq 取十进制文本。
fn offline_idempotency_key(
    tenant_id: &str,
    user_id: &str,
    device_id: &str,
    collapse_key: &str,
    disposition: PushDisposition,
    conversation_seq: u64,
    message_id: &str,
) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let disposition = (disposition as i32).to_string();
    let conversation_seq = conversation_seq.to_string();
    let mut hasher = Sha256::new();
    for part in [
        tenant_id,
        user_id,
        device_id,
        collapse_key,
        disposition.as_str(),
        conversation_seq.as_str(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    hasher.update(message_id.as_bytes());
    let digest = hasher.finalize();
    let mut key = String::with_capacity(64);
    for byte in digest.iter() {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

/// 一条离线任务里与目标设备无关的部分：同一个用户的每台设备各发一条，内容相同。
struct OfflineTaskSpec<'a> {
    tenant_id: &'a str,
    user_id: &'a str,
    conversation_id: &'a str,
    /// 任务的消息标识（事件路径上是事件标识）。
    message_id: &'a str,
    /// 要提示的那条消息的服务端 ID，用于幂等键与信封的 `message_id`；为空时退回 `message_id`。
    /// 正常路由与设备交接对同一条消息必须算出同一个键，而两条路径的任务标识并不相同；
    /// 设备拿到的 `message_id`（点开定位、到达回执）与推送决策链的键也都是它。
    alert_message_id: &'a str,
    priority: i32,
    expire_at: Option<i64>,
    /// 已收敛成只含本人的载荷。
    push_payload: &'a [u8],
    headers: &'a HashMap<String, String>,
    payload_kind: i32,
    alert_class: AlertClass,
    conversation_seq: u64,
}

/// 一条离线任务除内容以外的形状：处置、条数、预览与角标。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TaskShape {
    disposition: PushDisposition,
    /// 这条通知代表的消息数。
    count: u32,
    preview: NotificationPreview,
    /// 角标绝对值；`None` 表示不改设备上的角标。
    badge: Option<u32>,
}

impl TaskShape {
    /// 不经过闸门时的形状：处置按提示等级，一条任务代表一条消息，不带预览设置与角标。
    fn per_message(alert_class: AlertClass) -> Self {
        Self {
            disposition: disposition_of(alert_class),
            count: 1,
            preview: NotificationPreview::Unspecified,
            badge: None,
        }
    }
}

/// 给一台设备建离线任务。`now_ms` 只用于补过期时间。
fn offline_task(spec: &OfflineTaskSpec<'_>, device_id: &str, now_ms: i64) -> PushTaskEnvelope {
    offline_task_shaped(
        spec,
        device_id,
        now_ms,
        TaskShape::per_message(spec.alert_class),
    )
}

/// 按给定形状给一台设备建离线任务。幂等键带着处置：同一条消息的响铃与之后的静默更新、撤回替换
/// 是不同的任务。
fn offline_task_shaped(
    spec: &OfflineTaskSpec<'_>,
    device_id: &str,
    now_ms: i64,
    shape: TaskShape,
) -> PushTaskEnvelope {
    use flare_im_contracts::domain::push_policy::conversation_collapse_key;

    // 合并键按会话取。不属于任何会话的推送（系统通知、自定义推送）不合并：
    // 它们共用一个键的话，互不相干的通知会在设备上互相覆盖。
    let collapse_key = if spec.conversation_id.trim().is_empty() {
        String::new()
    } else {
        conversation_collapse_key(spec.tenant_id, spec.conversation_id)
    };
    let disposition = shape.disposition;
    let alert_message_id = if spec.alert_message_id.is_empty() {
        spec.message_id
    } else {
        spec.alert_message_id
    };
    PushTaskEnvelope {
        user_id: spec.user_id.to_string(),
        // 离线任务带的是要提示的那条消息的 ID，不是事件标识：设备按它定位消息、报到达回执，
        // Worker 与回执按它记推送决策链，三者与路由记的是同一条链。
        message_id: alert_message_id.to_string(),
        conversation_id: spec.conversation_id.to_string(),
        tenant_id: spec.tenant_id.to_string(),
        priority: spec.priority,
        expire_at: spec
            .expire_at
            .filter(|expire_at| *expire_at > 0)
            .or(Some(now_ms + default_task_ttl_ms(disposition))),
        push_payload: spec.push_payload.to_vec(),
        headers: spec.headers.clone(),
        payload_kind: spec.payload_kind,
        device_id: device_id.to_string(),
        idempotency_key: offline_idempotency_key(
            spec.tenant_id,
            spec.user_id,
            device_id,
            &collapse_key,
            disposition,
            spec.conversation_seq,
            alert_message_id,
        ),
        thread_key: collapse_key.clone(),
        collapse_key,
        disposition: disposition as i32,
        alert_class: spec.alert_class as i32,
        count: shape.count,
        badge: shape.badge,
        conversation_seq: spec.conversation_seq,
        preview: shape.preview as i32,
        digest_conversations: 0,
    }
}

/// 当前时间，Unix epoch 毫秒。
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 单条在线推送信封最多携带多少个用户 ID。
///
/// 决定"单条消息大小"与"群规模"是否解耦。1000 个 ID 约 16–24KB，远低于 NATS
/// 的 max_payload，也不会让 push-server 的内存随群规模线性膨胀。
const ONLINE_FANOUT_CHUNK: usize = 1000;

const PUSH_TASK_PUBLISH_CONCURRENCY: usize = 64;

/// 群的成员上限（可用 `PUSH_SERVER_OFFLINE_FANOUT_MEMBER_CAP` 覆盖）：超过它的群只枚举订阅者。
const DEFAULT_OFFLINE_FANOUT_MEMBER_CAP: usize = 1000;

fn rewrite_push_payload_user_ids(
    payload_kind: i32,
    payload: &[u8],
    user_ids: &[String],
) -> Result<Vec<u8>> {
    let kind =
        PushTaskPayloadKind::try_from(payload_kind).unwrap_or(PushTaskPayloadKind::Unspecified);
    match kind {
        PushTaskPayloadKind::Message => {
            let mut request = access_gateway::PushMessageRequest::decode(payload).map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "decode PushMessageRequest failed",
                )
                .details(e.to_string())
                .build_error()
            })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Event => {
            let mut request = access_gateway::PushEventRequest::decode(payload).map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "decode PushEventRequest failed",
                )
                .details(e.to_string())
                .build_error()
            })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Notification => {
            let mut request =
                access_gateway::PushNotificationRequest::decode(payload).map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::InvalidParameter,
                        "decode PushNotificationRequest failed",
                    )
                    .details(e.to_string())
                    .build_error()
                })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Ack => {
            let mut request = access_gateway::PushAckRequest::decode(payload).map_err(|e| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "decode PushAckRequest failed")
                    .details(e.to_string())
                    .build_error()
            })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Custom => {
            let mut request = access_gateway::PushCustomRequest::decode(payload).map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "decode PushCustomRequest failed",
                )
                .details(e.to_string())
                .build_error()
            })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Capability => {
            let mut request =
                access_gateway::PushCapabilityRequest::decode(payload).map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::InvalidParameter,
                        "decode PushCapabilityRequest failed",
                    )
                    .details(e.to_string())
                    .build_error()
                })?;
            request.user_ids = user_ids.to_vec();
            Ok(request.encode_to_vec())
        }
        PushTaskPayloadKind::Unspecified => Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "PushTaskPayloadKind unspecified",
        )
        .build_error()),
    }
}

impl PushRouterHandler {
    pub fn new(
        online_status: Arc<dyn OnlineStatusReader>,
        conversation_online_index: Arc<dyn ConversationOnlineIndexReader>,
        device_bindings: Arc<dyn DeviceBindingRepository>,
        publisher: Arc<dyn PushTaskPublisher>,
    ) -> Self {
        Self {
            online_status,
            conversation_online_index,
            device_bindings,
            publisher,
            conversation_ping_coalescer: None,
            notify_policy: None,
            message_lookup: None,
            recent_conversations: None,
            metrics: None,
            notify_gate: None,
            notify_waves: None,
            audience_cache: waves::audience_cache(),
            offline_fanout_member_cap: std::env::var("PUSH_SERVER_OFFLINE_FANOUT_MEMBER_CAP")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|cap: &usize| *cap > 0)
                .unwrap_or(DEFAULT_OFFLINE_FANOUT_MEMBER_CAP),
            trace: PushTraceRecorder::disabled(),
        }
    }

    /// 接入免打扰读取。不接线时行为与接入前完全一致（不过滤任何推送）。
    pub fn with_notify_policy(mut self, policy: Arc<dyn NotifyPolicyRepository>) -> Self {
        self.notify_policy = Some(policy);
        self
    }

    /// 接入补提示的消息读取。不接线时补提示用通用文案。
    pub fn with_message_lookup(mut self, lookup: Arc<dyn MessageLookupRepository>) -> Self {
        self.message_lookup = Some(lookup);
        self
    }

    /// 接入「有新未读的会话」读取，供网关失联的交接找出要补提示的会话。
    pub fn with_recent_conversations(
        mut self,
        reader: Arc<dyn RecentConversationRepository>,
    ) -> Self {
        self.recent_conversations = Some(reader);
        self
    }

    /// 接入路由指标。
    pub fn with_metrics(mut self, metrics: Arc<PushServerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 接入防打扰闸门。
    pub fn with_notify_gate(mut self, gate: Arc<NotifyGate>) -> Self {
        self.notify_gate = Some(gate);
        self
    }

    /// 接入群会话的波次。它的到期条目由闸门的到期循环领取，所以要与闸门一起接。
    pub fn with_notify_waves(mut self, waves: Arc<NotifyWaves>) -> Self {
        self.notify_waves = Some(waves);
        self
    }

    pub fn with_conversation_ping_coalesce_window(mut self, window: Duration) -> Self {
        if !window.is_zero() {
            self.conversation_ping_coalescer =
                Some(Arc::new(ConversationPingCoalescer::new(window)));
        }
        self
    }

    fn gateway_options(
        options: &Option<flare_proto::common::PushOptions>,
        device_ids: &[String],
    ) -> Option<access_gateway::PushOptions> {
        if options.is_none() && device_ids.is_empty() {
            return None;
        }

        let common = options.as_ref();
        Some(access_gateway::PushOptions {
            require_ack: common.map(|o| o.require_ack).unwrap_or(false),
            priority: common.map(|o| o.priority).unwrap_or(5),
            expire_at_ms: common.and_then(|o| o.expire_at).unwrap_or_default(),
            offline_push: false,
            device_ids: device_ids.to_vec(),
            platforms: Vec::new(),
            delivery_mode: 0,
            allow_duplicate: false,
            enforce_order: false,
            shard_key: String::new(),
        })
    }

    fn ack_from_payload(envelope_id: &str, payload: AckPayload) -> Ack {
        let mut metadata = HashMap::new();
        metadata.insert("conversation_id".to_string(), payload.conversation_id);
        metadata.insert("ack_type".to_string(), payload.ack_type);

        Ack {
            ack_id: Some(envelope_id.to_string()),
            ack_at: Some(payload.ack_at),
            payload: Some(ack::Payload::Push(PushAck {
                window_id: payload.message_id,
                ack_seq: payload.seq,
                ack_id: Some(envelope_id.to_string()),
                ack_at: Some(payload.ack_at),
                device_id: None,
                attributes: metadata,
            })),
        }
    }

    fn notification_from_payload(payload: NotificationPayload) -> NotificationMessage {
        let mut attributes = payload.attributes;
        if !payload.icon.is_empty() {
            attributes.insert("icon".to_string(), payload.icon);
        }
        if !payload.sound.is_empty() {
            attributes.insert("sound".to_string(), payload.sound);
        }
        if !payload.click_action.is_empty() {
            attributes.insert("click_action".to_string(), payload.click_action);
        }

        NotificationMessage {
            kind: NotificationKind::Custom as i32,
            notification_id: payload.notification_id,
            title: payload.title,
            body: payload.body,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes,
            action: None,
            payload: None,
        }
    }

    fn notification_from_system(payload: SystemPayload) -> NotificationMessage {
        NotificationMessage {
            kind: NotificationKind::System as i32,
            notification_id: format!("system-{}", uuid::Uuid::new_v4()),
            title: payload.title,
            body: payload.content,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes: payload.attributes,
            action: None,
            payload: Some(notification_message::Payload::System(Default::default())),
        }
    }

    fn custom_from_payload(payload: CustomPayload) -> CustomData {
        CustomData {
            r#type: payload.custom_type,
            payload: payload.custom_data,
            attributes: payload.attributes,
        }
    }

    fn push_task_payload(
        envelope: &PushEnvelope,
        target_user_ids: Vec<String>,
    ) -> Result<(PushTaskPayloadKind, Vec<u8>, String)> {
        let options = Self::gateway_options(&envelope.options, &envelope.target_device_ids);
        let payload = envelope.payload.clone().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "push envelope payload missing")
                .build_error()
        })?;

        match payload {
            push_envelope::Payload::Ack(ack) => {
                let req = access_gateway::PushAckRequest {
                    user_ids: target_user_ids,
                    ack: Some(Self::ack_from_payload(&envelope.envelope_id, ack)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Ack,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
            push_envelope::Payload::Notification(notification) => {
                let message_id = if notification.notification_id.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    notification.notification_id.clone()
                };
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_payload(notification)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    message_id,
                ))
            }
            push_envelope::Payload::Custom(custom) => {
                let message_id = if custom.custom_type.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    format!("{}:{}", envelope.envelope_id, custom.custom_type)
                };
                let req = access_gateway::PushCustomRequest {
                    user_ids: target_user_ids,
                    custom_data: Some(Self::custom_from_payload(custom)),
                    options,
                };
                Ok((PushTaskPayloadKind::Custom, req.encode_to_vec(), message_id))
            }
            push_envelope::Payload::Capability(packet) => {
                // Online only: alert_facts gives this kind no alert class, so no offline
                // task is ever made, and the gateway writes it as DataPacket.capability.
                let message_id = packet
                    .correlation_id
                    .clone()
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| envelope.envelope_id.clone());
                let req = access_gateway::PushCapabilityRequest {
                    user_ids: target_user_ids,
                    packet: Some(packet),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Capability,
                    req.encode_to_vec(),
                    message_id,
                ))
            }
            push_envelope::Payload::System(system) => {
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_system(system)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
        }
    }

    async fn load_reachability(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        self.online_status
            .device_reachability(ctx, user_ids)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to query device reachability",
                )
            })
    }

    /// 取这批人在该会话里的通知偏好；只针对**即将收到离线推送**的那批人查询。
    ///
    /// 这是通知偏好唯一的查询入口。查询失败、没接线、载荷不属于任何会话时结果是
    /// `Unavailable`，调用方据此放行（fail-open）：通知偏好是偏好不是安全边界，
    /// 「该静音的响了一声」远好过「该到的推送没到」。
    async fn notify_preferences(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> PreferenceLookup {
        let Some(policy) = self.notify_policy.as_ref() else {
            return PreferenceLookup::Unavailable;
        };
        if conversation_id.trim().is_empty() {
            return PreferenceLookup::Unavailable;
        }
        if user_ids.is_empty() {
            return PreferenceLookup::Resolved(HashMap::new());
        }
        match policy.preferences_for(ctx, conversation_id, user_ids).await {
            Ok(prefs) => PreferenceLookup::Resolved(prefs),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %conversation_id,
                    "query notify preferences failed; sending offline push anyway"
                );
                PreferenceLookup::Unavailable
            }
        }
    }

    /// 每个收件人要推送的设备，按收件人顺序。没有可推设备的人不出现在结果里。
    ///
    /// 发送者本人不在其中：自己发的消息不该让自己的另一台设备响。
    ///
    /// 绑定读不到时返回空表，只告警不报错：在线任务不依赖绑定，不能因为绑定所在的 Redis
    /// 抖了一下就把整条记录判成失败、连在线投递一起丢进死信。这一次没推出去的提示，
    /// 设备回来后由同步补齐内容。
    ///
    /// 给了 `route` 时，每位接收者的每台设备在决策链里记一条 `route`（绑定读不到时不记）。
    async fn push_target_devices(
        &self,
        ctx: &flare_server_core::context::Ctx,
        tenant_id: &str,
        user_ids: &[String],
        reachability: &HashMap<String, UserReachability>,
        sender_id: &str,
        route: Option<&trace::RouteTrace<'_>>,
    ) -> Vec<(String, Vec<String>)> {
        let mut seen = HashSet::with_capacity(user_ids.len());
        let recipients: Vec<String> = user_ids
            .iter()
            .filter(|user_id| user_id.as_str() != sender_id && seen.insert(user_id.as_str()))
            .cloned()
            .collect();
        if recipients.is_empty() {
            return Vec::new();
        }
        let bound = match self
            .device_bindings
            .bound_devices(ctx, tenant_id, &recipients)
            .await
        {
            Ok(bound) => bound,
            Err(error) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_binding_lookup_failure();
                }
                tracing::warn!(
                    error = %error,
                    recipients = recipients.len(),
                    "device push bindings unavailable; skipping offline push for this record"
                );
                return Vec::new();
            }
        };
        recipients
            .into_iter()
            .filter_map(|user_id| {
                let bound_devices = bound.get(&user_id);
                if let Some(route) = route {
                    self.trace_route(
                        tenant_id,
                        route,
                        &user_id,
                        bound_devices.map_or(&[][..], |devices| &devices[..]),
                        reachability.get(&user_id),
                    );
                }
                let devices = devices_to_push(bound_devices?, reachability.get(&user_id));
                (!devices.is_empty()).then_some((user_id, devices))
            })
            .collect()
    }

    /// 按在线状态发在线任务，另给每个收件人不活跃的已绑定设备发离线任务。
    async fn publish_targeted_tasks(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
        reachability: &HashMap<String, UserReachability>,
        template: PushTaskTemplate<'_>,
        online_only: bool,
    ) -> Result<()> {
        self.publish_targeted_tasks_inner(
            ctx,
            user_ids,
            reachability,
            template,
            online_only,
            OnlineLeg::Connected,
            None,
        )
        .await
    }

    /// 在线与离线是两条互不相干的路：
    /// - 在线任务发给谁由 `online_leg` 与连接决定（见 [`online_task_recipients`]）。
    /// - 离线任务按设备发：每个收件人「有推送绑定、且不是活跃设备」的每台设备一条。
    ///   电脑在线的人，已托管的手机照样收到推送；连着、却在后台不能自己提示的手机同样当场
    ///   收到，不等送达确认超时再交接；没有任何绑定的人一条也没有。
    ///
    /// 免打扰、「只接收@我」、发送者本人这些过滤只作用于离线这一路。
    #[allow(clippy::too_many_arguments)]
    async fn publish_targeted_tasks_inner(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
        reachability: &HashMap<String, UserReachability>,
        template: PushTaskTemplate<'_>,
        online_only: bool,
        online_leg: OnlineLeg,
        // 已经取到的通知偏好。大群路径枚举成员时顺带拿到了，别再查一遍。
        prefetched_preferences: Option<&HashMap<String, NotifyPreference>>,
    ) -> Result<()> {
        let facts = alert_facts(template.payload_kind, template.push_payload);
        // 不提示的载荷只走在线一路。
        let online_only = online_only || !class_alerts(facts.class);

        // 在线一路先定下来。下面针对推送目标的过滤都碰不到这份名单。
        let online_user_ids = online_task_recipients(user_ids, reachability, online_leg);
        let trace_key = trace::alert_trace_key(&template, &facts);

        // 先算出哪些人有设备要推，再一次性查通知偏好——没有可推设备的人不查：
        // 他们的事件走长连接，提不提示由客户端决定。
        let offline_candidates: Vec<(String, Vec<String>)> = if online_only {
            Vec::new()
        } else {
            self.push_target_devices(
                ctx,
                template.tenant_id,
                user_ids,
                reachability,
                &facts.sender_id,
                Some(&trace::RouteTrace {
                    trace_key: &trace_key,
                    conversation_id: template.conversation_id,
                    path: trace::PATH_DIRECT,
                }),
            )
            .await
        };
        // 一次取回全部偏好：免打扰与「只接收@我」读的是同一批参与者行，
        // 分两次查会让每条消息的 RPC 直接翻倍。
        let preferences = match prefetched_preferences {
            Some(prefs) => PreferenceLookup::Resolved(prefs.clone()),
            None => {
                let candidate_ids: Vec<String> = offline_candidates
                    .iter()
                    .map(|(user_id, _)| user_id.clone())
                    .collect();
                self.notify_preferences(ctx, template.conversation_id, &candidate_ids)
                    .await
            }
        };

        let now_ms = now_millis();
        let mut offline_tasks = Vec::new();
        // 闸门接线时，过了下面这些过滤的接收者交给提示规则逐设备处理。
        let mut gated: Vec<alerting::GatedRecipient> = Vec::new();
        for (user_id, devices) in &offline_candidates {
            let preference = preferences.preference_of(user_id);
            if let Some(reason) =
                push_suppressed_by_preference(preference, facts.mentions.mentions(user_id))
            {
                tracing::debug!(
                    user_id = %user_id,
                    conversation_id = %template.conversation_id,
                    reason,
                    "Skipping offline push"
                );
                self.trace_suppressed(
                    template.tenant_id,
                    user_id,
                    &trace_key,
                    template.conversation_id,
                    devices,
                    recipient_alert_class(facts.class, facts.mentions.names(user_id)),
                    preference,
                );
                continue;
            }
            // 离线任务的载荷必须收敛成"只含本人"。
            //
            // 原本直接复制 template.push_payload，而该 payload 里带着全量收件人
            // 名单：10 万人的群就是 10 万份、每份各含 10 万个用户 ID 的副本。
            // 线上实测 flare:im:push:offline:outbox 里 12,902 条占了 1.18GB
            // （约 91KB/条），把 Redis 顶满后 volatile-lru 又淘汰不掉 stream，
            // 于是所有写入被拒（OOM command not allowed），整个 IM 瘫痪。
            //
            // 收件人就是本人，名单里留别人既无用又是纯开销。
            let push_payload = rewrite_push_payload_user_ids(
                template.payload_kind,
                template.push_payload,
                std::slice::from_ref(user_id),
            )?;
            let alert_class = recipient_alert_class(facts.class, facts.mentions.names(user_id));
            let lane = route_lane(
                template.conversation_id,
                facts.conversation_kind,
                alert_class,
            );
            if self.notify_gate.is_some() {
                gated.push(alerting::GatedRecipient {
                    user_id: user_id.clone(),
                    devices: devices.clone(),
                    push_payload,
                    alert_class,
                    mentioned: facts.mentions.mention_state(user_id),
                    preference,
                    times: 1,
                    lane,
                });
                continue;
            }
            let spec = OfflineTaskSpec {
                tenant_id: template.tenant_id,
                user_id: user_id.as_str(),
                conversation_id: template.conversation_id,
                message_id: template.message_id,
                alert_message_id: facts.message_id.as_str(),
                priority: template.priority,
                expire_at: template.expire_at,
                push_payload: push_payload.as_slice(),
                headers: template.headers,
                payload_kind: template.payload_kind,
                alert_class,
                conversation_seq: facts.conversation_seq,
            };
            // 每台设备一条：同一个人的手机和平板各自有各自的通知槽位与送达结果。
            for device_id in devices {
                let envelope = offline_task(&spec, device_id, now_ms);
                offline_tasks.push(PendingPushTask {
                    user_id: user_id.clone(),
                    trace: Some(trace::TaskTrace::of(
                        &envelope,
                        template.conversation_id,
                        &trace_key,
                    )),
                    payload: envelope.encode_to_vec(),
                    online: false,
                    disposition: disposition_of(spec.alert_class),
                    lane,
                });
            }
        }
        if !gated.is_empty() {
            offline_tasks.extend(
                self.gated_offline_tasks(ctx, &template, &facts, reachability, gated, now_ms)
                    .await,
            );
        }

        // 在线推送按 ONLINE_FANOUT_CHUNK 分片，不把整份在线名单塞进一条信封。
        //
        // 原实现是 `request.user_ids = online_user_ids` —— 一条消息带走全群在线
        // 用户 ID。10 万人的群单条就是 1.6MB；线上实测 FLARE_PUSH 里 12,503 条
        // 占了 1.07GB（平均 86KB/条），既把 NATS 的 1GB 配额顶穿，也让
        // push-server 反复 OOM。
        //
        // 分片后每条信封只带一批用户，单条大小与群规模脱钩。分区键仍用
        // conversation_id，同一条消息的各分片落在同一主题上，保持投递顺序。
        let online_chunks = online_user_ids.chunks(ONLINE_FANOUT_CHUNK).len();
        let mut tasks = Vec::with_capacity(offline_tasks.len() + online_chunks);
        for chunk in online_user_ids.chunks(ONLINE_FANOUT_CHUNK) {
            let push_payload =
                rewrite_push_payload_user_ids(template.payload_kind, template.push_payload, chunk)?;
            // 在线任务不填按设备的字段：它按会话或按用户广播，由网关落到各条连接上。
            let task = PushTaskEnvelope {
                user_id: String::new(),
                message_id: template.message_id.to_string(),
                conversation_id: template.conversation_id.to_string(),
                tenant_id: template.tenant_id.to_string(),
                priority: template.priority,
                expire_at: template.expire_at,
                push_payload,
                headers: template.headers.clone(),
                payload_kind: template.payload_kind,
                ..Default::default()
            };
            tasks.push(PendingPushTask {
                user_id: template.conversation_id.to_string(),
                payload: task.encode_to_vec(),
                online: true,
                disposition: PushDisposition::Unspecified,
                lane: OfflineLane::Direct,
                trace: None,
            });
        }
        tasks.extend(offline_tasks);
        self.publish_pending_tasks(ctx, tasks).await
    }

    /// 并发发出一批任务：在线任务进在线主题，离线任务进各自优先级通道的主题。任何一条失败整批报错
    /// （已经发出的那些靠幂等键在重投时去重）。
    async fn publish_pending_tasks(
        &self,
        ctx: &flare_server_core::context::Ctx,
        tasks: Vec<PendingPushTask>,
    ) -> Result<()> {
        let publisher = self.publisher.clone();
        let metrics = self.metrics.clone();
        let results = stream::iter(tasks)
            .map(|task| {
                let publisher = publisher.clone();
                let metrics = metrics.clone();
                let traced = task
                    .trace
                    .clone()
                    .map(|trace| (trace, task.user_id.clone(), task.disposition, task.lane));
                async move {
                    if task.online {
                        publisher
                            .publish_online_task(ctx, Some(task.user_id.as_str()), task.payload)
                            .await
                            .map(|()| None)
                            .map_err(|e| {
                                map_infra_error(
                                    e,
                                    ErrorCode::ServiceUnavailable,
                                    "Failed to publish online push task",
                                )
                            })
                    } else {
                        publisher
                            .publish_offline_task(
                                ctx,
                                task.lane,
                                Some(task.user_id.as_str()),
                                task.payload,
                            )
                            .await
                            .map_err(|e| {
                                map_infra_error(
                                    e,
                                    ErrorCode::ServiceUnavailable,
                                    "Failed to publish offline push task",
                                )
                            })?;
                        if let Some(metrics) = &metrics {
                            metrics.record_offline_task(disposition_label(task.disposition));
                            metrics.record_offline_lane(task.lane.label());
                        }
                        // 决策链在收齐之后再记：并发的发布块里不借用 `self`，Future 才能跨线程。
                        Ok(traced)
                    }
                }
            })
            .buffer_unordered(PUSH_TASK_PUBLISH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut failure = None;
        for result in results {
            match result {
                Ok(Some((trace, user_id, disposition, lane))) => {
                    self.trace_task(&user_id, &trace, disposition, lane);
                }
                Ok(None) => {}
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// 整批事件都是已读回执时，只推在线、绝不生成离线任务。
    ///
    /// 离线用户拿到别人的已读回执毫无用处：它不该弹通知，读状态下次同步自然会带到。
    /// 但它走的是和普通消息一样的扇出，于是在大群里按成员数放大——
    /// 线上实测：一次登录客户端标记已读产生 11 条回执事件，在 10 万人的会话里
    /// 被展开成 25,613 条离线信封；push-server 随后满核空转，perf 显示时间全花在
    /// 反复 decode PushEventRequest 的 user_ids 重复字段上。
    ///
    /// 必须**整批**都是回执才成立：一批里混着真实消息事件时照常走离线推送，
    /// 否则会把该弹的通知一起吞掉。
    fn events_are_read_receipts_only(req: &access_gateway::PushEventRequest) -> bool {
        !req.events.is_empty()
            && req
                .events
                .iter()
                .all(|event| EventType::try_from(event.r#type) == Ok(EventType::EventReadReceipt))
    }

    fn pure_conversation_ping(req: &access_gateway::PushEventRequest) -> Option<(String, u64)> {
        if !req.events.is_empty() {
            return None;
        }
        if req.conversation_id.trim().is_empty() || req.max_conversation_seq == 0 {
            return None;
        }
        let mode = EventEnvelopeDeliveryMode::try_from(req.delivery_mode).ok()?;
        matches!(mode, EventEnvelopeDeliveryMode::Ping)
            .then(|| (req.conversation_id.clone(), req.max_conversation_seq))
    }

    fn event_conversation_id(req: &access_gateway::PushEventRequest) -> String {
        if !req.conversation_id.trim().is_empty() {
            return req.conversation_id.clone();
        }
        req.events
            .first()
            .map(|event| event.conversation_id.clone())
            .unwrap_or_default()
    }

    fn event_message_id(req: &access_gateway::PushEventRequest) -> String {
        if let Some(event) = req.events.first() {
            return if event.event_id.is_empty() {
                format!("event-{}", uuid::Uuid::new_v4())
            } else {
                event.event_id.clone()
            };
        }
        if let Some((conversation_id, max_seq)) = Self::pure_conversation_ping(req) {
            return format!("conversation-ping:{conversation_id}:{max_seq}");
        }
        format!("event-{}", uuid::Uuid::new_v4())
    }

    async fn publish_event_to_user_batch(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: &access_gateway::PushEventRequest,
        user_ids: &[String],
        online_only: bool,
    ) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let conversation_id = Self::event_conversation_id(req);
        let message_id = Self::event_message_id(req);

        let mut push_req = req.clone();
        push_req.user_ids = user_ids.to_vec();
        let push_payload = push_req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.request_tenant_id(ctx)?.to_string();
        // 设备可达状态必须查：把所有收件人的设备都当成活跃会让 `publish_targeted_tasks`
        // 一个离线任务都建不出来——群消息走的正是这条内联事件路径，于是不在线的群成员
        // 收不到任何推送通知，只能等下次自己打开 app 拉。
        //
        // 但**在线投递不能因此收窄**：内联事件本身就是实时投递的载体，只按在线状态
        // 二选一的话，任何被误判成离线的连接（在线索引有滞后）都会当场丢掉实时消息。
        // 所以下面用 `OnlineLeg::Everyone`：所有人照发在线任务（没连上的天然丢弃），
        // 不活跃的已绑定设备**另外**再补离线推送任务。宁可多一条推送，不能少一条消息。
        //
        // 只推在线的载荷（纯 ping、已读回执）用不到可达状态，不为它多读一次。
        let reachability = if online_only {
            HashMap::new()
        } else {
            self.load_reachability(ctx, user_ids).await?
        };
        self.publish_targeted_tasks_inner(
            ctx,
            user_ids,
            &reachability,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Event as i32,
            },
            online_only,
            // 内联事件：在线照发、离线补推，两者不互斥
            OnlineLeg::Everyone,
            None,
        )
        .await
    }

    async fn publish_event_to_online_index_batch(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: &access_gateway::PushEventRequest,
        user_ids: &[String],
    ) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let conversation_id = Self::event_conversation_id(req);
        let message_id = Self::event_message_id(req);

        let mut push_req = req.clone();
        push_req.user_ids = user_ids.to_vec();
        let push_payload = push_req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.request_tenant_id(ctx)?.to_string();
        // 名单本身来自会话在线索引：全员发在线任务，只推在线，用不到设备可达状态。
        self.publish_targeted_tasks_inner(
            ctx,
            user_ids,
            &HashMap::new(),
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Event as i32,
            },
            true,
            OnlineLeg::Everyone,
            None,
        )
        .await
    }

    /// 统一读扩散：成员制会话内联事件（ingest 不物化收件人 → user_ids 空）→ 发**一个**在线任务按会话广播。
    /// push-worker 解码后经 `broadcast_deliver_to_conversation` 扇给所有网关节点，各节点用本地会话订阅表
    /// 过滤投递（O(在线/节点)，与群人数无关）。离线成员靠 conversation 版本号增量拉兜底（orchestrator 已 bump）。
    async fn publish_event_broadcast_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
        conversation_id: String,
    ) -> Result<()> {
        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let message_id = Self::event_message_id(&req);
        let tenant_id = self.request_tenant_id(ctx)?.to_string();

        // user_ids 在读扩散下不再使用（网关按会话订阅投递）；保持为空避免误导下游。
        let push_payload = req.encode_to_vec();
        let message_id_for_offline = message_id.clone();
        let push_payload_for_offline = push_payload.clone();
        let task = PushTaskEnvelope {
            user_id: String::new(),
            message_id,
            conversation_id: conversation_id.clone(),
            tenant_id,
            priority,
            expire_at,
            push_payload,
            headers: HashMap::new(),
            payload_kind: PushTaskPayloadKind::Event as i32,
            ..Default::default()
        };
        self.publisher
            .publish_online_task(ctx, Some(&conversation_id), task.encode_to_vec())
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to publish conversation broadcast push task",
                )
            })?;

        // 上面的广播只到连着的设备（网关按会话订阅投递）。不活跃的设备靠离线推送：
        // 被点名的人直接推，其余进会话的波次，按会话限频枚举接收者。
        self.conversation_offline_alerts(
            ctx,
            &conversation_id,
            OfflineFanoutTemplate {
                message_id: &message_id_for_offline,
                priority,
                expire_at,
                push_payload: &push_payload_for_offline,
            },
        )
        .await;
        Ok(())
    }

    async fn handle_conversation_ping_without_recipients(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
        conversation_id: String,
    ) -> Result<()> {
        let online_user_ids = self
            .conversation_online_index
            .online_user_ids(ctx, &conversation_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to query conversation online index",
                )
            })?;
        if online_user_ids.is_empty() {
            tracing::trace!(
                conversation_id = %conversation_id,
                "Recipient-less conversation ping has no online users"
            );
            return Ok(());
        }
        self.publish_event_to_online_index_batch(ctx, &req, &online_user_ids)
            .await?;

        tracing::trace!(
            conversation_id = %conversation_id,
            online_user_count = online_user_ids.len(),
            "Resolved recipient-less conversation ping through online index"
        );
        Ok(())
    }

    async fn handle_coalesced_conversation_ping_without_recipients(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
        conversation_id: String,
        max_conversation_seq: u64,
    ) -> Result<()> {
        let Some(coalescer) = &self.conversation_ping_coalescer else {
            return self
                .handle_conversation_ping_without_recipients(ctx, req, conversation_id)
                .await;
        };

        let tenant_id = self.request_tenant_id(ctx)?.to_string();
        let key = ConversationPingCoalesceKey::new(tenant_id, conversation_id.clone());
        let pending =
            pending_conversation_ping_request(&req, &conversation_id, max_conversation_seq);

        match coalescer.observe(key.clone(), pending).await {
            ConversationPingCoalesceDecision::SendNow => {
                self.handle_conversation_ping_without_recipients(ctx, req, conversation_id)
                    .await
            }
            ConversationPingCoalesceDecision::ScheduleAfter(delay) => {
                self.spawn_coalesced_conversation_ping(ctx.clone(), coalescer.clone(), key, delay);
                Ok(())
            }
            ConversationPingCoalesceDecision::Suppressed => {
                tracing::trace!(
                    conversation_id = %conversation_id,
                    max_conversation_seq,
                    "Suppressed duplicate recipient-less conversation ping inside coalesce window"
                );
                Ok(())
            }
        }
    }

    fn spawn_coalesced_conversation_ping(
        &self,
        ctx: flare_server_core::context::Ctx,
        coalescer: Arc<ConversationPingCoalescer>,
        key: ConversationPingCoalesceKey,
        delay: Duration,
    ) {
        let handler = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let Some(pending) = coalescer.take_pending(&key).await else {
                return;
            };
            let Some((conversation_id, max_conversation_seq)) =
                PushRouterHandler::pure_conversation_ping(&pending)
            else {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    conversation_id = %key.conversation_id,
                    "Coalesced conversation ping lost pure-ping contract"
                );
                return;
            };

            if let Err(error) = handler
                .handle_conversation_ping_without_recipients(&ctx, pending, conversation_id.clone())
                .await
            {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    conversation_id = %conversation_id,
                    max_conversation_seq,
                    error = %error,
                    "Failed to dispatch trailing coalesced conversation ping"
                );
            }
        });
    }

    // A broker header is not authenticated, but a missing or noncanonical tenant
    // must never silently become the deployment default before provenance exists.
    fn request_tenant_id<'a>(&self, ctx: &'a flare_server_core::context::Ctx) -> Result<&'a str> {
        ctx.tenant_id()
            .filter(|tenant_id| {
                flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
                    && flare_im_contracts::utils::normalize_tenant_id(tenant_id) == *tenant_id
            })
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "canonical tenant is required for push task routing",
                )
                .build_error()
            })
    }

    pub async fn handle_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushMessageRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let message = req.messages.first().cloned().unwrap_or_default();
        let conversation_id = message.conversation_id.clone();
        let message_id = message.server_id.clone();
        let priority = 5;
        let expire_at = None;

        let push_payload = req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.request_tenant_id(ctx)?.to_string();
        let reachability = self.load_reachability(ctx, &req.user_ids).await?;
        self.publish_targeted_tasks(
            ctx,
            &req.user_ids,
            &reachability,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Message as i32,
            },
            false,
        )
        .await?;

        Ok(())
    }

    pub async fn handle_event(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
    ) -> Result<()> {
        // 已读与撤回是对已发出通知的修正，只在闸门接线时做。修正失败只记日志：
        // 它们不影响投递，事件本身照常路由。
        let corrections = self
            .notify_gate
            .is_some()
            .then(|| alerting::EventCorrections::of(&req))
            .filter(|corrections| !corrections.is_empty());
        let routed = self.route_event(ctx, req).await;
        if let Some(corrections) = corrections {
            self.apply_event_corrections(ctx, corrections).await;
        }
        routed
    }

    async fn route_event(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            if let Some((conversation_id, max_conversation_seq)) =
                Self::pure_conversation_ping(&req)
            {
                return self
                    .handle_coalesced_conversation_ping_without_recipients(
                        ctx,
                        req,
                        conversation_id,
                        max_conversation_seq,
                    )
                    .await;
            }
            // 统一读扩散：成员制会话内联事件（无收件人物化）→ 按会话广播，不再丢弃。
            if !req.events.is_empty() {
                let conversation_id = Self::event_conversation_id(&req);
                if !conversation_id.trim().is_empty() {
                    return self
                        .publish_event_broadcast_task(ctx, req, conversation_id)
                        .await;
                }
                tracing::warn!(
                    "handle_event: recipient-less inline event has empty conversation_id; dropping"
                );
            }
            return Ok(());
        }

        let online_only = Self::pure_conversation_ping(&req).is_some()
            || Self::events_are_read_receipts_only(&req);
        self.publish_event_to_user_batch(ctx, &req, &req.user_ids, online_only)
            .await
    }

    /// 处理统一推送信封
    ///
    /// ## 设计
    /// - 统一处理 ACK、通知、CustomData、系统消息
    /// - 支持全量推送、用户列表推送、设备列表推送
    pub async fn handle_push_envelope(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: PushEnvelope,
    ) -> Result<()> {
        let tenant_id = self.request_tenant_id(ctx)?;
        if envelope.tenant_id != tenant_id {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "push envelope tenant does not match routing context",
            )
            .build_error());
        }
        // 提取推送选项
        let priority = envelope.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = envelope.options.as_ref().and_then(|o| o.expire_at);

        // 根据目标类型处理
        let target_user_ids = match flare_proto::PushTargetType::try_from(envelope.target_type) {
            Ok(flare_proto::PushTargetType::All) => return unsupported_push_target("all"),
            Ok(flare_proto::PushTargetType::Users) => envelope.target_user_ids.clone(),
            Ok(flare_proto::PushTargetType::Devices) => return unsupported_push_target("devices"),
            Ok(flare_proto::PushTargetType::Unspecified) | Err(_) => {
                return unsupported_push_target("unspecified");
            }
        };

        if target_user_ids.is_empty() {
            return Ok(());
        }

        let (payload_kind, push_payload, message_id) =
            Self::push_task_payload(&envelope, target_user_ids.clone())?;
        let conversation_id = String::new();
        let template = PushTaskTemplate {
            message_id: &message_id,
            conversation_id: &conversation_id,
            tenant_id,
            priority,
            expire_at,
            push_payload: &push_payload,
            headers: &envelope.headers,
            payload_kind: payload_kind as i32,
        };

        // A plugin realtime packet skips the online index: every gateway writes it to the
        // recipients' own connections or drops it, so an index that lags behind a fresh connection
        // cannot lose it, and the lookup is not on the latency path. No offline task is made.
        if payload_kind == PushTaskPayloadKind::Capability {
            return self
                .publish_targeted_tasks_inner(
                    ctx,
                    &target_user_ids,
                    &HashMap::new(),
                    template,
                    true,
                    OnlineLeg::Everyone,
                    None,
                )
                .await;
        }

        let reachability = self.load_reachability(ctx, &target_user_ids).await?;
        self.publish_targeted_tasks(ctx, &target_user_ids, &reachability, template, false)
            .await?;

        Ok(())
    }
}

/// 设备交接的补提示。它是同一个路由器的另一个入口：复用上面唯一的通知偏好查询与离线任务
/// 的构造，放在子模块里只是为了不让这个文件继续变长。
mod handoff;

/// 防打扰：候选设备逐台过提示规则与闸门，已读与撤回的修正，到期的尾部刷新与推迟提示。
mod alerting;

/// 群会话的离线提示：热会话波次、大群订阅者、@所有人 配额，点名的成员直接推。
mod waves;

/// 推送决策链：各环节在做出结论的地方记一条。
mod trace;

fn unsupported_push_target<T>(target: &str) -> Result<T> {
    Err(ErrorBuilder::new(
        ErrorCode::InvalidParameter,
        format!("push target type `{target}` is not supported"),
    )
    .build_error())
}

#[cfg(test)]
mod coalescer_gc_tests {
    use super::*;

    fn key(cid: &str) -> ConversationPingCoalesceKey {
        ConversationPingCoalesceKey::new("0", cid)
    }

    fn ping() -> access_gateway::PushEventRequest {
        access_gateway::PushEventRequest::default()
    }

    /// 条目表只增不减的话，每个出现过的会话都会永久占一条。
    #[tokio::test]
    async fn idle_entries_are_swept_instead_of_accumulating_forever() {
        let mut c = ConversationPingCoalescer::new(Duration::from_millis(1));
        c.sweep_interval = Duration::ZERO; // 每次 observe 都清理，免去测试里的等待

        for i in 0..50 {
            c.observe(key(&format!("2A{i:016}")), ping()).await;
        }
        // 让所有条目越过窗口变为空闲
        tokio::time::sleep(Duration::from_millis(5)).await;
        // 再来一次 observe 触发清理
        c.observe(key("2Atrigger"), ping()).await;

        let remaining = c.state.lock().await.entries.len();
        assert!(remaining <= 2, "空闲条目应被清理，实际仍留 {remaining} 条");
    }

    /// 清理的正当性全靠这条：空闲条目与缺失键的决策必须一致。
    /// 若两者不同，清理就是在改变行为，而不只是回收内存。
    #[tokio::test]
    async fn sweeping_does_not_change_the_decision() {
        let window = Duration::from_millis(1);

        // 不清理：条目留着但已过窗口
        let mut kept = ConversationPingCoalescer::new(window);
        kept.sweep_interval = Duration::from_secs(3600);
        kept.observe(key("2Asame"), ping()).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let with_stale_entry = kept.observe(key("2Asame"), ping()).await;

        // 清理：同样的时序，但条目已被回收
        let mut swept = ConversationPingCoalescer::new(window);
        swept.sweep_interval = Duration::ZERO;
        swept.observe(key("2Asame"), ping()).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let after_sweep = swept.observe(key("2Asame"), ping()).await;

        assert!(matches!(
            with_stale_entry,
            ConversationPingCoalesceDecision::SendNow
        ));
        assert!(matches!(
            after_sweep,
            ConversationPingCoalesceDecision::SendNow
        ));
    }

    /// 挂起中的 ping 一旦被清理就永远发不出去了。
    ///
    /// 危险窗口很窄但真实存在：pending 只在 elapsed < window 时写入，延时任务
    /// 要等窗口走完才来 take_pending。这中间条目已经"过期"却仍挂着 ping——
    /// 清理谓词若只看时间不看 pending，就会在这一刻把通知丢掉。
    /// 测试必须把条目摆进这个状态，否则只是在验证一个不会发生的场景。
    #[tokio::test]
    async fn entries_with_a_queued_ping_survive_the_sweep() {
        let window = Duration::from_millis(50);
        let mut c = ConversationPingCoalescer::new(window);
        c.sweep_interval = Duration::ZERO;

        let k = key("2Apending");
        c.observe(k.clone(), ping()).await; // 建条目
        let decision = c.observe(k.clone(), ping()).await; // 窗口内 → 挂起
        assert!(matches!(
            decision,
            ConversationPingCoalesceDecision::ScheduleAfter(_)
        ));

        // 越过窗口：条目此刻既"过期"又挂着 ping，正是延时任务尚未取走的那一瞬
        tokio::time::sleep(window * 2).await;
        assert!(
            c.state.lock().await.entries[&k].pending.is_some(),
            "前置条件不成立：条目没挂住 ping，后面的断言就测不到东西"
        );

        c.observe(key("2Aother"), ping()).await; // 触发清理

        assert!(
            c.take_pending(&k).await.is_some(),
            "挂起的 ping 被清理掉了，这条通知将永久丢失"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::PushTaskEnvelope;
    use flare_proto::common::EventEnvelopeDeliveryMode;
    use flare_server_core::Context;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// 测试里每个用户默认绑定的那台设备。
    pub(super) fn phone(user_id: &str) -> String {
        format!("{user_id}-phone")
    }

    /// 「在线 / 离线」表换成设备可达视图：在线的人有会话，他默认的那台设备（见 `phone`）
    /// 是活跃的；离线的人没有任何会话。
    fn reachability_of(statuses: &HashMap<String, bool>) -> HashMap<String, UserReachability> {
        statuses
            .iter()
            .filter(|(_, online)| **online)
            .map(|(user_id, _)| {
                let mut reach = UserReachability::offline();
                reach.record_session(&phone(user_id), false);
                (user_id.clone(), reach)
            })
            .collect()
    }

    pub(super) struct MockOnlineStatusReader {
        pub(super) statuses: HashMap<String, bool>,
        pub(super) tenant_id: String,
    }

    #[async_trait]
    impl OnlineStatusReader for MockOnlineStatusReader {
        async fn device_reachability(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            user_ids: &[String],
        ) -> Result<HashMap<String, UserReachability>> {
            let mut reachability = reachability_of(&self.statuses);
            reachability.retain(|user_id, _| user_ids.contains(user_id));
            Ok(reachability)
        }

        fn default_tenant_id(&self) -> &str {
            &self.tenant_id
        }
    }

    /// 逐设备给出可达视图的在线来源，用于「电脑在线、手机已托管」这类按设备才分得清的用例。
    /// `fail` 模拟在线状态查不到。
    #[derive(Default)]
    struct DeviceReachability {
        users: HashMap<String, UserReachability>,
        fail: bool,
        reads: AtomicUsize,
    }

    impl DeviceReachability {
        /// 每个用户：(用户, 活跃设备, 连接还在但提示归推送通道的设备)，设备之间用空格分隔。
        /// 第三栏是已托管或在后台不能自己提示的设备。
        fn of(users: &[(&str, &str, &str)]) -> Self {
            Self {
                users: users
                    .iter()
                    .map(|(user_id, live, push_owned)| {
                        let mut reach = UserReachability::offline();
                        for device_id in live.split_whitespace() {
                            reach.record_session(device_id, false);
                        }
                        for device_id in push_owned.split_whitespace() {
                            reach.record_session(device_id, true);
                        }
                        (user_id.to_string(), reach)
                    })
                    .collect(),
                ..Default::default()
            }
        }
    }

    #[async_trait]
    impl OnlineStatusReader for DeviceReachability {
        async fn device_reachability(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            user_ids: &[String],
        ) -> Result<HashMap<String, UserReachability>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(flare_server_core::error::FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "online unavailable",
                ));
            }
            Ok(self
                .users
                .iter()
                .filter(|(user_id, _)| user_ids.contains(user_id))
                .map(|(user_id, reach)| (user_id.clone(), reach.clone()))
                .collect())
        }

        fn default_tenant_id(&self) -> &str {
            "tenant-a"
        }
    }

    /// 可控的设备绑定。`devices` 为 `None` 时每个用户都绑定了一台设备（见 `phone`）；
    /// `fail` 模拟绑定所在的 Redis 不可用。
    #[derive(Default)]
    pub(super) struct MockDeviceBindings {
        pub(super) devices: Option<HashMap<String, Vec<String>>>,
        pub(super) fail: bool,
        pub(super) batch_reads: AtomicUsize,
        pub(super) fresh_reads: AtomicUsize,
    }

    impl MockDeviceBindings {
        /// 只有列出的用户有绑定，各自绑定列出的设备（用空格分隔）。
        pub(super) fn of(devices: &[(&str, &str)]) -> Self {
            Self {
                devices: Some(
                    devices
                        .iter()
                        .map(|(user_id, devices)| {
                            (
                                user_id.to_string(),
                                devices.split_whitespace().map(str::to_string).collect(),
                            )
                        })
                        .collect(),
                ),
                ..Default::default()
            }
        }

        fn bound(&self, user_id: &str) -> BoundDevices {
            match &self.devices {
                Some(devices) => devices.get(user_id).cloned().unwrap_or_default().into(),
                None => vec![phone(user_id)].into(),
            }
        }

        fn unavailable() -> flare_server_core::error::FlareError {
            flare_server_core::error::FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "device binding registry unavailable",
            )
        }
    }

    #[async_trait]
    impl DeviceBindingRepository for MockDeviceBindings {
        async fn bound_devices(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _tenant_id: &str,
            user_ids: &[String],
        ) -> Result<HashMap<String, BoundDevices>> {
            self.batch_reads.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(Self::unavailable());
            }
            Ok(user_ids
                .iter()
                .map(|user_id| (user_id.clone(), self.bound(user_id)))
                .filter(|(_, devices)| !devices.is_empty())
                .collect())
        }

        async fn bound_devices_fresh(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _tenant_id: &str,
            user_id: &str,
        ) -> Result<BoundDevices> {
            self.fresh_reads.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(Self::unavailable());
            }
            Ok(self.bound(user_id))
        }
    }

    /// 每个用户都绑定了一台设备的路由器，其余依赖由用例给出。
    fn test_router(
        online_status: Arc<dyn OnlineStatusReader>,
        conversation_online_index: Arc<dyn ConversationOnlineIndexReader>,
        publisher: Arc<dyn PushTaskPublisher>,
    ) -> PushRouterHandler {
        PushRouterHandler::new(
            online_status,
            conversation_online_index,
            Arc::new(MockDeviceBindings::default()),
            publisher,
        )
    }

    #[test]
    fn missing_or_noncanonical_tenant_cannot_be_replaced_by_deployment_default() {
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::new(),
                tenant_id: "0".into(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(MockPushTaskPublisher::default()),
        );
        for tenant in [None, Some("default"), Some(" t1"), Some("t1:other")] {
            let mut ctx = Context::root();
            if let Some(tenant) = tenant {
                ctx = ctx.with_tenant_id(tenant);
            }
            let ctx = Arc::new(ctx);
            assert_eq!(
                handler.request_tenant_id(&ctx).unwrap_err().code(),
                Some(ErrorCode::PermissionDenied)
            );
        }
        let scoped = Arc::new(Context::root().with_tenant_id("t1"));
        assert_eq!(handler.request_tenant_id(&scoped).unwrap(), "t1");
    }

    #[tokio::test]
    async fn push_envelope_tenant_must_equal_routing_context() {
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::new(),
                tenant_id: "0".into(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(MockPushTaskPublisher::default()),
        );
        let ctx = Arc::new(Context::root().with_tenant_id("t1"));
        let envelope = PushEnvelope {
            tenant_id: "t2".into(),
            ..Default::default()
        };
        assert_eq!(
            handler
                .handle_push_envelope(&ctx, envelope)
                .await
                .unwrap_err()
                .code(),
            Some(ErrorCode::PermissionDenied)
        );
    }

    /// A plugin realtime packet becomes one online Capability task for every recipient, without
    /// an online index read, keeping the packet and its expiry; no offline task is ever made.
    #[tokio::test]
    async fn capability_envelopes_make_online_tasks_only() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::from([("u1".to_string(), true), ("u2".to_string(), false)]),
                tenant_id: "0".into(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        );
        let packet = flare_proto::common::CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: b"{}".to_vec(),
            correlation_id: Some("d-1".into()),
            ..Default::default()
        };
        let envelope = PushEnvelope {
            envelope_id: "env-1".into(),
            tenant_id: "t1".into(),
            target_type: flare_proto::PushTargetType::Users as i32,
            target_user_ids: vec!["u1".into(), "u2".into()],
            payload_kind: flare_proto::PushPayloadKind::Capability as i32,
            options: Some(flare_proto::common::PushOptions {
                priority: 9,
                expire_at: Some(1_730_000_060_000),
                ..Default::default()
            }),
            payload: Some(push_envelope::Payload::Capability(packet.clone())),
            ..Default::default()
        };
        let ctx = Arc::new(Context::root().with_tenant_id("t1"));
        handler.handle_push_envelope(&ctx, envelope).await.unwrap();

        assert!(publisher.offline.lock().unwrap().is_empty());
        let online = publisher.online.lock().unwrap();
        assert_eq!(online.len(), 1);
        let task = &online[0].1;
        assert_eq!(task.tenant_id, "t1");
        assert_eq!(task.message_id, "d-1");
        assert_eq!(task.payload_kind, PushTaskPayloadKind::Capability as i32);
        let request =
            access_gateway::PushCapabilityRequest::decode(task.push_payload.as_slice()).unwrap();
        assert_eq!(request.user_ids, vec!["u1", "u2"]);
        assert_eq!(request.packet, Some(packet));
        assert_eq!(
            request.options.map(|o| o.expire_at_ms),
            Some(1_730_000_060_000)
        );
    }

    #[derive(Default)]
    pub(super) struct MockPushTaskPublisher {
        pub(super) online: Mutex<Vec<(Option<String>, PushTaskEnvelope)>>,
        pub(super) offline: Mutex<Vec<(Option<String>, PushTaskEnvelope)>>,
        /// 每条离线任务走的优先级通道，与 `offline` 一一对应。
        pub(super) offline_lanes: Mutex<Vec<OfflineLane>>,
        /// 模拟 broker 不可用：离线任务发布失败。
        pub(super) fail_offline: bool,
    }

    impl MockPushTaskPublisher {
        /// 发出的离线任务，连同它们的通道。
        pub(super) fn offline_with_lanes(&self) -> Vec<(PushTaskEnvelope, OfflineLane)> {
            let offline = self.offline.lock().expect("offline task lock poisoned");
            let lanes = self
                .offline_lanes
                .lock()
                .expect("offline lane lock poisoned");
            offline
                .iter()
                .map(|(_, task)| task.clone())
                .zip(lanes.iter().copied())
                .collect()
        }
    }

    #[async_trait]
    impl PushTaskPublisher for MockPushTaskPublisher {
        async fn publish_online_task(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            key: Option<&str>,
            payload: Vec<u8>,
        ) -> Result<()> {
            let task = PushTaskEnvelope::decode(payload.as_slice())
                .expect("online task payload must decode");
            self.online
                .lock()
                .expect("online task lock poisoned")
                .push((key.map(ToString::to_string), task));
            Ok(())
        }

        async fn publish_offline_task(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            lane: OfflineLane,
            key: Option<&str>,
            payload: Vec<u8>,
        ) -> Result<()> {
            if self.fail_offline {
                return Err(flare_server_core::error::FlareError::system(
                    "mq publish failed",
                ));
            }
            let task = PushTaskEnvelope::decode(payload.as_slice())
                .expect("offline task payload must decode");
            // 两把锁按同一顺序拿，任务与通道一一对应。
            let mut offline = self.offline.lock().expect("offline task lock poisoned");
            let mut lanes = self
                .offline_lanes
                .lock()
                .expect("offline lane lock poisoned");
            offline.push((key.map(ToString::to_string), task));
            lanes.push(lane);
            Ok(())
        }
    }

    pub(super) struct MockConversationOnlineIndexReader {
        pub(super) user_ids: Vec<String>,
        pub(super) calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ConversationOnlineIndexReader for MockConversationOnlineIndexReader {
        async fn online_user_ids(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            conversation_id: &str,
        ) -> Result<Vec<String>> {
            assert_eq!(conversation_id, "conversation-large");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.user_ids.clone())
        }
    }

    /// 可控的免打扰来源：`muted` 是静音名单，`fail` 用来模拟查询不可用。
    #[derive(Default)]
    struct MockNotifyPolicy {
        muted: Vec<String>,
        mention_only: Vec<String>,
        /// 群路径用：`None` 模拟「超过成员上限」。
        participants: Option<HashMap<String, NotifyPreference>>,
        /// 超过成员上限时开启了「全部消息通知」的订阅者。
        subscribers: Vec<String>,
        fail: bool,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl NotifyPolicyRepository for MockNotifyPolicy {
        async fn preferences_for(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _conversation_id: &str,
            user_ids: &[String],
        ) -> std::result::Result<
            HashMap<String, NotifyPreference>,
            flare_server_core::error::FlareError,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(flare_server_core::error::FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "boom",
                ));
            }
            Ok(user_ids
                .iter()
                .map(|user_id| {
                    (
                        user_id.clone(),
                        NotifyPreference {
                            muted: self.muted.contains(user_id),
                            mention_only: self.mention_only.contains(user_id),
                        },
                    )
                })
                .collect())
        }

        async fn all_participants(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _conversation_id: &str,
            _cap: usize,
        ) -> std::result::Result<
            Option<HashMap<String, NotifyPreference>>,
            flare_server_core::error::FlareError,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.participants.clone())
        }

        async fn notification_subscribers(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _conversation_id: &str,
        ) -> std::result::Result<Vec<String>, flare_server_core::error::FlareError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.subscribers.clone())
        }

        async fn participant_page(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _conversation_id: &str,
            _cursor: &str,
            _limit: usize,
        ) -> std::result::Result<
            crate::domain::repository::ParticipantPage,
            flare_server_core::error::FlareError,
        > {
            Ok(crate::domain::repository::ParticipantPage::default())
        }
    }

    /// 造一个「一在线一离线」的场景，返回 (离线任务的用户列表, 在线任务条数, 策略查询次数)。
    async fn run_targeted_push(muted: Vec<String>, fail: bool) -> (Vec<String>, usize, usize) {
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::from([
                ("user-online".to_string(), true),
                ("user-offline".to_string(), false),
            ]),
            tenant_id: "tenant-a".to_string(),
        });
        let index = Arc::new(MockConversationOnlineIndexReader {
            user_ids: vec![],
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = test_router(online.clone(), index, publisher.clone()).with_notify_policy(
            Arc::new(MockNotifyPolicy {
                participants: None,
                mention_only: Vec::new(),
                muted,
                subscribers: Vec::new(),
                fail,
                calls: calls.clone(),
            }),
        );

        let ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        let user_ids = vec!["user-online".to_string(), "user-offline".to_string()];
        let statuses = HashMap::from([
            ("user-online".to_string(), true),
            ("user-offline".to_string(), false),
        ]);
        let headers = HashMap::new();
        handler
            .publish_targeted_tasks(
                &ctx,
                &user_ids,
                &reachability_of(&statuses),
                PushTaskTemplate {
                    message_id: "m-1",
                    conversation_id: "conversation-1",
                    tenant_id: "tenant-a",
                    priority: 5,
                    expire_at: None,
                    push_payload: &mobile_message_payload_fixture(),
                    headers: &headers,
                    payload_kind: PushTaskPayloadKind::Message as i32,
                },
                false,
            )
            .await
            .expect("publish");

        let offline_users: Vec<String> = publisher
            .offline
            .lock()
            .expect("offline lock")
            .iter()
            .map(|(_, task)| task.user_id.clone())
            .collect();
        let online_count = publisher.online.lock().expect("online lock").len();
        (offline_users, online_count, calls.load(Ordering::SeqCst))
    }

    /// 跑一次「群广播」，返回 (离线任务用户, 在线任务条数)。`members` 为 `None` 模拟超过成员上限，
    /// 这时只枚举 `subscribers`。
    async fn run_conversation_broadcast(
        members: Option<HashMap<String, NotifyPreference>>,
        subscribers: Vec<String>,
    ) -> (Vec<String>, usize) {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                // group-online 在线，group-offline 离线
                statuses: HashMap::from([("group-online".to_string(), true)]),
                tenant_id: "tenant-a".to_string(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: Vec::new(),
            mention_only: Vec::new(),
            participants: members,
            subscribers,
            fail: false,
            calls: Arc::new(AtomicUsize::new(0)),
        }));

        // 大群：读扩散下收件人为空，只带会话 id。
        let req = access_gateway::PushEventRequest {
            user_ids: Vec::new(),
            events: vec![flare_proto::common::Event {
                conversation_id: "conversation-large".to_string(),
                conversation_seq: 3,
                r#type: EventType::EventMessage as i32,
                payload: Some(flare_proto::common::event::Payload::Message(
                    flare_proto::common::Message {
                        conversation_id: "conversation-large".to_string(),
                        server_id: "m-3".to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }],
            conversation_id: "conversation-large".to_string(),
            max_conversation_seq: 3,
            delivery_mode: flare_proto::common::EventEnvelopeDeliveryMode::PingWithInline as i32,
            inline_events_truncated: false,
            options: None,
        };

        let ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        handler.handle_event(&ctx, req).await.expect("handle_event");
        let offline = publisher
            .offline
            .lock()
            .expect("offline lock")
            .iter()
            .map(|(_, task)| task.user_id.clone())
            .collect();
        let online = publisher.online.lock().expect("online lock").len();
        (offline, online)
    }

    /// 大群此前只发在线广播，离线成员一条推送都收不到。
    #[tokio::test]
    async fn all_push_routes_preserve_request_tenant_instead_of_deployment_default() {
        for route in ["message", "targeted_event", "online_index", "broadcast"] {
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = test_router(
                Arc::new(MockOnlineStatusReader {
                    statuses: HashMap::new(),
                    tenant_id: "0".to_string(),
                }),
                Arc::new(MockConversationOnlineIndexReader {
                    user_ids: vec!["same-user".to_string()],
                    calls: Arc::new(AtomicUsize::new(0)),
                }),
                publisher.clone(),
            )
            .with_notify_policy(Arc::new(MockNotifyPolicy {
                muted: vec![],
                mention_only: vec![],
                participants: Some(HashMap::from([(
                    "same-user".to_string(),
                    NotifyPreference::default(),
                )])),
                subscribers: Vec::new(),
                fail: false,
                calls: Arc::new(AtomicUsize::new(0)),
            }));
            for tenant in ["tenant-a", "tenant-b"] {
                let ctx = Arc::new(Context::root().with_tenant_id(tenant));
                let request = access_gateway::PushEventRequest {
                    conversation_id: "conversation-large".to_string(),
                    events: vec![flare_proto::common::Event {
                        conversation_id: "conversation-large".to_string(),
                        r#type: EventType::EventMessage as i32,
                        payload: Some(flare_proto::common::event::Payload::Message(
                            flare_proto::common::Message::default(),
                        )),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                match route {
                    "message" => handler
                        .handle_message(
                            &ctx,
                            access_gateway::PushMessageRequest {
                                user_ids: vec!["same-user".to_string()],
                                messages: vec![flare_proto::common::Message::default()],
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap(),
                    "targeted_event" => handler
                        .publish_event_to_user_batch(
                            &ctx,
                            &request,
                            &["same-user".to_string()],
                            false,
                        )
                        .await
                        .unwrap(),
                    "online_index" => handler
                        .publish_event_to_online_index_batch(
                            &ctx,
                            &request,
                            &["same-user".to_string()],
                        )
                        .await
                        .unwrap(),
                    "broadcast" => handler.handle_event(&ctx, request).await.unwrap(),
                    _ => unreachable!(),
                }
                let online = std::mem::take(&mut *publisher.online.lock().unwrap());
                let offline = std::mem::take(&mut *publisher.offline.lock().unwrap());
                assert!(!online.is_empty() || !offline.is_empty(), "{route}");
                if route == "broadcast" {
                    assert_eq!(online.len(), 1);
                    assert_eq!(offline.len(), 1, "offline fanout retains tenant too");
                }
                for (_, task) in online.into_iter().chain(offline) {
                    assert_eq!(task.tenant_id, tenant, "{route}");
                }
            }
        }
    }

    /// 大群此前只发在线广播，离线成员一条推送都收不到。
    #[tokio::test]
    async fn large_conversation_broadcast_also_pushes_offline_members() {
        let (offline, online) = run_conversation_broadcast(
            Some(HashMap::from([
                ("group-online".to_string(), NotifyPreference::default()),
                ("group-offline".to_string(), NotifyPreference::default()),
            ])),
            Vec::new(),
        )
        .await;
        assert_eq!(
            offline,
            vec!["group-offline".to_string()],
            "离线成员必须补一条离线推送任务，实际 {offline:?}"
        );
        // 在线仍走会话广播（一条），不给在线成员再发一份，否则会重复。
        assert_eq!(online, 1, "在线侧只应有那条会话广播任务");
    }

    /// 超过成员上限的群只推开启了「全部消息通知」的订阅者，不是整体跳过，也不是随机推一部分人。
    #[tokio::test]
    async fn an_oversized_conversation_pushes_its_subscribers() {
        let (offline, online) = run_conversation_broadcast(
            None,
            vec!["group-offline".to_string(), "group-online".to_string()],
        )
        .await;
        assert_eq!(
            offline,
            vec!["group-offline".to_string()],
            "订阅者里离线的那位收到推送，在线的不推，实际 {offline:?}"
        );
        assert_eq!(online, 1, "在线广播照常");

        let (offline, _) = run_conversation_broadcast(None, Vec::new()).await;
        assert!(
            offline.is_empty(),
            "没有订阅者就没有人要推，实际 {offline:?}"
        );
    }

    /// 「只接收@我」：没点名的消息不产生离线推送，点名的照常推。
    ///
    /// 注意这只影响**离线推送**——消息本身照常投递，时间线不会缺内容。
    async fn run_mention_only_push(
        text: &str,
        mentions: Vec<flare_proto::common::Mention>,
    ) -> Vec<String> {
        use flare_proto::common::{Message, MessageContent, TextContent, message_content::Content};

        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::from([("user-offline".to_string(), false)]),
                tenant_id: "tenant-a".to_string(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: Vec::new(),
            mention_only: vec!["user-offline".to_string()],
            participants: None,
            subscribers: Vec::new(),
            fail: false,
            calls: Arc::new(AtomicUsize::new(0)),
        }));

        let req = access_gateway::PushEventRequest {
            user_ids: vec!["user-offline".to_string()],
            events: vec![flare_proto::common::Event {
                conversation_id: "conversation-group".to_string(),
                conversation_seq: 9,
                r#type: EventType::EventMessage as i32,
                payload: Some(flare_proto::common::event::Payload::Message(Message {
                    conversation_id: "conversation-group".to_string(),
                    server_id: "m-9".to_string(),
                    content: Some(MessageContent {
                        content: Some(Content::Text(TextContent {
                            text: text.to_string(),
                            mentions,
                        })),
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            conversation_id: "conversation-group".to_string(),
            max_conversation_seq: 9,
            delivery_mode: flare_proto::common::EventEnvelopeDeliveryMode::PingWithInline as i32,
            inline_events_truncated: false,
            options: None,
        };

        let ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        handler.handle_event(&ctx, req).await.expect("handle_event");
        publisher
            .offline
            .lock()
            .expect("offline lock")
            .iter()
            .map(|(_, task)| task.user_id.clone())
            .collect()
    }

    /// 偏好查询**每条消息只做一次**。
    ///
    /// 免打扰与「只接收@我」读的是同一批参与者行；曾经分两个方法各查一次，
    /// 等于把大群的翻页 RPC 直接翻倍。这条用例钉住合并后的调用次数。
    #[tokio::test]
    async fn notify_preferences_are_fetched_once_per_push() {
        let calls = Arc::new(AtomicUsize::new(0));
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::from([("user-offline".to_string(), false)]),
                tenant_id: "tenant-a".to_string(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: Vec::new(),
            mention_only: Vec::new(),
            participants: None,
            subscribers: Vec::new(),
            fail: false,
            calls: calls.clone(),
        }));

        let ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        let headers = HashMap::new();
        handler
            .publish_targeted_tasks(
                &ctx,
                &["user-offline".to_string()],
                &HashMap::new(),
                PushTaskTemplate {
                    message_id: "m-1",
                    conversation_id: "conversation-1",
                    tenant_id: "tenant-a",
                    priority: 5,
                    expire_at: None,
                    push_payload: &mobile_message_payload_fixture(),
                    headers: &headers,
                    payload_kind: PushTaskPayloadKind::Message as i32,
                },
                false,
            )
            .await
            .expect("publish");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "一次推送只该查一次通知偏好，实际 {} 次",
            calls.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn mention_only_skips_offline_push_when_not_mentioned() {
        let offline = run_mention_only_push("大家早", Vec::new()).await;
        assert!(
            offline.is_empty(),
            "设了「只接收@我」的人，没点名的消息不该产生离线推送，实际 {offline:?}"
        );
    }

    #[tokio::test]
    async fn mention_only_still_pushes_when_mentioned() {
        let offline = run_mention_only_push(
            "@某人 看一下",
            vec![flare_proto::common::Mention {
                r#type: flare_proto::common::MentionType::User as i32,
                user_id: "user-offline".to_string(),
                ..Default::default()
            }],
        )
        .await;
        assert_eq!(
            offline,
            vec!["user-offline".to_string()],
            "点名了就必须推——这条用例防的是把「只接收@我」做成「什么都不接收」"
        );
    }

    #[tokio::test]
    async fn mention_all_counts_as_mentioning_everyone() {
        let offline = run_mention_only_push(
            "@所有人 通知",
            vec![flare_proto::common::Mention {
                r#type: flare_proto::common::MentionType::All as i32,
                ..Default::default()
            }],
        )
        .await;
        assert_eq!(offline, vec!["user-offline".to_string()]);
    }

    /// 群消息走「带收件人的内联事件」这条路。这里曾把所有收件人一律当成在线，
    /// 于是一个离线任务都建不出来——离线群成员收不到任何推送通知。
    /// 这条用例钉住：离线收件人必须产生离线任务。
    #[tokio::test]
    async fn inline_event_creates_offline_tasks_for_offline_recipients() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::from([
                    ("user-online".to_string(), true),
                    ("user-offline".to_string(), false),
                ]),
                tenant_id: "tenant-a".to_string(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        );

        let ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        let req = access_gateway::PushEventRequest {
            user_ids: vec!["user-online".to_string(), "user-offline".to_string()],
            events: vec![flare_proto::common::Event {
                conversation_id: "conversation-group".to_string(),
                conversation_seq: 7,
                r#type: EventType::EventMessage as i32,
                payload: Some(flare_proto::common::event::Payload::Message(
                    flare_proto::common::Message {
                        conversation_id: "conversation-group".to_string(),
                        server_id: "m-7".to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }],
            conversation_id: "conversation-group".to_string(),
            max_conversation_seq: 7,
            delivery_mode: flare_proto::common::EventEnvelopeDeliveryMode::PingWithInline as i32,
            inline_events_truncated: false,
            options: None,
        };

        handler.handle_event(&ctx, req).await.expect("handle_event");

        let offline_users: Vec<String> = publisher
            .offline
            .lock()
            .expect("offline lock")
            .iter()
            .map(|(_, task)| task.user_id.clone())
            .collect();
        assert_eq!(
            offline_users,
            vec!["user-offline".to_string()],
            "离线收件人必须产生离线推送任务，实际 {offline_users:?}"
        );
        assert_eq!(
            publisher.online.lock().expect("online lock").len(),
            1,
            "在线收件人仍走在线任务，不受影响"
        );
    }

    #[tokio::test]
    async fn muted_conversation_gets_no_offline_push() {
        let (offline_users, online_count, _) =
            run_targeted_push(vec!["user-offline".to_string()], false).await;
        assert!(
            offline_users.is_empty(),
            "设了免打扰的离线用户不该产生离线推送任务，实际 {offline_users:?}"
        );
        // 在线那一路完全不受影响：他们的事件走长连接，提不提示由客户端决定。
        assert_eq!(online_count, 1);
    }

    #[tokio::test]
    async fn sync_only_notice_ignores_mobile_mute_and_stale_online_index_without_offline_fanout() {
        use flare_proto::common::{
            Event, Message, MessageContent, NotificationContent, event::Payload,
            message_content::Content,
        };
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = test_router(
            Arc::new(MockOnlineStatusReader {
                statuses: HashMap::from([("recipient".into(), false)]),
                tenant_id: "tenant-a".into(),
            }),
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: vec![],
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: vec!["recipient".into()],
            mention_only: vec![],
            participants: None,
            subscribers: Vec::new(),
            fail: false,
            calls: calls.clone(),
        }));
        let req = access_gateway::PushEventRequest {
            user_ids: vec!["recipient".into()],
            events: vec![Event {
                r#type: EventType::EventMessage as i32,
                payload: Some(Payload::Message(Message {
                    content: Some(MessageContent {
                        content: Some(Content::Notification(NotificationContent::default())),
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            ..Default::default()
        };
        handler
            .handle_event(&Arc::new(Context::root().with_tenant_id("tenant-a")), req)
            .await
            .unwrap();
        assert_eq!(
            publisher.online.lock().unwrap().len(),
            1,
            "required client event still attempts the actual connection"
        );
        assert!(publisher.offline.lock().unwrap().is_empty());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no optional-notification directory lookup"
        );
    }

    #[tokio::test]
    async fn unmuted_user_still_gets_offline_push() {
        let (offline_users, _, _) = run_targeted_push(vec![], false).await;
        assert_eq!(offline_users, vec!["user-offline".to_string()]);
    }

    #[tokio::test]
    async fn mute_lookup_failure_still_sends_push() {
        // fail-open：免打扰是偏好不是安全边界，「该静音的响一声」远好过「该到的没到」。
        let (offline_users, _, _) = run_targeted_push(vec!["user-offline".to_string()], true).await;
        assert_eq!(offline_users, vec!["user-offline".to_string()]);
    }

    #[tokio::test]
    async fn online_only_push_never_queries_mute_settings() {
        // online_only 压根不会产生离线任务，不该为此多打一次查询。
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::from([("user-offline".to_string(), false)]),
            tenant_id: "tenant-a".to_string(),
        });
        let index = Arc::new(MockConversationOnlineIndexReader {
            user_ids: vec![],
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler =
            test_router(online, index, publisher).with_notify_policy(Arc::new(MockNotifyPolicy {
                participants: None,
                mention_only: Vec::new(),
                muted: vec!["user-offline".to_string()],
                subscribers: Vec::new(),
                fail: false,
                calls: calls.clone(),
            }));
        let headers = HashMap::new();
        handler
            .publish_targeted_tasks(
                &Arc::new(Context::root()),
                &["user-offline".to_string()],
                &HashMap::new(),
                PushTaskTemplate {
                    message_id: "m-1",
                    conversation_id: "conversation-1",
                    tenant_id: "tenant-a",
                    priority: 5,
                    expire_at: None,
                    push_payload: &mobile_message_payload_fixture(),
                    headers: &headers,
                    payload_kind: PushTaskPayloadKind::Message as i32,
                },
                true,
            )
            .await
            .expect("publish");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recipientless_conversation_ping_uses_online_index() {
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let online_index = Arc::new(MockConversationOnlineIndexReader {
            user_ids: vec!["online-a".to_string(), "online-c".to_string()],
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let handler = test_router(online, online_index.clone(), publisher.clone());
        let ctx: flare_server_core::context::Ctx = Arc::new(
            Context::with_request_id("req-recipientless-ping")
                .with_trace_id("trace-recipientless-ping")
                .with_tenant_id("tenant-a"),
        );

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 42,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("recipient-less ping should route");
        assert_eq!(online_index.calls.load(Ordering::SeqCst), 1);

        let online_tasks = publisher
            .online
            .lock()
            .expect("online task lock poisoned")
            .clone();
        let offline_tasks = publisher
            .offline
            .lock()
            .expect("offline task lock poisoned")
            .clone();
        assert_eq!(offline_tasks.len(), 0, "pure ping must skip offline tasks");
        assert_eq!(
            online_tasks
                .iter()
                .map(|(key, _)| key.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("conversation-large")]
        );

        for (_key, task) in online_tasks {
            assert_eq!(task.conversation_id, "conversation-large");
            assert_eq!(task.tenant_id, "tenant-a");
            let push = access_gateway::PushEventRequest::decode(task.push_payload.as_slice())
                .expect("push event request should decode");
            assert!(push.events.is_empty());
            assert_eq!(push.conversation_id, "conversation-large");
            assert_eq!(push.max_conversation_seq, 42);
            assert_eq!(push.delivery_mode, EventEnvelopeDeliveryMode::Ping as i32);
            assert!(push.inline_events_truncated);
            assert_eq!(
                push.user_ids,
                vec!["online-a".to_string(), "online-c".to_string()]
            );
        }
    }

    #[tokio::test]
    async fn recipientless_conversation_ping_coalesces_high_volume_watermarks_before_online_index_read()
     {
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let online_user_ids = vec![
            "member-000000".to_string(),
            "member-049999".to_string(),
            "member-099999".to_string(),
        ];
        let handler = test_router(
            online,
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: online_user_ids.clone(),
                calls: calls.clone(),
            }),
            publisher.clone(),
        )
        .with_conversation_ping_coalesce_window(Duration::from_millis(500));
        let ctx: flare_server_core::context::Ctx = Arc::new(
            Context::with_request_id("req-coalesced-ping")
                .with_trace_id("trace-coalesced-ping")
                .with_tenant_id("tenant-a"),
        );

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 41,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("first ping should route immediately");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            publisher
                .online
                .lock()
                .expect("online task lock poisoned")
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            vec![Some("conversation-large".to_string())],
            "first ping should publish one bulk task for users returned by the online index"
        );
        let online_tasks = publisher
            .online
            .lock()
            .expect("online task lock poisoned")
            .clone();
        let push =
            access_gateway::PushEventRequest::decode(online_tasks[0].1.push_payload.as_slice())
                .expect("bulk push event should decode");
        assert_eq!(push.user_ids, online_user_ids);

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 45,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("second ping should be coalesced");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "coalesced high-volume ping must not reread the online index immediately"
        );
        assert_eq!(
            publisher
                .online
                .lock()
                .expect("online task lock poisoned")
                .len(),
            1,
            "second ping should only update pending watermark inside coalesce window"
        );

        let key = ConversationPingCoalesceKey::new("tenant-a", "conversation-large");
        let pending = handler
            .conversation_ping_coalescer
            .as_ref()
            .expect("coalescer")
            .take_pending(&key)
            .await
            .expect("trailing ping should be pending");
        assert!(pending.events.is_empty());
        assert_eq!(pending.conversation_id, "conversation-large");
        assert_eq!(pending.max_conversation_seq, 45);
        assert_eq!(
            pending.delivery_mode,
            EventEnvelopeDeliveryMode::Ping as i32
        );
        assert!(pending.inline_events_truncated);
    }

    #[tokio::test]
    async fn unsupported_push_envelope_targets_fail_fast_without_publishing_tasks() {
        for target_type in [
            flare_proto::PushTargetType::All,
            flare_proto::PushTargetType::Devices,
            flare_proto::PushTargetType::Unspecified,
        ] {
            let online = Arc::new(MockOnlineStatusReader {
                statuses: HashMap::new(),
                tenant_id: "tenant-a".to_string(),
            });
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = test_router(
                online,
                Arc::new(MockConversationOnlineIndexReader {
                    user_ids: Vec::new(),
                    calls: Arc::new(AtomicUsize::new(0)),
                }),
                publisher.clone(),
            );
            let ctx: flare_server_core::context::Ctx = Arc::new(
                Context::with_request_id("req-unsupported-push-target")
                    .with_trace_id("trace-unsupported-push-target")
                    .with_tenant_id("tenant-a"),
            );

            let err = handler
                .handle_push_envelope(
                    &ctx,
                    PushEnvelope {
                        envelope_id: format!("envelope-{target_type:?}"),
                        tenant_id: "tenant-a".to_string(),
                        trace_id: "trace-unsupported-push-target".to_string(),
                        created_at: 1,
                        target_type: target_type as i32,
                        target_user_ids: Vec::new(),
                        target_device_ids: vec!["device-a".to_string()],
                        payload_kind: 0,
                        options: None,
                        payload: None,
                        headers: HashMap::new(),
                    },
                )
                .await
                .expect_err("unsupported push target must fail");
            assert!(
                err.to_string().contains("push target type"),
                "unexpected error: {err}"
            );
            assert!(
                publisher
                    .online
                    .lock()
                    .expect("online task lock poisoned")
                    .is_empty()
            );
            assert!(
                publisher
                    .offline
                    .lock()
                    .expect("offline task lock poisoned")
                    .is_empty()
            );
        }
    }

    // ---- 按设备判定在线、按设备发离线任务 ----

    pub(super) fn tenant_ctx() -> flare_server_core::context::Ctx {
        Arc::new(Context::root().with_tenant_id("tenant-a"))
    }

    /// 给定逐设备可达视图与绑定的路由器。
    fn device_router(
        reachability: Arc<DeviceReachability>,
        bindings: Arc<MockDeviceBindings>,
        publisher: Arc<MockPushTaskPublisher>,
    ) -> PushRouterHandler {
        PushRouterHandler::new(
            reachability,
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: Vec::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            bindings,
            publisher,
        )
    }

    pub(super) fn text_message(
        conversation_id: &str,
        server_id: &str,
        seq: u64,
        sender_id: &str,
    ) -> flare_proto::common::Message {
        use flare_proto::common::{MessageContent, TextContent, message_content::Content};
        flare_proto::common::Message {
            conversation_id: conversation_id.to_string(),
            server_id: server_id.to_string(),
            conversation_seq: seq,
            sender_id: sender_id.to_string(),
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "hi".to_string(),
                    mentions: Vec::new(),
                })),
            }),
            ..Default::default()
        }
    }

    pub(super) fn message_request(
        recipients: &[&str],
        message: flare_proto::common::Message,
    ) -> access_gateway::PushMessageRequest {
        access_gateway::PushMessageRequest {
            user_ids: recipients.iter().map(|user| user.to_string()).collect(),
            messages: vec![message],
            options: None,
        }
    }

    fn message_event_request(
        recipients: &[&str],
        message: flare_proto::common::Message,
    ) -> access_gateway::PushEventRequest {
        access_gateway::PushEventRequest {
            user_ids: recipients.iter().map(|user| user.to_string()).collect(),
            conversation_id: message.conversation_id.clone(),
            max_conversation_seq: message.conversation_seq,
            delivery_mode: EventEnvelopeDeliveryMode::PingWithInline as i32,
            inline_events_truncated: false,
            options: None,
            events: vec![flare_proto::common::Event {
                conversation_id: message.conversation_id.clone(),
                conversation_seq: message.conversation_seq,
                r#type: EventType::EventMessage as i32,
                event_id: format!("evt-{}", message.server_id),
                payload: Some(flare_proto::common::event::Payload::Message(message)),
                ..Default::default()
            }],
        }
    }

    /// 已发布的离线任务，按 (用户, 设备) 排序。
    pub(super) fn offline_tasks(publisher: &MockPushTaskPublisher) -> Vec<PushTaskEnvelope> {
        let mut tasks: Vec<PushTaskEnvelope> = publisher
            .offline
            .lock()
            .expect("offline lock")
            .iter()
            .map(|(_, task)| task.clone())
            .collect();
        tasks.sort_by(|left, right| {
            (&left.user_id, &left.device_id).cmp(&(&right.user_id, &right.device_id))
        });
        tasks
    }

    fn offline_targets(publisher: &MockPushTaskPublisher) -> Vec<(String, String)> {
        offline_tasks(publisher)
            .into_iter()
            .map(|task| (task.user_id, task.device_id))
            .collect()
    }

    /// 在线任务里的收件人，按发布顺序。
    fn online_recipients(publisher: &MockPushTaskPublisher) -> Vec<Vec<String>> {
        publisher
            .online
            .lock()
            .expect("online lock")
            .iter()
            .map(|(_, task)| {
                access_gateway::PushMessageRequest::decode(task.push_payload.as_slice())
                    .expect("online payload decodes")
                    .user_ids
            })
            .collect()
    }

    fn pair(user_id: &str, device_id: &str) -> (String, String) {
        (user_id.to_string(), device_id.to_string())
    }

    /// 这次改动要修的那个缺陷：电脑在线时，同一个人已托管的手机收不到离线推送。
    /// 在线任务照发给这个人（电脑要收），手机另外得到一条离线任务。
    #[tokio::test]
    async fn a_live_desktop_does_not_suppress_the_push_to_a_parked_phone() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::of(&[(
                "alice",
                "alice-desktop",
                "alice-phone",
            )])),
            // 电脑没有推送绑定，手机有。
            Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect("handle_message");

        assert_eq!(
            offline_targets(&publisher),
            vec![pair("alice", "alice-phone")],
            "已托管的手机必须收到离线任务，哪怕电脑在线"
        );
        assert_eq!(
            online_recipients(&publisher),
            vec![vec!["alice".to_string()]]
        );
        // 在线任务不填按设备的字段。
        let online = publisher.online.lock().unwrap();
        let task = &online[0].1;
        assert!(task.device_id.is_empty() && task.collapse_key.is_empty());
        assert!(task.idempotency_key.is_empty());
        assert_eq!(task.alert_class, AlertClass::Unspecified as i32);
        assert_eq!(task.disposition, PushDisposition::Unspecified as i32);
    }

    /// 在后台、链路保留、却不能自己提示的手机（会话记录里是 `deferred`），而且是这个人唯一的
    /// 会话：连接还在，在线任务照发，消息经链路送到；同时路由当场给这台手机发一条离线任务，
    /// 不必每条消息都等送达确认超时再交接。可达视图由真实的会话记录解析得出。
    #[tokio::test]
    async fn a_deferred_phone_gets_the_online_task_and_its_own_offline_task() {
        use crate::infrastructure::online::online_status_service::reachability_from_session_records;

        let record = br#"{"device_id":"alice-phone","device_platform":"ios","gateway_id":"g1","app_state":"deferred"}"#;
        let reachability = DeviceReachability {
            users: HashMap::from([(
                "alice".to_string(),
                reachability_from_session_records(&[record.to_vec()]),
            )]),
            ..Default::default()
        };
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(reachability),
            Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect("handle_message");

        assert_eq!(
            online_recipients(&publisher),
            vec![vec!["alice".to_string()]],
            "连接还在，消息照常经链路下发"
        );
        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1, "提示归推送通道，路由时直接推");
        assert_eq!(tasks[0].user_id, "alice");
        assert_eq!(tasks[0].device_id, "alice-phone");
        assert_eq!(tasks[0].disposition, PushDisposition::Loud as i32);
    }

    /// 同一台手机回到前台（`active`）或换成能自己提示的后台（`standby`）之后由链路负责提示，
    /// 不再推。
    #[tokio::test]
    async fn an_active_or_standby_phone_is_not_pushed() {
        use crate::infrastructure::online::online_status_service::reachability_from_session_records;

        for state in ["active", "standby"] {
            let record = format!(r#"{{"device_id":"alice-phone","app_state":"{state}"}}"#);
            let reachability = DeviceReachability {
                users: HashMap::from([(
                    "alice".to_string(),
                    reachability_from_session_records(&[record.into_bytes()]),
                )]),
                ..Default::default()
            };
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = device_router(
                Arc::new(reachability),
                Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
                publisher.clone(),
            );

            handler
                .handle_message(
                    &tenant_ctx(),
                    message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
                )
                .await
                .expect("handle_message");

            assert_eq!(
                online_recipients(&publisher),
                vec![vec!["alice".to_string()]],
                "{state}"
            );
            assert!(offline_tasks(&publisher).is_empty(), "{state}");
        }
    }

    /// 活跃的那台设备不推，同一个人另一台没连上的设备照推。
    #[tokio::test]
    async fn only_the_inactive_devices_of_a_user_are_pushed() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::of(&[("alice", "phone-a", "")])),
            Arc::new(MockDeviceBindings::of(&[("alice", "phone-a phone-b")])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect("handle_message");

        assert_eq!(offline_targets(&publisher), vec![pair("alice", "phone-b")]);
    }

    /// 没有任何推送绑定的人（只用桌面与 Web）一条离线任务都没有：没有通道可推。
    #[tokio::test]
    async fn a_user_without_push_bindings_gets_no_offline_task() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::of(&[])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect("handle_message");

        assert!(offline_targets(&publisher).is_empty());
        assert!(publisher.online.lock().unwrap().is_empty());
    }

    /// 一台设备一条任务：各有各的设备标识与幂等键，分区键仍是用户。
    #[tokio::test]
    async fn every_inactive_bound_device_gets_its_own_task() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::of(&[("carol", "phone tablet")])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["carol"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect("handle_message");

        let tasks = offline_tasks(&publisher);
        assert_eq!(
            offline_targets(&publisher),
            vec![pair("carol", "phone"), pair("carol", "tablet")]
        );
        assert_ne!(tasks[0].idempotency_key, tasks[1].idempotency_key);
        assert_eq!(tasks[0].collapse_key, tasks[1].collapse_key);
        assert!(
            publisher
                .offline
                .lock()
                .unwrap()
                .iter()
                .all(|(key, _)| key.as_deref() == Some("carol"))
        );
    }

    /// 历史事故的回归：过滤只能收窄推送目标，不能收窄在线任务。
    /// 设了免打扰的人电脑在线、手机已托管——手机不推，电脑照收。
    #[tokio::test]
    async fn mute_narrows_the_push_target_but_never_the_online_task() {
        for route in ["message", "event"] {
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = device_router(
                Arc::new(DeviceReachability::of(&[(
                    "alice",
                    "alice-desktop",
                    "alice-phone",
                )])),
                Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
                publisher.clone(),
            )
            .with_notify_policy(Arc::new(MockNotifyPolicy {
                muted: vec!["alice".to_string()],
                mention_only: Vec::new(),
                participants: None,
                subscribers: Vec::new(),
                fail: false,
                calls: Arc::new(AtomicUsize::new(0)),
            }));
            let message = text_message("conv-1", "m-1", 1, "bob");

            match route {
                "message" => handler
                    .handle_message(&tenant_ctx(), message_request(&["alice"], message))
                    .await
                    .expect("handle_message"),
                _ => handler
                    .handle_event(&tenant_ctx(), message_event_request(&["alice"], message))
                    .await
                    .expect("handle_event"),
            }

            assert!(offline_targets(&publisher).is_empty(), "{route}");
            assert_eq!(
                publisher.online.lock().unwrap().len(),
                1,
                "{route}: 免打扰不得影响在线投递"
            );
        }
    }

    /// 离线任务带齐按设备投递要用的字段。
    #[tokio::test]
    async fn offline_tasks_carry_the_per_device_fields() {
        use flare_im_contracts::domain::push_policy::conversation_collapse_key;

        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
            publisher.clone(),
        );

        let before = now_millis();
        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice", "bob"], text_message("conv-1", "m-7", 7, "bob")),
            )
            .await
            .expect("handle_message");
        let after = now_millis();

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        let collapse_key = conversation_collapse_key("tenant-a", "conv-1");
        assert_eq!(task.tenant_id, "tenant-a");
        assert_eq!(task.user_id, "alice");
        assert_eq!(task.device_id, "alice-phone");
        assert_eq!(task.message_id, "m-7");
        assert_eq!(task.conversation_id, "conv-1");
        assert_eq!(task.collapse_key, collapse_key);
        assert_eq!(task.thread_key, collapse_key);
        assert_eq!(task.alert_class, AlertClass::Normal as i32);
        assert_eq!(task.disposition, PushDisposition::Loud as i32);
        assert_eq!(task.count, 1);
        assert_eq!(task.badge, None, "不带角标：缺省表示不改设备上的角标");
        assert_eq!(task.conversation_seq, 7);
        assert_eq!(
            task.idempotency_key,
            offline_idempotency_key(
                "tenant-a",
                "alice",
                "alice-phone",
                &collapse_key,
                PushDisposition::Loud,
                7,
                "m-7",
            )
        );
        // 上游没给过期时间：响铃的任务 24 小时后过期。
        let expire_at = task.expire_at.expect("offline tasks always expire");
        assert!(
            (before + LOUD_TASK_TTL_MS..=after + LOUD_TASK_TTL_MS).contains(&expire_at),
            "{expire_at}"
        );
        // 载荷已收敛成只含本人。
        let payload = access_gateway::PushMessageRequest::decode(task.push_payload.as_slice())
            .expect("payload decodes");
        assert_eq!(payload.user_ids, vec!["alice".to_string()]);
    }

    /// 上游给了过期时间就用上游的。
    #[tokio::test]
    async fn an_upstream_expiry_is_kept() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
            publisher.clone(),
        );
        let mut request =
            message_event_request(&["alice"], text_message("conv-1", "m-7", 7, "bob"));
        request.options = Some(access_gateway::PushOptions {
            priority: 8,
            expire_at_ms: 1_999_000_000_000,
            ..Default::default()
        });

        handler
            .handle_event(&tenant_ctx(), request)
            .await
            .expect("handle_event");

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].expire_at, Some(1_999_000_000_000));
        assert_eq!(tasks[0].priority, 8);
    }

    /// 同一条消息无论经消息路径还是事件路径路由，同一台设备上算出同一个幂等键：
    /// broker 靠它去重，两条路径各推一次设备就会为一条消息响两声。
    #[tokio::test]
    async fn the_idempotency_key_is_the_same_on_every_route() {
        let mut keys = Vec::new();
        for route in ["message", "event"] {
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = device_router(
                Arc::new(DeviceReachability::default()),
                Arc::new(MockDeviceBindings::of(&[("alice", "alice-phone")])),
                publisher.clone(),
            );
            let message = text_message("conv-1", "m-7", 7, "bob");
            match route {
                "message" => handler
                    .handle_message(&tenant_ctx(), message_request(&["alice"], message))
                    .await
                    .expect("handle_message"),
                _ => handler
                    .handle_event(&tenant_ctx(), message_event_request(&["alice"], message))
                    .await
                    .expect("handle_event"),
            }
            let tasks = offline_tasks(&publisher);
            assert_eq!(tasks.len(), 1, "{route}");
            keys.push(tasks[0].idempotency_key.clone());
        }
        assert_eq!(keys[0], keys[1]);
    }

    #[test]
    fn idempotency_keys_are_stable_hex_digests_of_every_part() {
        let key = |tenant: &str, user: &str, device: &str, disposition, seq, message: &str| {
            offline_idempotency_key(tenant, user, device, "c:key", disposition, seq, message)
        };
        let base = key("t", "u", "d", PushDisposition::Loud, 7, "m");
        assert_eq!(base.len(), 64);
        assert!(
            base.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "{base}"
        );
        assert_eq!(base, key("t", "u", "d", PushDisposition::Loud, 7, "m"));
        for other in [
            key("t2", "u", "d", PushDisposition::Loud, 7, "m"),
            key("t", "u2", "d", PushDisposition::Loud, 7, "m"),
            key("t", "u", "d2", PushDisposition::Loud, 7, "m"),
            key("t", "u", "d", PushDisposition::Quiet, 7, "m"),
            key("t", "u", "d", PushDisposition::Loud, 8, "m"),
            key("t", "u", "d", PushDisposition::Loud, 7, "m2"),
            offline_idempotency_key("t", "u", "d", "c:other", PushDisposition::Loud, 7, "m"),
        ] {
            assert_ne!(base, other);
        }
        // 分隔符参与摘要：相邻两段怎么切都不会撞。
        assert_ne!(
            key("t", "ab", "c", PushDisposition::Loud, 7, "m"),
            key("t", "a", "bc", PushDisposition::Loud, 7, "m")
        );
    }

    /// 被点到名的人按「点名」提示，其他人按普通消息；两者都响铃。
    #[tokio::test]
    async fn a_named_recipient_is_alerted_as_a_mention() {
        use flare_proto::common::{MessageContent, TextContent, message_content::Content};

        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::default()),
            publisher.clone(),
        );
        let mut message = text_message("conv-1", "m-1", 1, "bob");
        message.content = Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "@alice 看一下".to_string(),
                mentions: vec![flare_proto::common::Mention {
                    r#type: flare_proto::common::MentionType::User as i32,
                    user_id: "alice".to_string(),
                    ..Default::default()
                }],
            })),
        });

        handler
            .handle_event(
                &tenant_ctx(),
                message_event_request(&["alice", "dave"], message),
            )
            .await
            .expect("handle_event");

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].user_id, "alice");
        assert_eq!(tasks[0].alert_class, AlertClass::Mention as i32);
        assert_eq!(tasks[1].user_id, "dave");
        assert_eq!(tasks[1].alert_class, AlertClass::Normal as i32);
        assert!(
            tasks
                .iter()
                .all(|task| task.disposition == PushDisposition::Loud as i32)
        );
    }

    /// 富文本的提及解析不出来：推送照发（不吞），但等级不升到「点名」——
    /// 点名会绕过响铃节制，拿不准不能升。
    #[tokio::test]
    async fn an_unparsed_mention_does_not_raise_the_alert_class() {
        use flare_proto::common::{MessageContent, message_content::Content};

        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::default()),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: Vec::new(),
            mention_only: vec!["alice".to_string()],
            participants: None,
            subscribers: Vec::new(),
            fail: false,
            calls: Arc::new(AtomicUsize::new(0)),
        }));
        let mut message = text_message("conv-1", "m-1", 1, "bob");
        message.content = Some(MessageContent {
            content: Some(Content::RichText(Default::default())),
        });

        handler
            .handle_event(&tenant_ctx(), message_event_request(&["alice"], message))
            .await
            .expect("handle_event");

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1, "拿不准是否点名时「只接收@我」照常放行");
        assert_eq!(tasks[0].alert_class, AlertClass::Normal as i32);
    }

    /// 只计角标、不响铃的通知类消息：静默等级，处置是静默更新。
    #[tokio::test]
    async fn a_silent_notice_is_delivered_quietly() {
        use flare_proto::common::{MessageContent, NotificationContent, message_content::Content};

        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::default()),
            publisher.clone(),
        );
        let mut message = text_message("conv-1", "m-1", 1, "system");
        message.content = Some(MessageContent {
            content: Some(Content::Notification(NotificationContent {
                show_badge: true,
                play_sound: false,
                ..Default::default()
            })),
        });

        let before = now_millis();
        handler
            .handle_message(&tenant_ctx(), message_request(&["alice"], message))
            .await
            .expect("handle_message");
        let after = now_millis();

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].alert_class, AlertClass::Silent as i32);
        assert_eq!(tasks[0].disposition, PushDisposition::Quiet as i32);
        // 上游没给过期时间：不响铃的任务十分钟后过期。
        let expire_at = tasks[0].expire_at.expect("offline tasks always expire");
        assert!(
            (before + QUIET_TASK_TTL_MS..=after + QUIET_TASK_TTL_MS).contains(&expire_at),
            "{expire_at}"
        );
    }

    /// 上游没给过期时间时，响铃与摘要活 24 小时；修正设备上已有通知或角标的那几种只活十分钟。
    #[test]
    fn the_default_expiry_follows_the_disposition() {
        for disposition in [PushDisposition::Loud, PushDisposition::Digest] {
            assert_eq!(
                default_task_ttl_ms(disposition),
                24 * 60 * 60 * 1000,
                "{disposition:?}"
            );
        }
        for disposition in [
            PushDisposition::Quiet,
            PushDisposition::Clear,
            PushDisposition::BadgeOnly,
            PushDisposition::RecallReplace,
        ] {
            assert_eq!(
                default_task_ttl_ms(disposition),
                10 * 60 * 1000,
                "{disposition:?}"
            );
        }
    }

    fn counted(metrics: &PushServerMetrics, disposition: PushDisposition) -> u64 {
        metrics
            .offline_tasks_total
            .with_label_values(&[disposition_label(disposition)])
            .get()
    }

    /// 发出去的离线任务按处置计数，一台设备一条；没发出去的不计。
    #[tokio::test]
    async fn emitted_offline_tasks_are_counted_by_disposition() {
        use flare_proto::common::{MessageContent, NotificationContent, message_content::Content};

        let metrics = Arc::new(PushServerMetrics::new());
        let bindings = || {
            Arc::new(MockDeviceBindings::of(&[
                ("alice", "alice-phone alice-tablet"),
                ("bob", "bob-phone"),
            ]))
        };
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            bindings(),
            Arc::new(MockPushTaskPublisher::default()),
        )
        .with_metrics(metrics.clone());

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice", "bob"], text_message("conv-1", "m-1", 1, "carol")),
            )
            .await
            .expect("handle_message");
        let mut notice = text_message("conv-1", "m-2", 2, "system");
        notice.content = Some(MessageContent {
            content: Some(Content::Notification(NotificationContent {
                show_badge: true,
                play_sound: false,
                ..Default::default()
            })),
        });
        handler
            .handle_message(&tenant_ctx(), message_request(&["alice"], notice))
            .await
            .expect("handle_message");

        assert_eq!(counted(&metrics, PushDisposition::Loud), 3);
        assert_eq!(counted(&metrics, PushDisposition::Quiet), 2);

        let unpublished = Arc::new(PushServerMetrics::new());
        device_router(
            Arc::new(DeviceReachability::default()),
            bindings(),
            Arc::new(MockPushTaskPublisher {
                fail_offline: true,
                ..Default::default()
            }),
        )
        .with_metrics(unpublished.clone())
        .handle_message(
            &tenant_ctx(),
            message_request(&["bob"], text_message("conv-1", "m-3", 3, "carol")),
        )
        .await
        .expect_err("broker outage");
        assert_eq!(counted(&unpublished, PushDisposition::Loud), 0);
    }

    /// 自己发的消息不该让自己的另一台设备响：以前靠「发送者此刻在线」隐含地挡住，
    /// 按设备判定之后得明说。在线任务照发给发送者，他的其他设备靠它同步。
    #[tokio::test]
    async fn the_sender_is_not_pushed_their_own_message() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::of(&[(
                "alice",
                "alice-desktop",
                "alice-phone",
            )])),
            Arc::new(MockDeviceBindings::of(&[
                ("alice", "alice-phone"),
                ("bob", "bob-phone"),
            ])),
            publisher.clone(),
        );

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice", "bob"], text_message("conv-1", "m-1", 1, "alice")),
            )
            .await
            .expect("handle_message");

        assert_eq!(offline_targets(&publisher), vec![pair("bob", "bob-phone")]);
        assert_eq!(
            online_recipients(&publisher),
            vec![vec!["alice".to_string()]]
        );
    }

    /// 绑定所在的 Redis 不可用：这次不推，但在线任务照发、整条记录不算失败。
    /// 绑定只是推送这一路的依赖，不能因为它把在线投递一起拖下水。
    #[tokio::test]
    async fn an_unavailable_binding_registry_never_blocks_the_online_task() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let metrics = Arc::new(PushServerMetrics::new());
        let handler = device_router(
            Arc::new(DeviceReachability::of(&[("alice", "alice-desktop", "")])),
            Arc::new(MockDeviceBindings {
                fail: true,
                ..Default::default()
            }),
            publisher.clone(),
        )
        .with_metrics(metrics.clone());

        handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice", "bob"], text_message("conv-1", "m-1", 1, "carol")),
            )
            .await
            .expect("binding failure must not fail the record");

        assert!(offline_targets(&publisher).is_empty());
        assert_eq!(
            online_recipients(&publisher),
            vec![vec!["alice".to_string()]]
        );
        assert_eq!(metrics.binding_lookup_failures_total.get(), 1);
    }

    /// 设备可达状态查不到时整条记录报可重试错误，什么都不发：按连接发在线任务的路径
    /// 不知道谁连着，发一半比不发更难收拾。
    #[tokio::test]
    async fn a_failed_reachability_lookup_fails_the_record_before_anything_is_published() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability {
                fail: true,
                ..Default::default()
            }),
            Arc::new(MockDeviceBindings::default()),
            publisher.clone(),
        );

        let error = handler
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], text_message("conv-1", "m-1", 1, "bob")),
            )
            .await
            .expect_err("reachability failure fails the record");

        assert!(error.is_retryable());
        assert!(publisher.online.lock().unwrap().is_empty());
        assert!(publisher.offline.lock().unwrap().is_empty());
    }

    /// 只推在线的载荷（已读回执、纯 ping）既不读设备可达状态也不读绑定。
    #[tokio::test]
    async fn online_only_events_read_neither_reachability_nor_bindings() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let reachability = Arc::new(DeviceReachability::default());
        let bindings = Arc::new(MockDeviceBindings::default());
        let handler = device_router(reachability.clone(), bindings.clone(), publisher.clone());
        let receipt = access_gateway::PushEventRequest {
            user_ids: vec!["alice".to_string()],
            events: vec![flare_proto::common::Event {
                r#type: EventType::EventReadReceipt as i32,
                conversation_id: "conv-1".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        handler
            .handle_event(&tenant_ctx(), receipt)
            .await
            .expect("handle_event");

        assert_eq!(publisher.online.lock().unwrap().len(), 1);
        assert!(publisher.offline.lock().unwrap().is_empty());
        assert_eq!(reachability.reads.load(Ordering::SeqCst), 0);
        assert_eq!(bindings.batch_reads.load(Ordering::SeqCst), 0);
    }

    /// 会话广播之后的补推同样按设备：连着的成员若有一台已托管的手机，手机照推；
    /// 在线任务只有那一条广播，不会因为把全体成员交下去而重复。
    #[tokio::test]
    async fn the_conversation_broadcast_pushes_the_parked_phone_of_a_connected_member() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::of(&[(
                "alice",
                "alice-desktop",
                "alice-phone",
            )])),
            Arc::new(MockDeviceBindings::of(&[
                ("alice", "alice-phone"),
                ("bob", "bob-phone"),
            ])),
            publisher.clone(),
        )
        .with_notify_policy(Arc::new(MockNotifyPolicy {
            muted: Vec::new(),
            mention_only: Vec::new(),
            participants: Some(HashMap::from([
                ("alice".to_string(), NotifyPreference::default()),
                ("bob".to_string(), NotifyPreference::default()),
                ("carol".to_string(), NotifyPreference::default()),
            ])),
            subscribers: Vec::new(),
            fail: false,
            calls: Arc::new(AtomicUsize::new(0)),
        }));

        // 读扩散：收件人为空，只带会话。carol 是发送者。
        handler
            .handle_event(
                &tenant_ctx(),
                message_event_request(&[], text_message("conv-big", "m-3", 3, "carol")),
            )
            .await
            .expect("handle_event");

        assert_eq!(
            offline_targets(&publisher),
            vec![pair("alice", "alice-phone"), pair("bob", "bob-phone")]
        );
        assert_eq!(
            publisher.online.lock().unwrap().len(),
            1,
            "在线侧只应有那条会话广播任务"
        );
    }

    /// 不属于任何会话的推送（系统通知）不带合并键：共用一个键的话互不相干的通知会互相覆盖。
    #[tokio::test]
    async fn a_conversationless_push_is_not_collapsed() {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = device_router(
            Arc::new(DeviceReachability::default()),
            Arc::new(MockDeviceBindings::default()),
            publisher.clone(),
        );
        let envelope = PushEnvelope {
            envelope_id: "env-9".into(),
            tenant_id: "tenant-a".into(),
            target_type: flare_proto::PushTargetType::Users as i32,
            target_user_ids: vec!["alice".into()],
            payload_kind: flare_proto::PushPayloadKind::Notification as i32,
            payload: Some(push_envelope::Payload::Notification(NotificationPayload {
                notification_id: "n-1".into(),
                title: "标题".into(),
                body: "正文".into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        handler
            .handle_push_envelope(&tenant_ctx(), envelope)
            .await
            .expect("handle_push_envelope");

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].device_id, phone("alice"));
        assert!(tasks[0].collapse_key.is_empty() && tasks[0].thread_key.is_empty());
        assert_eq!(tasks[0].alert_class, AlertClass::Normal as i32);
        assert_eq!(tasks[0].disposition, PushDisposition::Loud as i32);
        assert_eq!(tasks[0].conversation_seq, 0);
        assert_eq!(tasks[0].idempotency_key.len(), 64);
    }

    /// 在线任务的名单只由连接决定。这个函数的参数里没有通知偏好，这里把三种发法各走一遍。
    #[test]
    fn online_recipients_depend_on_sessions_only() {
        let users = vec!["live".to_string(), "parked".to_string(), "gone".to_string()];
        let reachability = HashMap::from([
            ("live".to_string(), {
                let mut reach = UserReachability::offline();
                reach.record_session("desktop", false);
                reach
            }),
            ("parked".to_string(), {
                let mut reach = UserReachability::offline();
                reach.record_session("phone", true);
                reach
            }),
        ]);

        assert_eq!(
            online_task_recipients(&users, &reachability, OnlineLeg::Connected),
            vec!["live".to_string(), "parked".to_string()],
            "已托管但连接还在的设备仍然收在线任务"
        );
        assert_eq!(
            online_task_recipients(&users, &reachability, OnlineLeg::Everyone),
            users
        );
        assert!(
            online_task_recipients(&users, &reachability, OnlineLeg::AlreadyBroadcast).is_empty()
        );
    }

    #[test]
    fn push_devices_are_the_bound_devices_that_are_not_live() {
        let bound: BoundDevices = vec!["phone".to_string(), "tablet".to_string()].into();
        let mut reach = UserReachability::offline();
        reach.record_session("phone", false);
        reach.record_session("tablet", true);
        reach.record_session("desktop", false);

        assert_eq!(devices_to_push(&bound, Some(&reach)), vec!["tablet"]);
        assert_eq!(devices_to_push(&bound, None), vec!["phone", "tablet"]);
        let none: BoundDevices = Vec::new().into();
        assert!(devices_to_push(&none, Some(&reach)).is_empty());
    }

    #[test]
    fn preferences_suppress_only_when_known() {
        let muted = NotifyPreference {
            muted: true,
            mention_only: false,
        };
        let mention_only = NotifyPreference {
            muted: false,
            mention_only: true,
        };
        assert!(push_suppressed_by_preference(muted, true).is_some());
        assert!(push_suppressed_by_preference(mention_only, false).is_some());
        assert!(push_suppressed_by_preference(mention_only, true).is_none());
        assert!(push_suppressed_by_preference(NotifyPreference::default(), false).is_none());

        let resolved = PreferenceLookup::Resolved(HashMap::from([("member".to_string(), muted)]));
        assert_eq!(resolved.preference_of("member"), muted);
        assert_eq!(
            resolved.preference_of("stranger"),
            NotifyPreference::default()
        );
        assert!(!resolved.is_known_non_member("member"));
        assert!(resolved.is_known_non_member("stranger"));
        // 查询失败时什么都不知道：既不抑制，也不能断定谁不是成员。
        assert_eq!(
            PreferenceLookup::Unavailable.preference_of("member"),
            NotifyPreference::default()
        );
        assert!(!PreferenceLookup::Unavailable.is_known_non_member("stranger"));
    }

    #[test]
    fn alert_facts_describe_the_message_that_will_be_shown() {
        let message = text_message("conv-1", "m-7", 7, "bob");
        let facts = alert_facts(
            PushTaskPayloadKind::Message as i32,
            &message_request(&["alice"], message.clone()).encode_to_vec(),
        );
        assert_eq!(facts.class, AlertClass::Normal);
        assert_eq!(facts.conversation_seq, 7);
        assert_eq!(facts.message_id, "m-7");
        assert_eq!(facts.sender_id, "bob");
        assert!(!facts.mentions.names("alice"));

        // 事件路径：seq 取事件的，消息标识仍是消息自己的。
        let mut request = message_event_request(&["alice"], message);
        request.events[0].conversation_seq = 9;
        let facts = alert_facts(PushTaskPayloadKind::Event as i32, &request.encode_to_vec());
        assert_eq!(facts.class, AlertClass::Normal);
        assert_eq!(facts.conversation_seq, 9);
        assert_eq!(facts.message_id, "m-7");
    }

    #[test]
    fn payloads_that_do_not_alert_have_no_alert_class() {
        let receipt = access_gateway::PushEventRequest {
            events: vec![flare_proto::common::Event {
                r#type: EventType::EventReadReceipt as i32,
                payload: Some(flare_proto::common::event::Payload::Message(text_message(
                    "conv-1", "m-1", 1, "bob",
                ))),
                ..Default::default()
            }],
            ..Default::default()
        };
        for (kind, payload) in [
            (PushTaskPayloadKind::Event, receipt.encode_to_vec()),
            (PushTaskPayloadKind::Event, vec![0xff, 0xff, 0xff]),
            (PushTaskPayloadKind::Message, vec![0xff, 0xff, 0xff]),
            (PushTaskPayloadKind::Message, Vec::new()),
            (PushTaskPayloadKind::Ack, Vec::new()),
            (PushTaskPayloadKind::Capability, Vec::new()),
            (PushTaskPayloadKind::Unspecified, Vec::new()),
        ] {
            assert!(
                !class_alerts(alert_facts(kind as i32, &payload).class),
                "{kind:?}"
            );
        }
        // 通知与自定义推送没有消息体，按普通消息提示。
        for kind in [
            PushTaskPayloadKind::Notification,
            PushTaskPayloadKind::Custom,
        ] {
            let facts = alert_facts(kind as i32, &[]);
            assert_eq!(facts.class, AlertClass::Normal, "{kind:?}");
            assert_eq!(facts.conversation_seq, 0);
            assert!(facts.sender_id.is_empty() && facts.message_id.is_empty());
        }
    }
}

#[cfg(test)]
mod online_fanout_chunking_tests {
    use super::*;

    /// 单条信封的大小必须与群规模脱钩。
    ///
    /// 原实现把整份在线名单塞进一条消息：10 万人群单条 1.6MB，线上实测
    /// FLARE_PUSH 里 12,503 条占 1.07GB（平均 86KB/条），既顶穿 NATS 的 1GB
    /// 配额，也让 push-server 反复 OOM 重启。
    #[test]
    fn envelope_count_scales_with_group_but_each_stays_bounded() {
        for members in [1usize, 999, 1000, 1001, 100_000] {
            let ids: Vec<String> = (0..members).map(|i| format!("u{i:08}")).collect();
            let chunks: Vec<_> = ids.chunks(ONLINE_FANOUT_CHUNK).collect();

            assert_eq!(
                chunks.len(),
                members.div_ceil(ONLINE_FANOUT_CHUNK),
                "{members} 人应切成 ceil(n/{ONLINE_FANOUT_CHUNK}) 片"
            );
            for c in &chunks {
                assert!(
                    c.len() <= ONLINE_FANOUT_CHUNK,
                    "任何一片都不得超过 {ONLINE_FANOUT_CHUNK}，否则大小又跟群规模挂钩了"
                );
            }
            // 分片不能丢人，也不能重复投递
            let total: usize = chunks.iter().map(|c| c.len()).sum();
            assert_eq!(total, members, "分片后用户总数必须守恒");
        }
    }

    /// 空名单不产生任何信封——否则每条消息都会多出一条空推送。
    #[test]
    fn empty_online_list_produces_no_envelope() {
        let ids: Vec<String> = Vec::new();
        assert_eq!(ids.chunks(ONLINE_FANOUT_CHUNK).len(), 0);
    }

    /// 分片大小必须真的有界，不能被改成一个等同于"不分片"的巨值。
    #[test]
    fn chunk_size_is_actually_bounded() {
        assert!(
            (1..=10_000).contains(&ONLINE_FANOUT_CHUNK),
            "分片过大等于没分片：单条信封会重新随群规模膨胀"
        );
    }
}

#[cfg(test)]
mod read_receipt_online_only_tests {
    use super::*;

    fn event(kind: EventType) -> flare_proto::common::Event {
        flare_proto::common::Event {
            r#type: kind as i32,
            conversation_id: "2ALOADGRP000000001".to_string(),
            ..Default::default()
        }
    }

    fn request(events: Vec<flare_proto::common::Event>) -> access_gateway::PushEventRequest {
        access_gateway::PushEventRequest {
            events,
            user_ids: (0..10).map(|i| format!("user{i:04}")).collect(),
            ..Default::default()
        }
    }

    /// 已读回执绝不能生成离线推送。
    ///
    /// 离线用户拿到别人的已读回执毫无用处（不该弹通知，读状态下次同步自然带到），
    /// 但它走的是和普通消息一样的扇出，在大群里按成员数放大。
    ///
    /// 线上实测：**一次登录**客户端标记已读产生 11 条回执事件，在 10 万人的会话里
    /// 被展开成 25,613 条离线信封，push-server 随后恒定满核 —— perf 显示时间全花在
    /// 反复 decode PushEventRequest 的 user_ids 重复字段上。清空队列能好，
    /// **下一次登录立刻复发**。
    #[test]
    fn read_receipts_never_produce_offline_tasks() {
        assert!(
            PushRouterHandler::events_are_read_receipts_only(&request(vec![event(
                EventType::EventReadReceipt
            )])),
            "纯已读回执必须判为只推在线"
        );
    }

    /// 混了真实消息事件时必须照常走离线推送，否则会把该弹的通知一起吞掉。
    #[test]
    fn a_real_message_in_the_batch_keeps_offline_push() {
        assert!(
            !PushRouterHandler::events_are_read_receipts_only(&request(vec![
                event(EventType::EventReadReceipt),
                event(EventType::EventMessage),
            ])),
            "批次里只要有一条真实消息，就不能整批降级成只推在线"
        );
    }

    /// 空批次不属于「纯回执」，交给既有的 pure_conversation_ping 判定。
    #[test]
    fn empty_batch_is_not_read_receipt_only() {
        assert!(!PushRouterHandler::events_are_read_receipts_only(&request(
            vec![]
        )));
    }
}

#[cfg(test)]
mod offline_payload_scoping_tests {
    use super::*;

    fn payload_with_recipients(n: usize) -> Vec<u8> {
        access_gateway::PushMessageRequest {
            user_ids: (0..n).map(|i| format!("user{i:08}")).collect(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// 离线任务是每人一条，载荷必须只含本人。
    ///
    /// 原实现直接复制 template.push_payload，而它带着全量收件人名单：
    /// 10 万人的群 = 10 万份、每份各含 10 万个 ID 的副本。线上实测
    /// flare:im:push:offline:outbox 里 12,902 条占 1.18GB（约 91KB/条），
    /// 把 Redis 顶满后写入全被拒（OOM command not allowed），整个 IM 瘫痪。
    #[test]
    fn offline_payload_keeps_only_the_target_user() {
        let bulk = payload_with_recipients(100_000);
        let scoped = rewrite_push_payload_user_ids(
            PushTaskPayloadKind::Message as i32,
            &bulk,
            std::slice::from_ref(&"user00000042".to_string()),
        )
        .expect("重写应成功");

        let decoded =
            access_gateway::PushMessageRequest::decode(scoped.as_slice()).expect("可解码");
        assert_eq!(
            decoded.user_ids,
            vec!["user00000042".to_string()],
            "离线载荷只应留下收件人本人"
        );
        assert!(
            scoped.len() * 100 < bulk.len(),
            "收敛后应小两个数量级：原 {} 字节，现 {} 字节",
            bulk.len(),
            scoped.len()
        );
    }

    /// 单条大小必须与群规模脱钩——这是整条修复的目的。
    #[test]
    fn offline_payload_size_is_independent_of_group_size() {
        let target = "user00000001".to_string();
        let small = rewrite_push_payload_user_ids(
            PushTaskPayloadKind::Message as i32,
            &payload_with_recipients(10),
            std::slice::from_ref(&target),
        )
        .expect("重写应成功");
        let huge = rewrite_push_payload_user_ids(
            PushTaskPayloadKind::Message as i32,
            &payload_with_recipients(100_000),
            std::slice::from_ref(&target),
        )
        .expect("重写应成功");

        assert_eq!(
            small.len(),
            huge.len(),
            "10 人群与 10 万人群产出的离线载荷必须一样大"
        );
    }
}
