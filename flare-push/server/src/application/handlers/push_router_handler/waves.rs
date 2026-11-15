//! 群会话的离线提示：热会话波次、大群订阅者、@所有人 配额。
//!
//! - 会话空闲时来的第一条当场枚举接收者；间隔进行中只在会话级累加（条数、最新一条、已经直接推过
//!   的人），间隔结束时由到期循环枚举一次。间隔按 3s、10s、30s、120s 自适应
//!   （[`crate::domain::model::WaveState`]）。
//! - 被点名的成员不等波次：按人直接进闸门，成本只与被点名的人数相关。提及来自消息内容，先按 id
//!   核对确实是成员。
//! - 成员不超过上限的群枚举全体成员；超过上限的只枚举开启了「全部消息通知」的订阅者。
//! - @所有人：成员不超过上限时当场枚举、人人按点名提示；超过上限时是一次分页流式的全员波次，
//!   每个会话每 10 分钟（租户可调）至多一次，超出配额按普通消息处理。
//! - 接收者（成员与偏好，或订阅者）按会话在进程内缓存 15 秒，一个波次只查一次。
//! - 通道：成员不超过上限的普通波次走群通道，大群波次与 @所有人 走批量通道，点名走单聊通道。
//!
//! 波次未接线（闸门关闭）时每条消息当场枚举、只推它自己；超过上限的 @所有人 不全员扇出。

use std::time::Duration;

use flare_proto::common::Message;

use super::alerting::GatedRecipient;
use super::trace::{PATH_SUBSCRIBER, PATH_WAVE, RouteTrace, TaskTrace};
use super::*;
use crate::application::notify_wave::{WaveRelease, WaveStep};
use crate::domain::model::{
    DEFAULT_ALL_MENTION_INTERVAL_MS, MentionAll, WaveArrival, WaveBatch, WaveDue, WaveKey,
    WaveMessage, WaveTrigger,
};

/// 接收者按多大一块处理：一块读一次可达状态与绑定、发完再取下一块，内存不随群规模增长。
const WAVE_CHUNK: usize = 500;

/// 全员波次每页取多少成员。
const EVERYONE_PAGE: usize = 500;

/// 全员波次最多翻多少页（50 万成员）。
const EVERYONE_MAX_PAGES: usize = 1_000;

/// 接收者在进程内缓存多久。
const AUDIENCE_CACHE_TTL: Duration = Duration::from_secs(15);

/// 缓存里最多放多少个接收者（按人数计权）。
const AUDIENCE_CACHE_WEIGHT: u64 = 500_000;

/// 到期枚举的波次任务的优先级，与单聊消息的离线任务相同。
const WAVE_TASK_PRIORITY: i32 = 5;

/// 一个群会话要枚举的接收者。
pub(super) enum WaveAudience {
    /// 不超过成员上限：全体成员及其偏好。
    Members(HashMap<String, NotifyPreference>),
    /// 超过成员上限：开启了「全部消息通知」的订阅者（会话服务已排除免打扰与仅 @我 的人）。
    Subscribers(Vec<String>),
}

impl WaveAudience {
    fn len(&self) -> usize {
        match self {
            Self::Members(members) => members.len(),
            Self::Subscribers(subscribers) => subscribers.len(),
        }
    }
}

pub(super) type AudienceCache = moka::future::Cache<WaveKey, Arc<WaveAudience>>;

pub(super) fn audience_cache() -> AudienceCache {
    moka::future::Cache::builder()
        .max_capacity(AUDIENCE_CACHE_WEIGHT)
        .weigher(|_key: &WaveKey, audience: &Arc<WaveAudience>| {
            u32::try_from(audience.len()).unwrap_or(u32::MAX).max(1)
        })
        .time_to_live(AUDIENCE_CACHE_TTL)
        .build()
}

/// 一批要发的内容与去处。
struct WaveEmission<'a> {
    key: &'a WaveKey,
    batch: &'a WaveBatch,
    /// 通知展示的那条消息（读不回正文时是只有标识的占位）。
    message: &'a Message,
    priority: i32,
    expire_at: Option<i64>,
    lane: OfflineLane,
}

/// 这一批里的一位接收者：偏好与要按几条推。
struct WaveRecipient {
    user_id: String,
    preference: NotifyPreference,
    times: u32,
}

/// 读不回正文时代替它的占位：只有标识。Worker 对没有内容的消息用通用文案。
fn wave_placeholder(conversation_id: &str, newest: &WaveMessage) -> Message {
    Message {
        server_id: newest.message_id.clone(),
        conversation_id: conversation_id.to_string(),
        conversation_seq: newest.seq,
        sender_id: newest.sender_id.clone(),
        ..Default::default()
    }
}

/// 载荷里那条要提示的消息：与 [`alert_facts`] 取的是同一条。
fn alerting_message(request: &access_gateway::PushEventRequest) -> Option<&Message> {
    use flare_im_contracts::domain::push_policy::event_alert_class;
    request
        .events
        .iter()
        .filter(|event| class_alerts(event_alert_class(event)))
        .find_map(|event| match &event.payload {
            Some(flare_proto::common::event::Payload::Message(message)) => Some(message),
            _ => None,
        })
}

impl PushRouterHandler {
    /// 群会话一条消息的离线提示：被点名的成员直接推，其余进会话的波次。
    ///
    /// 失败一律只告警不向上抛：在线广播已经发出去了，补推这一步失败不该把整条消息判成失败并
    /// 重试——重试会让在线成员收到重复消息。
    pub(super) async fn conversation_offline_alerts(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        template: OfflineFanoutTemplate<'_>,
    ) {
        let facts = alert_facts(PushTaskPayloadKind::Event as i32, template.push_payload);
        // 不提示的载荷不值得为它枚举接收者。
        if !class_alerts(facts.class) {
            return;
        }
        let Ok(request) = access_gateway::PushEventRequest::decode(template.push_payload) else {
            return;
        };
        let Some(shown) = alerting_message(&request) else {
            return;
        };
        let tenant_id = match self.request_tenant_id(ctx) {
            Ok(tenant_id) => tenant_id.to_string(),
            Err(error) => {
                tracing::warn!(error = %error, "Skipping group offline push: tenant required");
                return;
            }
        };
        if self.notify_policy.is_none() {
            return;
        }
        let key = WaveKey::new(tenant_id, conversation_id);
        // 存下来到期展示的正文：补上事件给出的 seq 与会话，读回时不必再查。
        let mut message = shown.clone();
        if message.conversation_seq == 0 {
            message.conversation_seq = facts.conversation_seq;
        }
        if message.conversation_id.trim().is_empty() {
            message.conversation_id = conversation_id.to_string();
        }

        let named = self.named_members(ctx, conversation_id, &facts).await;
        let direct: Vec<String> = named.keys().cloned().collect();
        let trigger = self.wave_trigger(ctx, &key, &facts).await;
        let arrival = WaveArrival {
            message: WaveMessage {
                message_id: facts.message_id.clone(),
                seq: facts.conversation_seq,
                sender_id: facts.sender_id.clone(),
            },
            class: facts.class as i32,
            mentions_unknown: facts.mentions.unknown,
            direct: &direct,
            trigger,
        };
        let body = message.encode_to_vec();
        // 波次的到期条目由闸门的到期循环领取：没有闸门就没有波次，每条当场枚举。
        let step = match (self.notify_waves.as_ref(), self.notify_gate.as_ref()) {
            (Some(waves), Some(_)) => waves.arrive(now_millis(), &key, &arrival, &body).await,
            _ => WaveStep::Flush(WaveRelease {
                batch: WaveBatch::single(&arrival),
                message: Some(body),
            }),
        };
        self.record_all_mention(trigger, step.mention_all());
        // 人人按点名提示时，被点名的人就在这一批里；否则他们不等波次，现在直接推。
        if step.mention_all() == MentionAll::No && !named.is_empty() {
            self.push_named_members(ctx, &key, &template, &named).await;
        }
        let WaveStep::Flush(release) = step else {
            return;
        };
        if release.batch.mention_all == MentionAll::Everyone {
            // 全员波次要一页页推完整个群，可能比这条记录的确认期限还长：放到后台，记录照常确认。
            // 进程中途退出时没推到的成员，打开应用时同步可见。
            let handler = self.clone();
            let ctx = ctx.clone();
            let (priority, expire_at) = (template.priority, template.expire_at);
            tokio::spawn(async move {
                handler
                    .emit_wave(&ctx, &key, release, priority, expire_at)
                    .await;
            });
            return;
        }
        self.emit_wave(ctx, &key, release, template.priority, template.expire_at)
            .await;
    }

    /// 波次到期：有累加就枚举一次。
    pub(super) async fn emit_due_wave(
        &self,
        ctx: &flare_server_core::context::Ctx,
        due: &WaveDue,
        now_ms: i64,
    ) {
        let Some(waves) = self.notify_waves.as_ref() else {
            return;
        };
        let Some(release) = waves.due(now_ms, due).await else {
            return;
        };
        self.emit_wave(ctx, &due.key, release, WAVE_TASK_PRIORITY, None)
            .await;
    }

    /// 这条消息怎么进波次：紧急与 @所有人 不等间隔。@所有人 要看群是否超过成员上限，
    /// 读不到接收者时按普通消息处理。
    async fn wave_trigger(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: &WaveKey,
        facts: &AlertFacts,
    ) -> WaveTrigger {
        if facts.class == AlertClass::Urgent {
            return WaveTrigger::Now;
        }
        if !facts.mentions.all {
            return WaveTrigger::Normal;
        }
        match self.wave_audience(ctx, key).await {
            Ok(audience) => match audience.as_ref() {
                WaveAudience::Members(_) => WaveTrigger::AllMembers,
                WaveAudience::Subscribers(_) => WaveTrigger::AllEveryone {
                    interval_ms: self
                        .notify_waves
                        .as_ref()
                        .map_or(DEFAULT_ALL_MENTION_INTERVAL_MS, |waves| {
                            waves.all_mention_interval_ms(&key.tenant_id)
                        }),
                },
            },
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %key.conversation_id,
                    "mention of everyone treated as a normal message: recipients unavailable"
                );
                WaveTrigger::Normal
            }
        }
    }

    fn record_all_mention(&self, trigger: WaveTrigger, honoured: MentionAll) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        match (trigger, honoured) {
            (WaveTrigger::AllMembers, _) => metrics.record_all_mention("members"),
            (WaveTrigger::AllEveryone { .. }, MentionAll::Everyone) => {
                metrics.record_all_mention("wave");
            }
            (WaveTrigger::AllEveryone { .. }, _) => metrics.record_all_mention("throttled"),
            _ => {}
        }
    }

    /// 消息点到的成员及其偏好，发送者本人除外。提及来自消息内容，不可信：按 id 查一次成员资格，
    /// 只留确实在会话里的人；查不到时谁也不直接推——在接收者里的人照样随波次收到。
    async fn named_members(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        facts: &AlertFacts,
    ) -> HashMap<String, NotifyPreference> {
        let mut users: Vec<String> = facts
            .mentions
            .users
            .iter()
            .filter(|user_id| !user_id.trim().is_empty() && **user_id != facts.sender_id)
            .cloned()
            .collect();
        if users.is_empty() {
            return HashMap::new();
        }
        users.sort();
        match self.notify_preferences(ctx, conversation_id, &users).await {
            PreferenceLookup::Resolved(members) => members,
            PreferenceLookup::Unavailable => HashMap::new(),
        }
    }

    /// 被点名的成员：不等波次，按人走正常的离线路由（偏好、提示规则与闸门），点名走单聊通道。
    async fn push_named_members(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: &WaveKey,
        template: &OfflineFanoutTemplate<'_>,
        named: &HashMap<String, NotifyPreference>,
    ) {
        let users: Vec<String> = named.keys().cloned().collect();
        let reachability = match self.load_reachability(ctx, &users).await {
            Ok(reachability) => reachability,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %key.conversation_id,
                    "mentioned members not pushed: device reachability unavailable"
                );
                return;
            }
        };
        let headers = HashMap::new();
        let routed = self
            .publish_targeted_tasks_inner(
                ctx,
                &users,
                &reachability,
                PushTaskTemplate {
                    message_id: template.message_id,
                    conversation_id: &key.conversation_id,
                    tenant_id: &key.tenant_id,
                    priority: template.priority,
                    expire_at: template.expire_at,
                    push_payload: template.push_payload,
                    headers: &headers,
                    payload_kind: PushTaskPayloadKind::Event as i32,
                },
                false,
                // 在线一路已由会话广播承担。
                OnlineLeg::AlreadyBroadcast,
                Some(named),
            )
            .await;
        match routed {
            Ok(()) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_wave_direct_mentions(users.len() as u64);
                }
            }
            Err(error) => tracing::warn!(
                error = %error,
                conversation_id = %key.conversation_id,
                "mentioned members not pushed"
            ),
        }
    }

    /// 群会话的接收者，进程内缓存 15 秒：成员不超过上限时是全体成员及其偏好，超过时是订阅者。
    async fn wave_audience(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: &WaveKey,
    ) -> Result<Arc<WaveAudience>> {
        if let Some(audience) = self.audience_cache.get(key).await {
            return Ok(audience);
        }
        let Some(policy) = self.notify_policy.as_ref() else {
            return Ok(Arc::new(WaveAudience::Members(HashMap::new())));
        };
        // 一次枚举同时拿到成员与偏好；超过上限的群改问订阅者，不翻整册成员。
        let audience = match policy
            .all_participants(ctx, &key.conversation_id, self.offline_fanout_member_cap)
            .await?
        {
            Some(members) => WaveAudience::Members(members),
            None => WaveAudience::Subscribers(
                policy
                    .notification_subscribers(ctx, &key.conversation_id)
                    .await?,
            ),
        };
        let audience = Arc::new(audience);
        self.audience_cache
            .insert(key.clone(), audience.clone())
            .await;
        Ok(audience)
    }

    /// 枚举一批的接收者，逐块进闸门并发出。
    async fn emit_wave(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: &WaveKey,
        release: WaveRelease,
        priority: i32,
        expire_at: Option<i64>,
    ) {
        let WaveRelease { batch, message } = release;
        let message = message
            .and_then(|body| Message::decode(body.as_slice()).ok())
            .unwrap_or_else(|| wave_placeholder(&key.conversation_id, &batch.newest));
        let audience = match self.wave_audience(ctx, key).await {
            Ok(audience) => audience,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %key.conversation_id,
                    count = batch.count,
                    "group offline push skipped: recipients unavailable"
                );
                return;
            }
        };
        let large = matches!(audience.as_ref(), WaveAudience::Subscribers(_));
        let lane = if large || batch.mention_all != MentionAll::No {
            OfflineLane::Bulk
        } else {
            OfflineLane::Group
        };
        let emission = WaveEmission {
            key,
            batch: &batch,
            message: &message,
            priority,
            expire_at,
            lane,
        };
        match audience.as_ref() {
            WaveAudience::Subscribers(subscribers) if batch.mention_all == MentionAll::Everyone => {
                self.emit_to_everyone(ctx, &emission, subscribers).await;
            }
            WaveAudience::Subscribers(subscribers) => {
                for chunk in subscribers.chunks(WAVE_CHUNK) {
                    let recipients = chunk
                        .iter()
                        .map(|user_id| WaveRecipient {
                            user_id: user_id.clone(),
                            preference: NotifyPreference::default(),
                            times: batch.times_for(user_id),
                        })
                        .collect();
                    self.emit_wave_chunk(ctx, &emission, recipients, "subscribers")
                        .await;
                }
            }
            WaveAudience::Members(members) => {
                // 先建好整批接收者再分块：跨 await 不持有指向成员表的借用切片，Future 才能跨线程。
                let mut pending: Vec<WaveRecipient> = members
                    .iter()
                    .map(|(user_id, preference)| WaveRecipient {
                        user_id: user_id.clone(),
                        preference: *preference,
                        times: batch.times_for(user_id),
                    })
                    .collect();
                while !pending.is_empty() {
                    let rest = pending.split_off(pending.len().min(WAVE_CHUNK));
                    let recipients = std::mem::replace(&mut pending, rest);
                    self.emit_wave_chunk(ctx, &emission, recipients, "members")
                        .await;
                }
            }
        }
    }

    /// 超过成员上限的全员 @所有人 波次：分页流式枚举全体成员，一页发完再取下一页，不在内存里攒
    /// 整册成员。订阅者按这一批的条数推，其余成员只为这条 @所有人 推一条。
    async fn emit_to_everyone(
        &self,
        ctx: &flare_server_core::context::Ctx,
        emission: &WaveEmission<'_>,
        subscribers: &[String],
    ) {
        let Some(policy) = self.notify_policy.as_ref() else {
            return;
        };
        let subscribers: HashSet<&str> = subscribers.iter().map(String::as_str).collect();
        let mut cursor = String::new();
        for _ in 0..EVERYONE_MAX_PAGES {
            let page = match policy
                .participant_page(ctx, &emission.key.conversation_id, &cursor, EVERYONE_PAGE)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        conversation_id = %emission.key.conversation_id,
                        "mention of everyone stopped: member page unavailable"
                    );
                    return;
                }
            };
            let recipients = page
                .members
                .into_iter()
                .map(|(user_id, preference)| {
                    let times = if subscribers.contains(user_id.as_str()) {
                        emission.batch.times_for(&user_id)
                    } else {
                        emission.batch.times_for_non_subscriber(&user_id)
                    };
                    WaveRecipient {
                        user_id,
                        preference,
                        times,
                    }
                })
                .collect();
            self.emit_wave_chunk(ctx, emission, recipients, "everyone")
                .await;
            match page.next_cursor {
                Some(next) => cursor = next,
                None => return,
            }
        }
        tracing::warn!(
            conversation_id = %emission.key.conversation_id,
            pages = EVERYONE_MAX_PAGES,
            "mention of everyone stopped at the page limit"
        );
    }

    /// 一块接收者：读可达状态与绑定，给不活跃的已绑定设备按这一批进闸门（闸门未接线时每台设备
    /// 一条）并发出。失败只告警：这一块的人打开应用时同步可见。
    async fn emit_wave_chunk(
        &self,
        ctx: &flare_server_core::context::Ctx,
        emission: &WaveEmission<'_>,
        recipients: Vec<WaveRecipient>,
        audience: &'static str,
    ) {
        let recipients: HashMap<String, WaveRecipient> = recipients
            .into_iter()
            .filter(|recipient| recipient.times > 0)
            .map(|recipient| (recipient.user_id.clone(), recipient))
            .collect();
        if recipients.is_empty() {
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_wave_recipients(audience, recipients.len() as u64);
        }
        let user_ids: Vec<String> = recipients.keys().cloned().collect();
        let reachability = match self.load_reachability(ctx, &user_ids).await {
            Ok(reachability) => reachability,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %emission.key.conversation_id,
                    "group offline push skipped for a chunk: device reachability unavailable"
                );
                return;
            }
        };
        let batch = emission.batch;
        let message_id = if batch.newest.message_id.is_empty() {
            format!("wave:{}:{}", emission.key.conversation_id, batch.newest.seq)
        } else {
            batch.newest.message_id.clone()
        };
        // 波次记在这一批最新一条消息的决策链上：枚举到的每位成员、每台设备各一条路由。
        let trace_key = flare_im_contracts::push_trace::trace_key(
            &message_id,
            &emission.key.conversation_id,
            batch.newest.seq,
        );
        let route = RouteTrace {
            trace_key: &trace_key,
            conversation_id: &emission.key.conversation_id,
            path: if audience == "members" {
                PATH_WAVE
            } else {
                PATH_SUBSCRIBER
            },
        };
        // 发送者本人已按条数从这一批里减掉，这里不再按发送者过滤。
        let candidates = self
            .push_target_devices(
                ctx,
                &emission.key.tenant_id,
                &user_ids,
                &reachability,
                "",
                Some(&route),
            )
            .await;
        if candidates.is_empty() {
            return;
        }

        let mention_all = batch.mention_all != MentionAll::No;
        let class = AlertClass::try_from(batch.class)
            .ok()
            .filter(|class| class_alerts(*class))
            .unwrap_or(AlertClass::Normal);
        let alert_class = recipient_alert_class(class, mention_all);
        let mentioned = if mention_all {
            Mentioned::Yes
        } else if batch.mentions_unknown {
            Mentioned::Unknown
        } else {
            Mentioned::No
        };
        let payload_for = |user_id: &str| {
            access_gateway::PushMessageRequest {
                user_ids: vec![user_id.to_string()],
                messages: vec![emission.message.clone()],
                options: None,
            }
            .encode_to_vec()
        };
        let headers = HashMap::new();
        let now_ms = now_millis();

        let tasks = if self.notify_gate.is_some() {
            let template = PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &emission.key.conversation_id,
                tenant_id: &emission.key.tenant_id,
                priority: emission.priority,
                expire_at: emission.expire_at,
                push_payload: &[],
                headers: &headers,
                payload_kind: PushTaskPayloadKind::Message as i32,
            };
            let facts = AlertFacts {
                class: alert_class,
                mentions: MentionTargets::default(),
                conversation_seq: batch.newest.seq,
                message_id: message_id.clone(),
                // 发送者已经减掉了：不再按「本人发的」整批丢掉。
                sender_id: String::new(),
                conversation_kind: ConversationKind::Group,
            };
            let gated: Vec<GatedRecipient> = candidates
                .into_iter()
                .filter_map(|(user_id, devices)| {
                    let recipient = recipients.get(&user_id)?;
                    let push_payload = payload_for(&user_id);
                    Some(GatedRecipient {
                        user_id,
                        devices,
                        push_payload,
                        alert_class,
                        mentioned,
                        preference: recipient.preference,
                        times: recipient.times,
                        lane: emission.lane,
                    })
                })
                .collect();
            self.gated_offline_tasks(ctx, &template, &facts, &reachability, gated, now_ms)
                .await
        } else {
            // 没有闸门：按偏好过滤后每台设备一条，条数照实写上。
            let mut tasks = Vec::new();
            for (user_id, devices) in candidates {
                let Some(recipient) = recipients.get(&user_id) else {
                    continue;
                };
                if push_suppressed_by_preference(recipient.preference, mentioned != Mentioned::No)
                    .is_some()
                {
                    self.trace_suppressed(
                        &emission.key.tenant_id,
                        &user_id,
                        &trace_key,
                        &emission.key.conversation_id,
                        &devices,
                        alert_class,
                        recipient.preference,
                    );
                    continue;
                }
                let push_payload = payload_for(&user_id);
                let spec = OfflineTaskSpec {
                    tenant_id: &emission.key.tenant_id,
                    user_id: &user_id,
                    conversation_id: &emission.key.conversation_id,
                    message_id: &message_id,
                    alert_message_id: &message_id,
                    priority: emission.priority,
                    expire_at: emission.expire_at,
                    push_payload: &push_payload,
                    headers: &headers,
                    payload_kind: PushTaskPayloadKind::Message as i32,
                    alert_class,
                    conversation_seq: batch.newest.seq,
                };
                let shape = TaskShape {
                    count: recipient.times,
                    ..TaskShape::per_message(alert_class)
                };
                for device_id in &devices {
                    let envelope = offline_task_shaped(&spec, device_id, now_ms, shape);
                    tasks.push(PendingPushTask {
                        user_id: user_id.clone(),
                        trace: Some(TaskTrace::of(
                            &envelope,
                            &emission.key.conversation_id,
                            &trace_key,
                        )),
                        payload: envelope.encode_to_vec(),
                        online: false,
                        disposition: shape.disposition,
                        lane: emission.lane,
                    });
                }
            }
            tasks
        };
        if let Err(error) = self.publish_pending_tasks(ctx, tasks).await {
            tracing::warn!(
                error = %error,
                conversation_id = %emission.key.conversation_id,
                "group offline push failed for a chunk"
            );
        }
    }
}

#[cfg(test)]
mod tests;
