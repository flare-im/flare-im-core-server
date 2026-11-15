//! 设备交接的补提示。
//!
//! 一台设备从「链路负责提示」转为「推送通道负责提示」时，已经写给它的连接、却没有被确认的
//! 那些消息还没人提示过。网关（或发现网关失联的 Online）把它们交过来，这里对这台设备补一次：
//! 按会话合并，每个会话只取最新一条，走与正常离线推送相同的偏好过滤、提示规则与闸门。
//!
//! 只读每个会话的最新一条，不逐条回放：补的是「有新消息」这件事，内容靠同步补齐。

use flare_im_contracts::domain::push_policy::message_alert_class;
use flare_im_contracts::push_trace::{TraceStage, trace_key};
use flare_proto::common::{DeviceHandoff, DeviceHandoffCause, DeviceHandoffItem, Message};

use super::trace::{PATH_CATCHUP, PATH_HANDOFF, TaskTrace, alert_trace_key};
use super::*;
use crate::domain::repository::RecentConversation;

/// 一次交接最多为多少个会话补提示。再多的会话同时弹出来只是一屏噪声；
/// 没轮到的会话内容不丢，打开应用同步即可见。
const MAX_HANDOFF_CONVERSATIONS: usize = 16;

/// 网关失联的交接最多补出多少条提示。这类交接一来就是整台网关的设备，
/// 范围又是从未读会话推出来的，比网关亲手记下的未确认条目更粗，上限也更紧。
const MAX_GATEWAY_LOST_ALERTS: usize = 5;

/// 网关失联时向会话服务要多少个候选会话（它单次至多给这么多）。比提示上限多要一些：
/// 候选里免打扰的、只接收@我却没被点名的会话不出提示，也不该占掉名额。
const GATEWAY_LOST_CANDIDATE_LIMIT: usize = 20;

/// 补提示任务的优先级，与单聊消息的离线任务相同。
const CATCH_UP_PRIORITY: i32 = 5;

/// 要补提示的一个会话：取这个会话里最新的那条。
#[derive(Debug, Clone, PartialEq, Eq)]
struct HandoffTarget {
    conversation_id: String,
    /// 最新一条的会话 seq；为 0 表示不知道，这时读不回消息，只能用通用文案。
    seq: u64,
    /// 最新一条的服务端消息 ID，可能为空。
    message_id: String,
}

/// 未确认条目按会话合并：每个会话留 seq 最高的一条，最近写出的会话排在前面，
/// 超出上限的丢掉最早的。
///
/// 条目按写出顺序排列，所以「最近」看的是一个会话最后一次出现的位置，而不是 seq——
/// 不同会话的 seq 没有可比性。
fn pending_targets(pending: &[DeviceHandoffItem]) -> Vec<HandoffTarget> {
    let mut latest: HashMap<&str, (usize, &DeviceHandoffItem)> = HashMap::new();
    for (position, item) in pending.iter().enumerate() {
        let conversation_id = item.conversation_id.trim();
        if conversation_id.is_empty() {
            continue;
        }
        match latest.get_mut(conversation_id) {
            Some((last_position, newest)) => {
                *last_position = position;
                if item.seq >= newest.seq {
                    *newest = item;
                }
            }
            None => {
                latest.insert(conversation_id, (position, item));
            }
        }
    }
    let mut ordered: Vec<(usize, HandoffTarget)> = latest
        .into_iter()
        .map(|(conversation_id, (position, item))| {
            (
                position,
                HandoffTarget {
                    conversation_id: conversation_id.to_string(),
                    seq: item.seq,
                    message_id: item.message_id.clone(),
                },
            )
        })
        .collect();
    ordered.sort_by_key(|(position, _)| std::cmp::Reverse(*position));
    ordered.truncate(MAX_HANDOFF_CONVERSATIONS);
    ordered.into_iter().map(|(_, target)| target).collect()
}

/// 会话服务给出的有新未读的会话换成补提示目标，顺序不变（最新的在前）。
///
/// 时间窗口、是否本人发送、是否未读已由会话服务筛过，这里不再筛；只丢掉没有会话 ID 的行，
/// 它无从提示。
fn gateway_lost_targets(conversations: Vec<RecentConversation>) -> Vec<HandoffTarget> {
    conversations
        .into_iter()
        .filter(|conversation| !conversation.conversation_id.trim().is_empty())
        .map(|conversation| HandoffTarget {
            conversation_id: conversation.conversation_id,
            seq: conversation.last_seq,
            message_id: conversation.last_message_id,
        })
        .collect()
}

/// 交接原因在指标里的标签。
fn handoff_cause_label(cause: DeviceHandoffCause) -> &'static str {
    match cause {
        DeviceHandoffCause::Parked => "parked",
        DeviceHandoffCause::AckTimeout => "ack_timeout",
        DeviceHandoffCause::LinkLost => "link_lost",
        DeviceHandoffCause::GatewayLost => "gateway_lost",
        DeviceHandoffCause::Unspecified => "unspecified",
    }
}

/// 读不回消息时用来代替它的占位：只有标识、没有内容。Worker 对没有展示信息的消息用通用文案，
/// 设备上是一条普通的「新消息」提示。
fn placeholder_message(target: &HandoffTarget) -> Message {
    Message {
        server_id: target.message_id.clone(),
        conversation_id: target.conversation_id.clone(),
        conversation_seq: target.seq,
        ..Default::default()
    }
}

/// 补提示任务的消息标识：读回的（或交接点名的）消息 ID；都没有时用会话与 seq 合成一个。
/// 它也是这条提示在推送决策链里的键。
fn catch_up_message_id(server_id: &str, target: &HandoffTarget) -> String {
    if server_id.is_empty() {
        format!("handoff:{}:{}", target.conversation_id, target.seq)
    } else {
        server_id.to_string()
    }
}

fn invalid_handoff(reason: &'static str) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(ErrorCode::InvalidParameter, reason).build_error()
}

/// 一次交接在决策链里的样子：原因、带来的未确认条目数、这一路的来由与设备当时的状态。
struct HandoffTrace {
    cause: &'static str,
    pending: String,
    path: &'static str,
    /// 托管、断链与网关失联时链路已经没了（`parked`）；确认超时的设备可能还连着（`unknown`）。
    state: &'static str,
}

impl HandoffTrace {
    fn of(cause: DeviceHandoffCause, pending: usize) -> Self {
        Self {
            cause: handoff_cause_label(cause),
            pending: pending.to_string(),
            path: if cause == DeviceHandoffCause::GatewayLost {
                PATH_CATCHUP
            } else {
                PATH_HANDOFF
            },
            state: if cause == DeviceHandoffCause::AckTimeout {
                "unknown"
            } else {
                "parked"
            },
        }
    }
}

impl PushRouterHandler {
    /// 处理一次设备交接：给这台设备补一次提示。
    ///
    /// 可以重复处理：产出的离线任务带幂等键，同一条提示被算出几次都只落盘一条。
    /// 所以下游暂时不可用时直接报可重试错误，由 broker 重投整条交接。
    pub async fn handle_device_handoff(
        &self,
        ctx: &flare_server_core::context::Ctx,
        handoff: DeviceHandoff,
    ) -> Result<()> {
        let tenant_id = self.request_tenant_id(ctx)?;
        if handoff.tenant_id != tenant_id {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "device handoff tenant does not match routing context",
            )
            .build_error());
        }
        let user_id = handoff.user_id.trim();
        let device_id = handoff.device_id.trim();
        if user_id.is_empty() || device_id.is_empty() {
            return Err(invalid_handoff("device handoff requires user and device"));
        }
        let cause = match DeviceHandoffCause::try_from(handoff.cause) {
            Ok(DeviceHandoffCause::Unspecified) | Err(_) => {
                return Err(invalid_handoff("device handoff cause is required"));
            }
            Ok(cause) => cause,
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_device_handoff(handoff_cause_label(cause));
        }
        let handoff_trace = HandoffTrace::of(cause, handoff.pending.len());

        // 没有推送绑定的设备没有通道可以接手（未登记、权限关闭、令牌失效）：不补提示，
        // 设备回来后靠同步补齐。直读绑定而不用缓存，见 `bound_devices_fresh`。
        let bound = self
            .device_bindings
            .bound_devices_fresh(ctx, tenant_id, user_id)
            .await
            .map_err(|e| {
                if let Some(metrics) = &self.metrics {
                    metrics.record_binding_lookup_failure();
                }
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to read device push bindings",
                )
            })?;
        if !bound.iter().any(|bound_device| bound_device == device_id) {
            tracing::debug!(
                user_id = %user_id,
                device_id = %device_id,
                cause = ?cause,
                "Skipping catch-up alert: device has no push binding"
            );
            // 交接带来的条目记下「没有通道接手」；网关失联的范围要再查一次，没有绑定就不查了。
            if cause != DeviceHandoffCause::GatewayLost && self.tracing_enabled() {
                for target in pending_targets(&handoff.pending) {
                    let key = trace_key(
                        &catch_up_message_id(&target.message_id, &target),
                        &target.conversation_id,
                        target.seq,
                    );
                    self.trace_handoff(
                        tenant_id,
                        user_id,
                        device_id,
                        &key,
                        &target.conversation_id,
                        &handoff_trace,
                    );
                    self.trace_entry(
                        tenant_id,
                        user_id,
                        &key,
                        &target.conversation_id,
                        device_id,
                        TraceStage::Route,
                        &[("state", "dark"), ("path", "no_binding")],
                    );
                }
            }
            return Ok(());
        }

        // 未确认条目已经按会话合并并截到上限；网关失联的候选比提示上限多，补够上限就停。
        let (targets, alert_cap) = match cause {
            DeviceHandoffCause::GatewayLost => (
                self.conversations_since_gateway_loss(ctx, user_id, handoff.since)
                    .await?,
                MAX_GATEWAY_LOST_ALERTS,
            ),
            _ => (pending_targets(&handoff.pending), MAX_HANDOFF_CONVERSATIONS),
        };
        let mut alerted = 0;
        for target in &targets {
            if alerted == alert_cap {
                break;
            }
            if self
                .catch_up_conversation(ctx, tenant_id, user_id, device_id, target, &handoff_trace)
                .await?
            {
                alerted += 1;
            }
        }
        Ok(())
    }

    /// 网关失联的交接不带未确认条目，要补提示的会话向会话服务问：`since` 之后有新未读的那些。
    ///
    /// 问不出范围（没接线，或交接没带窗口起点）时只能不补——不能退而给每台设备发一条没有
    /// 依据的「新消息」：一台网关失联就是它上面所有设备同时响一声。
    async fn conversations_since_gateway_loss(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_id: &str,
        since: i64,
    ) -> Result<Vec<HandoffTarget>> {
        let Some(reader) = self.recent_conversations.as_ref() else {
            tracing::warn!(
                user_id = %user_id,
                since,
                "Gateway-lost handoff not caught up: no recent conversation reader is wired"
            );
            return Ok(Vec::new());
        };
        // 没有窗口起点时「之后」就是全部历史，会把很久以前的未读重新提示一遍。
        if since <= 0 {
            tracing::warn!(
                user_id = %user_id,
                "Gateway-lost handoff not caught up: it carries no window start"
            );
            return Ok(Vec::new());
        }
        // 错误原样上抛：可不可以重试由读取方按失败原因定，被拒绝的读取重投也没用。
        let conversations = reader
            .recent_conversations(ctx, user_id, since, GATEWAY_LOST_CANDIDATE_LIMIT)
            .await?;
        Ok(gateway_lost_targets(conversations))
    }

    /// 读回要提示的那条消息。读不到（没接线、还没落库、已撤回、读取失败）一律是 `None`，
    /// 调用方用占位消息继续：提示可以没有内容，不能没有。
    async fn handoff_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        target: &HandoffTarget,
        user_id: &str,
    ) -> Option<Message> {
        let lookup = self.message_lookup.as_ref()?;
        if target.seq == 0 {
            return None;
        }
        match lookup
            .message_at(ctx, &target.conversation_id, target.seq, user_id)
            .await
        {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %target.conversation_id,
                    seq = target.seq,
                    "catch-up message lookup failed; alerting with generic content"
                );
                None
            }
        }
    }

    /// 交接在一条消息的决策链上：一条 `handoff`（原因、条目数），一条 `route`（设备状态与来由）。
    fn trace_handoff(
        &self,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        trace_key: &str,
        conversation_id: &str,
        handoff: &HandoffTrace,
    ) {
        if !self.tracing_enabled() {
            return;
        }
        self.trace_entry(
            tenant_id,
            user_id,
            trace_key,
            conversation_id,
            device_id,
            TraceStage::Handoff,
            &[
                ("cause", handoff.cause),
                ("pending", handoff.pending.as_str()),
            ],
        );
        self.trace_entry(
            tenant_id,
            user_id,
            trace_key,
            conversation_id,
            device_id,
            TraceStage::Route,
            &[("state", handoff.state), ("path", handoff.path)],
        );
    }

    /// 为一个会话给这台设备补一条离线任务。返回是否发出了任务：被成员资格、偏好或消息本身
    /// 挡掉的会话不出提示，也不算进调用方的提示上限。
    async fn catch_up_conversation(
        &self,
        ctx: &flare_server_core::context::Ctx,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        target: &HandoffTarget,
        handoff: &HandoffTrace,
    ) -> Result<bool> {
        // 成员资格与通知偏好是同一次查询：确定已经不在会话里的人不该再收到这个会话的提示；
        // 查询失败时两者都未知，照常提示。
        let preferences = self
            .notify_preferences(ctx, &target.conversation_id, &[user_id.to_string()])
            .await;
        if preferences.is_known_non_member(user_id) {
            tracing::debug!(
                user_id = %user_id,
                conversation_id = %target.conversation_id,
                "Skipping catch-up alert: user is not a participant"
            );
            return Ok(false);
        }
        let preference = preferences.preference_of(user_id);
        // 免打扰不看消息内容，先判掉，省一次读消息。
        if preference.muted {
            if self.tracing_enabled() {
                let key = trace_key(
                    &catch_up_message_id(&target.message_id, target),
                    &target.conversation_id,
                    target.seq,
                );
                self.trace_handoff(
                    tenant_id,
                    user_id,
                    device_id,
                    &key,
                    &target.conversation_id,
                    handoff,
                );
                self.trace_suppressed(
                    tenant_id,
                    user_id,
                    &key,
                    &target.conversation_id,
                    &[device_id.to_string()],
                    AlertClass::Unspecified,
                    preference,
                );
            }
            return Ok(false);
        }

        let loaded = self.handoff_message(ctx, target, user_id).await;
        // 决策链的键就是补提示任务带的消息标识（见 `catch_up_message_id`）。
        let message_id = catch_up_message_id(
            loaded
                .as_ref()
                .map_or(target.message_id.as_str(), |message| {
                    message.server_id.as_str()
                }),
            target,
        );
        let handoff_key = trace_key(&message_id, &target.conversation_id, target.seq);
        self.trace_handoff(
            tenant_id,
            user_id,
            device_id,
            &handoff_key,
            &target.conversation_id,
            handoff,
        );
        if let Some(message) = loaded.as_ref() {
            // 自己从别的设备发出的消息也会写到这条连接上，但不该让自己的手机响。
            if message.sender_id == user_id {
                self.trace_entry(
                    tenant_id,
                    user_id,
                    &handoff_key,
                    &target.conversation_id,
                    device_id,
                    TraceStage::Decide,
                    &[
                        (
                            "class",
                            super::trace::class_label(message_alert_class(message)),
                        ),
                        ("decision", "drop"),
                        ("reason", "self"),
                    ],
                );
                return Ok(false);
            }
            if !class_alerts(message_alert_class(message)) {
                return Ok(false);
            }
        }
        let message = loaded.unwrap_or_else(|| placeholder_message(target));
        let mentions = mentions_of_message(&message);
        if let Some(reason) = push_suppressed_by_preference(preference, mentions.mentions(user_id))
        {
            tracing::debug!(
                user_id = %user_id,
                conversation_id = %target.conversation_id,
                reason,
                "Skipping catch-up alert"
            );
            self.trace_suppressed(
                tenant_id,
                user_id,
                &handoff_key,
                &target.conversation_id,
                &[device_id.to_string()],
                recipient_alert_class(message_alert_class(&message), mentions.names(user_id)),
                preference,
            );
            return Ok(false);
        }

        let alert_class =
            recipient_alert_class(message_alert_class(&message), mentions.names(user_id));
        let conversation_seq = if message.conversation_seq == 0 {
            target.seq
        } else {
            message.conversation_seq
        };
        let facts = AlertFacts {
            class: alert_class,
            mentions: MentionTargets::default(),
            conversation_seq,
            message_id: message_id.clone(),
            sender_id: message.sender_id.clone(),
            conversation_kind: conversation_kind(
                message.conversation_type,
                &target.conversation_id,
            ),
        };
        let push_payload = access_gateway::PushMessageRequest {
            user_ids: vec![user_id.to_string()],
            messages: vec![message],
            options: None,
        }
        .encode_to_vec();
        let headers = HashMap::new();
        let template = PushTaskTemplate {
            message_id: &message_id,
            conversation_id: &target.conversation_id,
            tenant_id,
            priority: CATCH_UP_PRIORITY,
            expire_at: None,
            push_payload: &push_payload,
            headers: &headers,
            payload_kind: PushTaskPayloadKind::Message as i32,
        };
        let tasks = self
            .catch_up_tasks(
                ctx,
                &template,
                &facts,
                super::alerting::GatedRecipient {
                    user_id: user_id.to_string(),
                    devices: vec![device_id.to_string()],
                    push_payload: push_payload.clone(),
                    alert_class,
                    mentioned: mentions.mention_state(user_id),
                    preference,
                    times: 1,
                    lane: OfflineLane::Bulk,
                },
            )
            .await;
        for task in tasks {
            let (disposition, lane, trace) = (task.disposition, task.lane, task.trace);
            self.publisher
                .publish_offline_task(ctx, lane, Some(user_id), task.payload)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::ServiceUnavailable,
                        "Failed to publish catch-up push task",
                    )
                })?;
            if let Some(metrics) = &self.metrics {
                metrics.record_offline_task(disposition_label(disposition));
                metrics.record_offline_lane(lane.label());
                metrics.record_catch_up_alert();
            }
            if let Some(trace) = trace.as_ref() {
                self.trace_task(user_id, trace, disposition, lane);
            }
        }
        Ok(true)
    }

    /// 补提示的任务。闸门接线时与正常提示一样先决策、再进闸门：设备刚失去链路，一连串补提示
    /// 也只响该响的那几声，已经提示过的同一条只静默更新；别的设备正看着时按多端设置推迟。
    /// 未接线时每个会话一条。补提示走批量通道。
    async fn catch_up_tasks(
        &self,
        ctx: &flare_server_core::context::Ctx,
        template: &PushTaskTemplate<'_>,
        facts: &AlertFacts,
        recipient: super::alerting::GatedRecipient,
    ) -> Vec<PendingPushTask> {
        let now_ms = now_millis();
        if self.notify_gate.is_some() {
            // 可达状态只用来判断别的设备是否正看着；读不到就当没有，补提示不因此重投。
            let user_ids = vec![recipient.user_id.clone()];
            let reachability = match self.load_reachability(ctx, &user_ids).await {
                Ok(reachability) => reachability,
                Err(error) => {
                    tracing::debug!(error = %error, "catch-up without device reachability");
                    HashMap::new()
                }
            };
            return self
                .gated_offline_tasks(ctx, template, facts, &reachability, vec![recipient], now_ms)
                .await;
        }
        let spec = OfflineTaskSpec {
            tenant_id: template.tenant_id,
            user_id: recipient.user_id.as_str(),
            conversation_id: template.conversation_id,
            message_id: template.message_id,
            alert_message_id: template.message_id,
            priority: template.priority,
            expire_at: template.expire_at,
            push_payload: recipient.push_payload.as_slice(),
            headers: template.headers,
            payload_kind: template.payload_kind,
            alert_class: recipient.alert_class,
            conversation_seq: facts.conversation_seq,
        };
        let trace_key = alert_trace_key(template, facts);
        recipient
            .devices
            .iter()
            .map(|device_id| {
                let envelope = offline_task(&spec, device_id, now_ms);
                PendingPushTask {
                    user_id: recipient.user_id.clone(),
                    trace: Some(TaskTrace::of(
                        &envelope,
                        template.conversation_id,
                        &trace_key,
                    )),
                    payload: envelope.encode_to_vec(),
                    online: false,
                    disposition: disposition_of(recipient.alert_class),
                    lane: recipient.lane,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
