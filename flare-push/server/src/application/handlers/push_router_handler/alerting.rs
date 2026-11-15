//! 防打扰：把提示规则的结论落成离线任务。
//!
//! - 路由：已过发送者、免打扰、「只接收@我」与可达性过滤的每台候选设备，先按决策（勿扰时段、
//!   多端、通知类等级）决定不提示、静默、候选、推迟还是只更新角标，再交给闸门按会话槽位与设备
//!   预算决定响铃、静默更新、合并还是只计数。
//! - 修正：会话在别处被读，给这台设备发清除并更新角标；撤回的正是设备上展示的那条，用撤回文案
//!   替换。只对确实推过、槽位仍有效的设备发，成本有界。
//! - 到期：吞并的消息在间隔结束补发一次尾部刷新；推迟的提示到期后进闸门；群会话的波次到期后
//!   枚举一次接收者（见 `waves`）。
//!
//! 规则只在 `flare-notify-rules` 里，状态读写在 [`NotifyGate`]；这里不做任何「该不该响」的判断。

use std::collections::BTreeSet;

use flare_im_contracts::domain::push_policy::{
    conversation_collapse_key, device_digest_collapse_key,
};
use flare_notify_rules::{
    ConversationPrefs, DecideInput, Decision, DigestAlert, DueAction, SlotAction, Tier, UserPrefs,
    decide,
};
use flare_proto::common::{Message, event};

use super::trace::{TaskTrace, alert_trace_key};
use super::*;
use crate::application::notify_gate::{Admission, AdmitRequest};
use crate::domain::model::{
    BadgeChange, DelayedAlert, DeviceProfile, DeviceSignals, DueClaim, DueEntry, DueKind,
    GateDevice, GateSlotKey, SlotMessage,
};
use crate::domain::notify_inputs::{
    counts_toward_badge, local_minute_of_week, preview_to_proto, rules_alert_class,
};
use crate::domain::repository::BadgeWrite;
use flare_im_contracts::push_trace::{TraceStage, trace_key};

/// 尾部刷新、推迟提示与修正任务的优先级，与单聊消息的离线任务相同。
const GATED_TASK_PRIORITY: i32 = 5;

/// 一位已过发送者、免打扰与「只接收@我」过滤的接收者，接下来由提示规则逐设备处理。
pub(super) struct GatedRecipient {
    pub(super) user_id: String,
    pub(super) devices: Vec<String>,
    /// 已收敛成只含本人的载荷。
    pub(super) push_payload: Vec<u8>,
    pub(super) alert_class: AlertClass,
    pub(super) mentioned: flare_notify_rules::Mentioned,
    pub(super) preference: NotifyPreference,
    /// 按几条消息进闸门：平时是 1，会话波次的一批是这一批里还没推给他的条数。
    pub(super) times: u32,
    /// 这位接收者的任务走哪条优先级通道（摘要另走批量通道）。
    pub(super) lane: OfflineLane,
}

/// 一批事件里要修正的已发通知。
#[derive(Debug, Default)]
pub(super) struct EventCorrections {
    /// （会话，读者）→ 读到的 seq，同一批里取最大。
    reads: HashMap<(String, String), u64>,
    /// （会话，被撤回的消息）。
    recalls: Vec<(String, String)>,
}

impl EventCorrections {
    pub(super) fn of(req: &access_gateway::PushEventRequest) -> Self {
        let mut out = Self::default();
        for item in &req.events {
            let conversation_of = |own: &str| {
                [
                    own,
                    item.conversation_id.as_str(),
                    req.conversation_id.as_str(),
                ]
                .into_iter()
                .find(|id| !id.trim().is_empty())
                .unwrap_or_default()
                .to_string()
            };
            match &item.payload {
                Some(event::Payload::Read(read))
                    if item.r#type == EventType::EventReadReceipt as i32 =>
                {
                    let conversation_id = conversation_of(&read.conversation_id);
                    if conversation_id.is_empty() || read.user_id.trim().is_empty() {
                        continue;
                    }
                    let seq = out
                        .reads
                        .entry((conversation_id, read.user_id.clone()))
                        .or_insert(0);
                    *seq = (*seq).max(read.read_seq);
                }
                Some(event::Payload::Recall(recall))
                    if item.r#type == EventType::EventMessageRecall as i32 =>
                {
                    let conversation_id = conversation_of("");
                    if conversation_id.is_empty() || recall.server_msg_id.trim().is_empty() {
                        continue;
                    }
                    out.recalls
                        .push((conversation_id, recall.server_msg_id.clone()));
                }
                _ => {}
            }
        }
        out.recalls.sort();
        out.recalls.dedup();
        out
    }

    pub(super) fn is_empty(&self) -> bool {
        self.reads.is_empty() && self.recalls.is_empty()
    }
}

/// 不走 [`offline_task_shaped`] 的任务（清除、只更新角标）：没有消息内容。
struct BareTask<'a> {
    tenant_id: &'a str,
    user_id: &'a str,
    device_id: &'a str,
    conversation_id: &'a str,
    message_id: String,
    seq: u64,
    disposition: PushDisposition,
    badge: Option<u32>,
    /// 用会话的合并键：清除要按它找到那条通知。只更新角标的任务不带，免得替换掉通知。
    collapse: bool,
    lane: OfflineLane,
    /// 记到哪条消息的决策链上；`None` 不记。
    trace_key: Option<String>,
}

fn bare_task(task: BareTask<'_>, now_ms: i64) -> PendingPushTask {
    let collapse_key = if task.collapse {
        conversation_collapse_key(task.tenant_id, task.conversation_id)
    } else {
        String::new()
    };
    let push_payload = access_gateway::PushMessageRequest {
        user_ids: vec![task.user_id.to_string()],
        messages: Vec::new(),
        options: None,
    }
    .encode_to_vec();
    let envelope = PushTaskEnvelope {
        user_id: task.user_id.to_string(),
        message_id: task.message_id.clone(),
        conversation_id: task.conversation_id.to_string(),
        tenant_id: task.tenant_id.to_string(),
        priority: GATED_TASK_PRIORITY,
        expire_at: Some(now_ms + default_task_ttl_ms(task.disposition)),
        push_payload,
        headers: HashMap::new(),
        payload_kind: PushTaskPayloadKind::Message as i32,
        device_id: task.device_id.to_string(),
        idempotency_key: offline_idempotency_key(
            task.tenant_id,
            task.user_id,
            task.device_id,
            &collapse_key,
            task.disposition,
            task.seq,
            &task.message_id,
        ),
        thread_key: collapse_key.clone(),
        collapse_key,
        disposition: task.disposition as i32,
        alert_class: AlertClass::None as i32,
        count: 0,
        badge: task.badge,
        conversation_seq: task.seq,
        preview: NotificationPreview::Unspecified as i32,
        digest_conversations: 0,
    };
    PendingPushTask {
        user_id: task.user_id.to_string(),
        trace: task
            .trace_key
            .as_deref()
            .map(|key| TaskTrace::of(&envelope, task.conversation_id, key)),
        payload: envelope.encode_to_vec(),
        online: false,
        disposition: task.disposition,
        lane: task.lane,
    }
}

fn pending(
    spec: &OfflineTaskSpec<'_>,
    device_id: &str,
    now_ms: i64,
    shape: TaskShape,
    lane: OfflineLane,
    trace_key: &str,
) -> PendingPushTask {
    let envelope = offline_task_shaped(spec, device_id, now_ms, shape);
    PendingPushTask {
        user_id: spec.user_id.to_string(),
        trace: Some(TaskTrace::of(&envelope, spec.conversation_id, trace_key)),
        payload: envelope.encode_to_vec(),
        online: false,
        disposition: shape.disposition,
        lane,
    }
}

/// 摘要通知：「N 个会话有 M 条新消息」。一台设备一条（设备级合并键），负责响铃；
/// 各会话自己的通知照常静默更新。走批量通道。
fn digest_task(
    spec: &OfflineTaskSpec<'_>,
    device_id: &str,
    now_ms: i64,
    digest: DigestAlert,
    preview: NotificationPreview,
    badge: Option<u32>,
    trace_key: &str,
) -> PendingPushTask {
    let collapse_key = device_digest_collapse_key(spec.tenant_id, spec.user_id, device_id);
    let alert_message_id = if spec.alert_message_id.is_empty() {
        spec.message_id
    } else {
        spec.alert_message_id
    };
    let envelope = PushTaskEnvelope {
        user_id: spec.user_id.to_string(),
        message_id: alert_message_id.to_string(),
        // 摘要跨会话，点开回到会话列表。
        conversation_id: String::new(),
        tenant_id: spec.tenant_id.to_string(),
        priority: spec.priority,
        expire_at: spec
            .expire_at
            .filter(|expire_at| *expire_at > 0)
            .or(Some(now_ms + default_task_ttl_ms(PushDisposition::Digest))),
        push_payload: spec.push_payload.to_vec(),
        headers: spec.headers.clone(),
        payload_kind: spec.payload_kind,
        device_id: device_id.to_string(),
        idempotency_key: offline_idempotency_key(
            spec.tenant_id,
            spec.user_id,
            device_id,
            &collapse_key,
            PushDisposition::Digest,
            spec.conversation_seq,
            alert_message_id,
        ),
        thread_key: collapse_key.clone(),
        collapse_key,
        disposition: PushDisposition::Digest as i32,
        alert_class: AlertClass::Normal as i32,
        count: digest.messages,
        badge,
        // 不同会话的 seq 不可比，摘要不带，端上不会因此丢掉一条更新。
        conversation_seq: 0,
        preview: preview as i32,
        digest_conversations: u32::from(digest.conversations),
    };
    PendingPushTask {
        user_id: spec.user_id.to_string(),
        trace: Some(TaskTrace::of(&envelope, spec.conversation_id, trace_key)),
        payload: envelope.encode_to_vec(),
        online: false,
        disposition: PushDisposition::Digest,
        lane: OfflineLane::Bulk,
    }
}

/// 闸门的结论落成任务：响铃或静默更新会话通知（走 `lane`），摘要模式下另发一条摘要（走批量
/// 通道）；被合并的不发。
#[allow(clippy::too_many_arguments)]
fn admitted_tasks(
    spec: &OfflineTaskSpec<'_>,
    device_id: &str,
    now_ms: i64,
    admission: Admission,
    preview: NotificationPreview,
    ios: bool,
    lane: OfflineLane,
    trace_key: &str,
) -> Vec<PendingPushTask> {
    let badge = admission.badge.filter(|_| ios);
    let mut tasks = Vec::new();
    let disposition = match admission.slot {
        SlotAction::Loud { .. } => Some(PushDisposition::Loud),
        SlotAction::Quiet { .. } => Some(PushDisposition::Quiet),
        SlotAction::Defer { .. } | SlotAction::CountOnly { .. } => None,
    };
    if let Some(disposition) = disposition {
        tasks.push(pending(
            spec,
            device_id,
            now_ms,
            TaskShape {
                disposition,
                count: admission.slot.count(),
                preview,
                badge,
            },
            lane,
            trace_key,
        ));
    }
    if let Some(digest) = admission.digest {
        tasks.push(digest_task(
            spec, device_id, now_ms, digest, preview, badge, trace_key,
        ));
    }
    tasks
}

fn profile_of(
    profiles: &HashMap<String, Vec<DeviceProfile>>,
    user_id: &str,
    device_id: &str,
) -> DeviceProfile {
    profiles
        .get(user_id)
        .and_then(|profiles| {
            profiles
                .iter()
                .find(|profile| profile.device_id == device_id)
        })
        .cloned()
        .unwrap_or_else(|| DeviceProfile::unknown(device_id))
}

/// 读不回消息时代替它的占位：只有标识。Worker 对没有内容的消息用通用文案。
fn placeholder(conversation_id: &str, latest: &SlotMessage) -> Message {
    Message {
        server_id: latest.message_id.clone(),
        conversation_id: conversation_id.to_string(),
        conversation_seq: latest.seq,
        ..Default::default()
    }
}

fn alert_class_of(code: i32) -> AlertClass {
    AlertClass::try_from(code)
        .ok()
        .filter(|class| class_alerts(*class))
        .unwrap_or(AlertClass::Normal)
}

impl PushRouterHandler {
    /// 这批用户有推送绑定的设备与登记信息。读不到时返回空表：设备照样推，只是时区按 UTC、
    /// 不带角标。
    async fn gate_device_profiles(
        &self,
        ctx: &flare_server_core::context::Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> HashMap<String, Vec<DeviceProfile>> {
        match self
            .device_bindings
            .device_profiles(ctx, tenant_id, user_ids)
            .await
        {
            Ok(profiles) => profiles,
            Err(error) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_binding_lookup_failure();
                }
                tracing::warn!(error = %error, "device binding profiles unavailable");
                HashMap::new()
            }
        }
    }

    async fn gate_device_profile(
        &self,
        ctx: &flare_server_core::context::Ctx,
        device: &GateDevice,
    ) -> DeviceProfile {
        let profiles = self
            .gate_device_profiles(
                ctx,
                &device.tenant_id,
                std::slice::from_ref(&device.user_id),
            )
            .await;
        profile_of(&profiles, &device.user_id, &device.device_id)
    }

    async fn gate_device_signals(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        device: &GateDevice,
    ) -> DeviceSignals {
        let pair = (device.user_id.clone(), device.device_id.clone());
        gate.device_signals(ctx, &device.tenant_id, std::slice::from_ref(&pair))
            .await
            .remove(&pair)
            .unwrap_or_default()
    }

    async fn gate_preview(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        tenant_id: &str,
        user_id: &str,
    ) -> NotificationPreview {
        let prefs = gate
            .user_prefs(ctx, tenant_id, &[user_id.to_string()])
            .await;
        let user = prefs.get(user_id).copied().unwrap_or_default();
        preview_to_proto(gate.preview(tenant_id, user.preview))
    }

    /// 读回要展示的那条消息。读不到（没接线、已撤回、读取失败）是 `None`，调用方用占位继续。
    async fn gate_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        seq: u64,
        user_id: &str,
    ) -> Option<Message> {
        let lookup = self.message_lookup.as_ref()?;
        if seq == 0 {
            return None;
        }
        match lookup.message_at(ctx, conversation_id, seq, user_id).await {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %conversation_id,
                    seq,
                    "gated alert message lookup failed; alerting with generic content"
                );
                None
            }
        }
    }

    async fn publish_gated(
        &self,
        ctx: &flare_server_core::context::Ctx,
        task: PendingPushTask,
    ) -> Result<()> {
        let PendingPushTask {
            user_id,
            payload,
            disposition,
            lane,
            trace,
            ..
        } = task;
        self.publisher
            .publish_offline_task(ctx, lane, Some(user_id.as_str()), payload)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to publish gated push task",
                )
            })?;
        if let Some(metrics) = &self.metrics {
            metrics.record_offline_task(disposition_label(disposition));
            metrics.record_offline_lane(lane.label());
        }
        if let Some(trace) = trace.as_ref() {
            self.trace_task(&user_id, trace, disposition, lane);
        }
        Ok(())
    }

    /// 每位接收者的每台候选设备过一遍提示规则与闸门，给出要发的离线任务。
    ///
    /// 不会失败：设置、设备信号、绑定登记读不到都用默认值，闸门状态用不上就按会话第一条放行。
    pub(super) async fn gated_offline_tasks(
        &self,
        ctx: &flare_server_core::context::Ctx,
        template: &PushTaskTemplate<'_>,
        facts: &AlertFacts,
        reachability: &HashMap<String, UserReachability>,
        recipients: Vec<GatedRecipient>,
        now_ms: i64,
    ) -> Vec<PendingPushTask> {
        let Some(gate) = self.notify_gate.as_ref() else {
            return Vec::new();
        };
        let tenant_id = template.tenant_id;
        let user_ids: Vec<String> = recipients
            .iter()
            .map(|recipient| recipient.user_id.clone())
            .collect();
        // 一次路由各读一回：用户设置（带缓存）、设备登记（带缓存）、设备信号。
        let prefs = gate.user_prefs(ctx, tenant_id, &user_ids).await;
        let profiles = self.gate_device_profiles(ctx, tenant_id, &user_ids).await;
        let pairs: Vec<(String, String)> = recipients
            .iter()
            .flat_map(|recipient| {
                recipient
                    .devices
                    .iter()
                    .map(move |device_id| (recipient.user_id.clone(), device_id.clone()))
            })
            .collect();
        let signals = gate.device_signals(ctx, tenant_id, &pairs).await;

        let jobs: Vec<(&GatedRecipient, &String)> = recipients
            .iter()
            .flat_map(|recipient| {
                recipient
                    .devices
                    .iter()
                    .map(move |device_id| (recipient, device_id))
            })
            .collect();
        // 先把每台设备的 Future 建好再并发：流里不留借用的闭包，Future 才能证明自己可以跨线程。
        let gated: Vec<_> = jobs
            .into_iter()
            .map(|(recipient, device_id)| {
                let user = prefs.get(&recipient.user_id).copied().unwrap_or_default();
                let profile = profile_of(&profiles, &recipient.user_id, device_id);
                let device_signals = signals
                    .get(&(recipient.user_id.clone(), device_id.clone()))
                    .copied()
                    .unwrap_or_default();
                let other_device_attentive = reachability
                    .get(&recipient.user_id)
                    .is_some_and(|reach| reach.other_device_attentive(device_id));
                self.gate_device(
                    gate,
                    GatedDevice {
                        template,
                        facts,
                        recipient,
                        profile,
                        user,
                        signals: device_signals,
                        other_device_attentive,
                    },
                    now_ms,
                )
            })
            .collect();
        stream::iter(gated)
            .buffer_unordered(PUSH_TASK_PUBLISH_CONCURRENCY)
            .collect::<Vec<Vec<PendingPushTask>>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// 一台候选设备：决策，再按决策进闸门、推迟或只更新角标。
    async fn gate_device(
        &self,
        gate: &NotifyGate,
        device: GatedDevice<'_>,
        now_ms: i64,
    ) -> Vec<PendingPushTask> {
        let GatedDevice {
            template,
            facts,
            recipient,
            profile,
            user,
            signals,
            other_device_attentive,
        } = device;
        let tenant_id = template.tenant_id;
        let input = DecideInput {
            class: rules_alert_class(recipient.alert_class),
            mentioned: recipient.mentioned,
            from_self: facts.sender_id == recipient.user_id,
            conversation: ConversationPrefs {
                muted: recipient.preference.muted,
                mention_only: recipient.preference.mention_only,
            },
            user,
            local_time: Some(local_minute_of_week(&profile.time_zone, now_ms)),
            other_device_attentive,
        };
        let decision = decide(&input);
        gate.record_decision(decision);
        let trace_key = alert_trace_key(template, facts);
        self.trace_decision(
            tenant_id,
            &recipient.user_id,
            &trace_key,
            template.conversation_id,
            &profile.device_id,
            recipient.alert_class,
            decision,
            &input,
        );

        let spec = OfflineTaskSpec {
            tenant_id,
            user_id: recipient.user_id.as_str(),
            conversation_id: template.conversation_id,
            message_id: template.message_id,
            alert_message_id: facts.message_id.as_str(),
            priority: template.priority,
            expire_at: template.expire_at,
            push_payload: recipient.push_payload.as_slice(),
            headers: template.headers,
            payload_kind: template.payload_kind,
            alert_class: recipient.alert_class,
            conversation_seq: facts.conversation_seq,
        };
        let preview = preview_to_proto(gate.preview(tenant_id, user.preview));
        let device_id = profile.device_id.as_str();

        // 不属于任何会话的推送（系统通知、自定义推送）没有槽位：只按决策响不响，
        // 不合并、不推迟、不计角标。
        if template.conversation_id.trim().is_empty() {
            let disposition = match decision.without_delay() {
                Decision::Drop | Decision::BadgeOnly => return Vec::new(),
                Decision::Quiet => PushDisposition::Quiet,
                _ => disposition_of(recipient.alert_class),
            };
            return vec![pending(
                &spec,
                device_id,
                now_ms,
                TaskShape {
                    disposition,
                    count: 1,
                    preview,
                    badge: None,
                },
                recipient.lane,
                &trace_key,
            )];
        }

        let key = GateSlotKey::new(
            GateDevice::new(tenant_id, recipient.user_id.as_str(), device_id),
            template.conversation_id,
        );
        let message = SlotMessage {
            message_id: if facts.message_id.is_empty() {
                template.message_id.to_string()
            } else {
                facts.message_id.clone()
            },
            seq: facts.conversation_seq,
            alert_class: recipient.alert_class as i32,
        };
        let times = recipient.times.max(1);
        // 角标表只为 iOS 设备维护：只有它的推送带角标绝对值。一批进来的按条数加。
        let badge_change = (profile.ios && counts_toward_badge(recipient.alert_class))
            .then_some(BadgeChange::Add(times));

        let tier = match decision {
            Decision::Drop => return Vec::new(),
            Decision::BadgeOnly => {
                if !profile.ios {
                    return Vec::new();
                }
                let write = BadgeWrite {
                    change: badge_change.unwrap_or(BadgeChange::Get),
                    base: signals.badge_base,
                };
                let Some(badge) = gate.badge(&key.device, &key.conversation_id, write).await else {
                    return Vec::new();
                };
                return vec![bare_task(
                    BareTask {
                        tenant_id,
                        user_id: recipient.user_id.as_str(),
                        device_id,
                        conversation_id: template.conversation_id,
                        message_id: message.message_id.clone(),
                        seq: message.seq,
                        disposition: PushDisposition::BadgeOnly,
                        badge: Some(badge),
                        collapse: false,
                        lane: recipient.lane,
                        trace_key: Some(trace_key.clone()),
                    },
                    now_ms,
                )];
            }
            Decision::Delay { delay_ms, then } => {
                let alert = DelayedAlert {
                    key: key.clone(),
                    kind: facts.conversation_kind,
                    tier: then,
                    latest: message.clone(),
                    count: times,
                    created_ms: now_ms,
                    due_ms: now_ms.saturating_add(delay_ms),
                };
                let badge = badge_change.map(|change| BadgeWrite {
                    change,
                    base: signals.badge_base,
                });
                match gate.defer(&alert, delay_ms, badge).await {
                    Ok(_) => return Vec::new(),
                    Err(error) => {
                        // 无处登记到期：宁可现在提示，不丢提示。
                        tracing::warn!(error = %error, "delayed alert not registered; alerting now");
                        then
                    }
                }
            }
            other => other.tier().unwrap_or(Tier::Normal),
        };

        let admission = gate
            .admit(
                now_ms,
                &AdmitRequest {
                    key,
                    kind: facts.conversation_kind,
                    tier,
                    times,
                    message,
                    signals,
                    badge: badge_change,
                },
            )
            .await;
        self.trace_admission(
            tenant_id,
            &recipient.user_id,
            &trace_key,
            template.conversation_id,
            device_id,
            &admission,
        );
        admitted_tasks(
            &spec,
            device_id,
            now_ms,
            admission,
            preview,
            profile.ios,
            recipient.lane,
            &trace_key,
        )
    }

    /// 已读与撤回的修正。失败只记日志：修正是尽力而为，打开应用时会再校准一次。
    pub(super) async fn apply_event_corrections(
        &self,
        ctx: &flare_server_core::context::Ctx,
        corrections: EventCorrections,
    ) {
        let Some(gate) = self.notify_gate.as_ref() else {
            return;
        };
        let Ok(tenant_id) = self.request_tenant_id(ctx) else {
            return;
        };
        let now_ms = now_millis();
        for ((conversation_id, reader), read_seq) in &corrections.reads {
            let tasks = self
                .read_corrections(
                    ctx,
                    gate,
                    tenant_id,
                    conversation_id,
                    reader,
                    *read_seq,
                    now_ms,
                )
                .await;
            self.publish_corrections(ctx, tasks).await;
        }
        for (conversation_id, message_id) in &corrections.recalls {
            let tasks = self
                .recall_corrections(ctx, gate, tenant_id, conversation_id, message_id, now_ms)
                .await;
            self.publish_corrections(ctx, tasks).await;
        }
    }

    async fn publish_corrections(
        &self,
        ctx: &flare_server_core::context::Ctx,
        tasks: Vec<PendingPushTask>,
    ) {
        for task in tasks {
            if let Err(error) = self.publish_gated(ctx, task).await {
                tracing::warn!(error = %error, "notification correction not published");
            }
        }
    }

    /// 读者的每台设备：取消推迟的提示，关闭槽位；确实推过的发清除，角标变了的 iOS 设备发新角标。
    #[allow(clippy::too_many_arguments)]
    async fn read_corrections(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        tenant_id: &str,
        conversation_id: &str,
        reader: &str,
        read_seq: u64,
        now_ms: i64,
    ) -> Vec<PendingPushTask> {
        let profiles = self
            .gate_device_profiles(ctx, tenant_id, &[reader.to_string()])
            .await;
        let devices = profiles.get(reader).cloned().unwrap_or_default();
        if devices.is_empty() {
            return Vec::new();
        }
        let pairs: Vec<(String, String)> = devices
            .iter()
            .map(|profile| (reader.to_string(), profile.device_id.clone()))
            .collect();
        let signals = gate.device_signals(ctx, tenant_id, &pairs).await;
        let mut tasks = Vec::new();
        for profile in &devices {
            let key = GateSlotKey::new(
                GateDevice::new(tenant_id, reader, profile.device_id.as_str()),
                conversation_id,
            );
            let device_signals = signals
                .get(&(reader.to_string(), profile.device_id.clone()))
                .copied()
                .unwrap_or_default();
            let verdict = gate
                .read(now_ms, &key, read_seq, device_signals, profile.ios)
                .await;
            if verdict.delay_cancelled {
                gate.record_due(DueKind::Delay, "cancelled");
            }
            let badge = verdict.badge.filter(|_| profile.ios);
            // 修正记在设备上正展示着的那条消息的决策链上；不知道是哪条（槽位已关）就不记。
            let shown_key = (!verdict.shown.message_id.trim().is_empty() || verdict.shown.seq > 0)
                .then(|| {
                    trace_key(
                        &verdict.shown.message_id,
                        conversation_id,
                        verdict.shown.seq,
                    )
                });
            let correction = |disposition, badge: Option<u32>, collapse| {
                bare_task(
                    BareTask {
                        tenant_id,
                        user_id: reader,
                        device_id: profile.device_id.as_str(),
                        conversation_id,
                        message_id: format!("read:{conversation_id}:{read_seq}"),
                        seq: read_seq,
                        disposition,
                        badge,
                        collapse,
                        // 修正走单聊通道：迟到的清除只会让设备上多挂一会儿已读的通知。
                        lane: OfflineLane::Direct,
                        trace_key: shown_key.clone(),
                    },
                    now_ms,
                )
            };
            let trace_correction = |kind: &str| {
                if let Some(key) = shown_key.as_deref() {
                    self.trace_entry(
                        tenant_id,
                        reader,
                        key,
                        conversation_id,
                        &profile.device_id,
                        TraceStage::Correction,
                        &[("kind", kind)],
                    );
                }
            };
            if verdict.outcome.clear {
                tasks.push(correction(
                    PushDisposition::Clear,
                    badge.map(|badge| badge.count),
                    true,
                ));
                gate.record_correction("clear");
                trace_correction("clear");
            } else if let Some(badge) = badge.filter(|badge| badge.changed) {
                tasks.push(correction(
                    PushDisposition::BadgeOnly,
                    Some(badge.count),
                    false,
                ));
                gate.record_correction("badge");
                trace_correction("badge_only");
            }
        }
        tasks
    }

    /// 展示过这条消息的设备：正在展示的仍是它时，用撤回文案替换，条数减一。
    async fn recall_corrections(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
        now_ms: i64,
    ) -> Vec<PendingPushTask> {
        let devices = gate
            .shown_devices(tenant_id, conversation_id, message_id)
            .await;
        if devices.is_empty() {
            return Vec::new();
        }
        let users: Vec<String> = devices
            .iter()
            .map(|device| device.user_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let profiles = self.gate_device_profiles(ctx, tenant_id, &users).await;
        let prefs = gate.user_prefs(ctx, tenant_id, &users).await;
        let headers = HashMap::new();
        let mut tasks = Vec::new();
        for device in devices {
            let ios = profile_of(&profiles, &device.user_id, &device.device_id).ios;
            let key = GateSlotKey::new(device.clone(), conversation_id);
            let Some(verdict) = gate.recall(now_ms, &key, message_id, ios).await else {
                continue;
            };
            let message = self
                .gate_message(ctx, conversation_id, verdict.shown.seq, &device.user_id)
                .await
                .unwrap_or_else(|| placeholder(conversation_id, &verdict.shown));
            let push_payload = access_gateway::PushMessageRequest {
                user_ids: vec![device.user_id.clone()],
                messages: vec![message],
                options: None,
            }
            .encode_to_vec();
            let spec = OfflineTaskSpec {
                tenant_id,
                user_id: device.user_id.as_str(),
                conversation_id,
                message_id,
                alert_message_id: message_id,
                priority: GATED_TASK_PRIORITY,
                expire_at: None,
                push_payload: push_payload.as_slice(),
                headers: &headers,
                payload_kind: PushTaskPayloadKind::Message as i32,
                alert_class: alert_class_of(verdict.shown.alert_class),
                conversation_seq: verdict.shown.seq,
            };
            let user: UserPrefs = prefs.get(&device.user_id).copied().unwrap_or_default();
            let preview = preview_to_proto(gate.preview(tenant_id, user.preview));
            let recalled_key = trace_key(message_id, conversation_id, verdict.shown.seq);
            tasks.push(pending(
                &spec,
                &device.device_id,
                now_ms,
                TaskShape {
                    disposition: PushDisposition::RecallReplace,
                    count: verdict.count,
                    preview,
                    badge: verdict.badge.filter(|_| ios),
                },
                OfflineLane::Direct,
                &recalled_key,
            ));
            gate.record_correction("recall_replace");
            self.trace_entry(
                tenant_id,
                &device.user_id,
                &recalled_key,
                conversation_id,
                &device.device_id,
                TraceStage::Correction,
                &[("kind", "recall_replace")],
            );
        }
        tasks
    }

    /// 到期循环领出的一项：尾部刷新、推迟到期的提示，或群会话的波次。
    ///
    /// `ctx` 是这一项所属租户的服务身份上下文。下游暂时不可用时报错，这一项就此丢掉：
    /// 闸门与波次是软状态，通知暂时显示旧的内容或计数、或这一批没有提示，打开应用时同步纠正。
    pub async fn handle_notify_due(
        &self,
        ctx: &flare_server_core::context::Ctx,
        claim: DueClaim,
    ) -> Result<()> {
        let tenant_id = self.request_tenant_id(ctx)?;
        if tenant_id != claim.tenant_id() {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "due entry tenant does not match routing context",
            )
            .build_error());
        }
        // 领取时已经判定到期。实例之间时钟有偏差时按到期时刻算，不让它再被推迟一轮。
        let now_ms = now_millis().max(claim.due_ms());
        let entry = match claim {
            DueClaim::Wave(wave) => {
                self.emit_due_wave(ctx, &wave, now_ms).await;
                return Ok(());
            }
            DueClaim::Slot(entry) => entry,
        };
        let Some(gate) = self.notify_gate.as_ref() else {
            return Ok(());
        };
        match entry.kind {
            DueKind::Tail => self.emit_tail(ctx, gate, &entry, now_ms).await,
            DueKind::Delay => self.emit_delayed(ctx, gate, &entry, now_ms).await,
        }
    }

    /// 尾部刷新：静默替换会话通知，显示最新一条与正确的条数。
    async fn emit_tail(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        entry: &DueEntry,
        now_ms: i64,
    ) -> Result<()> {
        let device = &entry.key.device;
        let profile = self.gate_device_profile(ctx, device).await;
        let signals = self.gate_device_signals(ctx, gate, device).await;
        let Some(verdict) = gate.tail_due(now_ms, entry, signals, profile.ios).await else {
            gate.record_due(DueKind::Tail, "nothing");
            return Ok(());
        };
        let count = match verdict.action {
            DueAction::Tail { count } => count,
            DueAction::Defer { .. } => {
                gate.record_due(DueKind::Tail, "rescheduled");
                return Ok(());
            }
            DueAction::Nothing => {
                gate.record_due(DueKind::Tail, "nothing");
                return Ok(());
            }
        };
        let conversation_id = entry.key.conversation_id.as_str();
        let latest = &verdict.side.latest;
        // 读不回来（还没落库、已被撤回）也照样发：通知的条数要对，文案用通用的。
        let message = self
            .gate_message(ctx, conversation_id, latest.seq, &device.user_id)
            .await
            .unwrap_or_else(|| placeholder(conversation_id, latest));
        let message_id = if latest.message_id.is_empty() {
            format!("tail:{conversation_id}:{}", latest.seq)
        } else {
            latest.message_id.clone()
        };
        let push_payload = access_gateway::PushMessageRequest {
            user_ids: vec![device.user_id.clone()],
            messages: vec![message],
            options: None,
        }
        .encode_to_vec();
        let headers = HashMap::new();
        let spec = OfflineTaskSpec {
            tenant_id: &device.tenant_id,
            user_id: &device.user_id,
            conversation_id,
            message_id: &message_id,
            alert_message_id: &message_id,
            priority: GATED_TASK_PRIORITY,
            expire_at: None,
            push_payload: &push_payload,
            headers: &headers,
            payload_kind: PushTaskPayloadKind::Message as i32,
            alert_class: alert_class_of(latest.alert_class),
            conversation_seq: latest.seq,
        };
        let preview = self
            .gate_preview(ctx, gate, &device.tenant_id, &device.user_id)
            .await;
        // 尾部刷新记在它展示的最新一条消息的决策链上：吞并的那几条到这里才更新到设备上。
        let tail_key = trace_key(&message_id, conversation_id, latest.seq);
        let tail_count = count.to_string();
        self.trace_entry(
            &device.tenant_id,
            &device.user_id,
            &tail_key,
            conversation_id,
            &device.device_id,
            TraceStage::Gate,
            &[("result", "quiet"), ("count", tail_count.as_str())],
        );
        // 尾部刷新只把通知改成最新的内容与条数，不响铃：走群通道，不挤占单聊。
        let task = pending(
            &spec,
            &device.device_id,
            now_ms,
            TaskShape {
                disposition: PushDisposition::Quiet,
                count,
                preview,
                badge: verdict.badge.filter(|_| profile.ios),
            },
            OfflineLane::Group,
            &tail_key,
        );
        self.publish_gated(ctx, task).await?;
        gate.record_due(DueKind::Tail, "emitted");
        Ok(())
    }

    /// 推迟到期：期间在别处读过就已经取消了；设备回过前台、或此刻由链路负责提示，也不再推。
    /// 其余这一批按条数进闸门。
    async fn emit_delayed(
        &self,
        ctx: &flare_server_core::context::Ctx,
        gate: &NotifyGate,
        entry: &DueEntry,
        now_ms: i64,
    ) -> Result<()> {
        let Some(alert) = gate.take_delayed(entry).await else {
            gate.record_due(DueKind::Delay, "nothing");
            return Ok(());
        };
        let device = &entry.key.device;
        let signals = self.gate_device_signals(ctx, gate, device).await;
        if signals.active_at_ms > alert.created_ms {
            gate.record_due(DueKind::Delay, "cancelled");
            return Ok(());
        }
        if let Ok(reachability) = self
            .load_reachability(ctx, std::slice::from_ref(&device.user_id))
            .await
            && reachability
                .get(&device.user_id)
                .is_some_and(|reach| reach.is_live(&device.device_id))
        {
            gate.record_due(DueKind::Delay, "cancelled");
            return Ok(());
        }
        let profile = self.gate_device_profile(ctx, device).await;
        let admission = gate
            .admit(
                now_ms,
                &AdmitRequest {
                    key: entry.key.clone(),
                    kind: alert.kind,
                    tier: alert.tier,
                    times: alert.count,
                    message: alert.latest.clone(),
                    signals,
                    badge: profile.ios.then_some(BadgeChange::Get),
                },
            )
            .await;
        let conversation_id = entry.key.conversation_id.as_str();
        let message = self
            .gate_message(ctx, conversation_id, alert.latest.seq, &device.user_id)
            .await
            .unwrap_or_else(|| placeholder(conversation_id, &alert.latest));
        let message_id = if alert.latest.message_id.is_empty() {
            format!("delay:{conversation_id}:{}", alert.latest.seq)
        } else {
            alert.latest.message_id.clone()
        };
        let push_payload = access_gateway::PushMessageRequest {
            user_ids: vec![device.user_id.clone()],
            messages: vec![message],
            options: None,
        }
        .encode_to_vec();
        let headers = HashMap::new();
        let spec = OfflineTaskSpec {
            tenant_id: &device.tenant_id,
            user_id: &device.user_id,
            conversation_id,
            message_id: &message_id,
            alert_message_id: &message_id,
            priority: GATED_TASK_PRIORITY,
            expire_at: None,
            push_payload: &push_payload,
            headers: &headers,
            payload_kind: PushTaskPayloadKind::Message as i32,
            alert_class: alert_class_of(alert.latest.alert_class),
            conversation_seq: alert.latest.seq,
        };
        let preview = self
            .gate_preview(ctx, gate, &device.tenant_id, &device.user_id)
            .await;
        // 推迟到期的这一批记在其中最新一条的决策链上（推迟时的决策也记在那里）。
        let delayed_key = trace_key(&message_id, conversation_id, alert.latest.seq);
        self.trace_admission(
            &device.tenant_id,
            &device.user_id,
            &delayed_key,
            conversation_id,
            &device.device_id,
            &admission,
        );
        for task in admitted_tasks(
            &spec,
            &device.device_id,
            now_ms,
            admission,
            preview,
            profile.ios,
            OfflineLane::of_delayed(alert.kind, alert.tier),
            &delayed_key,
        ) {
            self.publish_gated(ctx, task).await?;
        }
        gate.record_due(DueKind::Delay, "emitted");
        Ok(())
    }
}

/// 一台候选设备的输入。
struct GatedDevice<'a> {
    template: &'a PushTaskTemplate<'a>,
    facts: &'a AlertFacts,
    recipient: &'a GatedRecipient,
    profile: DeviceProfile,
    user: UserPrefs,
    signals: DeviceSignals,
    other_device_attentive: bool,
}

#[cfg(test)]
mod tests;
