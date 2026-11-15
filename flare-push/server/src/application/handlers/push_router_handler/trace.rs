//! 推送决策链：路由、交接、决策、闸门、建任务与修正，各在做出结论的地方记一条（格式与键见
//! `flare_im_contracts::push_trace`）。
//!
//! - 记录只进写入端的有界队列，不等 Redis、不会失败；写入端关掉时不拼记录。
//! - 每条都带 `conv`。键是离线任务带给设备的那个 `message_id`：要提示的那条消息的服务端 ID，
//!   读不回消息时是合成的占位标识，不属于会话的推送（通知、自定义推送）是任务的标识。Worker 与
//!   到达回执按同一个值记，三者落在同一条链上。
//! - 只记结论与标签，不记载荷、文案或令牌。

use std::collections::BTreeSet;

use flare_im_contracts::push_trace::{PushTraceEntry, TraceStage, trace_key};
use flare_im_service_kit::push_trace::PushTraceRecorder;
use flare_notify_rules::{DecideInput, Decision, SlotAction};

use super::*;
use crate::application::notify_gate::Admission;
use crate::domain::model::LinkState;

/// 一路路由的来由：按消息直接路由。
pub(super) const PATH_DIRECT: &str = "direct";
/// 群会话波次枚举到的成员。
pub(super) const PATH_WAVE: &str = "wave";
/// 超过成员上限的群枚举到的订阅者（含全员 @所有人 波次）。
pub(super) const PATH_SUBSCRIBER: &str = "subscriber";
/// 设备交接带来的未确认条目。
pub(super) const PATH_HANDOFF: &str = "handoff";
/// 网关失联后按未读会话补的提示。
pub(super) const PATH_CATCHUP: &str = "catchup";
/// 设备由链路负责提示。
const PATH_ONLINE: &str = "online";
/// 没有可用的推送绑定。
const PATH_NO_BINDING: &str = "no_binding";

/// 路由时要记下的：这条消息的键、会话，以及这一路的来由（`direct`、`wave`、`subscriber`）。
pub(super) struct RouteTrace<'a> {
    pub(super) trace_key: &'a str,
    pub(super) conversation_id: &'a str,
    pub(super) path: &'static str,
}

/// 一条离线任务发出后要记下的：哪台设备、属于哪条消息、合并键与角标。处置与通道在任务上。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskTrace {
    pub(super) tenant_id: String,
    pub(super) device_id: String,
    pub(super) conversation_id: String,
    pub(super) trace_key: String,
    pub(super) collapse: String,
    pub(super) badge: Option<u32>,
}

impl TaskTrace {
    /// 由要发出的信封得出。`conversation_id` 取消息所在的会话：摘要的信封不带会话。
    pub(super) fn of(envelope: &PushTaskEnvelope, conversation_id: &str, trace_key: &str) -> Self {
        Self {
            tenant_id: envelope.tenant_id.clone(),
            device_id: envelope.device_id.clone(),
            conversation_id: conversation_id.to_string(),
            trace_key: trace_key.to_string(),
            collapse: envelope.collapse_key.clone(),
            badge: envelope.badge,
        }
    }
}

/// 一条消息在决策链里的键（见模块说明）：与 `offline_task_shaped` 给信封的 `message_id` 同一个取法。
pub(super) fn alert_trace_key(template: &PushTaskTemplate<'_>, facts: &AlertFacts) -> String {
    let message_id = if facts.message_id.is_empty() {
        template.message_id
    } else {
        facts.message_id.as_str()
    };
    trace_key(message_id, template.conversation_id, facts.conversation_seq)
}

/// 提示等级在决策链里的名字。
pub(super) fn class_label(class: AlertClass) -> &'static str {
    match class {
        AlertClass::Unspecified => "unspecified",
        AlertClass::None => "none",
        AlertClass::Silent => "silent",
        AlertClass::Normal => "normal",
        AlertClass::Mention => "mention",
        AlertClass::Urgent => "urgent",
    }
}

/// 决策在决策链里的名字：点名与紧急的候选也是 `candidate`，等级另见 `class`。
fn decision_label(decision: Decision) -> &'static str {
    match decision {
        Decision::Drop => "drop",
        Decision::Quiet => "quiet",
        Decision::Candidate { .. } => "candidate",
        Decision::Delay { .. } => "delay",
        Decision::BadgeOnly => "badge_only",
    }
}

/// 决策的原因：哪一项输入让结论成了这样。只读规则的输入与结论，不重新做决策。
fn decision_reason(decision: Decision, input: &DecideInput) -> &'static str {
    match decision {
        Decision::Drop if input.from_self => "self",
        Decision::Drop if input.conversation.muted => "muted",
        Decision::Drop if input.conversation.mention_only => "mention_only",
        Decision::Quiet if input.user.active_quiet_hours(input.local_time).is_some() => {
            "quiet_hours"
        }
        Decision::Delay { .. } => "other_device_active",
        Decision::BadgeOnly => "policy_badge_only",
        _ => "none",
    }
}

/// 优先级通道在决策链里的名字：单聊通道就是原来的离线主题。
pub(super) fn lane_label(lane: OfflineLane) -> &'static str {
    match lane {
        OfflineLane::Direct => "offline",
        OfflineLane::Group => "group",
        OfflineLane::Bulk => "bulk",
    }
}

/// 闸门结论在决策链里的名字：状态用不上而放行的是 `fail_open`，进入摘要的是 `digest`。
fn admission_label(admission: &Admission) -> &'static str {
    if admission.fail_open {
        return "fail_open";
    }
    if admission.digest.is_some() {
        return "digest";
    }
    match admission.slot {
        SlotAction::Loud { .. } => "loud",
        SlotAction::Quiet { .. } => "quiet",
        SlotAction::Defer { .. } => "defer",
        SlotAction::CountOnly { .. } => "count_only",
    }
}

impl PushRouterHandler {
    /// 接入推送决策链的写入端。不接线时什么都不记。
    pub fn with_trace(mut self, trace: PushTraceRecorder) -> Self {
        self.trace = trace;
        self
    }

    pub(super) fn tracing_enabled(&self) -> bool {
        self.trace.is_enabled()
    }

    /// 记一条：`fields` 之外总带上 `conv`。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_entry(
        &self,
        tenant_id: &str,
        user_id: &str,
        trace_key: &str,
        conversation_id: &str,
        device_id: &str,
        stage: TraceStage,
        fields: &[(&str, &str)],
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let mut entry =
            PushTraceEntry::new(now_millis(), stage, device_id).with_conversation(conversation_id);
        for (key, value) in fields {
            entry = entry.with(*key, *value);
        }
        self.trace.record(tenant_id, user_id, trace_key, entry);
    }

    /// 路由：这位接收者的每台设备当时的状态，以及它的提示归谁。
    ///
    /// - 由链路负责提示的设备：`path = online`；
    /// - 有推送绑定、不由链路负责的设备：`path` 是这一路的来由，状态缺省为 `parked`（链路已断）；
    /// - 连着、却没有推送绑定的设备：`path = no_binding`；
    /// - 一台设备都认不出时记一条不带设备的 `no_binding`：没有会话是 `dark`，有会话却认不出设备是
    ///   `unknown`。
    pub(super) fn trace_route(
        &self,
        tenant_id: &str,
        route: &RouteTrace<'_>,
        user_id: &str,
        bound: &[String],
        reach: Option<&UserReachability>,
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let bound_set: BTreeSet<&str> = bound.iter().map(String::as_str).collect();
        let mut devices = bound_set.clone();
        if let Some(reach) = reach {
            devices.extend(reach.live_devices.iter().map(String::as_str));
            devices.extend(reach.link_states.keys().map(String::as_str));
        }
        let trace = |device_id: &str, state: &str, path: &str| {
            self.trace_entry(
                tenant_id,
                user_id,
                route.trace_key,
                route.conversation_id,
                device_id,
                TraceStage::Route,
                &[("state", state), ("path", path)],
            );
        };
        if devices.is_empty() {
            let state = if reach.is_some_and(|reach| reach.has_session) {
                "unknown"
            } else {
                "dark"
            };
            trace("", state, PATH_NO_BINDING);
            return;
        }
        for device_id in devices {
            let link_state = reach.and_then(|reach| reach.link_state(device_id));
            let live = reach.is_some_and(|reach| reach.is_live(device_id));
            if live {
                trace(
                    device_id,
                    link_state.map_or("active", LinkState::label),
                    PATH_ONLINE,
                );
            } else if bound_set.contains(device_id) {
                trace(
                    device_id,
                    link_state.map_or("parked", LinkState::label),
                    route.path,
                );
            } else {
                trace(
                    device_id,
                    link_state.map_or("dark", LinkState::label),
                    PATH_NO_BINDING,
                );
            }
        }
    }

    /// 决策：规则的结论与原因。推迟的带推迟时长。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_decision(
        &self,
        tenant_id: &str,
        user_id: &str,
        trace_key: &str,
        conversation_id: &str,
        device_id: &str,
        class: AlertClass,
        decision: Decision,
        input: &DecideInput,
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let delay_ms = match decision {
            Decision::Delay { delay_ms, .. } => delay_ms.to_string(),
            _ => String::new(),
        };
        let mut fields = vec![
            ("class", class_label(class)),
            ("decision", decision_label(decision)),
            ("reason", decision_reason(decision, input)),
        ];
        if !delay_ms.is_empty() {
            fields.push(("delay_ms", delay_ms.as_str()));
        }
        self.trace_entry(
            tenant_id,
            user_id,
            trace_key,
            conversation_id,
            device_id,
            TraceStage::Decide,
            &fields,
        );
    }

    /// 决策之前就被会话偏好挡掉（没接闸门时，或大群波次里）：记成不提示，原因是免打扰或只接收@我。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_suppressed(
        &self,
        tenant_id: &str,
        user_id: &str,
        trace_key: &str,
        conversation_id: &str,
        devices: &[String],
        class: AlertClass,
        preference: NotifyPreference,
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let reason = if preference.muted {
            "muted"
        } else {
            "mention_only"
        };
        for device_id in devices {
            self.trace_entry(
                tenant_id,
                user_id,
                trace_key,
                conversation_id,
                device_id,
                TraceStage::Decide,
                &[
                    ("class", class_label(class)),
                    ("decision", "drop"),
                    ("reason", reason),
                ],
            );
        }
    }

    /// 闸门：结论、条数，推迟到期时刻（吞并时）。
    pub(super) fn trace_admission(
        &self,
        tenant_id: &str,
        user_id: &str,
        trace_key: &str,
        conversation_id: &str,
        device_id: &str,
        admission: &Admission,
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let count = admission.slot.count().to_string();
        let due_ms = match admission.slot {
            SlotAction::Defer { due_ms, .. } => due_ms.to_string(),
            _ => String::new(),
        };
        let mut fields = vec![
            ("result", admission_label(admission)),
            ("count", count.as_str()),
        ];
        if !due_ms.is_empty() {
            fields.push(("due_ms", due_ms.as_str()));
        }
        self.trace_entry(
            tenant_id,
            user_id,
            trace_key,
            conversation_id,
            device_id,
            TraceStage::Gate,
            &fields,
        );
    }

    /// 发出的离线任务：处置、合并键、通道与角标。
    pub(super) fn trace_task(
        &self,
        user_id: &str,
        task: &TaskTrace,
        disposition: PushDisposition,
        lane: OfflineLane,
    ) {
        if !self.trace.is_enabled() {
            return;
        }
        let badge = task
            .badge
            .map(|badge| badge.to_string())
            .unwrap_or_default();
        let mut fields = vec![
            ("disposition", disposition_label(disposition)),
            ("collapse", task.collapse.as_str()),
            ("lane", lane_label(lane)),
        ];
        if !badge.is_empty() {
            fields.push(("badge", badge.as_str()));
        }
        self.trace_entry(
            &task.tenant_id,
            user_id,
            &task.trace_key,
            &task.conversation_id,
            &task.device_id,
            TraceStage::Task,
            &fields,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_notify_rules::{ConversationPrefs, Mentioned, QuietHours, UserPrefs};

    fn input() -> DecideInput {
        DecideInput {
            class: flare_notify_rules::AlertClass::Normal,
            mentioned: Mentioned::No,
            ..DecideInput::default()
        }
    }

    #[test]
    fn reasons_name_the_input_that_shaped_the_decision() {
        let from_self = DecideInput {
            from_self: true,
            ..input()
        };
        assert_eq!(decision_reason(Decision::Drop, &from_self), "self");
        let muted = DecideInput {
            conversation: ConversationPrefs {
                muted: true,
                mention_only: false,
            },
            ..input()
        };
        assert_eq!(decision_reason(Decision::Drop, &muted), "muted");
        let mention_only = DecideInput {
            conversation: ConversationPrefs {
                muted: false,
                mention_only: true,
            },
            ..input()
        };
        assert_eq!(
            decision_reason(Decision::Drop, &mention_only),
            "mention_only"
        );
        let night = DecideInput {
            user: UserPrefs {
                quiet_hours: Some(QuietHours {
                    start_minute: 0,
                    end_minute: 0,
                    weekdays: QuietHours::EVERY_DAY,
                    allow_mentions: false,
                }),
                ..UserPrefs::default()
            },
            local_time: flare_notify_rules::MinuteOfWeek::new(2, 23 * 60),
            ..input()
        };
        assert_eq!(decision_reason(Decision::Quiet, &night), "quiet_hours");
        assert_eq!(decision_reason(Decision::Quiet, &input()), "none");
        assert_eq!(
            decision_reason(
                Decision::Delay {
                    delay_ms: 15_000,
                    then: flare_notify_rules::Tier::Normal
                },
                &input()
            ),
            "other_device_active"
        );
        assert_eq!(
            decision_reason(Decision::BadgeOnly, &input()),
            "policy_badge_only"
        );
        assert_eq!(
            decision_reason(Decision::Candidate { priority: true }, &input()),
            "none"
        );
    }

    #[test]
    fn labels_follow_the_trace_vocabulary() {
        assert_eq!(
            decision_label(Decision::Candidate { priority: true }),
            "candidate"
        );
        assert_eq!(decision_label(Decision::BadgeOnly), "badge_only");
        assert_eq!(lane_label(OfflineLane::Direct), "offline");
        assert_eq!(lane_label(OfflineLane::Group), "group");
        assert_eq!(lane_label(OfflineLane::Bulk), "bulk");
        assert_eq!(class_label(AlertClass::Mention), "mention");
        let admission = |slot, digest, fail_open| Admission {
            slot,
            digest,
            badge: None,
            fail_open,
        };
        assert_eq!(
            admission_label(&admission(SlotAction::Loud { count: 1 }, None, false)),
            "loud"
        );
        assert_eq!(
            admission_label(&admission(SlotAction::Loud { count: 1 }, None, true)),
            "fail_open"
        );
        assert_eq!(
            admission_label(&admission(
                SlotAction::Quiet { count: 3 },
                Some(flare_notify_rules::DigestAlert {
                    conversations: 3,
                    messages: 9
                }),
                false
            )),
            "digest"
        );
        assert_eq!(
            admission_label(&admission(
                SlotAction::Defer {
                    due_ms: 5,
                    count: 2
                },
                None,
                false
            )),
            "defer"
        );
    }

    #[test]
    fn the_key_is_the_alerting_message_or_the_task_outside_conversations() {
        let headers = HashMap::new();
        let template = |message_id: &'static str| PushTaskTemplate {
            message_id,
            conversation_id: "c-1",
            tenant_id: "t-1",
            priority: 5,
            expire_at: None,
            push_payload: &[],
            headers: &headers,
            payload_kind: PushTaskPayloadKind::Event as i32,
        };
        let facts = |message_id: &str, seq: u64| AlertFacts {
            message_id: message_id.to_string(),
            conversation_seq: seq,
            ..AlertFacts::silent()
        };
        // 要提示的消息优先；读不出消息时用任务的标识（通知、自定义推送）；都没有时是「会话#seq」。
        assert_eq!(
            alert_trace_key(&template("event-1"), &facts("m-9", 7)),
            "m-9"
        );
        assert_eq!(
            alert_trace_key(&template("event-1"), &facts("", 0)),
            "event-1"
        );
        assert_eq!(alert_trace_key(&template(""), &facts("", 7)), "c-1#7");
    }
}
