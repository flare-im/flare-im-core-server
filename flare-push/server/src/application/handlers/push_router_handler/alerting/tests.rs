use std::sync::atomic::AtomicUsize;

use flare_notify_rules::{
    Gate, GateConfig, Ladder, MultiDevicePolicy, PreviewMode, QuietHours, UserPrefs,
};
use flare_proto::common::{ConversationType, Event, MessageRecallEvent, ReadReceiptEvent};
use flare_server_core::error::FlareError;

use super::super::tests::{
    MockConversationOnlineIndexReader, MockPushTaskPublisher, message_request, offline_tasks,
    tenant_ctx, text_message,
};
use super::*;
use crate::application::notify_gate::NotifyGate;
use crate::domain::model::{NotifyPolicy, TenantNotifyPolicy};
use crate::domain::repository::{
    BoundDevices, DeviceBindingRepository, UserNotifySettingsRepository,
};
use crate::infrastructure::notify::MemoryGateStore;

const TENANT: &str = "tenant-a";

/// alice 只有一台 iPhone（`alice-phone`）有推送绑定。
struct IosBindings;

#[async_trait]
impl DeviceBindingRepository for IosBindings {
    async fn bound_devices(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _tenant_id: &str,
        user_ids: &[String],
    ) -> std::result::Result<HashMap<String, BoundDevices>, FlareError> {
        Ok(user_ids
            .iter()
            .filter(|user_id| user_id.as_str() == "alice")
            .map(|user_id| (user_id.clone(), vec!["alice-phone".to_string()].into()))
            .collect())
    }

    async fn bound_devices_fresh(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _tenant_id: &str,
        user_id: &str,
    ) -> std::result::Result<BoundDevices, FlareError> {
        Ok(if user_id == "alice" {
            vec!["alice-phone".to_string()].into()
        } else {
            Vec::new().into()
        })
    }

    async fn device_profiles(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _tenant_id: &str,
        user_ids: &[String],
    ) -> std::result::Result<HashMap<String, Vec<DeviceProfile>>, FlareError> {
        Ok(user_ids
            .iter()
            .filter(|user_id| user_id.as_str() == "alice")
            .map(|user_id| {
                (
                    user_id.clone(),
                    vec![DeviceProfile {
                        device_id: "alice-phone".into(),
                        time_zone: "Asia/Shanghai".into(),
                        ios: true,
                    }],
                )
            })
            .collect())
    }
}

/// alice 的手机已托管；`desktop` 为真时她的电脑在前台且正在看。
struct Reach {
    desktop: bool,
}

#[async_trait]
impl OnlineStatusReader for Reach {
    async fn device_reachability(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        let mut alice = UserReachability::offline();
        alice.record_session("alice-phone", true);
        if self.desktop {
            alice.record_session("alice-desktop", false);
            alice.record_attentive("alice-desktop");
        }
        Ok(user_ids
            .iter()
            .filter(|user_id| user_id.as_str() == "alice")
            .map(|user_id| (user_id.clone(), alice.clone()))
            .collect())
    }

    fn default_tenant_id(&self) -> &str {
        TENANT
    }
}

struct Settings(UserPrefs);

#[async_trait]
impl UserNotifySettingsRepository for Settings {
    async fn settings_for(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _tenant_id: &str,
        user_ids: &[String],
    ) -> std::result::Result<HashMap<String, UserPrefs>, FlareError> {
        Ok(user_ids
            .iter()
            .map(|user_id| (user_id.clone(), self.0))
            .collect())
    }
}

struct Rig {
    router: PushRouterHandler,
    gate: Arc<NotifyGate>,
    publisher: Arc<MockPushTaskPublisher>,
}

fn rig(desktop: bool, prefs: UserPrefs, tenant: TenantNotifyPolicy) -> Rig {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let gate = Arc::new(
        NotifyGate::new(
            Arc::new(MemoryGateStore::primary()),
            Arc::new(MemoryGateStore::fallback()),
            Arc::new(NotifyPolicy::new(HashMap::from([(
                TENANT.to_string(),
                tenant,
            )]))),
        )
        .with_settings(Arc::new(Settings(prefs))),
    );
    let router = PushRouterHandler::new(
        Arc::new(Reach { desktop }),
        Arc::new(MockConversationOnlineIndexReader {
            user_ids: Vec::new(),
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        Arc::new(IosBindings),
        publisher.clone(),
    )
    .with_notify_gate(gate.clone());
    Rig {
        router,
        gate,
        publisher,
    }
}

fn group_message(server_id: &str, seq: u64) -> flare_proto::common::Message {
    let mut message = text_message("g-1", server_id, seq, "bob");
    message.conversation_type = ConversationType::Group as i32;
    message
}

impl Rig {
    async fn send(&self, server_id: &str, seq: u64) {
        self.router
            .handle_message(
                &tenant_ctx(),
                message_request(&["alice"], group_message(server_id, seq)),
            )
            .await
            .expect("handle_message");
    }

    async fn event(&self, event: Event) {
        self.router
            .handle_event(
                &tenant_ctx(),
                access_gateway::PushEventRequest {
                    user_ids: vec!["bob".to_string()],
                    conversation_id: "g-1".to_string(),
                    events: vec![event],
                    ..Default::default()
                },
            )
            .await
            .expect("handle_event");
    }

    /// 把到期队列里的全部条目当作已到期处理。
    async fn run_due(&self) -> usize {
        let entries = self.gate.claim_due(i64::MAX, 100).await;
        let claimed = entries.len();
        for entry in entries {
            self.router
                .handle_notify_due(&tenant_ctx(), entry)
                .await
                .expect("due entry");
        }
        claimed
    }

    fn tasks(&self) -> Vec<PushTaskEnvelope> {
        offline_tasks(&self.publisher)
    }
}

fn read_receipt(reader: &str, read_seq: u64) -> Event {
    Event {
        conversation_id: "g-1".to_string(),
        r#type: EventType::EventReadReceipt as i32,
        event_id: "evt-read".to_string(),
        payload: Some(event::Payload::Read(ReadReceiptEvent {
            conversation_id: "g-1".to_string(),
            read_seq,
            user_id: reader.to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn recall(server_id: &str) -> Event {
    Event {
        conversation_id: "g-1".to_string(),
        r#type: EventType::EventMessageRecall as i32,
        event_id: "evt-recall".to_string(),
        payload: Some(event::Payload::Recall(MessageRecallEvent {
            server_msg_id: server_id.to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn disposition(task: &PushTaskEnvelope) -> PushDisposition {
    PushDisposition::try_from(task.disposition).unwrap_or(PushDisposition::Unspecified)
}

/// 同一个群接连两条：第一条响铃，第二条被合并，间隔结束补一次静默的尾部刷新，条数是 2。
/// iOS 设备的任务带角标绝对值。
#[tokio::test]
async fn a_burst_rings_once_then_ends_with_a_quiet_tail() {
    let rig = rig(false, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    rig.send("m-2", 2).await;

    let tasks = rig.tasks();
    assert_eq!(tasks.len(), 1, "第二条被合并，不立即推送");
    assert_eq!(disposition(&tasks[0]), PushDisposition::Loud);
    assert_eq!(tasks[0].count, 1);
    assert_eq!(tasks[0].badge, Some(1));
    assert_eq!(tasks[0].preview, NotificationPreview::Full as i32);

    assert_eq!(rig.run_due().await, 1);
    let tasks = rig.tasks();
    assert_eq!(tasks.len(), 2);
    let tail = tasks
        .iter()
        .find(|task| disposition(task) == PushDisposition::Quiet)
        .expect("tail refresh");
    assert_eq!(tail.count, 2);
    assert_eq!(tail.conversation_seq, 2);
    assert_eq!(tail.message_id, "m-2");
    assert_eq!(tail.badge, Some(2));
    assert_eq!(tail.collapse_key, tasks[0].collapse_key);
}

/// 群里的普通提示与尾部刷新走群通道，已读之后的清除走单聊通道。
#[tokio::test]
async fn group_alerts_tails_and_corrections_take_their_lanes() {
    let rig = rig(false, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    rig.send("m-2", 2).await;
    assert_eq!(rig.run_due().await, 1);
    rig.event(read_receipt("alice", 2)).await;
    let lanes: Vec<(PushDisposition, OfflineLane)> = rig
        .publisher
        .offline_with_lanes()
        .iter()
        .map(|(task, lane)| (disposition(task), *lane))
        .collect();
    assert_eq!(
        lanes,
        vec![
            (PushDisposition::Loud, OfflineLane::Group),
            (PushDisposition::Quiet, OfflineLane::Group),
            (PushDisposition::Clear, OfflineLane::Direct),
        ]
    );
}

/// 用户在电脑上正看着：手机的提示推迟；推迟期间在电脑上读了，就不再推。
#[tokio::test]
async fn a_read_on_the_desktop_cancels_the_delayed_phone_alert() {
    let rig = rig(true, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    assert!(rig.tasks().is_empty(), "别的设备在看，手机先不响");

    rig.event(read_receipt("alice", 1)).await;
    assert_eq!(rig.run_due().await, 0, "推迟登记已随已读取消");
    assert!(
        rig.tasks()
            .iter()
            .all(|task| disposition(task) != PushDisposition::Loud)
    );
}

/// 推迟期间没人读：到期后照常进闸门，响铃。
#[tokio::test]
async fn an_unread_delayed_alert_rings_when_due() {
    let rig = rig(true, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    rig.send("m-2", 2).await;
    assert!(rig.tasks().is_empty());

    assert_eq!(rig.run_due().await, 1, "同一会话只有一条推迟登记");
    let tasks = rig.tasks();
    let loud: Vec<_> = tasks
        .iter()
        .filter(|task| disposition(task) == PushDisposition::Loud)
        .collect();
    assert_eq!(loud.len(), 1);
    assert_eq!(loud[0].count, 2, "推迟期间来的两条一起算");
    assert_eq!(loud[0].message_id, "m-2");
}

/// 多端「只更新角标」：别的设备在看时，iOS 手机只收到角标。
#[tokio::test]
async fn badge_only_policy_sends_just_the_badge() {
    let prefs = UserPrefs {
        multi_device: MultiDevicePolicy::BadgeOnly,
        ..UserPrefs::default()
    };
    let rig = rig(true, prefs, TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    let tasks = rig.tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(disposition(&tasks[0]), PushDisposition::BadgeOnly);
    assert_eq!(tasks[0].badge, Some(1));
    assert!(tasks[0].collapse_key.is_empty(), "只改角标，不替换通知");
}

/// 会话在别的设备上被读：确实推过的这台设备收到清除，角标减去这个会话的贡献。
#[tokio::test]
async fn a_read_elsewhere_clears_the_notification_and_the_badge() {
    let rig = rig(false, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    rig.event(read_receipt("alice", 1)).await;

    let tasks = rig.tasks();
    let clear = tasks
        .iter()
        .find(|task| disposition(task) == PushDisposition::Clear)
        .expect("clear correction");
    assert_eq!(clear.device_id, "alice-phone");
    assert_eq!(clear.badge, Some(0));
    assert_eq!(clear.collapse_key, tasks[0].collapse_key);

    // 同一个已读再来一遍（重投、分批）：没有第二条清除。
    rig.event(read_receipt("alice", 1)).await;
    assert_eq!(
        rig.tasks()
            .iter()
            .filter(|task| disposition(task) == PushDisposition::Clear)
            .count(),
        1
    );
    // 别人读了不关 alice 的通知。
    rig.event(read_receipt("bob", 1)).await;
    assert_eq!(rig.tasks().len(), 2);
}

/// 撤回的正是设备上展示的那条：用撤回文案替换，条数减一；不是展示的那条不修正。
#[tokio::test]
async fn recalling_the_shown_message_replaces_the_notification() {
    let rig = rig(false, UserPrefs::default(), TenantNotifyPolicy::default());
    rig.send("m-1", 1).await;
    rig.event(recall("m-0")).await;
    assert_eq!(rig.tasks().len(), 1, "没展示过的消息被撤回不用修正");

    rig.event(recall("m-1")).await;
    let tasks = rig.tasks();
    let replaced = tasks
        .iter()
        .find(|task| disposition(task) == PushDisposition::RecallReplace)
        .expect("recall replace");
    assert_eq!(replaced.count, 0);
    assert_eq!(replaced.badge, Some(0));
    assert_eq!(replaced.collapse_key, tasks[0].collapse_key);
}

/// 预览取用户设置与租户强制隐藏中更严的；勿扰时段里普通消息静默送达。
#[tokio::test]
async fn preview_and_quiet_hours_shape_the_task() {
    let prefs = UserPrefs {
        preview: PreviewMode::SenderOnly,
        // 全天勿扰。
        quiet_hours: Some(QuietHours {
            start_minute: 0,
            end_minute: 0,
            weekdays: QuietHours::EVERY_DAY,
            allow_mentions: false,
        }),
        ..UserPrefs::default()
    };
    let strict = TenantNotifyPolicy {
        force_hidden_preview: true,
        ..TenantNotifyPolicy::default()
    };
    let rig = rig(false, prefs, strict);
    rig.send("m-1", 1).await;
    let tasks = rig.tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(disposition(&tasks[0]), PushDisposition::Quiet);
    assert_eq!(tasks[0].preview, NotificationPreview::Hidden as i32);
}

/// 摘要模式：一分钟内六个会话都来了消息，从第六个起各会话只静默更新，另有一条设备级的摘要。
#[tokio::test]
async fn many_conversations_at_once_enter_digest_mode() {
    // 摘要间隔调到最短，这一轮里就能看到摘要响铃。
    let config = GateConfig {
        digest_ladder: Ladder::new(&[1]).unwrap(),
        device_min_loud_gap_ms: 0,
        ..GateConfig::default()
    };
    let tenant = TenantNotifyPolicy {
        gate: Gate::new(config),
        ..TenantNotifyPolicy::default()
    };
    let rig = rig(false, UserPrefs::default(), tenant);
    for index in 0..8u64 {
        let mut message = text_message(&format!("g-{index}"), &format!("m-{index}"), 1, "bob");
        message.conversation_type = ConversationType::Group as i32;
        rig.router
            .handle_message(&tenant_ctx(), message_request(&["alice"], message))
            .await
            .expect("handle_message");
        // 摘要要求距进入摘要模式至少一个间隔（1 毫秒）。
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    }
    let tasks = rig.tasks();
    let digest: Vec<_> = tasks
        .iter()
        .filter(|task| disposition(task) == PushDisposition::Digest)
        .collect();
    assert!(!digest.is_empty(), "摘要通知要响");
    let digest_key = device_digest_collapse_key(TENANT, "alice", "alice-phone");
    assert!(digest.iter().all(|task| task.collapse_key == digest_key));
    assert!(digest.iter().all(|task| task.digest_conversations >= 1));
    assert!(digest.iter().all(|task| task.conversation_id.is_empty()));
}

#[test]
fn event_corrections_collect_reads_and_recalls() {
    let request = access_gateway::PushEventRequest {
        conversation_id: "g-1".to_string(),
        events: vec![
            read_receipt("alice", 3),
            read_receipt("alice", 5),
            read_receipt("", 9),
            recall("m-1"),
            recall("m-1"),
            Event {
                r#type: EventType::EventMessage as i32,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let corrections = EventCorrections::of(&request);
    assert_eq!(
        corrections.reads,
        HashMap::from([(("g-1".to_string(), "alice".to_string()), 5)])
    );
    assert_eq!(
        corrections.recalls,
        vec![("g-1".to_string(), "m-1".to_string())]
    );
    assert!(EventCorrections::of(&access_gateway::PushEventRequest::default()).is_empty());
}
