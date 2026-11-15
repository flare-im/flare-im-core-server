use std::sync::atomic::{AtomicUsize, Ordering};

use flare_notify_rules::{Gate, GateConfig};
use flare_proto::common::{
    ConversationType, Mention, MentionType, MessageContent, message_content::Content,
};
use flare_server_core::error::FlareError;

use super::super::tests::{
    MockConversationOnlineIndexReader, MockDeviceBindings, MockOnlineStatusReader,
    MockPushTaskPublisher, tenant_ctx, text_message,
};
use super::*;
use crate::application::notify_gate::NotifyGate;
use crate::application::notify_wave::NotifyWaves;
use crate::domain::model::{NotifyPolicy, TenantNotifyPolicy};
use crate::domain::repository::ParticipantPage;
use crate::infrastructure::notify::MemoryGateStore;

const GROUP: &str = "g-1";

/// 全员波次每页给几个人：小于成员数，才能看到分页。
const TEST_PAGE: usize = 2;

/// 群成员。`members` 为 `None` 模拟超过成员上限：这时只有 `subscribers` 订阅了全部消息，
/// 全体成员（`everyone`）按页给出。
#[derive(Default)]
struct Group {
    members: Option<HashMap<String, NotifyPreference>>,
    subscribers: Vec<String>,
    everyone: Vec<String>,
    enumerations: AtomicUsize,
    pages: AtomicUsize,
}

impl Group {
    fn small(members: &[&str]) -> Self {
        Self {
            members: Some(
                members
                    .iter()
                    .map(|user_id| (user_id.to_string(), NotifyPreference::default()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn large(subscribers: &[&str], everyone: &[&str]) -> Self {
        Self {
            members: None,
            subscribers: subscribers
                .iter()
                .map(|user_id| user_id.to_string())
                .collect(),
            everyone: everyone.iter().map(|user_id| user_id.to_string()).collect(),
            ..Default::default()
        }
    }

    fn is_member(&self, user_id: &str) -> bool {
        match &self.members {
            Some(members) => members.contains_key(user_id),
            None => self.everyone.iter().any(|member| member == user_id),
        }
    }
}

#[async_trait]
impl NotifyPolicyRepository for Group {
    async fn preferences_for(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
        user_ids: &[String],
    ) -> std::result::Result<HashMap<String, NotifyPreference>, FlareError> {
        Ok(user_ids
            .iter()
            .filter(|user_id| self.is_member(user_id))
            .map(|user_id| (user_id.clone(), NotifyPreference::default()))
            .collect())
    }

    async fn all_participants(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
        _cap: usize,
    ) -> std::result::Result<Option<HashMap<String, NotifyPreference>>, FlareError> {
        self.enumerations.fetch_add(1, Ordering::SeqCst);
        Ok(self.members.clone())
    }

    async fn notification_subscribers(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
    ) -> std::result::Result<Vec<String>, FlareError> {
        Ok(self.subscribers.clone())
    }

    async fn participant_page(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
        cursor: &str,
        _limit: usize,
    ) -> std::result::Result<ParticipantPage, FlareError> {
        self.pages.fetch_add(1, Ordering::SeqCst);
        let start: usize = cursor.parse().unwrap_or(0);
        let end = (start + TEST_PAGE).min(self.everyone.len());
        Ok(ParticipantPage {
            members: self.everyone[start..end]
                .iter()
                .map(|user_id| (user_id.clone(), NotifyPreference::default()))
                .collect(),
            next_cursor: (end < self.everyone.len()).then(|| end.to_string()),
        })
    }
}

struct Rig {
    router: PushRouterHandler,
    gate: Arc<NotifyGate>,
    publisher: Arc<MockPushTaskPublisher>,
    group: Arc<Group>,
}

/// 闸门与波次共用一对进程内存储；人人离线，每人一台手机（`{user}-phone`）。设备最小响铃间隔
/// 调成 0，点名在同一秒里也能响。
fn rig(group: Group) -> Rig {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let primary = Arc::new(MemoryGateStore::primary());
    let fallback = Arc::new(MemoryGateStore::fallback());
    let tenant = TenantNotifyPolicy {
        gate: Gate::new(GateConfig {
            device_min_loud_gap_ms: 0,
            ..GateConfig::default()
        }),
        ..TenantNotifyPolicy::default()
    };
    let policy = Arc::new(NotifyPolicy::new(HashMap::from([(
        "tenant-a".to_string(),
        tenant,
    )])));
    let gate = Arc::new(NotifyGate::new(
        primary.clone(),
        fallback.clone(),
        policy.clone(),
    ));
    let waves = Arc::new(NotifyWaves::new(primary, fallback, policy));
    let group = Arc::new(group);
    let router = PushRouterHandler::new(
        Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        }),
        Arc::new(MockConversationOnlineIndexReader {
            user_ids: Vec::new(),
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        Arc::new(MockDeviceBindings::default()),
        publisher.clone(),
    )
    .with_notify_policy(group.clone())
    .with_notify_gate(gate.clone())
    .with_notify_waves(waves);
    Rig {
        router,
        gate,
        publisher,
        group,
    }
}

fn mention(user_id: &str) -> Mention {
    Mention {
        r#type: MentionType::User as i32,
        user_id: user_id.to_string(),
        ..Default::default()
    }
}

fn mention_all() -> Mention {
    Mention {
        r#type: MentionType::All as i32,
        ..Default::default()
    }
}

/// bob 在群里发的第 `seq` 条，点到 `mentions`。
fn group_message(seq: u64, mentions: Vec<Mention>) -> Message {
    let mut message = text_message(GROUP, &format!("m-{seq}"), seq, "bob");
    message.conversation_type = ConversationType::Group as i32;
    if let Some(MessageContent {
        content: Some(Content::Text(text)),
        ..
    }) = message.content.as_mut()
    {
        text.mentions = mentions;
    }
    message
}

impl Rig {
    /// 读扩散的群消息：收件人为空，只带会话。
    async fn send(&self, message: Message) {
        let seq = message.conversation_seq;
        self.router
            .handle_event(
                &tenant_ctx(),
                access_gateway::PushEventRequest {
                    conversation_id: GROUP.to_string(),
                    max_conversation_seq: seq,
                    delivery_mode: EventEnvelopeDeliveryMode::PingWithInline as i32,
                    events: vec![flare_proto::common::Event {
                        conversation_id: GROUP.to_string(),
                        conversation_seq: seq,
                        r#type: EventType::EventMessage as i32,
                        payload: Some(flare_proto::common::event::Payload::Message(message)),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await
            .expect("handle_event");
    }

    /// 把到期队列里的全部条目当作已到期处理，返回领取了几条。
    async fn run_due(&self) -> usize {
        let claims = self.gate.claim_due(i64::MAX, 100).await;
        let claimed = claims.len();
        for claim in claims {
            self.router
                .handle_notify_due(&tenant_ctx(), claim)
                .await
                .expect("due entry");
        }
        claimed
    }

    /// 发出的离线任务：（用户，处置，提示等级，条数，通道），按发出顺序。
    fn sent(&self) -> Vec<(String, PushDisposition, AlertClass, u32, OfflineLane)> {
        self.publisher
            .offline_with_lanes()
            .into_iter()
            .map(|(task, lane)| {
                (
                    task.user_id.clone(),
                    PushDisposition::try_from(task.disposition)
                        .unwrap_or(PushDisposition::Unspecified),
                    AlertClass::try_from(task.alert_class).unwrap_or(AlertClass::Unspecified),
                    task.count,
                    lane,
                )
            })
            .collect()
    }

    /// 等后台的全员波次发出至少 `count` 条离线任务（至多两秒）。
    async fn wait_for(&self, count: usize) {
        for _ in 0..400 {
            if self.publisher.offline_with_lanes().len() >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("expected {count} offline tasks, got {:?}", self.sent());
    }

    fn sent_to(&self, user_id: &str) -> usize {
        self.sent()
            .iter()
            .filter(|(user, ..)| user == user_id)
            .count()
    }
}

fn users(sent: &[(String, PushDisposition, AlertClass, u32, OfflineLane)]) -> Vec<String> {
    let mut users: Vec<String> = sent.iter().map(|(user, ..)| user.clone()).collect();
    users.sort();
    users
}

/// 热群：第一条当场枚举，紧跟着的只累加；间隔结束枚举一次，接收者只查过一次（缓存）。
/// 设备上第一条响铃，后面的合并成一次静默的尾部刷新，条数是全部。
#[tokio::test]
async fn a_hot_group_enumerates_once_per_wave() {
    let rig = rig(Group::small(&["alice", "bob", "carol"]));
    rig.send(group_message(1, Vec::new())).await;
    let first = rig.sent();
    assert_eq!(users(&first), vec!["alice", "carol"], "发送者本人不推");
    assert!(first.iter().all(
        |(_, disposition, _, _, lane)| *disposition == PushDisposition::Loud
            && *lane == OfflineLane::Group
    ));

    rig.send(group_message(2, Vec::new())).await;
    rig.send(group_message(3, Vec::new())).await;
    assert_eq!(rig.sent().len(), 2, "间隔里的消息只在会话级累加");

    assert_eq!(rig.run_due().await, 1, "一个会话一个波次到期条目");
    assert_eq!(rig.sent().len(), 2, "这一批进闸门后被合并，等尾部刷新");
    assert_eq!(rig.run_due().await, 2, "两台设备各一个尾部刷新");
    let tails: Vec<_> = rig.sent().into_iter().skip(2).collect();
    assert_eq!(users(&tails), vec!["alice", "carol"]);
    assert!(tails.iter().all(|(_, disposition, _, count, lane)| {
        *disposition == PushDisposition::Quiet && *count == 3 && *lane == OfflineLane::Group
    }));
    assert_eq!(
        rig.group.enumerations.load(Ordering::SeqCst),
        1,
        "一个波次只查一次接收者，15 秒内的下一个波次用缓存"
    );
}

/// 被点名的成员不等波次：当场按点名推、走单聊通道；这一条不再随波次重复推给他。
#[tokio::test]
async fn a_mentioned_member_is_pushed_now_and_not_again_with_the_wave() {
    let rig = rig(Group::small(&["alice", "bob", "carol"]));
    rig.send(group_message(1, Vec::new())).await;
    rig.send(group_message(2, vec![mention("carol")])).await;

    let sent = rig.sent();
    assert_eq!(sent.len(), 3, "alice 的那条在波次里等着");
    assert_eq!(
        sent[2],
        (
            "carol".to_string(),
            PushDisposition::Loud,
            AlertClass::Mention,
            2,
            OfflineLane::Direct
        )
    );

    assert_eq!(rig.run_due().await, 1);
    // 这一批只有 m-2：carol 已经直接推过，alice 的那条被合并等尾部刷新。
    assert_eq!(rig.sent_to("carol"), 2);
    assert_eq!(rig.sent().len(), 3);
}

/// 提及来自消息内容：点到不在群里的人不推。
#[tokio::test]
async fn mentions_of_non_members_are_ignored() {
    let rig = rig(Group::small(&["alice", "bob"]));
    rig.send(group_message(1, vec![mention("mallory")])).await;
    assert_eq!(users(&rig.sent()), vec!["alice"]);
}

/// 超过成员上限的群只枚举订阅者，走批量通道；被点名的成员照样直接推。
#[tokio::test]
async fn an_oversized_group_waves_to_its_subscribers_on_the_bulk_lane() {
    let rig = rig(Group::large(&["alice"], &["alice", "bob", "carol", "dave"]));
    rig.send(group_message(1, vec![mention("carol")])).await;
    let sent = rig.sent();
    assert_eq!(users(&sent), vec!["alice", "carol"], "没订阅的 dave 不推");
    for (user, _, class, _, lane) in &sent {
        match user.as_str() {
            "alice" => assert_eq!((*class, *lane), (AlertClass::Normal, OfflineLane::Bulk)),
            "carol" => assert_eq!((*class, *lane), (AlertClass::Mention, OfflineLane::Direct)),
            other => panic!("unexpected recipient {other}"),
        }
    }
}

/// 超过成员上限的群里 @所有人 是一次分页流式的全员波次，人人按点名提示、走批量通道；
/// 十分钟内再来一次按普通消息处理，只给订阅者。
#[tokio::test]
async fn mention_of_everyone_in_an_oversized_group_is_rate_limited() {
    let rig = rig(Group::large(
        &["alice"],
        &["alice", "bob", "carol", "dave", "erin"],
    ));
    rig.send(group_message(1, vec![mention_all()])).await;
    // 全员波次在后台推。
    rig.wait_for(4).await;
    let sent = rig.sent();
    assert_eq!(users(&sent), vec!["alice", "carol", "dave", "erin"]);
    assert!(sent.iter().all(|(_, disposition, class, _, lane)| {
        *disposition == PushDisposition::Loud
            && *class == AlertClass::Mention
            && *lane == OfflineLane::Bulk
    }));
    assert_eq!(
        rig.group.pages.load(Ordering::SeqCst),
        3,
        "五个人每页两个，翻三页"
    );

    rig.send(group_message(2, vec![mention_all()])).await;
    rig.run_due().await;
    rig.run_due().await;
    for user in ["carol", "dave", "erin"] {
        assert_eq!(
            rig.sent_to(user),
            1,
            "配额外的 @所有人 不再推给没订阅的 {user}"
        );
    }
    assert_eq!(rig.group.pages.load(Ordering::SeqCst), 3);
}

/// 成员不超过上限的群里 @所有人 不等波次：人人按点名提示，走批量通道。
#[tokio::test]
async fn mention_of_everyone_in_a_small_group_alerts_every_member_now() {
    let rig = rig(Group::small(&["alice", "bob", "carol"]));
    rig.send(group_message(1, Vec::new())).await;
    rig.send(group_message(2, vec![mention_all()])).await;
    let sent: Vec<_> = rig.sent().into_iter().skip(2).collect();
    assert_eq!(users(&sent), vec!["alice", "carol"]);
    assert!(sent.iter().all(|(_, disposition, class, _, lane)| {
        *disposition == PushDisposition::Loud
            && *class == AlertClass::Mention
            && *lane == OfflineLane::Bulk
    }));
}
