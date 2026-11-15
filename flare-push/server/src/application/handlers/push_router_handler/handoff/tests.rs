use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use flare_im_contracts::domain::push_policy::conversation_collapse_key;
use flare_server_core::error::FlareError;

use super::super::tests::{
    MockConversationOnlineIndexReader, MockDeviceBindings, MockOnlineStatusReader,
    MockPushTaskPublisher, message_request, offline_tasks, tenant_ctx, text_message,
};
use super::*;

fn item(conversation_id: &str, seq: u64, message_id: &str) -> DeviceHandoffItem {
    DeviceHandoffItem {
        conversation_id: conversation_id.to_string(),
        seq,
        message_id: message_id.to_string(),
    }
}

fn target(conversation_id: &str, seq: u64, message_id: &str) -> HandoffTarget {
    HandoffTarget {
        conversation_id: conversation_id.to_string(),
        seq,
        message_id: message_id.to_string(),
    }
}

/// alice 的手机（`alice-phone`）的一次交接。
fn handoff(cause: DeviceHandoffCause, pending: Vec<DeviceHandoffItem>) -> DeviceHandoff {
    DeviceHandoff {
        tenant_id: "tenant-a".to_string(),
        user_id: "alice".to_string(),
        device_id: "alice-phone".to_string(),
        connection_id: "conn-1".to_string(),
        gateway_id: "gw-1".to_string(),
        cause: cause as i32,
        since: 0,
        occurred_at: 1_760_000_000_000,
        pending,
    }
}

/// 会话成员与偏好：按 (会话, 用户) 给出；没列出的就是确定不是成员。
#[derive(Default)]
struct Membership {
    members: HashMap<(String, String), NotifyPreference>,
    fail: bool,
    calls: AtomicUsize,
}

impl Membership {
    fn of(members: &[(&str, &str, NotifyPreference)]) -> Self {
        Self {
            members: members
                .iter()
                .map(|(conversation_id, user_id, preference)| {
                    (
                        (conversation_id.to_string(), user_id.to_string()),
                        *preference,
                    )
                })
                .collect(),
            ..Default::default()
        }
    }
}

#[async_trait]
impl NotifyPolicyRepository for Membership {
    async fn preferences_for(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        user_ids: &[String],
    ) -> std::result::Result<HashMap<String, NotifyPreference>, FlareError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "conversation service unavailable",
            ));
        }
        Ok(user_ids
            .iter()
            .filter_map(|user_id| {
                self.members
                    .get(&(conversation_id.to_string(), user_id.clone()))
                    .map(|preference| (user_id.clone(), *preference))
            })
            .collect())
    }

    async fn all_participants(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
        _cap: usize,
    ) -> std::result::Result<Option<HashMap<String, NotifyPreference>>, FlareError> {
        Ok(None)
    }

    async fn notification_subscribers(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
    ) -> std::result::Result<Vec<String>, FlareError> {
        Ok(Vec::new())
    }

    async fn participant_page(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        _conversation_id: &str,
        _cursor: &str,
        _limit: usize,
    ) -> std::result::Result<crate::domain::repository::ParticipantPage, FlareError> {
        Ok(crate::domain::repository::ParticipantPage::default())
    }
}

/// 可读回的消息：按 (会话, seq) 给出。`fail` 模拟 Storage Reader 不可用。
#[derive(Default)]
struct Messages {
    stored: Vec<Message>,
    fail: bool,
    reads: Mutex<Vec<(String, u64, String)>>,
}

#[async_trait]
impl MessageLookupRepository for Messages {
    async fn message_at(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        seq: u64,
        user_id: &str,
    ) -> std::result::Result<Option<Message>, FlareError> {
        self.reads
            .lock()
            .unwrap()
            .push((conversation_id.to_string(), seq, user_id.to_string()));
        if self.fail {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "storage reader unavailable",
            ));
        }
        Ok(self
            .stored
            .iter()
            .find(|message| {
                message.conversation_id == conversation_id && message.conversation_seq == seq
            })
            .cloned())
    }
}

/// 会话服务对「有新未读的会话」的回答，已按它的规则筛好、从新到旧。`fail` 是读取失败的原因。
#[derive(Default)]
struct Recent {
    conversations: Vec<RecentConversation>,
    fail: Option<ErrorCode>,
    asks: Mutex<Vec<(String, i64, usize)>>,
}

impl Recent {
    fn of(conversations: Vec<RecentConversation>) -> Self {
        Self {
            conversations,
            ..Default::default()
        }
    }
}

#[async_trait]
impl RecentConversationRepository for Recent {
    async fn recent_conversations(
        &self,
        _ctx: &flare_server_core::context::Ctx,
        user_id: &str,
        since: i64,
        limit: usize,
    ) -> std::result::Result<Vec<RecentConversation>, FlareError> {
        self.asks
            .lock()
            .unwrap()
            .push((user_id.to_string(), since, limit));
        if let Some(code) = self.fail {
            return Err(FlareError::localized(code, "conversation read failed"));
        }
        Ok(self.conversations.clone())
    }
}

/// bob 发来、alice 还没读的一个会话，最后一条是 seq 4。
fn unread(conversation_id: &str, last_message_at: i64) -> RecentConversation {
    RecentConversation {
        conversation_id: conversation_id.to_string(),
        last_seq: 4,
        last_message_id: format!("m-{conversation_id}"),
        last_sender_id: "bob".to_string(),
        last_message_at,
        unread: 2,
    }
}

/// 一次网关失联的交接，网关最后可见时间是 `since`。
fn gateway_loss(since: i64) -> DeviceHandoff {
    let mut lost = handoff(DeviceHandoffCause::GatewayLost, Vec::new());
    lost.since = since;
    lost
}

/// alice 的手机与平板都有绑定的路由器；补提示用到的其余依赖由用例接上。
fn router(
    publisher: Arc<MockPushTaskPublisher>,
    bindings: Arc<MockDeviceBindings>,
) -> PushRouterHandler {
    PushRouterHandler::new(
        Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        }),
        Arc::new(MockConversationOnlineIndexReader {
            user_ids: Vec::new(),
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        bindings,
        publisher,
    )
}

fn alice_devices() -> Arc<MockDeviceBindings> {
    Arc::new(MockDeviceBindings::of(&[(
        "alice",
        "alice-phone alice-tablet",
    )]))
}

fn member(conversation_id: &str) -> (&str, &str, NotifyPreference) {
    (conversation_id, "alice", NotifyPreference::default())
}

fn task_message(task: &PushTaskEnvelope) -> Message {
    let mut request = access_gateway::PushMessageRequest::decode(task.push_payload.as_slice())
        .expect("catch-up payload decodes");
    assert_eq!(request.user_ids, vec!["alice".to_string()]);
    assert_eq!(request.messages.len(), 1);
    request.messages.remove(0)
}

#[test]
fn pending_items_collapse_to_the_newest_seq_per_conversation_most_recent_first() {
    let targets = pending_targets(&[
        item("conv-a", 5, "a5"),
        item("conv-b", 9, "b9"),
        item("conv-a", 7, "a7"),
        item("conv-a", 6, "a6"),
        item("conv-c", 2, "c2"),
        item("conv-b", 10, "b10"),
    ]);
    assert_eq!(
        targets,
        vec![
            target("conv-b", 10, "b10"),
            target("conv-c", 2, "c2"),
            // conv-a 最后一次出现在 conv-c 之前，所以排在后面；它留下的是 seq 最高的 a7，
            // 不是最后出现的 a6。
            target("conv-a", 7, "a7"),
        ]
    );
}

#[test]
fn pending_items_without_a_conversation_are_ignored() {
    assert!(pending_targets(&[]).is_empty());
    assert_eq!(
        pending_targets(&[item("", 3, "x"), item("  ", 4, "y"), item(" conv ", 5, "z")]),
        vec![target("conv", 5, "z")]
    );
}

/// 超出上限时丢的是最早写出的会话，留下的仍是最近的排前。
#[test]
fn pending_conversations_are_capped_keeping_the_most_recent() {
    let pending: Vec<DeviceHandoffItem> = (0..40)
        .map(|index| item(&format!("conv-{index:02}"), index + 1, "m"))
        .collect();
    let targets = pending_targets(&pending);
    assert_eq!(targets.len(), MAX_HANDOFF_CONVERSATIONS);
    assert_eq!(targets[0].conversation_id, "conv-39");
    assert_eq!(targets[15].conversation_id, "conv-24");
}

/// 会话服务的回答原样换成目标：顺序不动，不再筛一遍；只有没有会话 ID 的行被丢掉。
#[test]
fn gateway_loss_targets_keep_the_conversation_service_order() {
    let targets = gateway_lost_targets(vec![
        unread("c3", 1_800),
        unread("", 1_700),
        unread("c2", 1_600),
        unread("  ", 1_500),
        unread("c1", 1_400),
    ]);
    assert_eq!(
        targets,
        vec![
            target("c3", 4, "m-c3"),
            target("c2", 4, "m-c2"),
            target("c1", 4, "m-c1"),
        ]
    );
}

/// 补提示只给交接的那台设备，每个会话一条，内容是读回来的最新一条消息。
#[tokio::test]
async fn a_handoff_alerts_exactly_the_handed_off_device() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let bindings = alice_devices();
    let messages = Arc::new(Messages {
        stored: vec![
            text_message("conv-1", "m-6", 6, "bob"),
            text_message("conv-2", "m-3", 3, "carol"),
        ],
        ..Default::default()
    });
    let handler = router(publisher.clone(), bindings.clone())
        .with_notify_policy(Arc::new(Membership::of(&[
            member("conv-1"),
            member("conv-2"),
        ])))
        .with_message_lookup(messages.clone());

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::AckTimeout,
                vec![
                    item("conv-1", 5, "m-5"),
                    item("conv-2", 3, "m-3"),
                    item("conv-1", 6, "m-6"),
                ],
            ),
        )
        .await
        .expect("handoff");

    let tasks = offline_tasks(&publisher);
    assert_eq!(tasks.len(), 2, "每个会话一条");
    assert!(
        tasks.iter().all(|task| task.device_id == "alice-phone"),
        "平板没有交接，不该被补提示"
    );
    assert!(publisher.online.lock().unwrap().is_empty());
    assert!(
        publisher
            .offline
            .lock()
            .unwrap()
            .iter()
            .all(|(key, _)| key.as_deref() == Some("alice"))
    );

    // 最近写出的会话在前：conv-1（最后一条是 seq 6），然后 conv-2。
    let first = &tasks[0];
    let collapse_key = conversation_collapse_key("tenant-a", "conv-1");
    assert_eq!(first.conversation_id, "conv-1");
    assert_eq!(first.message_id, "m-6");
    assert_eq!(first.conversation_seq, 6);
    assert_eq!(first.tenant_id, "tenant-a");
    assert_eq!(first.user_id, "alice");
    assert_eq!(first.collapse_key, collapse_key);
    assert_eq!(first.thread_key, collapse_key);
    assert_eq!(first.alert_class, AlertClass::Normal as i32);
    assert_eq!(first.disposition, PushDisposition::Loud as i32);
    assert_eq!(first.count, 1);
    assert_eq!(first.badge, None);
    assert_eq!(first.payload_kind, PushTaskPayloadKind::Message as i32);
    assert!(first.expire_at.is_some());
    assert_eq!(
        first.idempotency_key,
        offline_idempotency_key(
            "tenant-a",
            "alice",
            "alice-phone",
            &collapse_key,
            PushDisposition::Loud,
            6,
            "m-6",
        )
    );
    assert_eq!(task_message(first), text_message("conv-1", "m-6", 6, "bob"));
    assert_eq!(tasks[1].conversation_id, "conv-2");
    assert_eq!(tasks[1].conversation_seq, 3);

    // 只读每个会话最新的一条，按 alice 的可见性读；绑定直读一次，不走缓存路径。
    assert_eq!(
        *messages.reads.lock().unwrap(),
        vec![
            ("conv-1".to_string(), 6, "alice".to_string()),
            ("conv-2".to_string(), 3, "alice".to_string()),
        ]
    );
    assert_eq!(bindings.fresh_reads.load(Ordering::SeqCst), 1);
    assert_eq!(bindings.batch_reads.load(Ordering::SeqCst), 0);
}

/// 正常路由与交接对同一条消息算出同一个幂等键，broker 才能把两条路径的任务去重成一条。
#[tokio::test]
async fn catch_up_and_normal_routing_share_one_idempotency_key() {
    let message = text_message("conv-1", "m-6", 6, "bob");

    let routed = Arc::new(MockPushTaskPublisher::default());
    router(routed.clone(), alice_devices())
        .handle_message(&tenant_ctx(), message_request(&["alice"], message.clone()))
        .await
        .expect("handle_message");
    let routed_key = offline_tasks(&routed)
        .into_iter()
        .find(|task| task.device_id == "alice-phone")
        .expect("normal routing pushes the phone")
        .idempotency_key;

    let caught_up = Arc::new(MockPushTaskPublisher::default());
    router(caught_up.clone(), alice_devices())
        .with_message_lookup(Arc::new(Messages {
            stored: vec![message],
            ..Default::default()
        }))
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(DeviceHandoffCause::LinkLost, vec![item("conv-1", 6, "m-6")]),
        )
        .await
        .expect("handoff");

    let tasks = offline_tasks(&caught_up);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].idempotency_key, routed_key);
}

/// 同一条交接处理两遍，产出的任务幂等键相同：重投不会让设备多响一声。
#[tokio::test]
async fn a_redelivered_handoff_produces_the_same_keys() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler = router(publisher.clone(), alice_devices());
    for _ in 0..2 {
        handler
            .handle_device_handoff(
                &tenant_ctx(),
                handoff(DeviceHandoffCause::Parked, vec![item("conv-1", 6, "m-6")]),
            )
            .await
            .expect("handoff");
    }
    let tasks = offline_tasks(&publisher);
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].idempotency_key, tasks[1].idempotency_key);
}

/// 没有推送绑定的设备没有通道可以接手：不补提示，也不必再查别的。
#[tokio::test]
async fn a_device_without_a_binding_is_not_alerted() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let membership = Arc::new(Membership::of(&[member("conv-1")]));
    let handler = router(
        publisher.clone(),
        Arc::new(MockDeviceBindings::of(&[("alice", "alice-tablet")])),
    )
    .with_notify_policy(membership.clone());

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(DeviceHandoffCause::LinkLost, vec![item("conv-1", 6, "m-6")]),
        )
        .await
        .expect("handoff");

    assert!(publisher.offline.lock().unwrap().is_empty());
    assert_eq!(membership.calls.load(Ordering::SeqCst), 0);
}

/// 免打扰的会话、已经不在其中的会话不补提示；其余照常。
#[tokio::test]
async fn muted_and_left_conversations_are_skipped() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let messages = Arc::new(Messages::default());
    let handler = router(publisher.clone(), alice_devices())
        .with_notify_policy(Arc::new(Membership::of(&[
            (
                "conv-muted",
                "alice",
                NotifyPreference {
                    muted: true,
                    mention_only: false,
                },
            ),
            member("conv-ok"),
            // conv-left 里有别人，没有 alice。
            ("conv-left", "bob", NotifyPreference::default()),
        ])))
        .with_message_lookup(messages.clone());

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::AckTimeout,
                vec![
                    item("conv-muted", 1, "m-1"),
                    item("conv-left", 2, "m-2"),
                    item("conv-ok", 3, "m-3"),
                ],
            ),
        )
        .await
        .expect("handoff");

    let tasks = offline_tasks(&publisher);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].conversation_id, "conv-ok");
    assert_eq!(
        messages.reads.lock().unwrap().len(),
        1,
        "被偏好或成员资格挡掉的会话不必读消息"
    );
}

/// 偏好查不到时放行：既不知道偏好，也不能断定不是成员。
#[tokio::test]
async fn a_failed_preference_lookup_still_alerts() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler =
        router(publisher.clone(), alice_devices()).with_notify_policy(Arc::new(Membership {
            fail: true,
            ..Default::default()
        }));

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::AckTimeout,
                vec![item("conv-1", 6, "m-6")],
            ),
        )
        .await
        .expect("handoff");

    assert_eq!(offline_tasks(&publisher).len(), 1);
}

/// 「只接收@我」看的是读回来的那条消息：没点名不提示，点了名按「点名」提示。
#[tokio::test]
async fn mention_only_follows_the_loaded_message() {
    use flare_proto::common::{MessageContent, TextContent, message_content::Content};

    let mention_only = NotifyPreference {
        muted: false,
        mention_only: true,
    };
    let mut naming = text_message("conv-named", "m-2", 2, "bob");
    naming.content = Some(MessageContent {
        content: Some(Content::Text(TextContent {
            text: "@alice".to_string(),
            mentions: vec![flare_proto::common::Mention {
                r#type: flare_proto::common::MentionType::User as i32,
                user_id: "alice".to_string(),
                ..Default::default()
            }],
        })),
    });
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler = router(publisher.clone(), alice_devices())
        .with_notify_policy(Arc::new(Membership::of(&[
            ("conv-plain", "alice", mention_only),
            ("conv-named", "alice", mention_only),
        ])))
        .with_message_lookup(Arc::new(Messages {
            stored: vec![text_message("conv-plain", "m-1", 1, "bob"), naming],
            ..Default::default()
        }));

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::AckTimeout,
                vec![item("conv-plain", 1, "m-1"), item("conv-named", 2, "m-2")],
            ),
        )
        .await
        .expect("handoff");

    let tasks = offline_tasks(&publisher);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].conversation_id, "conv-named");
    assert_eq!(tasks[0].alert_class, AlertClass::Mention as i32);
}

/// 自己从别的设备发的消息、以及本来就不提示的消息，不补提示。
#[tokio::test]
async fn own_and_non_alerting_messages_are_not_caught_up() {
    use flare_proto::common::{MessageContent, NotificationContent, message_content::Content};

    let mut sync_only = text_message("conv-sync", "m-2", 2, "system");
    sync_only.content = Some(MessageContent {
        content: Some(Content::Notification(NotificationContent::default())),
    });
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler =
        router(publisher.clone(), alice_devices()).with_message_lookup(Arc::new(Messages {
            stored: vec![text_message("conv-own", "m-1", 1, "alice"), sync_only],
            ..Default::default()
        }));

    handler
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::LinkLost,
                vec![item("conv-own", 1, "m-1"), item("conv-sync", 2, "m-2")],
            ),
        )
        .await
        .expect("handoff");

    assert!(publisher.offline.lock().unwrap().is_empty());
}

/// 消息读不回来（读取失败、还没落库、没接线）仍然提示，用只有标识的占位消息：
/// Worker 对它用通用文案。提示可以没有内容，不能没有。
#[tokio::test]
async fn an_unloadable_message_still_alerts_with_a_generic_payload() {
    for lookup in [
        Some(Messages {
            fail: true,
            ..Default::default()
        }),
        Some(Messages::default()),
        None,
    ] {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let mut handler = router(publisher.clone(), alice_devices());
        if let Some(lookup) = lookup {
            handler = handler.with_message_lookup(Arc::new(lookup));
        }

        handler
            .handle_device_handoff(
                &tenant_ctx(),
                handoff(
                    DeviceHandoffCause::AckTimeout,
                    vec![item("conv-1", 6, "m-6"), item("conv-2", 0, "")],
                ),
            )
            .await
            .expect("handoff");

        let tasks = offline_tasks(&publisher);
        assert_eq!(tasks.len(), 2);
        // 最近写出的在前：conv-2（没有 seq 也没有消息标识），然后 conv-1。
        let anonymous = &tasks[0];
        assert_eq!(anonymous.conversation_id, "conv-2");
        assert_eq!(anonymous.message_id, "handoff:conv-2:0");
        assert_eq!(anonymous.conversation_seq, 0);

        let generic = &tasks[1];
        assert_eq!(generic.message_id, "m-6");
        assert_eq!(generic.conversation_seq, 6);
        assert_eq!(generic.alert_class, AlertClass::Normal as i32);
        assert_eq!(generic.disposition, PushDisposition::Loud as i32);
        let placeholder = task_message(generic);
        assert_eq!(placeholder.server_id, "m-6");
        assert_eq!(placeholder.conversation_id, "conv-1");
        assert_eq!(placeholder.conversation_seq, 6);
        assert!(placeholder.content.is_none() && placeholder.offline_push_info.is_none());
    }
}

/// 网关失联的交接不带未确认条目。没有会话读取时无从补提示：不报错，也不乱提示。
#[tokio::test]
async fn a_gateway_loss_without_a_conversation_reader_alerts_nothing() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler = router(publisher.clone(), alice_devices());

    handler
        .handle_device_handoff(&tenant_ctx(), gateway_loss(1_000))
        .await
        .expect("handoff");

    assert!(publisher.offline.lock().unwrap().is_empty());
}

/// 没带窗口起点的网关失联交接不去问会话服务：「之后」会变成全部历史。
#[tokio::test]
async fn a_gateway_loss_without_a_window_start_alerts_nothing() {
    for since in [0, -1] {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let recent = Arc::new(Recent::of(vec![unread("conv-old", 500)]));
        let handler =
            router(publisher.clone(), alice_devices()).with_recent_conversations(recent.clone());

        handler
            .handle_device_handoff(&tenant_ctx(), gateway_loss(since))
            .await
            .expect("handoff");

        assert!(
            publisher.offline.lock().unwrap().is_empty(),
            "since={since}"
        );
        assert!(recent.asks.lock().unwrap().is_empty(), "since={since}");
    }
}

/// 网关失联的交接向会话服务问出窗口内有新未读的会话，给交接的那台设备各补一条，
/// 内容是读回来的最后一条消息。
#[tokio::test]
async fn a_gateway_loss_alerts_conversations_with_new_unread_messages() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let recent = Arc::new(Recent::of(vec![
        unread("conv-b", 2_100),
        unread("conv-a", 2_000),
    ]));
    let messages = Arc::new(Messages {
        stored: vec![text_message("conv-b", "m-conv-b", 4, "bob")],
        ..Default::default()
    });
    let handler = router(publisher.clone(), alice_devices())
        .with_recent_conversations(recent.clone())
        .with_message_lookup(messages.clone());

    handler
        .handle_device_handoff(&tenant_ctx(), gateway_loss(1_000))
        .await
        .expect("handoff");

    // 按用户、按网关最后可见时间问，要的候选数就是会话服务单次给的上限。
    assert_eq!(
        *recent.asks.lock().unwrap(),
        vec![("alice".to_string(), 1_000, GATEWAY_LOST_CANDIDATE_LIMIT)]
    );
    let published: Vec<PushTaskEnvelope> = publisher
        .offline
        .lock()
        .unwrap()
        .iter()
        .map(|(_, task)| task.clone())
        .collect();
    assert_eq!(published.len(), 2);
    assert!(published.iter().all(|task| task.device_id == "alice-phone"));
    assert!(publisher.online.lock().unwrap().is_empty());
    // 最新的会话先补。
    assert_eq!(published[0].conversation_id, "conv-b");
    assert_eq!(published[0].message_id, "m-conv-b");
    assert_eq!(published[0].conversation_seq, 4);
    assert_eq!(published[0].disposition, PushDisposition::Loud as i32);
    assert_eq!(
        task_message(&published[0]),
        text_message("conv-b", "m-conv-b", 4, "bob")
    );
    // conv-a 的消息读不回来：用占位消息照常提示。
    assert_eq!(published[1].conversation_id, "conv-a");
    assert_eq!(published[1].message_id, "m-conv-a");
    assert!(task_message(&published[1]).content.is_none());
}

/// 网关失联的交接至多补五条。免打扰的候选不出提示，也不占名额；补够之后不再往下问。
#[tokio::test]
async fn a_gateway_loss_alerts_at_most_five_conversations() {
    let muted = NotifyPreference {
        muted: true,
        mention_only: false,
    };
    // 八个候选，从新到旧 conv-0 到 conv-7；最新的两个免打扰。
    let ids: Vec<String> = (0..8).map(|index| format!("conv-{index}")).collect();
    let members: Vec<(&str, &str, NotifyPreference)> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let preference = if index < 2 {
                muted
            } else {
                NotifyPreference::default()
            };
            (id.as_str(), "alice", preference)
        })
        .collect();
    let membership = Arc::new(Membership::of(&members));
    let recent = Arc::new(Recent::of(
        ids.iter()
            .enumerate()
            .map(|(index, id)| unread(id, 9_000 - index as i64))
            .collect(),
    ));
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler = router(publisher.clone(), alice_devices())
        .with_notify_policy(membership.clone())
        .with_recent_conversations(recent);

    handler
        .handle_device_handoff(&tenant_ctx(), gateway_loss(1_000))
        .await
        .expect("handoff");

    let alerted: Vec<String> = publisher
        .offline
        .lock()
        .unwrap()
        .iter()
        .map(|(_, task)| task.conversation_id.clone())
        .collect();
    assert_eq!(
        alerted,
        vec!["conv-2", "conv-3", "conv-4", "conv-5", "conv-6"]
    );
    assert_eq!(alerted.len(), MAX_GATEWAY_LOST_ALERTS);
    assert_eq!(
        membership.calls.load(Ordering::SeqCst),
        7,
        "补够五条后 conv-7 不必再查"
    );
}

/// 未确认条目的交接不受网关失联的上限约束：合并后的每个会话都补。
#[tokio::test]
async fn a_pending_handoff_is_not_held_to_the_gateway_loss_cap() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let pending: Vec<DeviceHandoffItem> = (0..8)
        .map(|index| item(&format!("conv-{index}"), 3, "m-3"))
        .collect();

    router(publisher.clone(), alice_devices())
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(DeviceHandoffCause::LinkLost, pending),
        )
        .await
        .expect("handoff");

    assert_eq!(publisher.offline.lock().unwrap().len(), 8);
}

/// 会话服务暂时答不上来时整条交接重投；被它拒绝的读取重投也没用，直接进 DLQ。
#[tokio::test]
async fn a_failed_gateway_loss_read_keeps_the_readers_retryability() {
    for (code, retryable) in [
        (ErrorCode::ServiceUnavailable, true),
        (ErrorCode::PermissionDenied, false),
    ] {
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let handler = router(publisher.clone(), alice_devices()).with_recent_conversations(
            Arc::new(Recent {
                fail: Some(code),
                ..Default::default()
            }),
        );

        let error = handler
            .handle_device_handoff(&tenant_ctx(), gateway_loss(1_000))
            .await
            .expect_err("conversation read failed");

        assert_eq!(error.code(), Some(code));
        assert_eq!(error.is_retryable(), retryable, "{code:?}");
        assert!(publisher.offline.lock().unwrap().is_empty());
    }
}

/// 交接按原因计数，补出的每条提示同时计入补提示与离线任务；绑定读失败单独计数。
#[tokio::test]
async fn handoffs_and_catch_up_alerts_are_counted() {
    let metrics = Arc::new(PushServerMetrics::new());
    let handoffs = |cause: &str| {
        metrics
            .device_handoffs_total
            .with_label_values(&[cause])
            .get()
    };

    router(Arc::new(MockPushTaskPublisher::default()), alice_devices())
        .with_metrics(metrics.clone())
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(
                DeviceHandoffCause::AckTimeout,
                vec![item("conv-1", 6, "m-6"), item("conv-2", 3, "m-3")],
            ),
        )
        .await
        .expect("handoff");

    assert_eq!(handoffs("ack_timeout"), 1);
    assert_eq!(metrics.catch_up_alerts_total.get(), 2);
    assert_eq!(
        metrics
            .offline_tasks_total
            .with_label_values(&["loud"])
            .get(),
        2
    );
    assert_eq!(metrics.binding_lookup_failures_total.get(), 0);

    router(
        Arc::new(MockPushTaskPublisher::default()),
        Arc::new(MockDeviceBindings {
            fail: true,
            ..Default::default()
        }),
    )
    .with_metrics(metrics.clone())
    .handle_device_handoff(&tenant_ctx(), gateway_loss(1_000))
    .await
    .expect_err("binding outage");

    assert_eq!(handoffs("gateway_lost"), 1);
    assert_eq!(metrics.binding_lookup_failures_total.get(), 1);
    assert_eq!(metrics.catch_up_alerts_total.get(), 2);
}

#[test]
fn every_cause_has_its_own_label() {
    assert_eq!(handoff_cause_label(DeviceHandoffCause::Parked), "parked");
    assert_eq!(
        handoff_cause_label(DeviceHandoffCause::AckTimeout),
        "ack_timeout"
    );
    assert_eq!(
        handoff_cause_label(DeviceHandoffCause::LinkLost),
        "link_lost"
    );
    assert_eq!(
        handoff_cause_label(DeviceHandoffCause::GatewayLost),
        "gateway_lost"
    );
}

/// 租户不符、缺用户或设备、原因未指定的交接是不可重试的错误，由消费者送进 DLQ。
#[tokio::test]
async fn malformed_and_foreign_handoffs_are_rejected_for_good() {
    let publisher = Arc::new(MockPushTaskPublisher::default());
    let handler = router(publisher.clone(), alice_devices());
    let pending = || vec![item("conv-1", 6, "m-6")];

    let mut foreign = handoff(DeviceHandoffCause::AckTimeout, pending());
    foreign.tenant_id = "tenant-b".to_string();
    let mut no_user = handoff(DeviceHandoffCause::AckTimeout, pending());
    no_user.user_id = " ".to_string();
    let mut no_device = handoff(DeviceHandoffCause::AckTimeout, pending());
    no_device.device_id.clear();
    let unspecified = handoff(DeviceHandoffCause::Unspecified, pending());
    let mut unknown_cause = handoff(DeviceHandoffCause::AckTimeout, pending());
    unknown_cause.cause = 99;

    for (rejected, code) in [
        (foreign, ErrorCode::PermissionDenied),
        (no_user, ErrorCode::InvalidParameter),
        (no_device, ErrorCode::InvalidParameter),
        (unspecified, ErrorCode::InvalidParameter),
        (unknown_cause, ErrorCode::InvalidParameter),
    ] {
        let error = handler
            .handle_device_handoff(&tenant_ctx(), rejected)
            .await
            .expect_err("rejected handoff");
        assert_eq!(error.code(), Some(code));
        assert!(!error.is_retryable());
    }
    assert!(publisher.offline.lock().unwrap().is_empty());
}

/// 绑定读不到、任务发不出去是暂时性失败：报可重试错误，由 broker 重投整条交接。
#[tokio::test]
async fn downstream_outages_are_retryable() {
    let pending = || vec![item("conv-1", 6, "m-6")];

    let registry_down = router(
        Arc::new(MockPushTaskPublisher::default()),
        Arc::new(MockDeviceBindings {
            fail: true,
            ..Default::default()
        }),
    );
    let error = registry_down
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(DeviceHandoffCause::AckTimeout, pending()),
        )
        .await
        .expect_err("binding outage");
    assert!(error.is_retryable());

    let broker_down = router(
        Arc::new(MockPushTaskPublisher {
            fail_offline: true,
            ..Default::default()
        }),
        alice_devices(),
    );
    let error = broker_down
        .handle_device_handoff(
            &tenant_ctx(),
            handoff(DeviceHandoffCause::AckTimeout, pending()),
        )
        .await
        .expect_err("broker outage");
    assert!(error.is_retryable());
}

/// 闸门接线时补提示与正常提示一样进闸门、走批量通道：同一条消息再补一次只静默更新，
/// 紧跟着补的同一会话的下一条被合并，一连串补提示不会接连响铃。
#[tokio::test]
async fn catch_up_alerts_go_through_the_gate_on_the_bulk_lane() {
    use crate::application::notify_gate::NotifyGate;
    use crate::domain::model::NotifyPolicy;
    use crate::infrastructure::notify::MemoryGateStore;

    let publisher = Arc::new(MockPushTaskPublisher::default());
    let messages = Arc::new(Messages {
        stored: vec![
            text_message("conv-1", "m-6", 6, "bob"),
            text_message("conv-1", "m-7", 7, "bob"),
        ],
        ..Default::default()
    });
    let gate = Arc::new(NotifyGate::new(
        Arc::new(MemoryGateStore::primary()),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    ));
    let handler = router(publisher.clone(), alice_devices())
        .with_notify_policy(Arc::new(Membership::of(&[member("conv-1")])))
        .with_message_lookup(messages)
        .with_notify_gate(gate);

    for pending in [
        vec![item("conv-1", 6, "m-6")],
        vec![item("conv-1", 6, "m-6")],
        vec![item("conv-1", 7, "m-7")],
    ] {
        handler
            .handle_device_handoff(
                &tenant_ctx(),
                handoff(DeviceHandoffCause::LinkLost, pending),
            )
            .await
            .expect("handoff");
    }

    let sent = publisher.offline_with_lanes();
    assert!(sent.iter().all(|(_, lane)| *lane == OfflineLane::Bulk));
    let dispositions: Vec<i32> = sent.iter().map(|(task, _)| task.disposition).collect();
    assert_eq!(
        dispositions,
        vec![PushDisposition::Loud as i32, PushDisposition::Quiet as i32],
        "第一次响铃，重复的那次静默更新，下一条被合并"
    );
    assert!(sent.iter().all(|(task, _)| task.device_id == "alice-phone"));
}
