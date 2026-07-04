use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_message_pipeline::{MqPushRepository, PushRepository};
use flare_proto::common::Message;
use flare_server_core::error::Result;
use tracing::instrument;

use crate::domain::repository::{
    UserSyncCompensationRepository, UserSyncCompensationTask, UserSyncIndexRepository,
};

/// 主消息流 fanout 服务。
///
/// 输入消息已经由 `flare-message-ingest` 完成校验、seq 分配、conversation ensure、WAL 和 Hook。
/// 本服务只负责把主流中的持久消息拆到存储流与推送流。
pub struct MessageFanoutService {
    push_repository: Arc<MqPushRepository>,
    user_sync_index: Option<Arc<dyn UserSyncIndexRepository>>,
    user_sync_compensation: Option<Arc<dyn UserSyncCompensationRepository>>,
    inline_message_push_enabled: bool,
}

struct UserSyncCompensationRequest<'a> {
    ctx: &'a Ctx,
    recipient_user_ids: &'a [String],
    conversation_id: &'a str,
    max_conversation_seq: u64,
    occurred_at_ms: i64,
    large_conversation: bool,
    source_error: &'a str,
}

impl MessageFanoutService {
    pub fn new(push_repository: Arc<MqPushRepository>) -> Self {
        Self {
            push_repository,
            user_sync_index: None,
            user_sync_compensation: None,
            inline_message_push_enabled: true,
        }
    }

    pub fn with_user_sync_index(
        mut self,
        user_sync_index: Arc<dyn UserSyncIndexRepository>,
    ) -> Self {
        self.user_sync_index = Some(user_sync_index);
        self
    }

    pub fn with_user_sync_compensation(
        mut self,
        user_sync_compensation: Arc<dyn UserSyncCompensationRepository>,
    ) -> Self {
        self.user_sync_compensation = Some(user_sync_compensation);
        self
    }

    pub fn with_inline_message_push_enabled(mut self, enabled: bool) -> Self {
        self.inline_message_push_enabled = enabled;
        self
    }

    fn normalize_single_chat_routing(
        &self,
        mut message: Message,
        recipient_user_ids: &[String],
    ) -> Message {
        use crate::domain::model::ConversationType;

        if ConversationType::from_proto(message.conversation_type) != ConversationType::Single {
            return message;
        }

        let sender_id = message.sender_id.trim();
        let Some(peer_id) = recipient_user_ids
            .iter()
            .map(|id| id.trim())
            .find(|id| !id.is_empty() && *id != sender_id)
        else {
            return message;
        };

        if message.channel_id.trim() != peer_id {
            tracing::warn!(
                conversation_id = %message.conversation_id,
                message_id = %message.server_id,
                sender_id = %message.sender_id,
                old_channel_id = %message.channel_id,
                normalized_channel_id = %peer_id,
                "Normalized single chat message channel_id from resolved recipient"
            );
            message.channel_id = peer_id.to_string();
        }

        message
    }

    #[instrument(skip(self, recipient_user_ids), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
        recipient_count = recipient_user_ids.len(),
        large_conversation,
    ))]
    pub async fn persist_and_push_with_recipients(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
        large_conversation: bool,
    ) -> Result<()> {
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %message.server_id,
            recipient_count = recipient_user_ids.len(),
            large_conversation,
            "Fanout persistent message to storage and push topics"
        );

        let message = if large_conversation {
            message
        } else {
            self.normalize_single_chat_routing(message, &recipient_user_ids)
        };
        let conversation_id = message.conversation_id.clone();
        let recipient_count = recipient_user_ids.len();
        // 统一读扩散：大群也始终内联 event 投递（经会话级 publish + 网关在线订阅广播），不再发空收件人 ping。
        // 仅当显式关闭内联推送时才回退 notify+pull ping。
        let use_notify_pull_ping = !self.inline_message_push_enabled;
        self.push_repository
            .persistence_only_message(ctx, message.clone(), conversation_id.clone())
            .await?;
        if let Some(user_sync_index) = &self.user_sync_index {
            let sync_result = if large_conversation {
                user_sync_index
                    .record_conversation_version_bump(
                        ctx,
                        &conversation_id,
                        message.conversation_seq,
                        message.created_at,
                    )
                    .await
                    .map(|version| {
                        tracing::trace!(
                            conversation_id = %conversation_id,
                            conversation_seq = message.conversation_seq,
                            conversation_version = version,
                            "Recorded large conversation sync version"
                        );
                    })
            } else {
                user_sync_index
                    .record_conversation_change(
                        ctx,
                        &recipient_user_ids,
                        &conversation_id,
                        message.conversation_seq,
                        message.created_at,
                    )
                    .await
            };
            if let Err(error) = sync_result {
                tracing::warn!(
                    error = %error,
                    conversation_id = %conversation_id,
                    message_id = %message.server_id,
                    conversation_seq = message.conversation_seq,
                    recipient_count,
                    large_conversation,
                    "User sync index update deferred; fanout continues"
                );
                let source_error = error.to_string();
                self.enqueue_user_sync_compensation(UserSyncCompensationRequest {
                    ctx,
                    recipient_user_ids: &recipient_user_ids,
                    conversation_id: &conversation_id,
                    max_conversation_seq: message.conversation_seq,
                    occurred_at_ms: message.created_at,
                    large_conversation,
                    source_error: &source_error,
                })
                .await;
            }
        }
        if use_notify_pull_ping {
            tracing::info!(
                conversation_id = %conversation_id,
                message_id = %message.server_id,
                recipient_count,
                large_conversation,
                inline_message_push_enabled = self.inline_message_push_enabled,
                "Persistent message fanout uses notify+pull ping"
            );
            let ping_recipient_user_ids = if large_conversation {
                Vec::new()
            } else {
                recipient_user_ids
            };
            self.push_repository
                .push_only_message_ping(
                    ctx,
                    &message,
                    ping_recipient_user_ids,
                    conversation_id,
                    large_conversation,
                )
                .await
        } else {
            self.push_repository
                .push_only_message_inline_event(ctx, &message, recipient_user_ids, conversation_id)
                .await
        }
    }

    async fn enqueue_user_sync_compensation(&self, request: UserSyncCompensationRequest<'_>) {
        let Some(repository) = &self.user_sync_compensation else {
            return;
        };
        let due_at_ms = UserSyncCompensationTask::due_now_ms();
        let task = if request.large_conversation {
            UserSyncCompensationTask::conversation_version_bump(
                request.ctx,
                request.conversation_id,
                request.max_conversation_seq,
                request.occurred_at_ms,
                due_at_ms,
            )
        } else {
            UserSyncCompensationTask::eager_user_changes(
                request.ctx,
                request.recipient_user_ids,
                request.conversation_id,
                request.max_conversation_seq,
                request.occurred_at_ms,
                due_at_ms,
            )
        };

        let Some(task) = task else {
            return;
        };
        if let Err(error) = repository.enqueue(task.clone()).await {
            tracing::warn!(
                error = %error,
                source_error = %request.source_error,
                task_id = %task.task_id,
                conversation_id = %request.conversation_id,
                "failed to enqueue user_sync compensation task"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::constants::headers::{
        DELIVERY_MODE_PING, DELIVERY_MODE_PING_WITH_INLINE, HEADER_DELIVERY_MODE,
        HEADER_INLINE_EVENTS_TRUNCATED,
    };
    use flare_im_contracts::constants::topics::{TOPIC_MESSAGE_CREATED, TOPIC_PUSH_EVENTS};
    use flare_proto::common::{
        ConversationType as ProtoConversationType, EventType, MqEnvelope, MqPayloadKind,
        TextContent, message_content, mq_envelope,
    };
    use flare_server_core::Context;
    use flare_server_core::error::Result as FlareResult;
    use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct CapturedSend {
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
        headers: Option<HashMap<String, String>>,
    }

    #[derive(Default)]
    struct CapturingProducer {
        sends: Mutex<Vec<CapturedSend>>,
        events: Option<Arc<Mutex<Vec<String>>>>,
    }

    #[async_trait::async_trait]
    impl Producer for CapturingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            payload: Vec<u8>,
            headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            self.sends
                .lock()
                .expect("capture producer poisoned")
                .push(CapturedSend {
                    topic: topic.to_string(),
                    key: key.map(ToString::to_string),
                    payload,
                    headers,
                });
            if let Some(events) = &self.events {
                events
                    .lock()
                    .expect("events lock poisoned")
                    .push(topic.to_string());
            }
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "capturing-producer"
        }
    }

    struct RecordingUserSyncIndex {
        events: Arc<Mutex<Vec<String>>>,
        fail: bool,
    }

    impl RecordingUserSyncIndex {
        fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                events,
                fail: false,
            }
        }

        fn failing(events: Arc<Mutex<Vec<String>>>) -> Self {
            Self { events, fail: true }
        }
    }

    #[async_trait::async_trait]
    impl UserSyncIndexRepository for RecordingUserSyncIndex {
        async fn record_conversation_change(
            &self,
            _ctx: &Ctx,
            _user_ids: &[String],
            _conversation_id: &str,
            _max_conversation_seq: u64,
            _occurred_at_ms: i64,
        ) -> FlareResult<()> {
            if self.fail {
                return Err(flare_server_core::error::FlareError::system(
                    "injected user sync failure".to_string(),
                ));
            }
            self.events
                .lock()
                .expect("events lock poisoned")
                .push("sync-index".to_string());
            Ok(())
        }

        async fn record_conversation_version_bump(
            &self,
            _ctx: &Ctx,
            _conversation_id: &str,
            _max_conversation_seq: u64,
            _occurred_at_ms: i64,
        ) -> FlareResult<u64> {
            if self.fail {
                return Err(flare_server_core::error::FlareError::system(
                    "injected user sync failure".to_string(),
                ));
            }
            self.events
                .lock()
                .expect("events lock poisoned")
                .push("sync-conversation-version".to_string());
            Ok(1)
        }

        async fn diff_changed_conversations(
            &self,
            _ctx: &Ctx,
            _known: &[(String, u64)],
        ) -> FlareResult<Vec<crate::domain::repository::ConversationChange>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct RecordingUserSyncCompensation {
        tasks: Mutex<Vec<UserSyncCompensationTask>>,
    }

    #[async_trait::async_trait]
    impl UserSyncCompensationRepository for RecordingUserSyncCompensation {
        async fn enqueue(&self, task: UserSyncCompensationTask) -> FlareResult<()> {
            self.tasks
                .lock()
                .expect("compensation tasks lock poisoned")
                .push(task);
            Ok(())
        }

        async fn claim_due(&self, _limit: usize) -> FlareResult<Vec<UserSyncCompensationTask>> {
            Ok(Vec::new())
        }

        async fn mark_completed(&self, _task_id: &str) -> FlareResult<()> {
            Ok(())
        }

        async fn mark_failed(
            &self,
            _task: UserSyncCompensationTask,
            _error: &str,
            _retry_after_ms: i64,
        ) -> FlareResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn persistent_message_fanout_writes_storage_before_push() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let producer = Arc::new(CapturingProducer {
            sends: Mutex::new(Vec::new()),
            events: Some(events.clone()),
        });
        let repository = MqPushRepository::new(producer.clone());
        let service = MessageFanoutService::new(repository)
            .with_user_sync_index(Arc::new(RecordingUserSyncIndex::new(events.clone())));
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-mainline-contract")
                .with_trace_id("trace-mainline-contract")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            conversation_type: ProtoConversationType::Single as i32,
            channel_id: "stale-peer".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 42,
            created_at: 1_700_000_000_000,
            ..Message::default()
        };

        service
            .persist_and_push_with_recipients(
                &ctx,
                message,
                vec!["sender-a".to_string(), "peer-b".to_string()],
                false,
            )
            .await
            .expect("fanout should succeed");

        let captured = producer
            .sends
            .lock()
            .expect("capture producer poisoned")
            .clone();
        assert_eq!(
            captured.len(),
            2,
            "persistent messages must fan out to storage and push topics"
        );
        assert_eq!(
            events.lock().expect("events lock poisoned").as_slice(),
            [TOPIC_MESSAGE_CREATED, "sync-index", TOPIC_PUSH_EVENTS],
            "user sync index must be updated after storage publish and before inline event push publish"
        );

        let storage = &captured[0];
        assert_eq!(storage.topic, TOPIC_MESSAGE_CREATED);
        assert_eq!(storage.key.as_deref(), Some("conversation-a"));
        let storage_headers = storage.headers.as_ref().expect("storage headers");
        assert_eq!(
            storage_headers.get("x-tenant-id").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(
            storage_headers.get("x-trace-id").map(String::as_str),
            Some("trace-mainline-contract")
        );
        assert_eq!(
            storage_headers.get("persistence-only").map(String::as_str),
            Some("true")
        );
        let storage_envelope =
            MqEnvelope::decode(storage.payload.as_slice()).expect("decode storage envelope");
        assert!(storage_envelope.persistence_only);
        assert!(!storage_envelope.push_only);
        assert!(storage_envelope.recipient_user_ids.is_empty());

        let Some(mq_envelope::Payload::Message(storage_message)) = storage_envelope.payload else {
            panic!("storage envelope must carry message payload");
        };
        assert_eq!(storage_message.server_id, "message-a");
        assert_eq!(
            storage_message.channel_id, "peer-b",
            "single-chat channel_id must be normalized before storage fanout"
        );

        let push = &captured[1];
        assert_eq!(push.topic, TOPIC_PUSH_EVENTS);
        assert_eq!(push.key.as_deref(), Some("conversation-a"));
        let push_headers = push.headers.as_ref().expect("push headers");
        assert_eq!(
            push_headers.get("push-only").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            push_headers.get(HEADER_DELIVERY_MODE).map(String::as_str),
            Some(DELIVERY_MODE_PING_WITH_INLINE)
        );
        assert_eq!(
            push_headers
                .get(HEADER_INLINE_EVENTS_TRUNCATED)
                .map(String::as_str),
            Some("false")
        );
        let push_envelope =
            MqEnvelope::decode(push.payload.as_slice()).expect("decode inline event envelope");
        assert!(push_envelope.push_only);
        assert!(!push_envelope.persistence_only);
        assert_eq!(push_envelope.payload_kind, MqPayloadKind::Event as i32);
        assert_eq!(
            push_envelope.recipient_user_ids,
            vec!["sender-a".to_string(), "peer-b".to_string()]
        );

        let Some(mq_envelope::Payload::Event(push_event)) = push_envelope.payload else {
            panic!("push envelope must carry inline message event payload");
        };
        assert_eq!(push_event.event_id, "message-inline:message-a");
        assert_eq!(push_event.r#type, EventType::EventMessage as i32);
        let Some(flare_proto::common::event::Payload::Message(push_message)) = push_event.payload
        else {
            panic!("inline event must carry message payload");
        };
        assert_eq!(push_message.server_id, "message-a");
        assert_eq!(push_message.channel_id, "peer-b");
    }

    #[tokio::test]
    async fn large_conversation_fanout_uses_inline_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let producer = Arc::new(CapturingProducer {
            sends: Mutex::new(Vec::new()),
            events: Some(events.clone()),
        });
        let repository = MqPushRepository::new(producer.clone());
        let service = MessageFanoutService::new(repository)
            .with_user_sync_index(Arc::new(RecordingUserSyncIndex::new(events.clone())));
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-large-conversation-ping")
                .with_trace_id("trace-large-conversation-ping")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-large-a".to_string(),
            conversation_id: "conversation-large-a".to_string(),
            conversation_type: ProtoConversationType::Group as i32,
            channel_id: "conversation-large-a".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 777,
            created_at: 1_700_000_000_777,
            content: Some(flare_proto::common::MessageContent {
                content: Some(message_content::Content::Text(TextContent {
                    text: "large room payload should stay off push ping".to_string(),
                    mentions: vec![],
                })),
            }),
            ..Message::default()
        };

        service
            .persist_and_push_with_recipients(&ctx, message, Vec::new(), true)
            .await
            .expect("fanout should succeed");

        let captured = producer
            .sends
            .lock()
            .expect("capture producer poisoned")
            .clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            events.lock().expect("events lock poisoned").as_slice(),
            [
                TOPIC_MESSAGE_CREATED,
                "sync-conversation-version",
                TOPIC_PUSH_EVENTS
            ],
            "large conversation fanout must persist, bump conversation version, then publish inline event"
        );

        let push = &captured[1];
        assert_eq!(push.topic, TOPIC_PUSH_EVENTS);
        assert_eq!(push.key.as_deref(), Some("conversation-large-a"));
        let push_headers = push.headers.as_ref().expect("push headers");
        assert_eq!(
            push_headers.get("push-only").map(String::as_str),
            Some("true")
        );
        // 统一读扩散：大群也内联 event 投递（PingWithInline），经会话级 publish + 网关在线订阅广播。
        assert_eq!(
            push_headers.get(HEADER_DELIVERY_MODE).map(String::as_str),
            Some(DELIVERY_MODE_PING_WITH_INLINE)
        );
        assert_eq!(
            push_headers
                .get(HEADER_INLINE_EVENTS_TRUNCATED)
                .map(String::as_str),
            Some("false")
        );

        let push_envelope =
            MqEnvelope::decode(push.payload.as_slice()).expect("decode inline event envelope");
        assert!(push_envelope.push_only);
        assert!(!push_envelope.persistence_only);
        assert_eq!(push_envelope.payload_kind, MqPayloadKind::Event as i32);
        assert_eq!(push_envelope.conversation_id, "conversation-large-a");
        assert_eq!(push_envelope.seq, 777);
        assert_eq!(
            push_envelope.recipient_user_ids,
            Vec::<String>::new(),
            "large conversation inline event must not carry materialized push recipients"
        );

        let Some(mq_envelope::Payload::Event(event)) = push_envelope.payload else {
            panic!("inline event envelope must carry event");
        };
        assert_eq!(event.event_id, "message-inline:message-large-a");
        assert_eq!(event.conversation_id, "conversation-large-a");
        assert_eq!(event.conversation_seq, 777);
        assert_eq!(event.r#type, EventType::EventMessage as i32);
        assert!(
            event.payload.is_some(),
            "inline event must carry message payload for read-fanout broadcast"
        );
    }

    #[tokio::test]
    async fn inline_push_kill_switch_uses_notify_pull_ping_for_small_conversation() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let producer = Arc::new(CapturingProducer {
            sends: Mutex::new(Vec::new()),
            events: Some(events.clone()),
        });
        let repository = MqPushRepository::new(producer.clone());
        let service = MessageFanoutService::new(repository)
            .with_inline_message_push_enabled(false)
            .with_user_sync_index(Arc::new(RecordingUserSyncIndex::new(events.clone())));
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-inline-kill-switch")
                .with_trace_id("trace-inline-kill-switch")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-inline-off".to_string(),
            conversation_id: "conversation-inline-off".to_string(),
            conversation_type: ProtoConversationType::Single as i32,
            channel_id: "peer-b".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 9,
            created_at: 1_700_000_000_009,
            ..Message::default()
        };

        service
            .persist_and_push_with_recipients(
                &ctx,
                message,
                vec!["sender-a".to_string(), "peer-b".to_string()],
                false,
            )
            .await
            .expect("fanout should succeed");

        let captured = producer
            .sends
            .lock()
            .expect("capture producer poisoned")
            .clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            events.lock().expect("events lock poisoned").as_slice(),
            [TOPIC_MESSAGE_CREATED, "sync-index", TOPIC_PUSH_EVENTS],
            "kill switch must preserve storage and sync index ordering before ping"
        );

        let push = &captured[1];
        let push_headers = push.headers.as_ref().expect("push headers");
        assert_eq!(
            push_headers.get(HEADER_DELIVERY_MODE).map(String::as_str),
            Some(DELIVERY_MODE_PING)
        );
        let push_envelope =
            MqEnvelope::decode(push.payload.as_slice()).expect("decode ping envelope");
        assert!(!push_envelope.large_conversation);
        assert_eq!(
            push_envelope.recipient_user_ids,
            vec!["sender-a".to_string(), "peer-b".to_string()]
        );
        let Some(mq_envelope::Payload::Event(event)) = push_envelope.payload else {
            panic!("ping envelope must carry event watermark");
        };
        assert_eq!(event.event_id, "message-ping:message-inline-off");
        assert!(event.payload.is_none());
    }

    #[tokio::test]
    async fn user_sync_failure_does_not_block_large_conversation_fanout() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let producer = Arc::new(CapturingProducer {
            sends: Mutex::new(Vec::new()),
            events: Some(events.clone()),
        });
        let repository = MqPushRepository::new(producer.clone());
        let compensation = Arc::new(RecordingUserSyncCompensation::default());
        let service = MessageFanoutService::new(repository)
            .with_user_sync_index(Arc::new(RecordingUserSyncIndex::failing(events.clone())))
            .with_user_sync_compensation(compensation.clone());
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-user-sync-failure")
                .with_trace_id("trace-user-sync-failure")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-sync-failure".to_string(),
            conversation_id: "conversation-sync-failure".to_string(),
            conversation_type: ProtoConversationType::Group as i32,
            channel_id: "conversation-sync-failure".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 1001,
            created_at: 1_700_000_001_001,
            ..Message::default()
        };

        service
            .persist_and_push_with_recipients(&ctx, message, Vec::new(), true)
            .await
            .expect("fanout should continue when user sync is deferred");

        assert_eq!(
            events.lock().expect("events lock poisoned").as_slice(),
            [TOPIC_MESSAGE_CREATED, TOPIC_PUSH_EVENTS],
            "user_sync failure must not block push publication"
        );
        let captured = producer.sends.lock().expect("capture producer poisoned");
        assert_eq!(captured.len(), 2);
        let push = &captured[1];
        let push_envelope =
            MqEnvelope::decode(push.payload.as_slice()).expect("decode inline event envelope");
        assert_eq!(push_envelope.conversation_id, "conversation-sync-failure");
        // 统一读扩散：大群内联 event（不再设置 envelope.large_conversation ping 标记）。
        assert!(!push_envelope.large_conversation);
        assert_eq!(push_envelope.recipient_user_ids, Vec::<String>::new());
        let compensation_tasks = compensation
            .tasks
            .lock()
            .expect("compensation tasks lock poisoned");
        assert_eq!(compensation_tasks.len(), 1);
        assert_eq!(
            compensation_tasks[0].kind,
            crate::domain::repository::UserSyncCompensationKind::ConversationVersionBump
        );
        assert_eq!(
            compensation_tasks[0].conversation_id,
            "conversation-sync-failure"
        );
        assert_eq!(compensation_tasks[0].max_conversation_seq, 1001);
        assert!(
            compensation_tasks[0].user_ids.is_empty(),
            "large conversation compensation must not retain materialized recipients"
        );
    }
}
