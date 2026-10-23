//! 事件处理器（编排层）- 负责编排领域服务
//!
//! ## 核心职责
//! 1. 事件校验（调用 EventDomainService）
//! 2. 准入：不推进读者水位的已读回执在此丢弃，不分配序列号、不扇出
//! 3. 序列号分配（调用 EventDomainService）
//! 4. 事件推送（调用 EventDomainService）
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - CQRS：Command Handler 负责写操作

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_im_message_pipeline::{MqPushRepository, PushRepository};
use flare_im_seq::SequenceAllocator;
use flare_proto::common::Event;
use tracing::instrument;

use crate::domain::PersistenceMode;
use crate::domain::service::event_domain_service::SequenceAllocatorPort;
use crate::domain::service::{EventAdmission, EventDomainService};
use flare_server_core::error::Result;

/// 事件处理器（编排层）
pub struct EventHandler<PR = MqPushRepository, SA = SequenceAllocator>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    /// 事件领域服务
    event_domain_service: Arc<EventDomainService<PR, SA>>,
}

impl<PR, SA> Clone for EventHandler<PR, SA>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    fn clone(&self) -> Self {
        Self {
            event_domain_service: Arc::clone(&self.event_domain_service),
        }
    }
}

/// 慢事件扇出日志阈值（毫秒），env `FLARE_EVENT_SLOW_LOG_MS`，默认 200。
fn event_slow_log_threshold_ms() -> u64 {
    static VALUE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        parse_event_slow_log_ms(std::env::var("FLARE_EVENT_SLOW_LOG_MS").ok().as_deref())
    })
}

/// 纯函数便于测试：非法值回落默认，不能回落成 0（那会给每条事件写一行日志）。
fn parse_event_slow_log_ms(raw: Option<&str>) -> u64 {
    const DEFAULT_MS: u64 = 200;
    match raw {
        Some(v) => v.trim().parse::<u64>().unwrap_or(DEFAULT_MS),
        None => DEFAULT_MS,
    }
}

/// 事件的租户只来自上下文；缺失即拒绝，不再按租户 "0" 分配 seq 与发布（显式的 "0" 照常）。
fn event_tenant(ctx: &Ctx) -> Result<String> {
    normalize_optional_tenant_id(ctx.tenant_id()).ok_or_else(|| {
        flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::InvalidParameter,
            "tenant required to handle a message event"
        )
    })
}

impl<PR, SA> EventHandler<PR, SA>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    pub fn new(event_domain_service: Arc<EventDomainService<PR, SA>>) -> Self {
        Self {
            event_domain_service,
        }
    }

    /// 处理事件
    ///
    /// # 编排流程
    /// 1. 校验事件
    /// 2. 分配序列号
    /// 3. 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn handle_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let tenant_id = event_tenant(ctx)?;
        // 所有事件统一走通用流程（校验 → 授权 → 分配序列号 → 推送）。
        // 会话标题/头像/删除等读模型变更走直接 gRPC（flare-conversation 同步写读模型），
        // 不经事件路径——避免与直接 API 形成双路径。
        self.handle_general_event(ctx, &tenant_id, event, true)
            .await
    }

    /// An event an in-process system producer of this service built (the burn worker, the burn
    /// command after its caller checked the message): validated, sequenced and fanned out like any
    /// other, without the operator authorization of [`Self::handle_event`]. No RPC reaches it.
    pub async fn handle_system_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let tenant_id = event_tenant(ctx)?;
        self.handle_general_event(ctx, &tenant_id, event, false)
            .await
    }

    /// 处理通用事件
    ///
    /// # 编排流程
    /// 1. 校验事件
    /// 2. 准入（重复的已读回执到此为止）
    /// 3. 分配序列号
    /// 4. 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    async fn handle_general_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: Event,
        authorize: bool,
    ) -> Result<()> {
        // 分段计时：十万人群的一条已读回执实测端到端 52 秒，而 orchestrator CPU
        // 只有 0.6%、Redis 累计 1.27s、Postgres 全程空闲——即"在等"，但等在哪
        // 这条链路上没有任何数字。两人单聊同样的事件只要 15ms，所以耗时随接收者
        // 规模增长。先量出是三步里的哪一步，别再靠猜。
        let started = std::time::Instant::now();

        // 1. 校验事件
        self.event_domain_service
            .validate_event(ctx, tenant_id, &event)
            .await?;
        // 1b. Authorize the operation against its operator and, for message operations, the
        // stored message, so nobody edits or recalls another member's message: every entry
        // reaches this point.
        let event = if authorize {
            self.event_domain_service
                .authorize_operation(ctx, tenant_id, event)
                .await?
        } else {
            event
        };
        let validate_ms = started.elapsed().as_millis() as u64;

        // 2. Admission. A read receipt that does not raise its reader's watermark is a
        // repeat: without this gate each resend became a new seq fanned out to every
        // member (a client loop drove a 4-message chat to seq 6463 at ~40 events/s).
        let read_claim = match self
            .event_domain_service
            .admit_event(tenant_id, &event)
            .await
        {
            EventAdmission::Admit { read_claim } => read_claim,
            EventAdmission::StaleReadReceipt {
                read_seq,
                watermark,
            } => {
                tracing::debug!(
                    conversation_id = %event.conversation_id,
                    read_seq,
                    watermark,
                    "skip read receipt: read_seq does not advance the reader's watermark"
                );
                return Ok(());
            }
        };

        // 3-4. Allocate and fan out. On failure hand the watermark back so the client's
        // retry of the same position is admitted.
        let (event_with_seq, allocate_ms, push_ms) =
            match self.allocate_and_push(ctx, tenant_id, event).await {
                Ok(done) => done,
                Err(error) => {
                    if let Some(claim) = read_claim {
                        self.event_domain_service
                            .release_read_claim(tenant_id, claim)
                            .await;
                    }
                    return Err(error);
                }
            };

        let total_ms = started.elapsed().as_millis() as u64;
        if total_ms >= event_slow_log_threshold_ms() {
            tracing::info!(
                conversation_id = %event_with_seq.conversation_id,
                event_type = event_with_seq.r#type,
                validate_ms,
                allocate_ms,
                push_ms,
                total_ms,
                "slow event fanout"
            );
        }

        Ok(())
    }

    /// Allocate the seq and fan out; returns the sequenced event and both timings (ms).
    async fn allocate_and_push(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: Event,
    ) -> Result<(Event, u64, u64)> {
        let allocate_started = std::time::Instant::now();
        let event_with_seq = self
            .event_domain_service
            .allocate_seq(ctx, tenant_id, event)
            .await?;
        let allocate_ms = allocate_started.elapsed().as_millis() as u64;

        let push_started = std::time::Instant::now();
        self.event_domain_service
            .push_event(ctx, event_with_seq.clone(), PersistenceMode::Auto)
            .await?;
        let push_ms = push_started.elapsed().as_millis() as u64;

        Ok((event_with_seq, allocate_ms, push_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::{event_tenant, parse_event_slow_log_ms};
    use flare_im_contracts::Ctx;
    use flare_server_core::Context;
    use std::sync::Arc;

    /// ExecuteEvent 与内部事件入口在上下文缺租户时拒绝，不按租户 "0" 处理（W11-T）。
    #[test]
    fn events_require_a_context_tenant() {
        for missing in [
            Context::with_request_id("req"),
            Context::with_request_id("req").with_tenant_id(""),
            Context::with_request_id("req").with_tenant_id("  "),
        ] {
            let ctx: Ctx = Arc::new(missing);
            let error = event_tenant(&ctx).expect_err("no tenant");
            assert_eq!(
                error.code(),
                Some(flare_server_core::error::ErrorCode::InvalidParameter)
            );
        }
        for tenant in ["tenant-a", "0"] {
            let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id(tenant));
            assert_eq!(event_tenant(&ctx).expect("tenant"), tenant);
        }
    }

    #[test]
    fn slow_log_threshold_rejects_garbage_without_falling_back_to_zero() {
        assert_eq!(parse_event_slow_log_ms(None), 200);
        assert_eq!(parse_event_slow_log_ms(Some("500")), 500);
        assert_eq!(parse_event_slow_log_ms(Some(" 500 ")), 500);
        assert_eq!(parse_event_slow_log_ms(Some("0")), 0);
        assert_eq!(parse_event_slow_log_ms(Some("abc")), 200);
        assert_eq!(parse_event_slow_log_ms(Some("")), 200);
        assert_eq!(parse_event_slow_log_ms(Some("-1")), 200);
    }
}

/// Client read acks reach the orchestrator as `EVENT_READ_RECEIPT` (route
/// `ack_to_push_proxy`). Only a receipt that raises its reader's watermark may cost
/// a conversation seq and a fanout.
#[cfg(test)]
mod read_ack_admission_tests {
    use super::EventHandler;
    use crate::domain::repository::{
        ReadWatermarkAdvance, ReadWatermarkRepository, RecipientRepository,
    };
    use crate::domain::service::EventDomainService;
    use crate::domain::service::event_domain_service::SequenceAllocatorPort;
    use flare_im_contracts::Ctx;
    use flare_im_message_pipeline::{ConversationType, PushRepository};
    use flare_proto::common::{
        Event, EventType, Message, PushEnvelope, ReadReceiptEvent, event::Payload,
    };
    use flare_server_core::Context;
    use flare_server_core::error::{FlareError, Result};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    const SINGLE_CHAT: &str = "1ADW0CVC4ESXQ0046R";

    /// Captures what reaches the MQ boundary; `fail_publish` simulates a fanout outage.
    #[derive(Default)]
    struct RecordingPushRepository {
        published: Mutex<Vec<(Event, Vec<String>)>>,
        fail_publish: AtomicBool,
    }

    impl PushRepository for RecordingPushRepository {
        async fn publish_message(
            &self,
            _ctx: &Ctx,
            _message: Message,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
            _large_conversation: bool,
        ) -> Result<()> {
            unreachable!("event handling never publishes messages")
        }

        async fn publish_event(
            &self,
            _ctx: &Ctx,
            event: Event,
            recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            if self.fail_publish.load(Ordering::SeqCst) {
                return Err(FlareError::system("injected fanout outage"));
            }
            self.published
                .lock()
                .expect("published lock poisoned")
                .push((event, recipient_user_ids));
            Ok(())
        }

        async fn persistence_only_message(
            &self,
            _ctx: &Ctx,
            _message: Message,
            _conversation_id: String,
        ) -> Result<()> {
            unreachable!("event handling never persists messages")
        }

        async fn persistence_only_event(
            &self,
            _ctx: &Ctx,
            _event: Event,
            _conversation_id: String,
        ) -> Result<()> {
            unreachable!("durable events go through publish_event")
        }

        async fn push_only_message(
            &self,
            _ctx: &Ctx,
            _message: Message,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            unreachable!("event handling never pushes messages")
        }

        async fn push_only_event(
            &self,
            _ctx: &Ctx,
            _event: Event,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            unreachable!("durable events go through publish_event")
        }

        async fn publish_push_envelope(&self, _ctx: &Ctx, _envelope: PushEnvelope) -> Result<()> {
            unreachable!("event handling never publishes push envelopes")
        }
    }

    #[derive(Default)]
    struct CountingSequenceAllocator {
        allocated: AtomicU64,
    }

    impl SequenceAllocatorPort for CountingSequenceAllocator {
        async fn allocate_seq(&self, _conversation_id: &str, _tenant_id: &str) -> Result<u64> {
            Ok(self.allocated.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    struct TwoMembers;

    impl RecipientRepository for TwoMembers {
        fn get_message_recipients<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _conversation_id: &'a str,
            _conversation_type: ConversationType,
            _channel_id: Option<&'a str>,
            _sender_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
            unreachable!("event handling never resolves message recipients")
        }

        fn get_event_recipients<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _message_id: &'a str,
            _conversation_id: &'a str,
            _event_type: EventType,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
            unreachable!("event recipients come from the member list")
        }

        fn get_conversation_members<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _conversation_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
            Box::pin(async { Ok(vec!["reader".to_string(), "peer".to_string()]) })
        }

        fn get_conversation_member_count<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _conversation_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(2) })
        }
    }

    /// Same contract as `RedisReadWatermarkRepository`: strict compare-and-raise per
    /// (tenant, conversation, reader), fenced rollback.
    #[derive(Default)]
    struct InMemoryReadWatermark {
        positions: Mutex<HashMap<(String, String, String), u64>>,
        unavailable: AtomicBool,
    }

    #[async_trait::async_trait]
    impl ReadWatermarkRepository for InMemoryReadWatermark {
        async fn advance(
            &self,
            tenant_id: &str,
            conversation_id: &str,
            user_id: &str,
            read_seq: u64,
        ) -> Result<ReadWatermarkAdvance> {
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(FlareError::system("injected watermark outage"));
            }
            let mut positions = self.positions.lock().expect("positions lock poisoned");
            let current = positions
                .entry((
                    tenant_id.to_string(),
                    conversation_id.to_string(),
                    user_id.to_string(),
                ))
                .or_insert(0);
            if read_seq <= *current {
                return Ok(ReadWatermarkAdvance::NotAdvanced { current: *current });
            }
            let previous = std::mem::replace(current, read_seq);
            Ok(ReadWatermarkAdvance::Advanced { previous })
        }

        async fn rollback(
            &self,
            tenant_id: &str,
            conversation_id: &str,
            user_id: &str,
            advanced_to: u64,
            previous: u64,
        ) -> Result<()> {
            let mut positions = self.positions.lock().expect("positions lock poisoned");
            let key = (
                tenant_id.to_string(),
                conversation_id.to_string(),
                user_id.to_string(),
            );
            if let Some(current) = positions.get_mut(&key)
                && *current == advanced_to
            {
                *current = previous;
            }
            Ok(())
        }
    }

    struct Harness {
        handler: EventHandler<RecordingPushRepository, CountingSequenceAllocator>,
        push: Arc<RecordingPushRepository>,
        seq: Arc<CountingSequenceAllocator>,
        watermark: Arc<InMemoryReadWatermark>,
    }

    /// Stored messages by server id, for the message operation checks.
    struct StoredMessages(HashMap<String, Message>);

    #[async_trait::async_trait]
    impl crate::domain::repository::MessageTargetRepository for StoredMessages {
        async fn message(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            server_msg_id: &str,
        ) -> Result<Option<Message>> {
            Ok(self.0.get(server_msg_id).cloned())
        }
    }

    impl Harness {
        fn new() -> Self {
            Self::with_messages(Vec::new())
        }

        fn with_messages(messages: Vec<Message>) -> Self {
            let push = Arc::new(RecordingPushRepository::default());
            let seq = Arc::new(CountingSequenceAllocator::default());
            let watermark = Arc::new(InMemoryReadWatermark::default());
            let targets = StoredMessages(
                messages
                    .into_iter()
                    .map(|message| (message.server_id.clone(), message))
                    .collect(),
            );
            let service =
                EventDomainService::new(push.clone(), Arc::new(TwoMembers), seq.clone(), None)
                    .with_read_watermark(watermark.clone())
                    .with_message_targets(Arc::new(targets));
            Self {
                handler: EventHandler::new(Arc::new(service)),
                push,
                seq,
                watermark,
            }
        }

        fn allocated_seqs(&self) -> u64 {
            self.seq.allocated.load(Ordering::SeqCst)
        }

        fn published(&self) -> Vec<(Event, Vec<String>)> {
            self.push
                .published
                .lock()
                .expect("published lock poisoned")
                .clone()
        }

        /// `(user_id, read_seq, conversation_seq)` of every published read receipt.
        fn published_reads(&self) -> Vec<(String, u64, u64)> {
            self.published()
                .into_iter()
                .filter_map(|(event, _)| match event.payload {
                    Some(Payload::Read(read)) => {
                        Some((read.user_id, read.read_seq, event.conversation_seq))
                    }
                    _ => None,
                })
                .collect()
        }
    }

    /// The context the route forwards for the connected user `reader`.
    fn ctx(request_id: &str) -> Ctx {
        ctx_as(request_id, "reader")
    }

    fn ctx_as(request_id: &str, user: &str) -> Ctx {
        Arc::new(
            Context::with_request_id(request_id)
                .with_tenant_id("tenant-a")
                .with_user_id(user)
                .with_actor(
                    flare_server_core::context::ActorContext::new(user)
                        .with_type(flare_server_core::context::ActorType::User),
                ),
        )
    }

    /// Capability publishes plugin shared state with its own signed system context.
    fn system_ctx(request_id: &str) -> Ctx {
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        use flare_im_service_kit::plugin_platform::{
            CARD_POST_KID, STATE_PUBLISH_ROLE, STATE_PUBLISHER_ID,
        };
        let key = ed25519_dalek::SigningKey::from_bytes(&[29; 32]);
        let signer = flare_server_core::SignedContextSigner::from_private_key(
            &key.to_pkcs8_pem(Default::default()).unwrap(),
        )
        .unwrap()
        .with_kid(CARD_POST_KID)
        .unwrap();
        let mut keys = flare_server_core::auth::KeySet::empty();
        keys.insert(
            CARD_POST_KID,
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let token = signer
            .sign(
                &flare_server_core::SignedContextIdentity::new(
                    flare_server_core::SignedActorType::System,
                    STATE_PUBLISHER_ID,
                    "tenant-a",
                    request_id,
                )
                .with_roles(vec![STATE_PUBLISH_ROLE.into()]),
            )
            .unwrap();
        let mut carrier = tonic::Request::new(());
        carrier
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
            carrier.metadata(),
            &flare_server_core::SignedContextVerifier::from_key_set(keys),
        )
        .unwrap()
    }

    /// The event route builds from a client `ReadAck`.
    fn read_ack_event(user_id: &str, read_seq: u64) -> Event {
        Event {
            conversation_id: SINGLE_CHAT.to_string(),
            conversation_seq: 0,
            r#type: EventType::EventReadReceipt as i32,
            created_at: 1,
            event_id: uuid::Uuid::new_v4().to_string(),
            request_id: None,
            payload: Some(Payload::Read(ReadReceiptEvent {
                conversation_id: SINGLE_CHAT.to_string(),
                read_seq,
                user_id: user_id.to_string(),
                message_ids: Vec::new(),
                read_at: Some(1),
            })),
        }
    }

    #[tokio::test]
    async fn repeated_read_acks_allocate_no_seq_and_emit_no_event() {
        let h = Harness::new();

        h.handler
            .handle_event(&ctx("ack-1"), read_ack_event("reader", 3420))
            .await
            .expect("first ack");
        assert_eq!(h.allocated_seqs(), 1);
        assert_eq!(h.published_reads(), vec![("reader".to_string(), 3420, 1)]);
        assert_eq!(h.published()[0].1, vec!["peer".to_string()]);

        // The iOS loop: the same position resent on every inbound event, plus a second
        // device of the same reader reporting it, plus a late, older position.
        for attempt in 0..50 {
            h.handler
                .handle_event(
                    &ctx(&format!("resend-{attempt}")),
                    read_ack_event("reader", 3420),
                )
                .await
                .expect("resent ack is a silent no-op");
        }
        h.handler
            .handle_event(&ctx("device-2"), read_ack_event("reader", 3420))
            .await
            .expect("second device ack is a silent no-op");
        h.handler
            .handle_event(&ctx("late"), read_ack_event("reader", 3000))
            .await
            .expect("older ack is a silent no-op");
        assert_eq!(h.allocated_seqs(), 1, "repeats must not allocate seqs");
        assert_eq!(h.published().len(), 1, "repeats must not fan out");

        // An advancing ack is sequenced and fanned out as before.
        h.handler
            .handle_event(&ctx("ack-2"), read_ack_event("reader", 3421))
            .await
            .expect("advancing ack");
        assert_eq!(h.allocated_seqs(), 2);
        assert_eq!(
            h.published_reads(),
            vec![
                ("reader".to_string(), 3420, 1),
                ("reader".to_string(), 3421, 2)
            ]
        );
    }

    #[tokio::test]
    async fn one_members_watermark_never_suppresses_another_members_ack() {
        let h = Harness::new();

        for (request_id, user_id) in [("a-1", "reader"), ("b-1", "peer"), ("b-2", "peer")] {
            h.handler
                .handle_event(&ctx_as(request_id, user_id), read_ack_event(user_id, 3420))
                .await
                .expect("read ack");
        }

        assert_eq!(h.allocated_seqs(), 2);
        assert_eq!(
            h.published_reads(),
            vec![
                ("reader".to_string(), 3420, 1),
                ("peer".to_string(), 3420, 2)
            ]
        );
    }

    #[tokio::test]
    async fn failed_fanout_hands_the_position_back_for_a_retry() {
        let h = Harness::new();

        h.push.fail_publish.store(true, Ordering::SeqCst);
        h.handler
            .handle_event(&ctx("ack-1"), read_ack_event("reader", 3420))
            .await
            .expect_err("fanout outage surfaces");
        assert!(h.published().is_empty());

        h.push.fail_publish.store(false, Ordering::SeqCst);
        h.handler
            .handle_event(&ctx("ack-1-retry"), read_ack_event("reader", 3420))
            .await
            .expect("retry of the same position");
        assert_eq!(h.published_reads().len(), 1);
    }

    #[tokio::test]
    async fn watermark_outage_admits_read_acks() {
        let h = Harness::new();
        h.watermark.unavailable.store(true, Ordering::SeqCst);

        for request_id in ["ack-1", "ack-1-again"] {
            h.handler
                .handle_event(&ctx(request_id), read_ack_event("reader", 3420))
                .await
                .expect("read ack");
        }

        assert_eq!(h.allocated_seqs(), 2);
        assert_eq!(h.published().len(), 2);
    }

    #[tokio::test]
    async fn receipts_naming_messages_and_other_events_are_not_gated() {
        let h = Harness::new();

        let mut per_message = read_ack_event("reader", 3420);
        if let Some(Payload::Read(read)) = per_message.payload.as_mut() {
            read.message_ids = vec!["m-1".to_string()];
        }
        let mut custom = read_ack_event("reader", 3420);
        custom.r#type = EventType::EventCustom as i32;
        custom.payload = Some(Payload::Custom(flare_proto::common::CustomEvent {
            namespace: "biz".into(),
            name: "nudge".into(),
            ..Default::default()
        }));

        for event in [per_message.clone(), per_message, custom.clone(), custom] {
            let event = Event {
                event_id: uuid::Uuid::new_v4().to_string(),
                ..event
            };
            h.handler
                .handle_event(&ctx("other"), event)
                .await
                .expect("ungated event");
        }

        assert_eq!(h.allocated_seqs(), 4);
        assert_eq!(h.published().len(), 4);
    }

    /// Authorization at the single event entry: a member recalling or editing another member's
    /// message is refused before the event costs a seq or reaches anyone; the sender's own recall
    /// goes out. The WebSocket uplink, `ExecuteEvent` and `MessageActionService` all end here.
    #[tokio::test]
    async fn another_members_recall_or_edit_never_gets_a_seq() {
        let sent = Message {
            server_id: "m-reader".into(),
            conversation_id: SINGLE_CHAT.into(),
            sender_id: "reader".into(),
            created_at: chrono::Utc::now().timestamp_millis() - 1_000,
            content: Some(flare_proto::common::MessageContent {
                content: Some(flare_proto::common::message_content::Content::Text(
                    flare_proto::common::TextContent {
                        text: "hi".into(),
                        mentions: vec![],
                    },
                )),
            }),
            ..Default::default()
        };
        let h = Harness::with_messages(vec![sent]);
        let recall = || Event {
            conversation_id: SINGLE_CHAT.to_string(),
            r#type: EventType::EventMessageRecall as i32,
            event_id: uuid::Uuid::new_v4().to_string(),
            payload: Some(Payload::Recall(flare_proto::common::MessageRecallEvent {
                server_msg_id: "m-reader".into(),
                time_limit_seconds: Some(i32::MAX),
                allow_admin_recall: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        };
        let edit = Event {
            conversation_id: SINGLE_CHAT.to_string(),
            r#type: EventType::EventMessageEdit as i32,
            event_id: uuid::Uuid::new_v4().to_string(),
            payload: Some(Payload::Edit(flare_proto::common::MessageEditEvent {
                server_msg_id: "m-reader".into(),
                new_content: Some(flare_proto::common::MessageContent {
                    content: Some(flare_proto::common::message_content::Content::Text(
                        flare_proto::common::TextContent {
                            text: "forged".into(),
                            mentions: vec![],
                        },
                    )),
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        for forged in [recall(), edit] {
            let error = h
                .handler
                .handle_event(&ctx_as("peer-op", "peer"), forged)
                .await
                .expect_err("another member's operation");
            assert_eq!(
                error.code(),
                Some(flare_server_core::error::ErrorCode::PermissionDenied)
            );
        }
        assert_eq!(h.allocated_seqs(), 0);
        assert!(h.published().is_empty());

        h.handler
            .handle_event(&ctx_as("own-op", "reader"), recall())
            .await
            .expect("the sender recalls");
        assert_eq!(h.allocated_seqs(), 1);
        let published = h.published();
        let Some(Payload::Recall(sent)) = &published[0].0.payload else {
            panic!("recall published");
        };
        assert_eq!(sent.time_limit_seconds, None, "client grants are cleared");
    }

    fn plugin_state_event(conversation_id: &str, version: u64) -> Event {
        Event {
            conversation_id: conversation_id.to_string(),
            r#type: EventType::EventMessagePluginState as i32,
            created_at: 1,
            event_id: uuid::Uuid::new_v4().to_string(),
            payload: Some(Payload::PluginState(
                flare_proto::common::PluginStateEvent {
                    server_msg_id: "m-1".into(),
                    plugin_id: "example.poll".into(),
                    entity: "poll-1".into(),
                    version,
                    values_json: r#"{"counts":[1,1]}"#.into(),
                },
            )),
            ..Default::default()
        }
    }

    /// Plugin shared state rides the generic event path: a seq per event, a single chat goes to
    /// every member (both sides see the card change), a group is a conversation broadcast.
    #[tokio::test]
    async fn plugin_state_events_get_a_seq_and_reach_every_member() {
        let h = Harness::new();
        for version in [1, 2] {
            h.handler
                .handle_event(
                    &system_ctx("state"),
                    plugin_state_event(SINGLE_CHAT, version),
                )
                .await
                .expect("plugin state event");
        }
        h.handler
            .handle_event(
                &system_ctx("state"),
                plugin_state_event("2AW1QQ2SKVWFEPJRXN", 3),
            )
            .await
            .expect("group plugin state event");
        assert_eq!(h.allocated_seqs(), 3);
        let published = h.published();
        assert_eq!(
            published
                .iter()
                .map(|(event, recipients)| (event.conversation_seq, recipients.clone()))
                .collect::<Vec<_>>(),
            vec![
                (1, vec!["reader".to_string(), "peer".to_string()]),
                (2, vec!["reader".to_string(), "peer".to_string()]),
                (3, Vec::new()),
            ]
        );

        // A malformed state never costs a seq.
        let mut broken = plugin_state_event(SINGLE_CHAT, 0);
        h.handler
            .handle_event(&system_ctx("broken"), broken.clone())
            .await
            .expect_err("version 0");
        broken.payload = None;
        h.handler
            .handle_event(&system_ctx("broken"), broken)
            .await
            .expect_err("no payload");
        assert_eq!(h.allocated_seqs(), 3);

        // A member's uplink cannot forge or freeze a card's state.
        h.handler
            .handle_event(
                &ctx("member"),
                plugin_state_event(SINGLE_CHAT, i64::MAX as u64),
            )
            .await
            .expect_err("plugin state from a member");
        assert_eq!(h.allocated_seqs(), 3);
    }
}
