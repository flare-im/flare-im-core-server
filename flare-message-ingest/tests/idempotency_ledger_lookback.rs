//! 发送幂等账本回查，经 `MessageIngestHandler::handle_send_message`（真实的 seq 分配、发布与幂等占位守卫；
//! Redis 去重换成内存实现，账本换成内存实现并记下每次读取）：
//!
//! - 离线 2 小时后用同一 `client_msg_id` 重发：Redis 里首发结果早已过期，客户端创建时间早于窗口，回查账本
//!   命中，回首发的 server_id / seq / 持久性，不生成新消息，结果回填进 Redis；
//! - 新消息（创建时间就在眼前）、没带创建时间的消息不读账本；
//! - 账本读不了时回可重试错误、释放占位；账本里是另一会话的消息时按新消息发出。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_contracts::Ctx;
use flare_im_hooks::hooks::{HookDispatcher, HookRegistry};
use flare_im_message_pipeline::MqPushRepository;
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_message_ingest::application::MessageIngestHandler;
use flare_message_ingest::application::commands::SendMessageCommand;
use flare_message_ingest::application::extension::{
    ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting,
};
use flare_message_ingest::config::SessionCreationMode;
use flare_message_ingest::domain::model::{
    ConversationType, LedgerLookbackPolicy, MessageDefaults,
};
use flare_message_ingest::domain::repository::{
    IdempotencyBegin, IdempotencyLedger, IdempotentRecord, IngestIdempotencyStore, LedgerRecord,
    RecipientRepository, WalRepositoryItem,
};
use flare_message_ingest::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use flare_message_ingest::infrastructure::persistence::wal_repository_impl::NoopWalRepository;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    Message, MessageContent, MessageSource, MessageType, MqEnvelope, SendAckDurability,
    TextContent, mq_envelope,
};
use flare_server_core::Context;
use flare_server_core::error::{ErrorCode, FlareError, Result};
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TENANT: &str = "tenant-a";
const GROUP: &str = "2-group-a";
const SENDER: &str = "alice";
/// 与生产默认一致的 Redis 结果 TTL。
const REDIS_RESULT_TTL: Duration = Duration::from_secs(3_600);
const HOUR_MS: i64 = 3_600_000;

/// Redis 去重的内存替身：首发结果过期，等于这里没有这个键。
#[derive(Default)]
struct MemoryIdempotency {
    slots: Mutex<HashMap<String, Option<IdempotentRecord>>>,
}

impl MemoryIdempotency {
    fn committed(&self, key: &str) -> Option<IdempotentRecord> {
        self.slots.lock().unwrap().get(key).cloned().flatten()
    }

    fn holds(&self, key: &str) -> bool {
        self.slots.lock().unwrap().contains_key(key)
    }
}

#[async_trait::async_trait]
impl IngestIdempotencyStore for MemoryIdempotency {
    async fn begin(&self, key: &str) -> Result<IdempotencyBegin> {
        let mut slots = self.slots.lock().unwrap();
        Ok(match slots.get(key) {
            None => {
                slots.insert(key.to_string(), None);
                IdempotencyBegin::Fresh
            }
            Some(None) => IdempotencyBegin::InFlight,
            Some(Some(record)) => IdempotencyBegin::Replay(record.clone()),
        })
    }

    async fn commit(&self, key: &str, record: &IdempotentRecord) -> Result<()> {
        self.slots
            .lock()
            .unwrap()
            .insert(key.to_string(), Some(record.clone()));
        Ok(())
    }

    async fn rollback(&self, key: &str) -> Result<()> {
        self.slots.lock().unwrap().remove(key);
        Ok(())
    }
}

/// 账本的内存替身：记下每次读取，`fail` 打开时读取报错。
#[derive(Default)]
struct MemoryLedger {
    rows: Mutex<HashMap<(String, String, String), LedgerRecord>>,
    lookups: AtomicUsize,
    fail: AtomicBool,
}

impl MemoryLedger {
    fn insert(&self, client_msg_id: &str, record: LedgerRecord) {
        self.rows
            .lock()
            .unwrap()
            .insert((TENANT.into(), SENDER.into(), client_msg_id.into()), record);
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl IdempotencyLedger for MemoryLedger {
    async fn find(
        &self,
        tenant_id: &str,
        sender_id: &str,
        client_msg_id: &str,
    ) -> Result<Option<LedgerRecord>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(FlareError::system("ledger database unavailable"));
        }
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(tenant_id.into(), sender_id.into(), client_msg_id.into()))
            .cloned())
    }
}

/// 每条发布到主队列的消息，从信封里解出来。
#[derive(Default)]
struct CapturingProducer {
    published: Mutex<Vec<Message>>,
}

impl CapturingProducer {
    fn published(&self) -> Vec<Message> {
        self.published.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Producer for CapturingProducer {
    async fn send(
        &self,
        _ctx: &Ctx,
        _topic: &str,
        _key: Option<&str>,
        payload: Vec<u8>,
        _headers: Option<HashMap<String, String>>,
    ) -> std::result::Result<(), ProducerError> {
        if let Ok(MqEnvelope {
            payload: Some(mq_envelope::Payload::Message(message)),
            ..
        }) = MqEnvelope::decode(payload.as_slice())
        {
            self.published.lock().unwrap().push(message);
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

struct NoRecipients;

type Recipients<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

impl RecipientRepository for NoRecipients {
    fn get_message_recipients<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
        _conversation_type: ConversationType,
        _channel_id: Option<&'a str>,
        _sender_id: &'a str,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_event_recipients<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _message_id: &'a str,
        _conversation_id: &'a str,
        _event_type: flare_proto::common::EventType,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_conversation_members<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_conversation_member_count<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
    ) -> Recipients<'a, usize> {
        Box::pin(async { Ok(0) })
    }
}

/// 序列分配器够用的 RESP：`EXEC` 回递增的 seq，其余回 `+OK`。
async fn spawn_fake_redis() -> String {
    fn next_command(buf: &[u8]) -> Option<(usize, String)> {
        fn line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
            let end = buf.get(from..)?.windows(2).position(|w| w == b"\r\n")? + from;
            Some((&buf[from..end], end + 2))
        }
        let (header, mut pos) = line(buf, 0)?;
        let count: usize = std::str::from_utf8(header.strip_prefix(b"*")?)
            .ok()?
            .parse()
            .ok()?;
        let mut name = String::new();
        for index in 0..count {
            let (len_line, next) = line(buf, pos)?;
            let len: usize = std::str::from_utf8(len_line.strip_prefix(b"$")?)
                .ok()?
                .parse()
                .ok()?;
            if buf.len() < next + len + 2 {
                return None;
            }
            if index == 0 {
                name = String::from_utf8_lossy(&buf[next..next + len]).to_ascii_uppercase();
            }
            pos = next + len + 2;
        }
        Some((pos, name))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seq = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seq = Arc::clone(&seq);
            tokio::spawn(async move {
                let mut pending = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => pending.extend_from_slice(&chunk[..n]),
                    }
                    while let Some((consumed, name)) = next_command(&pending) {
                        pending.drain(..consumed);
                        let reply = match name.as_str() {
                            "INCR" => "+QUEUED\r\n".to_string(),
                            "EXEC" => {
                                format!("*1\r\n:{}\r\n", seq.fetch_add(1, Ordering::SeqCst) + 1)
                            }
                            "PING" => "+PONG\r\n".to_string(),
                            _ => "+OK\r\n".to_string(),
                        };
                        if socket.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    format!("redis://{addr}")
}

struct Harness {
    handler: MessageIngestHandler,
    store: Arc<MemoryIdempotency>,
    ledger: Arc<MemoryLedger>,
    producer: Arc<CapturingProducer>,
}

async fn harness() -> Harness {
    let redis = Arc::new(redis::Client::open(spawn_fake_redis().await).unwrap());
    let sequence_allocator = SequenceAllocator::new(redis, 100)
        .await
        .expect("fake redis handshake");
    let producer = Arc::new(CapturingProducer::default());
    let message_ingest_service = Arc::new(MessageIngestService::new(
        MqPushRepository::new(producer.clone()),
        Arc::new(NoRecipients),
        Arc::new(WalRepositoryItem::Noop(Arc::new(NoopWalRepository))),
        Arc::new(sequence_allocator),
        MessageDefaults {
            default_business_type: "im".to_string(),
            default_conversation_type: ConversationType::Group,
            default_sender_type: "user".to_string(),
            default_tenant_id: None,
        },
        MessageIngestServiceOptions::new(),
    ));
    let extension_orchestrator = Arc::new(ExtensionOrchestrator::new(
        Arc::new(HookExecutionService::new(
            Arc::new(HookDispatcher::new(HookRegistry::new())),
            None,
        )),
        ExtensionPolicy::new(true),
        ExtensionRouting::new(Vec::new(), Vec::new()),
    ));
    let store = Arc::new(MemoryIdempotency::default());
    let ledger = Arc::new(MemoryLedger::default());
    let handler = MessageIngestHandler::new(
        message_ingest_service,
        extension_orchestrator,
        Arc::new(ConversationEnsureService::new(
            None,
            SessionCreationMode::Async,
            None,
        )),
        Arc::new(MessageOrchestratorMetrics::new()),
    )
    .with_idempotency(store.clone())
    .with_idempotency_ledger(ledger.clone(), LedgerLookbackPolicy::new(REDIS_RESULT_TTL));
    Harness {
        handler,
        store,
        ledger,
        producer,
    }
}

fn user_ctx() -> Ctx {
    Arc::new(
        Context::with_request_id("req-user")
            .with_tenant_id(TENANT)
            .with_user_id(SENDER),
    )
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 客户端组装的文本消息，`created_at` 是客户端创建时间（本地发件箱重发时原样带回）。
fn text(client_msg_id: &str, created_at: i64) -> SendMessageCommand {
    let message = Message {
        conversation_id: GROUP.to_string(),
        conversation_type: flare_proto::common::ConversationType::Group as i32,
        sender_id: SENDER.to_string(),
        client_msg_id: client_msg_id.to_string(),
        source: MessageSource::User as i32,
        message_type: MessageType::Text as i32,
        created_at,
        content: Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "hello".to_string(),
                mentions: vec![],
            })),
        }),
        ..Default::default()
    };
    SendMessageCommand {
        conversation_id: message.conversation_id.clone(),
        message,
        burn_enabled: false,
        burn_after_read_seconds: None,
    }
}

fn redis_key(client_msg_id: &str) -> String {
    format!("idem:{TENANT}:{SENDER}:{GROUP}:{client_msg_id}")
}

fn original(conversation_id: &str) -> LedgerRecord {
    LedgerRecord {
        conversation_id: conversation_id.to_string(),
        server_id: "srv-original".to_string(),
        conversation_seq: 7,
        durability: SendAckDurability::Persisted,
    }
}

/// 离线 2 小时后用同一 client_msg_id 重发：Redis 没有首发结果（过期），账本里有。回首发结果，不分配 seq、
/// 不发布新消息；结果回填进 Redis，下一次重发在 Redis 就命中。
#[tokio::test]
async fn a_resend_two_hours_after_going_offline_replays_the_ledger_result_without_a_new_message() {
    let h = harness().await;
    h.ledger.insert("cli-offline", original(GROUP));
    let created_two_hours_ago = now_ms() - 2 * HOUR_MS;

    let outcome = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-offline", created_two_hours_ago))
        .await
        .expect("an old resend is answered");

    assert_eq!(outcome.message_id, "srv-original");
    assert_eq!(outcome.conversation_seq, 7);
    assert_eq!(outcome.durability, SendAckDurability::Persisted);
    assert!(
        h.producer.published().is_empty(),
        "no second message may be published for the same client message"
    );
    assert_eq!(h.ledger.lookups(), 1);
    let backfilled = h
        .store
        .committed(&redis_key("cli-offline"))
        .expect("the ledger result is written back to Redis");
    assert_eq!(backfilled.server_id, "srv-original");
    assert_eq!(backfilled.conversation_seq, 7);

    // 再重发一次：Redis 命中，不再读账本。
    let again = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-offline", created_two_hours_ago))
        .await
        .expect("replayed from Redis");
    assert_eq!(again.message_id, "srv-original");
    assert_eq!(h.ledger.lookups(), 1);
    assert!(h.producer.published().is_empty());
}

/// 新消息的创建时间就在眼前（或客户端没给）：不读账本，照常分配 seq 并发布。
#[tokio::test]
async fn a_new_message_never_reads_the_ledger() {
    let h = harness().await;
    // 即使账本里碰巧有同一键，新消息也不去读它。
    h.ledger.insert("cli-new", original(GROUP));
    let fresh = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-new", now_ms()))
        .await
        .expect("a new message is sent");
    let unset = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-unset", 0))
        .await
        .expect("a message without a client time is sent");
    // 50 分钟前创建：仍在 Redis 窗口（1 小时减 5 分钟余量）之内。
    let recent = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-recent", now_ms() - 50 * 60_000))
        .await
        .expect("a recent message is sent");

    assert_eq!(
        h.ledger.lookups(),
        0,
        "the hot path must not touch PostgreSQL"
    );
    let published = h.producer.published();
    assert_eq!(published.len(), 3);
    for (outcome, published) in [fresh, unset, recent].iter().zip(&published) {
        assert_ne!(outcome.message_id, "srv-original");
        assert_eq!(outcome.message_id, published.server_id);
    }
}

/// 旧发送但账本里没有它（首发从没落库）：读一次账本，按新消息发出。
#[tokio::test]
async fn an_old_send_missing_from_the_ledger_is_sent_as_new() {
    let h = harness().await;
    let outcome = h
        .handler
        .handle_send_message(
            &user_ctx(),
            text("cli-never-stored", now_ms() - 3 * HOUR_MS),
        )
        .await
        .expect("sent as a new message");
    assert_eq!(h.ledger.lookups(), 1);
    let published = h.producer.published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].server_id, outcome.message_id);
}

/// 账本里同一 client_msg_id 属于另一会话：重发不会换会话，这是复用，按新消息发出。
#[tokio::test]
async fn a_ledger_entry_of_another_conversation_is_not_replayed() {
    let h = harness().await;
    h.ledger.insert("cli-reused", original("2-group-b"));
    let outcome = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-reused", now_ms() - 2 * HOUR_MS))
        .await
        .expect("sent as a new message");
    assert_ne!(outcome.message_id, "srv-original");
    assert_eq!(h.producer.published().len(), 1);
}

/// 账本读不了：回可重试错误，不生成消息，占位释放；账本恢复后同一重发回首发结果。
#[tokio::test]
async fn an_unreadable_ledger_asks_the_client_to_retry_and_releases_the_reservation() {
    let h = harness().await;
    h.ledger.insert("cli-retry", original(GROUP));
    h.ledger.fail.store(true, Ordering::SeqCst);
    let created = now_ms() - 2 * HOUR_MS;

    let error = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-retry", created))
        .await
        .expect_err("an unreadable ledger must not let a possible duplicate through");
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    assert!(h.producer.published().is_empty());
    assert!(
        !h.store.holds(&redis_key("cli-retry")),
        "the reservation is released so the retry is processed"
    );

    h.ledger.fail.store(false, Ordering::SeqCst);
    let outcome = h
        .handler
        .handle_send_message(&user_ctx(), text("cli-retry", created))
        .await
        .expect("the retry replays the first result");
    assert_eq!(outcome.message_id, "srv-original");
    assert!(h.producer.published().is_empty());
}
