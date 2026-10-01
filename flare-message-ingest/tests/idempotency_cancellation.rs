//! 发送处理被取消时，摄入幂等占位不能一直挂到 TTL（W6-S2 F3）。
//!
//! 停机时入口与 message-ingest 同时停止，进行中的发送在 `begin` 占位之后、提交或回滚之前被
//! 取消；此前占位要等 180 s 才过期，期间同一 `client_msg_id` 的重发都得到
//! `duplicate send in progress`。这里用 tokio 超时取消 `handle_send_message`：
//! - 取消发生在 WAL 之前（序列号分配挂起）：占位回滚，重发正常处理；
//! - 取消发生在 WAL 之后（发布挂起）：占位按已确定的结果提交，重发拿回同一个 server_id 与 seq，
//!   不再发布第二次。

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
use flare_message_ingest::domain::model::{ConversationType, MessageDefaults};
use flare_message_ingest::domain::repository::{
    IdempotencyBegin, IdempotentRecord, IngestIdempotencyStore, RecipientRepository,
    WalRepositoryItem,
};
use flare_message_ingest::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use flare_message_ingest::infrastructure::persistence::wal_repository_impl::NoopWalRepository;
use flare_proto::common::message_content::Content;
use flare_proto::common::{MessageContent, SendAckDurability, TextContent};
use flare_server_core::Context;
use flare_server_core::error::Result;
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 调用方等待一次发送的上限；到点即取消 `handle_send_message`。
const CALLER_TIMEOUT: Duration = Duration::from_millis(300);
/// 取消后等待占位被收尾的上限（远小于占位 TTL 180 s）。
const SETTLE_LIMIT: Duration = Duration::from_secs(2);

/// 内存版幂等存储，语义同 Redis 实现：占位中 → InFlight，已提交 → Replay，回滚即删除。
#[derive(Default)]
struct MemoryIdempotency {
    slots: Mutex<HashMap<String, Option<IdempotentRecord>>>,
    calls: Mutex<Vec<String>>,
}

impl MemoryIdempotency {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    async fn wait_for_call(&self, prefix: &str) -> Option<String> {
        let started = tokio::time::Instant::now();
        while started.elapsed() < SETTLE_LIMIT {
            if let Some(call) = self.calls().into_iter().find(|c| c.starts_with(prefix)) {
                return Some(call);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }
}

#[async_trait::async_trait]
impl IngestIdempotencyStore for MemoryIdempotency {
    async fn begin(&self, key: &str) -> Result<IdempotencyBegin> {
        self.calls.lock().unwrap().push("begin".into());
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
        self.calls.lock().unwrap().push(format!(
            "commit {} {}",
            record.server_id, record.conversation_seq
        ));
        self.slots
            .lock()
            .unwrap()
            .insert(key.to_string(), Some(record.clone()));
        Ok(())
    }

    async fn rollback(&self, key: &str) -> Result<()> {
        self.calls.lock().unwrap().push("rollback".into());
        self.slots.lock().unwrap().remove(key);
        Ok(())
    }
}

/// 发布到 broker：`hang` 为真时永不返回（模拟停机时发布被打断）。
#[derive(Default)]
struct GatedProducer {
    hang: AtomicBool,
    sends: AtomicUsize,
}

#[async_trait::async_trait]
impl Producer for GatedProducer {
    async fn send(
        &self,
        _ctx: &Ctx,
        _topic: &str,
        _key: Option<&str>,
        _payload: Vec<u8>,
        _headers: Option<HashMap<String, String>>,
    ) -> std::result::Result<(), ProducerError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        if self.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
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
        "gated-producer"
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

/// 第一条完整 RESP 数组命令的（长度，命令名）。
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

/// 只够序列号分配用的 RESP 端点：`MULTI`/`INCR`/`EXEC` 按事务应答，`EXEC` 返回递增的 seq；
/// `exec_hangs` 为真时 `EXEC` 暂不应答（模拟停机时序列号分配被打断）。其余命令答 `+OK`。
async fn spawn_fake_redis(exec_hangs: Arc<AtomicBool>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seq = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let exec_hangs = Arc::clone(&exec_hangs);
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
                                // 挂起期间不应答；同一连接上的应答有序，放开后再补上。
                                while exec_hangs.load(Ordering::SeqCst) {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                                let next = seq.fetch_add(1, Ordering::SeqCst) + 1;
                                format!("*1\r\n:{next}\r\n")
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
    producer: Arc<GatedProducer>,
    exec_hangs: Arc<AtomicBool>,
}

async fn harness() -> Harness {
    let exec_hangs = Arc::new(AtomicBool::new(false));
    let redis =
        Arc::new(redis::Client::open(spawn_fake_redis(Arc::clone(&exec_hangs)).await).unwrap());
    let sequence_allocator = SequenceAllocator::new(redis, 100)
        .await
        .expect("fake redis handshake");
    let producer = Arc::new(GatedProducer::default());
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
    let hooks = Arc::new(HookDispatcher::new(HookRegistry::new()));
    let extension_orchestrator = Arc::new(ExtensionOrchestrator::new(
        Arc::new(HookExecutionService::new(hooks, None)),
        ExtensionPolicy::new(true),
        ExtensionRouting::new(Vec::new(), Vec::new()),
    ));
    let store = Arc::new(MemoryIdempotency::default());
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
    .with_idempotency(store.clone());
    Harness {
        handler,
        store,
        producer,
        exec_hangs,
    }
}

fn ctx() -> Ctx {
    Arc::new(
        Context::with_request_id("req-1")
            .with_tenant_id("tenant-a")
            .with_user_id("alice")
            .with_device_id("device-a"),
    )
}

fn command() -> SendMessageCommand {
    let message = flare_proto::common::Message {
        conversation_id: "group-1".to_string(),
        conversation_type: flare_proto::common::ConversationType::Group as i32,
        sender_id: "alice".to_string(),
        client_msg_id: "client-1".to_string(),
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
        sync: false,
        burn_enabled: false,
        burn_after_read_seconds: None,
    }
}

#[tokio::test]
async fn a_send_cancelled_before_the_wal_releases_its_reservation() {
    let harness = harness().await;
    let ctx = ctx();
    harness.exec_hangs.store(true, Ordering::SeqCst);
    let cancelled = tokio::time::timeout(
        CALLER_TIMEOUT,
        harness.handler.handle_send_message(&ctx, command()),
    )
    .await;
    assert!(cancelled.is_err(), "send should hang in seq allocation");

    let released = harness.store.wait_for_call("rollback").await;
    println!("calls after cancel: {:?}", harness.store.calls());
    assert!(released.is_some(), "reservation kept after cancellation");

    // 重发：正常处理，不再被占位挡住。
    harness.exec_hangs.store(false, Ordering::SeqCst);
    let retry = tokio::time::timeout(
        Duration::from_secs(5),
        harness.handler.handle_send_message(&ctx, command()),
    )
    .await
    .expect("retry finishes");
    let outcome = retry.expect("retry is processed, not rejected as in progress");
    assert!(!outcome.message_id.is_empty());
    assert_eq!(harness.producer.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_send_cancelled_after_the_wal_commits_its_outcome() {
    let harness = harness().await;
    let ctx = ctx();
    harness.producer.hang.store(true, Ordering::SeqCst);
    let cancelled = tokio::time::timeout(
        CALLER_TIMEOUT,
        harness.handler.handle_send_message(&ctx, command()),
    )
    .await;
    assert!(cancelled.is_err(), "send should hang in publish");
    assert_eq!(harness.producer.sends.load(Ordering::SeqCst), 1);

    let committed = harness
        .store
        .wait_for_call("commit")
        .await
        .expect("accepted send cancelled without committing its outcome");
    println!("calls after cancel: {:?}", harness.store.calls());
    assert!(
        !harness.store.calls().iter().any(|c| c == "rollback"),
        "an accepted send must not release its reservation"
    );

    // 重发：拿回首发的 server_id 与 seq，不再发布。
    harness.producer.hang.store(false, Ordering::SeqCst);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        harness.handler.handle_send_message(&ctx, command()),
    )
    .await
    .expect("retry finishes")
    .expect("retry replays the first outcome");
    assert_eq!(
        committed,
        format!("commit {} {}", outcome.message_id, outcome.conversation_seq)
    );
    assert_eq!(outcome.conversation_seq, 1);
    assert_eq!(outcome.durability, SendAckDurability::WalAccepted);
    assert_eq!(harness.producer.sends.load(Ordering::SeqCst), 1);
}
