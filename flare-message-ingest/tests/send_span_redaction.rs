//! The send path spans must carry identifiers only: a message body or a
//! caller context recorded as a span field reaches every log line below it.
//!
//! This test is its own test binary and must stay the only test in it. It
//! asserts on log output, and tracing's callsite interest cache is
//! process-wide: with a single thread-local subscriber (`set_default`),
//! tracing-core treats the process as having just one dispatcher, so the first
//! thread to hit a callsite computes its interest from *its own* default
//! dispatcher. A parallel test in the same binary that reaches
//! `handle_send_message` (or any span below it) first, without a subscriber,
//! caches `never` for that callsite, and this test's subscriber stops seeing
//! it. Inside the lib unit-test binary that turns into an intermittent failure
//! as soon as another test exercises the send path.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_contracts::Ctx;
use flare_im_hooks::hooks::{HookDispatcher, HookRegistry};
use flare_im_message_pipeline::MqPushRepository;
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_message_ingest::application::MessageIngestHandler;
use flare_message_ingest::application::commands::{SendMessageCommand, SendSystemMessageCommand};
use flare_message_ingest::application::extension::{
    ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting,
};
use flare_message_ingest::config::SessionCreationMode;
use flare_message_ingest::domain::model::{ConversationType, MessageDefaults};
use flare_message_ingest::domain::repository::{RecipientRepository, WalRepositoryItem};
use flare_message_ingest::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use flare_message_ingest::infrastructure::persistence::wal_repository_impl::NoopWalRepository;
use flare_proto::common::message_content::Content;
use flare_proto::common::{MessageContent, TextContent};
use flare_server_core::Context;
use flare_server_core::error::Result;
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::fmt::format::FmtSpan;

const SECRET_BODY: &str = "BODY-THAT-MUST-NOT-BE-LOGGED";

struct AcceptingProducer;

#[async_trait::async_trait]
impl Producer for AcceptingProducer {
    async fn send(
        &self,
        _ctx: &Ctx,
        _topic: &str,
        _key: Option<&str>,
        _payload: Vec<u8>,
        _headers: Option<HashMap<String, String>>,
    ) -> std::result::Result<(), ProducerError> {
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
        "accepting-producer"
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

/// Length of the first complete RESP array command in `buf`, if any.
fn complete_resp_command(buf: &[u8]) -> Option<usize> {
    fn line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
        let end = buf.get(from..)?.windows(2).position(|w| w == b"\r\n")? + from;
        Some((&buf[from..end], end + 2))
    }
    let (header, mut pos) = line(buf, 0)?;
    let count: usize = std::str::from_utf8(header.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    for _ in 0..count {
        let (len_line, next) = line(buf, pos)?;
        let len: usize = std::str::from_utf8(len_line.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        pos = next + len + 2;
        if buf.len() < pos {
            return None;
        }
    }
    Some(pos)
}

/// Minimal RESP endpoint: accepts the connection handshake and answers
/// every command with `+OK`, so seq allocation fails fast after the
/// earlier send stages (and their spans) have run.
async fn spawn_fake_redis() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut pending = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => pending.extend_from_slice(&chunk[..n]),
                    }
                    while let Some(consumed) = complete_resp_command(&pending) {
                        pending.drain(..consumed);
                        if socket.write_all(b"+OK\r\n").await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    format!("redis://{addr}")
}

async fn handler() -> MessageIngestHandler {
    let redis = Arc::new(redis::Client::open(spawn_fake_redis().await).unwrap());
    let sequence_allocator = SequenceAllocator::new(redis, 100)
        .await
        .expect("fake redis handshake");
    let message_ingest_service = Arc::new(MessageIngestService::new(
        MqPushRepository::new(Arc::new(AcceptingProducer)),
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
    MessageIngestHandler::new(
        message_ingest_service,
        extension_orchestrator,
        Arc::new(ConversationEnsureService::new(
            None,
            SessionCreationMode::Async,
            None,
        )),
        Arc::new(MessageOrchestratorMetrics::new()),
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
                text: format!("hello {SECRET_BODY}"),
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

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn send_path_spans_never_log_the_message_body_or_context() {
    let handler = handler().await;
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    // FmtSpan::NEW renders every span's recorded fields even without events.
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::NEW)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let ctx: Ctx = Arc::new(
        Context::with_request_id("req-1")
            .with_trace_id("trace-1")
            .with_tenant_id("tenant-a")
            .with_user_id("alice")
            .with_device_id("device-a"),
    );
    let bounded = Duration::from_secs(10);
    let _ = tokio::time::timeout(bounded, handler.handle_send_message(&ctx, command()))
        .await
        .expect("single send finishes");
    let _ = tokio::time::timeout(bounded, handler.batch_send_message(&ctx, vec![command()]))
        .await
        .expect("batch send finishes");
    let system = command();
    let _ = tokio::time::timeout(
        bounded,
        handler.send_system_message(
            &ctx,
            SendSystemMessageCommand {
                conversation_id: system.conversation_id,
                message: system.message,
                system_message_type: "notice".to_string(),
            },
        ),
    )
    .await
    .expect("system send finishes");
    let _ = handler
        .send_custom_data(&ctx, SECRET_BODY.as_bytes().to_vec())
        .await;

    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    for span in [
        "handle_send_message{",
        "batch_send_message{",
        "send_system_message{",
        "send_custom_data{",
        "validate_message{",
        "execute_pre_send{",
        "prepare_submission{",
        "decorate_message{",
        "allocate_seq_for_submission{",
    ] {
        assert!(output.contains(span), "missing {span}: {output}");
    }
    assert!(output.contains("client_msg_id=client-1"), "{output}");
    assert!(output.contains("conversation_id=group-1"), "{output}");
    assert!(!output.contains(SECRET_BODY), "{output}");
    assert!(!output.contains("TextContent"), "{output}");
    assert!(!output.contains("ContextInner"), "{output}");
}
