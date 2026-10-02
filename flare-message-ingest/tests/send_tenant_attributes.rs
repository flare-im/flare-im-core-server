//! 发送路径上的租户来源（W11-T）。
//!
//! Storage Writer 曾按消息 attributes 里的 `tenant_id` 选择落库租户；PreSend Hook 又能整体改写
//! attributes（`apply_draft_to_request`）。这里经真实的 `MessageIngestHandler` 发送：Hook 改出
//! 别的租户声明时在 PreSend 之后即被拒绝；上下文没有租户时直接拒绝，不按租户 "0" 处理。
//! Redis 是只回 `+OK` 的假端点，正常消息会在分配 seq 时失败，所以断言只看错误码与原因。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use flare_im_contracts::Ctx;
use flare_im_hooks::hooks::{
    HookDispatcher, HookMetadata, HookRegistry, HookSelector, MessageDraft, PreSendDecision,
    PreSendHook,
};
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
use flare_message_ingest::domain::repository::{RecipientRepository, WalRepositoryItem};
use flare_message_ingest::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use flare_message_ingest::infrastructure::persistence::wal_repository_impl::NoopWalRepository;
use flare_proto::common::message_content::Content;
use flare_proto::common::{MessageContent, TextContent};
use flare_server_core::Context;
use flare_server_core::error::{ErrorCode, Result};
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct AcceptingProducer;

#[async_trait]
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

/// PreSend Hook：把草稿的 attributes 里写上别的租户。
struct TenantRewritingHook {
    key: &'static str,
}

#[async_trait]
impl PreSendHook for TenantRewritingHook {
    async fn handle(&self, _ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        draft
            .metadata
            .insert(self.key.to_string(), "tenant-b".to_string());
        PreSendDecision::Continue
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

/// Minimal RESP endpoint on 127.0.0.1: answers every command with `+OK`.
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

async fn handler(hook: Option<TenantRewritingHook>) -> MessageIngestHandler {
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
    let registry = HookRegistry::new();
    if let Some(hook) = hook {
        registry
            .register_pre_send(
                HookMetadata::default().with_name("tenant-rewrite"),
                HookSelector::default(),
                Arc::new(hook),
            )
            .await;
    }
    let extension_orchestrator = Arc::new(ExtensionOrchestrator::new(
        Arc::new(HookExecutionService::new(
            Arc::new(HookDispatcher::new(registry)),
            None,
        )),
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

fn ctx(tenant: Option<&str>) -> Ctx {
    let mut ctx = Context::with_request_id("req-1").with_user_id("alice");
    if let Some(tenant) = tenant {
        ctx = ctx.with_tenant_id(tenant);
    }
    Arc::new(ctx)
}

const BOUND: Duration = Duration::from_secs(10);

#[tokio::test]
async fn pre_send_hook_cannot_move_the_message_to_another_tenant() {
    for key in ["tenant_id", "x-tenant-id"] {
        let handler = handler(Some(TenantRewritingHook { key })).await;
        let err = tokio::time::timeout(
            BOUND,
            handler.handle_send_message(&ctx(Some("tenant-a")), command()),
        )
        .await
        .expect("send finishes")
        .expect_err("a hook-declared foreign tenant must not be sent");
        assert_eq!(
            err.code(),
            Some(ErrorCode::PermissionDenied),
            "{key}: {err}"
        );
        assert!(
            err.reason().contains("another tenant"),
            "{key}: {}",
            err.reason()
        );
    }
}

#[tokio::test]
async fn send_without_a_context_tenant_is_rejected_not_sent_as_tenant_zero() {
    let handler = handler(None).await;
    for tenant in [None, Some(""), Some("  ")] {
        let err = tokio::time::timeout(BOUND, handler.handle_send_message(&ctx(tenant), command()))
            .await
            .expect("send finishes")
            .expect_err("no tenant, no send");
        assert_eq!(
            err.code(),
            Some(ErrorCode::InvalidParameter),
            "{tenant:?}: {err}"
        );
        // 在入口的租户闸门处拒绝，而不是更晚的 Hook 或发布阶段（那两处另有自己的兜底拒绝）。
        assert!(
            err.reason().contains("tenant required to send a message"),
            "{}",
            err.reason()
        );
    }

    // 显式的租户 "0" 不受影响：它走完前面的阶段，停在假 Redis 的 seq 分配上。
    let err = tokio::time::timeout(
        BOUND,
        handler.handle_send_message(&ctx(Some("0")), command()),
    )
    .await
    .expect("send finishes")
    .expect_err("fake redis cannot allocate seq");
    assert!(
        !err.reason().contains("tenant required"),
        "{}",
        err.reason()
    );
    assert_ne!(err.code(), Some(ErrorCode::PermissionDenied), "{err}");
}
