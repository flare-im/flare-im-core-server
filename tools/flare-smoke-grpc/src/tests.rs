//! 离线流程测试：本地 127.0.0.1 上起一个与 Message Ingest 同样验签的假服务，
//! 数据库回读用脚本化的读者；不连任何真实服务。

use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use flare_grpc_proto::conversation::conversation_read_service_server::{
    ConversationReadService, ConversationReadServiceServer,
};
use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, GetConversationDetailRequest,
    GetConversationDetailResponse, ListConversationParticipantsRequest,
    ListConversationParticipantsResponse, ListConversationsRequest, ListConversationsResponse,
};
use flare_grpc_proto::message::message_send_service_server::{
    MessageSendService, MessageSendServiceServer,
};
use flare_grpc_proto::message::{
    BatchSendMessageRequest, BatchSendMessageResponse, SendAckRequest, SendAckResponse,
    SendCustomDataRequest, SendCustomDataResponse, SendMessageRequest, SendMessageResponse,
    SendSystemMessageRequest, SendSystemMessageResponse,
};
use flare_grpc_proto::storage::storage_reader_service_server::{
    StorageReaderService, StorageReaderServiceServer,
};
use flare_grpc_proto::storage::*;
use flare_proto::common::{ConversationSummary, MessageSource, SendAckDurability};
use flare_server_core::auth::SignedActorType;
use flare_server_core::grpc::middleware::signed_context::{
    SIGNED_CONTEXT_HEADER, VerifiedContext, verify_signed_context_metadata,
};
use flare_server_core::{SignedContextVerifier, SignedFirstContextLayer};
use tonic::metadata::KeyRef;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use super::*;
use crate::durability::Observation;
use crate::signed::tests::{key_pair, legacy_identity_keys};

const TENANT: &str = "e2e-0123456789abcdef0123";
const SENDER: &str = "7312345678901234567";
const RECIPIENT: &str = "7312345678901234568";
const PASSWORD: &str = "Qx7mV2pL9sKd4RtN8wYb3HjF";

#[derive(Default)]
struct Seen {
    requests: usize,
    accepted: usize,
    metadata_keys: Vec<String>,
    signed_contexts: usize,
    actor: String,
    tenant: String,
}

struct FakeIngest {
    verifier: SignedContextVerifier,
    seen: Arc<Mutex<Seen>>,
}

#[tonic::async_trait]
impl MessageSendService for FakeIngest {
    /// 与 message-ingest 的入口同样把关：只认验过签的 `x-flare-ctx`，user 身份必须是发送者。
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> std::result::Result<Response<SendMessageResponse>, Status> {
        {
            let mut seen = self.seen.lock().unwrap();
            seen.requests += 1;
            seen.metadata_keys = request
                .metadata()
                .keys()
                .map(|key| match key {
                    KeyRef::Ascii(key) => key.as_str().to_owned(),
                    KeyRef::Binary(key) => key.as_str().to_owned(),
                })
                .collect();
            seen.signed_contexts = request
                .metadata()
                .get_all(SIGNED_CONTEXT_HEADER)
                .iter()
                .count();
        }
        let ctx = verify_signed_context_metadata(request.metadata(), &self.verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .cloned()
            .ok_or_else(|| Status::unauthenticated("signed message identity required"))?;
        let message = request
            .into_inner()
            .message
            .ok_or_else(|| Status::invalid_argument("message required"))?;
        if claims.actor_type != SignedActorType::User
            || claims.actor_id != message.sender_id
            || message.source != MessageSource::User as i32
        {
            return Err(Status::permission_denied(
                "message sender does not match signed user",
            ));
        }
        let mut seen = self.seen.lock().unwrap();
        seen.accepted += 1;
        seen.actor = claims.actor_id;
        seen.tenant = claims.tenant_id;
        Ok(Response::new(SendMessageResponse {
            success: true,
            server_msg_id: "srv-smoke-grpc-1".into(),
            conversation_seq: 7,
            sent_at: None,
            timeline: None,
            durability: SendAckDurability::BrokerAccepted as i32,
        }))
    }

    async fn batch_send_message(
        &self,
        _: Request<BatchSendMessageRequest>,
    ) -> std::result::Result<Response<BatchSendMessageResponse>, Status> {
        Err(Status::unimplemented("not used by the smoke"))
    }

    async fn send_system_message(
        &self,
        _: Request<SendSystemMessageRequest>,
    ) -> std::result::Result<Response<SendSystemMessageResponse>, Status> {
        Err(Status::unimplemented("not used by the smoke"))
    }

    async fn send_ack(
        &self,
        _: Request<SendAckRequest>,
    ) -> std::result::Result<Response<SendAckResponse>, Status> {
        Err(Status::unimplemented("not used by the smoke"))
    }

    async fn send_custom_data(
        &self,
        _: Request<SendCustomDataRequest>,
    ) -> std::result::Result<Response<SendCustomDataResponse>, Status> {
        Err(Status::unimplemented("not used by the smoke"))
    }
}

/// 一次签名读：哪个 RPC、签名里的谁（读接口的身份边界验过之后的上下文）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct SignedRead {
    rpc: &'static str,
    actor_type: SignedActorType,
    actor: String,
    tenant: String,
}

#[derive(Default)]
struct ReadsSeen {
    reads: Vec<SignedRead>,
    /// 进入身份边界之前，请求带的全部元数据键。
    metadata_keys: Vec<Vec<String>>,
}

/// 与 Core 的读接口同样把关：外面套 `SignedFirstContextLayer`（要求签名），处理器只看边界放进来
/// 的已验证上下文。回的数据固定：消息 srv-smoke-grpc-1 在 seq 7，会话列表里该会话的 seq 是
/// `conversation_seq`。
#[derive(Clone)]
struct FakeReads {
    seen: Arc<Mutex<ReadsSeen>>,
    conversation_seq: u64,
}

impl FakeReads {
    fn record<T>(
        &self,
        rpc: &'static str,
        request: &Request<T>,
    ) -> std::result::Result<(), Status> {
        let ctx = request
            .extensions()
            .get::<flare_server_core::context::Ctx>()
            .ok_or_else(|| Status::internal("no context"))?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .ok_or_else(|| Status::unauthenticated("signed read identity required"))?;
        self.seen.lock().unwrap().reads.push(SignedRead {
            rpc,
            actor_type: claims.actor_type,
            actor: claims.actor_id.clone(),
            tenant: claims.tenant_id.clone(),
        });
        Ok(())
    }
}

fn unused<T>() -> std::result::Result<Response<T>, Status> {
    Err(Status::unimplemented("not used by the smoke"))
}

#[tonic::async_trait]
impl StorageReaderService for FakeReads {
    async fn query_messages_by_seq(
        &self,
        request: Request<QueryMessagesBySeqRequest>,
    ) -> std::result::Result<Response<QueryMessagesBySeqResponse>, Status> {
        self.record("QueryMessagesBySeq", &request)?;
        let request = request.into_inner();
        assert_eq!(request.after_seq, 6);
        assert_eq!(request.user_id, RECIPIENT);
        Ok(Response::new(QueryMessagesBySeqResponse {
            messages: vec![flare_proto::common::Message {
                server_id: "srv-smoke-grpc-1".into(),
                conversation_id: request.conversation_id,
                conversation_seq: 7,
                ..Default::default()
            }],
            last_seq: 7,
            ..Default::default()
        }))
    }

    async fn query_messages(
        &self,
        _: Request<QueryMessagesRequest>,
    ) -> std::result::Result<Response<QueryMessagesResponse>, Status> {
        unused()
    }

    async fn query_conversations_message_windows(
        &self,
        _: Request<QueryConversationsMessageWindowsRequest>,
    ) -> std::result::Result<Response<QueryConversationsMessageWindowsResponse>, Status> {
        unused()
    }

    async fn search_messages(
        &self,
        _: Request<SearchMessagesRequest>,
    ) -> std::result::Result<Response<SearchMessagesResponse>, Status> {
        unused()
    }

    async fn get_message(
        &self,
        _: Request<GetMessageRequest>,
    ) -> std::result::Result<Response<GetMessageResponse>, Status> {
        unused()
    }

    async fn query_message_events(
        &self,
        _: Request<QueryMessageEventsRequest>,
    ) -> std::result::Result<Response<QueryMessageEventsResponse>, Status> {
        unused()
    }

    async fn query_message_edit_history(
        &self,
        _: Request<QueryMessageEditHistoryRequest>,
    ) -> std::result::Result<Response<QueryMessageEditHistoryResponse>, Status> {
        unused()
    }

    async fn query_message_read_list(
        &self,
        _: Request<QueryMessageReadListRequest>,
    ) -> std::result::Result<Response<QueryMessageReadListResponse>, Status> {
        unused()
    }

    async fn query_message_marks(
        &self,
        _: Request<QueryMessageMarksRequest>,
    ) -> std::result::Result<Response<QueryMessageMarksResponse>, Status> {
        unused()
    }

    async fn query_message_reactions(
        &self,
        _: Request<QueryMessageReactionsRequest>,
    ) -> std::result::Result<Response<QueryMessageReactionsResponse>, Status> {
        unused()
    }

    async fn list_message_tags(
        &self,
        _: Request<ListMessageTagsRequest>,
    ) -> std::result::Result<Response<ListMessageTagsResponse>, Status> {
        unused()
    }

    async fn export_messages(
        &self,
        _: Request<ExportMessagesRequest>,
    ) -> std::result::Result<Response<ExportMessagesResponse>, Status> {
        unused()
    }

    async fn query_message_write_ledger(
        &self,
        _: Request<QueryMessageWriteLedgerRequest>,
    ) -> std::result::Result<Response<QueryMessageWriteLedgerResponse>, Status> {
        unused()
    }

    async fn query_conversation_events(
        &self,
        _: Request<QueryConversationEventsRequest>,
    ) -> std::result::Result<Response<QueryConversationEventsResponse>, Status> {
        unused()
    }

    async fn get_conversation_message_head(
        &self,
        _: Request<GetConversationMessageHeadRequest>,
    ) -> std::result::Result<Response<GetConversationMessageHeadResponse>, Status> {
        unused()
    }
}

#[tonic::async_trait]
impl ConversationReadService for FakeReads {
    async fn list_conversations(
        &self,
        request: Request<ListConversationsRequest>,
    ) -> std::result::Result<Response<ListConversationsResponse>, Status> {
        self.record("ListConversations", &request)?;
        Ok(Response::new(ListConversationsResponse {
            conversations: vec![
                ConversationSummary {
                    conversation_id: "some-other-conversation".into(),
                    max_conversation_seq: 99,
                    ..Default::default()
                },
                ConversationSummary {
                    conversation_id: cid::single_chat_conversation_id(SENDER, RECIPIENT),
                    max_conversation_seq: self.conversation_seq,
                    ..Default::default()
                },
            ],
            next_cursor: String::new(),
            has_more: false,
        }))
    }

    async fn conversation_bootstrap(
        &self,
        _: Request<ConversationBootstrapRequest>,
    ) -> std::result::Result<Response<ConversationBootstrapResponse>, Status> {
        unused()
    }

    async fn get_conversation_detail(
        &self,
        _: Request<GetConversationDetailRequest>,
    ) -> std::result::Result<Response<GetConversationDetailResponse>, Status> {
        unused()
    }

    async fn list_conversation_participants(
        &self,
        _: Request<ListConversationParticipantsRequest>,
    ) -> std::result::Result<Response<ListConversationParticipantsResponse>, Status> {
        unused()
    }
}

/// 假 Core：Ingest 按 `ingest_public` 验签；两个读接口套与 Core 相同的身份边界（要求签名），按
/// `reads_public` 验签。三者同一端口。
async fn start_core(
    ingest_public: &str,
    reads_public: &str,
    conversation_seq: u64,
) -> (String, Arc<Mutex<Seen>>, Arc<Mutex<ReadsSeen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let reads_seen = Arc::new(Mutex::new(ReadsSeen::default()));
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    let ingest = MessageSendServiceServer::new(FakeIngest {
        verifier: SignedContextVerifier::from_public_key(ingest_public).unwrap(),
        seen: Arc::clone(&seen),
    });
    let reads = FakeReads {
        seen: Arc::clone(&reads_seen),
        conversation_seq,
    };
    let boundary = SignedFirstContextLayer::new(Some(
        SignedContextVerifier::from_public_key(reads_public).unwrap(),
    ))
    .require_signed(true);
    let keys_seen = Arc::clone(&reads_seen);
    let record_keys = move |request: Request<()>| {
        let keys = request
            .metadata()
            .keys()
            .map(|key| match key {
                KeyRef::Ascii(key) => key.as_str().to_owned(),
                KeyRef::Binary(key) => key.as_str().to_owned(),
            })
            .collect();
        keys_seen.lock().unwrap().metadata_keys.push(keys);
        Ok(request)
    };
    tokio::spawn(
        tonic::transport::Server::builder()
            .layer(tonic::service::InterceptorLayer::new(record_keys))
            .add_service(ingest)
            .add_service(boundary.layer(StorageReaderServiceServer::new(reads.clone())))
            .add_service(boundary.layer(ConversationReadServiceServer::new(reads)))
            .serve_with_incoming(incoming),
    );
    (format!("http://127.0.0.1:{port}"), seen, reads_seen)
}

async fn start_ingest(public_key: &str) -> (String, Arc<Mutex<Seen>>) {
    let (endpoint, seen, _) = start_core(public_key, public_key, 7).await;
    (endpoint, seen)
}

struct Fixture {
    dir: PathBuf,
    key: PathBuf,
    password: PathBuf,
    private_key: String,
    public_key: String,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("flare-smoke-flow-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let (private_key, public_key) = key_pair(11);
        let key = write_private(&dir, "signing.key", &private_key);
        let password = write_private(&dir, "db.password", &format!("{PASSWORD}\n"));
        Self {
            dir,
            key,
            password,
            private_key,
            public_key,
        }
    }

    fn args(&self, endpoint: &str, db_port: u16) -> Vec<String> {
        [
            "--ingest-endpoint",
            endpoint,
            "--storage-reader-endpoint",
            endpoint,
            "--conversation-endpoint",
            endpoint,
            "--tenant-id",
            TENANT,
            "--sender-id",
            SENDER,
            "--recipient-id",
            RECIPIENT,
            "--signing-key-file",
            self.key.to_str().unwrap(),
            "--db-host",
            "127.0.0.1",
            "--db-port",
            &db_port.to_string(),
            "--db-user",
            "flare_core_app",
            "--db-password-file",
            self.password.to_str().unwrap(),
            "--timeout-seconds",
            "1",
        ]
        .iter()
        .map(|value| value.to_string())
        .collect()
    }

    fn config(&self, endpoint: &str) -> Config {
        match config::parse(&self.args(endpoint, 5432), |_| None).unwrap() {
            Parsed::Run(config) => *config,
            Parsed::Help => unreachable!(),
        }
    }

    fn secrets(&self) -> [&str; 2] {
        [self.private_key.as_str(), PASSWORD]
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write_private(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}

struct FakeReader {
    observations: Vec<Observation>,
    calls: usize,
}

impl DurabilityReader for FakeReader {
    async fn observe(&mut self, expected: &Expectation) -> Result<Observation> {
        assert_eq!(expected.tenant_id, TENANT);
        assert_eq!(expected.server_msg_id, "srv-smoke-grpc-1");
        assert_eq!(expected.seq, 7);
        self.calls += 1;
        Ok(if self.observations.len() > 1 {
            self.observations.remove(0)
        } else {
            self.observations[0].clone()
        })
    }
}

fn durable() -> Observation {
    Observation {
        transaction_read_only: true,
        messages_row_count: 1,
        ledger_write_state: Some("archive_persisted".into()),
        sender_conversation_seq: Some(7),
        recipient_conversation_seq: Some(7),
    }
}

fn report(bytes: &[u8]) -> std::collections::BTreeMap<String, String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

#[tokio::test]
async fn signed_send_is_accepted_and_read_back() {
    let fixture = Fixture::new();
    let (endpoint, seen, reads_seen) =
        start_core(&fixture.public_key, &fixture.public_key, 7).await;
    let config = fixture.config(&endpoint);
    let prepared = Prepared::load(&config).unwrap();
    let mut reader = FakeReader {
        observations: vec![
            Observation {
                ledger_write_state: Some("broker_accepted".into()),
                ..durable()
            },
            durable(),
        ],
        calls: 0,
    };
    let mut out = Vec::new();
    smoke(
        &config,
        &prepared,
        &mut reader,
        &mut out,
        Duration::from_millis(5),
    )
    .await
    .unwrap();

    let report = report(&out);
    assert_eq!(report["mode"], "grpc");
    assert_eq!(report["status"], "pass");
    assert_eq!(report["server_msg_id"], "srv-smoke-grpc-1");
    assert_eq!(report["conversation_seq"], "7");
    assert_eq!(
        report["conversation_id"],
        cid::single_chat_conversation_id(SENDER, RECIPIENT)
    );
    assert_eq!(
        report["send_durability"],
        "SEND_ACK_DURABILITY_BROKER_ACCEPTED"
    );
    assert_eq!(report["messages_row_count"], "1");
    assert_eq!(report["ledger_write_state"], "archive_persisted");
    assert_eq!(report["readback_recipient_conversation_seq"], "7");
    assert_eq!(reader.calls, 2);
    assert_eq!(report["grpc_readback"], "signed");
    assert_eq!(report["grpc_readback_storage_message_seq"], "7");
    assert_eq!(report["grpc_readback_sender_conversation_seq"], "7");
    assert_eq!(report["grpc_readback_recipient_conversation_seq"], "7");
    assert_eq!(report["storage_read_rpc"], readback::STORAGE_READ_RPC);
    assert_eq!(
        report["conversation_read_rpc"],
        readback::CONVERSATION_READ_RPC
    );

    // 回读全部经签名入口，每个读者读的是自己：收件人读消息，收发双方各读自己的会话列表。
    {
        let reads_seen = reads_seen.lock().unwrap();
        let read = |rpc: &'static str, actor: &str| SignedRead {
            rpc,
            actor_type: SignedActorType::User,
            actor: actor.to_owned(),
            tenant: TENANT.to_owned(),
        };
        assert_eq!(
            reads_seen.reads,
            [
                read("QueryMessagesBySeq", RECIPIENT),
                read("ListConversations", SENDER),
                read("ListConversations", RECIPIENT),
            ]
        );
        for keys in &reads_seen.metadata_keys {
            assert_eq!(
                keys.iter()
                    .filter(|key| key.as_str() == SIGNED_CONTEXT_HEADER)
                    .count(),
                1,
                "{keys:?}"
            );
            for key in legacy_identity_keys()
                .iter()
                .chain([&"authorization".to_owned()])
            {
                assert!(!keys.contains(key), "{key} sent: {keys:?}");
            }
        }
    }

    let seen = seen.lock().unwrap();
    assert_eq!(seen.requests, 1, "exactly one send");
    assert_eq!(seen.accepted, 1);
    assert_eq!(seen.signed_contexts, 1);
    assert_eq!(seen.actor, SENDER);
    assert_eq!(seen.tenant, TENANT);
    for key in legacy_identity_keys()
        .iter()
        .chain([&"authorization".to_owned()])
    {
        assert!(
            !seen.metadata_keys.contains(key),
            "{key} sent: {:?}",
            seen.metadata_keys
        );
    }
    let text = String::from_utf8_lossy(&out);
    for secret in fixture.secrets() {
        assert!(!text.contains(secret.trim()));
    }
}

#[tokio::test]
async fn a_signature_from_another_key_is_rejected() {
    let fixture = Fixture::new();
    let (_, foreign_public) = key_pair(12);
    let (endpoint, seen) = start_ingest(&foreign_public).await;
    let config = fixture.config(&endpoint);
    let prepared = Prepared::load(&config).unwrap();
    let mut reader = FakeReader {
        observations: vec![durable()],
        calls: 0,
    };
    let mut out = Vec::new();
    let error = smoke(
        &config,
        &prepared,
        &mut reader,
        &mut out,
        Duration::from_millis(5),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("rejected the signed send"), "{error}");
    assert!(error.contains("Unauthenticated"), "{error}");
    assert_eq!(reader.calls, 0, "no read-back after a rejected send");
    assert!(out.is_empty());
    assert_eq!(seen.lock().unwrap().accepted, 0);
}

/// 读接口验不过签（别的密钥）时立刻失败，不等超时，也不再做数据库核对。
#[tokio::test]
async fn a_read_the_core_cannot_verify_fails_the_smoke() {
    let fixture = Fixture::new();
    let (_, foreign_public) = key_pair(12);
    let (endpoint, seen, reads_seen) = start_core(&fixture.public_key, &foreign_public, 7).await;
    let mut config = fixture.config(&endpoint);
    config.timeout = Duration::from_secs(30);
    let prepared = Prepared::load(&config).unwrap();
    let mut reader = FakeReader {
        observations: vec![durable()],
        calls: 0,
    };
    let mut out = Vec::new();
    let started = std::time::Instant::now();
    let error = smoke(
        &config,
        &prepared,
        &mut reader,
        &mut out,
        Duration::from_millis(5),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(started.elapsed() < Duration::from_secs(10), "{error}");
    assert!(error.contains("rejected the signed read"), "{error}");
    assert!(error.contains("Unauthenticated"), "{error}");
    assert!(error.contains(readback::STORAGE_READ_RPC), "{error}");
    assert_eq!(seen.lock().unwrap().accepted, 1);
    assert!(reads_seen.lock().unwrap().reads.is_empty());
    assert_eq!(reader.calls, 0);
    assert!(out.is_empty());
}

/// 会话列表的 seq 一直低于发送 seq：超时后失败并说明是哪一方。
#[tokio::test]
async fn a_conversation_list_behind_the_send_fails_after_the_timeout() {
    let fixture = Fixture::new();
    let (endpoint, _, reads_seen) = start_core(&fixture.public_key, &fixture.public_key, 6).await;
    let config = fixture.config(&endpoint);
    let prepared = Prepared::load(&config).unwrap();
    let mut reader = FakeReader {
        observations: vec![durable()],
        calls: 0,
    };
    let mut out = Vec::new();
    let error = smoke(
        &config,
        &prepared,
        &mut reader,
        &mut out,
        Duration::from_millis(20),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("signed gRPC read-back"), "{error}");
    assert!(
        error.contains("sender conversation list seq 6 is below 7"),
        "{error}"
    );
    assert!(
        error.contains("recipient conversation list seq 6 is below 7"),
        "{error}"
    );
    assert!(reads_seen.lock().unwrap().reads.len() >= 3);
    assert!(!String::from_utf8_lossy(&out).contains("status=pass"));
}

#[tokio::test]
async fn missing_archive_state_fails_after_the_timeout() {
    let fixture = Fixture::new();
    let (endpoint, _) = start_ingest(&fixture.public_key).await;
    let config = fixture.config(&endpoint);
    let prepared = Prepared::load(&config).unwrap();
    let mut reader = FakeReader {
        observations: vec![Observation {
            ledger_write_state: Some("broker_accepted".into()),
            recipient_conversation_seq: None,
            ..durable()
        }],
        calls: 0,
    };
    let mut out = Vec::new();
    let error = smoke(
        &config,
        &prepared,
        &mut reader,
        &mut out,
        Duration::from_millis(20),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("did not become durable"), "{error}");
    assert!(
        error.contains("ledger_write_state=broker_accepted"),
        "{error}"
    );
    assert!(
        error.contains("recipient conversation row missing"),
        "{error}"
    );
    assert!(!String::from_utf8_lossy(&out).contains("status=pass"));
}

/// 一个只计数、从不应答的监听端口。
fn silent_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn assert_never_connected(listener: &TcpListener) {
    match listener.accept() {
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        other => panic!("unexpected connection: {other:?}"),
    }
}

#[tokio::test]
async fn dry_run_validates_everything_and_opens_no_connection() {
    let fixture = Fixture::new();
    let (ingest, ingest_port) = silent_listener();
    let (database, db_port) = silent_listener();
    let mut args = fixture.args(&format!("http://127.0.0.1:{ingest_port}"), db_port);
    args.push("--dry-run".into());
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = execute(
        &args,
        |_| None,
        &mut out,
        &mut err,
        Duration::from_millis(5),
    )
    .await;
    assert_eq!(code, 0, "{}", String::from_utf8_lossy(&err));
    let plan = report(&out);
    assert_eq!(plan["status"], "dry-run");
    assert_eq!(plan["signing_key"], "loaded;test_signature=ok");
    assert_eq!(plan["connections_opened"], "0");
    assert_never_connected(&ingest);
    assert_never_connected(&database);
    let text = String::from_utf8_lossy(&out).to_string() + &String::from_utf8_lossy(&err);
    for secret in fixture.secrets() {
        assert!(!text.contains(secret.trim()));
    }
}

#[tokio::test]
async fn refusals_happen_before_any_connection() {
    let fixture = Fixture::new();
    let (ingest, ingest_port) = silent_listener();
    let (database, db_port) = silent_listener();
    let base = fixture.args(&format!("http://127.0.0.1:{ingest_port}"), db_port);
    let replace = |flag: &str, value: &str| {
        let mut args = base.clone();
        let position = args.iter().position(|arg| arg == flag).unwrap();
        args[position + 1] = value.to_owned();
        args
    };
    let default_password = write_private(&fixture.dir, "default.password", "flare123\n");
    let open_key = fixture.dir.join("open.key");
    std::fs::write(&open_key, &fixture.private_key).unwrap();
    std::fs::set_permissions(&open_key, std::fs::Permissions::from_mode(0o644)).unwrap();
    let mut without_key = base.clone();
    let position = without_key
        .iter()
        .position(|arg| arg == "--signing-key-file")
        .unwrap();
    without_key.drain(position..position + 2);
    let cases: Vec<(Vec<String>, &str)> = vec![
        (
            replace(
                "--signing-key-file",
                fixture.dir.join("absent.key").to_str().unwrap(),
            ),
            "signing key file",
        ),
        (
            replace("--signing-key-file", open_key.to_str().unwrap()),
            "chmod 600",
        ),
        (
            replace("--db-password-file", default_password.to_str().unwrap()),
            "known default",
        ),
        (replace("--tenant-id", "0"), "isolated fixture tenant"),
        (without_key, "--signing-key-file is required"),
    ];
    for (args, message) in cases {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = execute(
            &args,
            |_| None,
            &mut out,
            &mut err,
            Duration::from_millis(5),
        )
        .await;
        let text = String::from_utf8_lossy(&err).to_string();
        assert_eq!(code, EXIT_INVALID, "{message}: {text}");
        assert!(text.contains(message), "{message}: {text}");
        assert!(out.is_empty());
        assert!(!text.contains("flare123"));
        assert!(!text.contains("PRIVATE KEY"));
    }
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let env = |name: &str| (name == "PGPASSWORD").then(|| PASSWORD.to_owned());
    assert_eq!(
        execute(&base, env, &mut out, &mut err, Duration::from_millis(5)).await,
        EXIT_INVALID
    );
    assert!(!String::from_utf8_lossy(&err).contains(PASSWORD));
    assert_never_connected(&ingest);
    assert_never_connected(&database);
}

#[tokio::test]
async fn nothing_is_sent_when_the_read_only_session_cannot_open() {
    let fixture = Fixture::new();
    let (endpoint, seen) = start_ingest(&fixture.public_key).await;
    // 绑定后立即释放，得到一个大概率无人监听的端口。
    let closed_port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let args = fixture.args(&endpoint, closed_port);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = execute(
        &args,
        |_| None,
        &mut out,
        &mut err,
        Duration::from_millis(5),
    )
    .await;
    let text = String::from_utf8_lossy(&err).to_string();
    assert_eq!(code, EXIT_FAILED, "{text}");
    assert!(text.contains("read-only database session"), "{text}");
    assert!(!text.contains(PASSWORD));
    assert_eq!(
        seen.lock().unwrap().requests,
        0,
        "no send without read-back"
    );
}

#[test]
fn sources_never_build_unsigned_identity_metadata() {
    let sources = [
        ("main.rs", include_str!("main.rs")),
        ("error.rs", include_str!("error.rs")),
        ("config.rs", include_str!("config.rs")),
        ("durability.rs", include_str!("durability.rs")),
        ("ingest.rs", include_str!("ingest.rs")),
        ("readback.rs", include_str!("readback.rs")),
        ("signed.rs", include_str!("signed.rs")),
        ("cid.rs", include_str!("cid.rs")),
        ("redact.rs", include_str!("redact.rs")),
        ("secret_file.rs", include_str!("secret_file.rs")),
    ];
    for (name, source) in sources {
        for key in legacy_identity_keys() {
            assert!(!source.contains(&key), "{key} in {name}");
        }
        assert!(!source.contains("request_with_context"), "{name}");
        assert!(!source.contains("metadata_mut"), "{name}");
    }
    let ingest = include_str!("ingest.rs");
    assert_eq!(ingest.matches(".send_message(").count(), 1);
    // 两个读调用都只能用 signed_request 构造。
    let readback = include_str!("readback.rs");
    assert_eq!(readback.matches("signed_request(").count(), 2);
    assert_eq!(readback.matches("tonic::Request::new").count(), 0);
    assert!(ingest.contains("signed_request(build_request(message), &token)?"));
    let signed = include_str!("signed.rs");
    assert!(signed.contains("set_signed_context_token(&mut request, &token.0)"));
}

/// 真实 PostgreSQL（带 Core 迁移）上的端到端：假 Ingest 验签 + 只读会话回读。
/// 需要一次性的库：FLARE_SMOKE_GRPC_LIVE_PG_PORT（127.0.0.1 上，库 flare_core）与
/// FLARE_SMOKE_GRPC_LIVE_PG_PASSWORD_FILE（角色 flare_core_app 的 600 口令文件）。
/// 测试会写入一组夹具行（随机 e2e 租户），只用于一次性库。
#[tokio::test]
#[ignore = "needs a throwaway PostgreSQL with Core migrations"]
async fn live_postgres_read_back_is_read_only() {
    use sqlx::Connection;
    let port: u16 = std::env::var("FLARE_SMOKE_GRPC_LIVE_PG_PORT")
        .expect("FLARE_SMOKE_GRPC_LIVE_PG_PORT")
        .parse()
        .unwrap();
    let password_file = PathBuf::from(
        std::env::var("FLARE_SMOKE_GRPC_LIVE_PG_PASSWORD_FILE")
            .expect("FLARE_SMOKE_GRPC_LIVE_PG_PASSWORD_FILE"),
    );
    let password =
        secret_file::read_db_password(&password_file, "flare_core_app", "flare_core").unwrap();
    let tenant = format!("e2e-{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
    let conversation = cid::single_chat_conversation_id(SENDER, RECIPIENT);

    // 夹具：一个可写的普通会话写入发送后应有的行。
    let target = config::DbTarget {
        host: "127.0.0.1".into(),
        port,
        database: "flare_core".into(),
        user: "flare_core_app".into(),
        password_file: password_file.clone(),
    };
    let writable = sqlx::postgres::PgConnectOptions::new_without_pgpass()
        .host("127.0.0.1")
        .port(port)
        .username("flare_core_app")
        .password(&password)
        .database("flare_core");
    let mut seed = sqlx::postgres::PgConnection::connect_with(&writable)
        .await
        .unwrap();
    for (sql, binds) in [
        (
            "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, last_message_seq) \
             VALUES ($1, $2, 1, 7)",
            vec![tenant.clone(), conversation.clone()],
        ),
        (
            "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id) \
             VALUES ($1, $2, $3), ($1, $2, $4)",
            vec![
                tenant.clone(),
                conversation.clone(),
                SENDER.into(),
                RECIPIENT.into(),
            ],
        ),
        (
            "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq) \
             VALUES ($1, 'srv-smoke-grpc-1', $2, $3, 7)",
            vec![tenant.clone(), conversation.clone(), SENDER.into()],
        ),
        (
            "INSERT INTO message_write_ledger (tenant_id, server_id, conversation_id, seq, write_state) \
             VALUES ($1, 'srv-smoke-grpc-1', $2, 7, 'archive_persisted')",
            vec![tenant.clone(), conversation.clone()],
        ),
    ] {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = query.bind(bind);
        }
        query.execute(&mut seed).await.unwrap();
    }
    seed.close().await.unwrap();

    // 只读会话：写入被数据库拒绝。
    let mut reader = PgReader::connect(&target, &password).await.unwrap();
    let denied = sqlx::query("UPDATE conversations SET last_message_seq = 0 WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(reader.connection())
        .await
        .unwrap_err()
        .to_string();
    assert!(denied.contains("read-only transaction"), "{denied}");
    let expected = Expectation {
        tenant_id: tenant.clone(),
        server_msg_id: "srv-smoke-grpc-1".into(),
        conversation_id: conversation.clone(),
        sender_id: SENDER.into(),
        recipient_id: RECIPIENT.into(),
        seq: 7,
    };
    let observed = reader.observe(&expected).await.unwrap();
    assert!(observed.problems(&expected).is_empty(), "{observed:?}");
    reader.close().await;

    // 整条链路：真二进制的入口函数 + 假 Ingest + 真只读会话。
    let fixture = Fixture::new();
    let (endpoint, seen) = start_ingest(&fixture.public_key).await;
    let mut args = fixture.args(&endpoint, port);
    let tenant_position = args.iter().position(|arg| arg == "--tenant-id").unwrap();
    args[tenant_position + 1] = tenant.clone();
    let password_position = args
        .iter()
        .position(|arg| arg == "--db-password-file")
        .unwrap();
    args[password_position + 1] = password_file.to_str().unwrap().to_owned();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = execute(
        &args,
        |_| None,
        &mut out,
        &mut err,
        Duration::from_millis(50),
    )
    .await;
    assert_eq!(code, 0, "{}", String::from_utf8_lossy(&err));
    let report = report(&out);
    assert_eq!(report["status"], "pass");
    assert_eq!(report["ledger_write_state"], "archive_persisted");
    assert_eq!(report["readback_sender_conversation_seq"], "7");
    assert_eq!(seen.lock().unwrap().accepted, 1);
    assert!(!String::from_utf8_lossy(&out).contains(&password));
}

/// 会话回读与 Core 会话列表同口径（`durability::CONVERSATION_ROWS`）：
/// 非 0 租户的 `conversations.last_message_seq` 在栈上恒为 NULL（会话服务只为租户 0 推进），
/// 回读要靠成员可见范围内的消息尾；可见下限之前的消息、已删除的会话不算。
/// 需要的一次性库同 `live_postgres_read_back_is_read_only`。
#[tokio::test]
#[ignore = "needs a throwaway PostgreSQL with Core migrations"]
async fn live_postgres_conversation_seq_follows_the_conversation_list() {
    use sqlx::Connection;
    let port: u16 = std::env::var("FLARE_SMOKE_GRPC_LIVE_PG_PORT")
        .expect("FLARE_SMOKE_GRPC_LIVE_PG_PORT")
        .parse()
        .unwrap();
    let password_file = PathBuf::from(
        std::env::var("FLARE_SMOKE_GRPC_LIVE_PG_PASSWORD_FILE")
            .expect("FLARE_SMOKE_GRPC_LIVE_PG_PASSWORD_FILE"),
    );
    let password =
        secret_file::read_db_password(&password_file, "flare_core_app", "flare_core").unwrap();
    let conversation = cid::single_chat_conversation_id(SENDER, RECIPIENT);
    let writable = sqlx::postgres::PgConnectOptions::new_without_pgpass()
        .host("127.0.0.1")
        .port(port)
        .username("flare_core_app")
        .password(&password)
        .database("flare_core");
    let target = config::DbTarget {
        host: "127.0.0.1".into(),
        port,
        database: "flare_core".into(),
        user: "flare_core_app".into(),
        password_file: password_file.clone(),
    };

    // (会话生命周期, 接收方可见下限) → 期望 (发送方 seq, 接收方 seq)
    let cases: [(&str, i64, Option<i64>, Option<i64>); 3] = [
        ("active", 0, Some(7), Some(7)),
        ("active", 7, Some(7), Some(0)),
        ("deleted", 0, None, None),
    ];
    for (lifecycle, recipient_visible_from, sender_seq, recipient_seq) in cases {
        let tenant = format!("e2e-{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
        let mut seed = sqlx::postgres::PgConnection::connect_with(&writable)
            .await
            .unwrap();
        // 栈上发送后的真实形态：会话行的 last_message_seq 为 NULL，消息行与写账本已落库。
        sqlx::query(
            "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, lifecycle_state) \
             VALUES ($1, $2, 1, $3)",
        )
        .bind(&tenant)
        .bind(&conversation)
        .bind(lifecycle)
        .execute(&mut seed)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id, visible_from_seq) \
             VALUES ($1, $2, $3, 0), ($1, $2, $4, $5)",
        )
        .bind(&tenant)
        .bind(&conversation)
        .bind(SENDER)
        .bind(RECIPIENT)
        .bind(recipient_visible_from)
        .execute(&mut seed)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq) \
             VALUES ($1, 'srv-smoke-grpc-tail', $2, $3, 7)",
        )
        .bind(&tenant)
        .bind(&conversation)
        .bind(SENDER)
        .execute(&mut seed)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO message_write_ledger (tenant_id, server_id, conversation_id, seq, write_state) \
             VALUES ($1, 'srv-smoke-grpc-tail', $2, 7, 'archive_persisted')",
        )
        .bind(&tenant)
        .bind(&conversation)
        .execute(&mut seed)
        .await
        .unwrap();
        seed.close().await.unwrap();

        let expected = Expectation {
            tenant_id: tenant.clone(),
            server_msg_id: "srv-smoke-grpc-tail".into(),
            conversation_id: conversation.clone(),
            sender_id: SENDER.into(),
            recipient_id: RECIPIENT.into(),
            seq: 7,
        };
        let mut reader = PgReader::connect(&target, &password).await.unwrap();
        let observed = reader.observe(&expected).await.unwrap();
        reader.close().await;
        let case = format!("lifecycle={lifecycle} recipient_visible_from={recipient_visible_from}");
        assert_eq!(observed.messages_row_count, 1, "{case}");
        assert_eq!(
            observed.ledger_write_state.as_deref(),
            Some("archive_persisted"),
            "{case}"
        );
        assert_eq!(observed.sender_conversation_seq, sender_seq, "{case}");
        assert_eq!(observed.recipient_conversation_seq, recipient_seq, "{case}");
        let problems = observed.problems(&expected);
        match (lifecycle, recipient_visible_from) {
            ("active", 0) => assert!(problems.is_empty(), "{case}: {problems:?}"),
            ("active", _) => assert_eq!(
                problems,
                vec!["recipient conversation seq=0 (want >= 7)".to_owned()],
                "{case}"
            ),
            _ => assert_eq!(
                problems,
                vec![
                    "recipient conversation row missing".to_owned(),
                    "sender conversation row missing".to_owned(),
                ],
                "{case}"
            ),
        }
    }
}
