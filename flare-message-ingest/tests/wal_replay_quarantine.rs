//! 没记租户、也没配置默认租户的 WAL 条目被隔离（W13-R2）：每个条目在本进程只记一次 ERROR。
//! 条目不删，每个领取租约到期都会被再领一次；修复前每轮都记 ERROR。
//!
//! 本文件是独立的测试二进制，且必须只有这一个测试：它断言日志输出，而 tracing 的 callsite
//! interest 缓存是进程级的（见 `send_span_redaction.rs` 的说明），同一二进制里先跑到同一
//! callsite、没有 subscriber 的并行测试会让这里的 subscriber 看不到它。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use flare_im_contracts::Ctx;
use flare_message_ingest::application::handlers::{WalReplayHandler, WalReplayPublisher};
use flare_message_ingest::domain::repository::{WalPendingMessage, WalRepository};
use flare_message_ingest::domain::{MessageProfile, MessageSubmission, PersistenceMode};
use flare_proto::common::message_content::Content;
use flare_proto::common::{Message, MessageContent, TextContent};
use flare_server_core::error::Result;

/// 只读的 WAL：`list_pending` 每次都交回同一批条目（相当于租约到期后再被领取），`remove`
/// 记下被删的 id。
struct FixedWal {
    entries: Vec<WalPendingMessage>,
    removed: Mutex<Vec<String>>,
}

impl WalRepository for FixedWal {
    fn append<'a>(
        &'a self,
        _submission: &'a MessageSubmission,
        _tenant_id: &'a str,
        _write_proof: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn find_by_message_id<'a>(
        &'a self,
        _message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Message>>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }

    fn list_pending<'a>(
        &'a self,
        _limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>> {
        let entries = self.entries.clone();
        Box::pin(async move { Ok(entries) })
    }

    fn remove<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.removed
            .lock()
            .expect("removed poisoned")
            .push(message_id.to_string());
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct RecordingPublisher {
    tenants: Mutex<Vec<String>>,
}

impl WalReplayPublisher for RecordingPublisher {
    fn push_wal_message<'a>(
        &'a self,
        ctx: &'a Ctx,
        _submission: &'a MessageSubmission,
        _profile: &'a MessageProfile,
        _persistence_mode: PersistenceMode,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.tenants
            .lock()
            .expect("tenants poisoned")
            .push(ctx.tenant_id().unwrap_or_default().to_string());
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("captured logs poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CapturedLogs {
    fn error_lines_for(&self, message_id: &str) -> usize {
        let text = String::from_utf8(self.0.lock().expect("captured logs poisoned").clone())
            .expect("utf8 logs");
        text.lines()
            .filter(|line| line.contains("ERROR") && line.contains(message_id))
            .count()
    }
}

fn pending(message_id: &str, tenant_id: &str) -> WalPendingMessage {
    WalPendingMessage {
        write_proof: None,
        message_id: message_id.to_string(),
        tenant_id: tenant_id.to_string(),
        message: Message {
            server_id: message_id.to_string(),
            conversation_id: "conv-1".to_string(),
            sender_id: "sender-1".to_string(),
            conversation_seq: 7,
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "hello".to_string(),
                    mentions: vec![],
                })),
            }),
            ..Message::default()
        },
    }
}

#[tokio::test]
async fn tenantless_wal_entries_are_reported_once_per_entry() {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let wal = Arc::new(FixedWal {
        entries: vec![
            pending("msg-orphan-1", ""),
            pending("msg-orphan-2", "  "),
            pending("msg-ok", "tenant-a"),
        ],
        removed: Mutex::new(Vec::new()),
    });
    let publisher = Arc::new(RecordingPublisher::default());
    let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None);

    for round in 1..=3 {
        let report = handler.replay_once(10).await.expect("replay round");
        assert_eq!(report.quarantined, 2, "round {round}: {report:?}");
        assert_eq!(report.failed, 0, "round {round}: {report:?}");
        assert_eq!(report.replayed, 1, "round {round}: {report:?}");
    }

    for orphan in ["msg-orphan-1", "msg-orphan-2"] {
        assert_eq!(logs.error_lines_for(orphan), 1, "{orphan} is reported once");
    }
    assert_eq!(
        *wal.removed.lock().expect("removed poisoned"),
        vec!["msg-ok"; 3],
        "only the entry with a tenant is removed"
    );
    assert_eq!(
        *publisher.tenants.lock().expect("tenants poisoned"),
        vec!["tenant-a"; 3],
        "nothing published as tenant 0"
    );
}
