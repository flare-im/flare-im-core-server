//! 消息持久化领域服务
//!
//! 单一职责：消息与操作消息的存储。不负责会话更新、未读数、游标、媒体校验等（由会话服务/编排层负责）。

use std::sync::Arc;

use anyhow::{Result, anyhow};
use flare_im_core::utils::{
    current_millis, embed_timeline_in_extra_map, extract_timeline_from_extra,
};
use flare_server_core::context::Ctx;
use tracing::{instrument, warn};

use flare_im_core::utils::datetime_to_timestamp;

use crate::domain::events::{AckEvent, AckStatus};
use crate::domain::model::{Event, EventPayload, EventType, PersistenceResult, PreparedMessage};
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
    MessageIdempotencyRepository, WalCleanupRepository,
};

/// 消息持久化领域服务
///
/// 只做：幂等 → 热缓存(可选) → 归档(PostgreSQL) → WAL 清理(可选) → ACK 发布(可选)
///
/// 使用泛型参数而非 trait objects，符合 Rust 2024 规范
pub struct MessagePersistenceDomainService<I, H, A, E, W, P>
where
    I: MessageIdempotencyRepository + Send + Sync,
    H: HotCacheRepository + Send + Sync,
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
    W: WalCleanupRepository + Send + Sync,
    P: AckPublisher + Send + Sync,
{
    idempotency_repo: Option<Arc<I>>,
    hot_cache_repo: Option<Arc<H>>,
    archive_repo: Option<Arc<A>>,
    event_stream_repo: Option<Arc<E>>,
    wal_cleanup_repo: Option<Arc<W>>,
    ack_publisher: Option<Arc<P>>,
}

impl<I, H, A, E, W, P> MessagePersistenceDomainService<I, H, A, E, W, P>
where
    I: MessageIdempotencyRepository + Send + Sync,
    H: HotCacheRepository + Send + Sync,
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
    W: WalCleanupRepository + Send + Sync,
    P: AckPublisher + Send + Sync,
{
    pub fn new(
        idempotency_repo: Option<Arc<I>>,
        hot_cache_repo: Option<Arc<H>>,
        archive_repo: Option<Arc<A>>,
        event_stream_repo: Option<Arc<E>>,
        wal_cleanup_repo: Option<Arc<W>>,
        ack_publisher: Option<Arc<P>>,
    ) -> Self {
        Self {
            idempotency_repo,
            hot_cache_repo,
            archive_repo,
            event_stream_repo,
            wal_cleanup_repo,
            ack_publisher,
        }
    }

    /// 准备消息（从 MessageEnvelope / TopicEventEnvelope 解析后的命令）
    pub fn prepare_message(
        &self,
        request: crate::application::commands::ProcessStoreMessageCommand,
    ) -> Result<PreparedMessage> {
        let conversation_id = if request.conversation_id.is_empty() {
            request
                .message
                .as_ref()
                .map(|m| m.conversation_id.clone())
                .unwrap_or_default()
        } else {
            request.conversation_id.clone()
        };

        let mut message = request
            .message
            .ok_or_else(|| anyhow!("missing message payload"))?;

        if let Some(ref tenant) = request.tenant {
            message
                .extra
                .insert("tenant_id".to_string(), tenant.tenant_id.clone());
        } else if let Some(tenant_id) = request.metadata.get("x-tenant-id") {
            message
                .extra
                .insert("tenant_id".to_string(), tenant_id.clone());
        }
        if message.conversation_id.is_empty() {
            message.conversation_id = conversation_id.clone();
        }
        if message.server_id.is_empty() {
            return Err(anyhow!("Message server_id cannot be empty"));
        }

        // 与 proto MessageStatus 语义对齐：Created=1, Sent=2
        if message.status == 1 || message.status == 0 {
            message.status = 2;
        }

        let mut timeline = extract_timeline_from_extra(&message.extra, current_millis());
        timeline.persisted_ts = Some(current_millis());
        embed_timeline_in_extra_map(&mut message.extra, &timeline);

        Ok(PreparedMessage {
            conversation_id,
            message_id: message.server_id.clone(),
            message,
            timeline,
            sync: request.sync,
        })
    }

    /// 幂等性检查（client_msg_id 优先，否则 server_id）
    #[instrument(skip(self), fields(message_id = %prepared.message_id))]
    pub async fn check_idempotency(&self, ctx: &Ctx, prepared: &PreparedMessage) -> Result<bool> {
        match &self.idempotency_repo {
            Some(repo) => {
                if !prepared.message.client_msg_id.is_empty() {
                    match repo
                        .is_new_by_client_msg_id(
                            ctx,
                            &prepared.message.client_msg_id,
                            Some(&prepared.message.sender_id),
                        )
                        .await
                    {
                        Ok(v) => Ok(v),
                        Err(e) => {
                            warn!(error = ?e, "idempotency by client_msg_id failed, fallback to message_id");
                            repo.is_new(ctx, &prepared.message_id).await
                        }
                    }
                } else {
                    repo.is_new(ctx, &prepared.message_id).await
                }
            }
            None => Ok(true),
        }
    }

    /// 持久化单条：热缓存(可选) + 归档 + 事件流(供 Sync)
    #[instrument(skip(self), fields(message_id = %prepared.message_id))]
    pub async fn persist_message(&self, ctx: &Ctx, prepared: PreparedMessage) -> Result<()> {
        if let Some(repo) = &self.hot_cache_repo {
            repo.store_hot(ctx, &prepared.message).await?;
        }
        if let Some(repo) = &self.archive_repo {
            repo.store_archive(ctx, &prepared.message).await?;
        }
        if let Some(repo) = &self.event_stream_repo {
            let event = build_event_message(&prepared.message);
            if let Err(e) = repo.append_event_to_stream(ctx, &event).await {
                warn!(error = ?e, message_id = %prepared.message_id, "append_event_to_stream failed");
            }
        }
        Ok(())
    }

    /// 批量持久化
    #[instrument(skip(self), fields(batch_size = prepared.len()))]
    pub async fn persist_batch(&self, ctx: &Ctx, prepared: Vec<PreparedMessage>) -> Result<()> {
        if prepared.is_empty() {
            return Ok(());
        }
        let messages: Vec<crate::domain::model::Message> =
            prepared.iter().map(|p| p.message.clone()).collect();
        if let Some(repo) = &self.hot_cache_repo {
            repo.store_hot_batch(ctx, &messages).await?;
        }
        if let Some(repo) = &self.archive_repo {
            repo.store_archive_batch(ctx, &messages).await?;
        }
        if let Some(repo) = &self.event_stream_repo {
            for msg in &prepared {
                let event = build_event_message(&msg.message);
                if let Err(e) = repo.append_event_to_stream(ctx, &event).await {
                    warn!(error = ?e, message_id = %msg.message_id, "append_event_to_stream failed");
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn cleanup_wal(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        if let Some(repo) = &self.wal_cleanup_repo
            && let Err(e) = repo.remove(ctx, message_id).await
        {
            warn!(error = ?e, message_id = %message_id, "WAL cleanup failed");
        }
        Ok(())
    }

    pub async fn publish_ack(&self, ctx: &Ctx, result: &PersistenceResult) -> Result<()> {
        if let Some(publisher) = &self.ack_publisher {
            let persisted_ts = result.timeline.persisted_ts.unwrap_or_else(current_millis);
            let event = AckEvent {
                message_id: &result.message_id,
                conversation_id: &result.conversation_id,
                status: AckStatus::from_deduplicated(result.deduplicated),
                ingestion_ts: result.timeline.ingestion_ts,
                persisted_ts,
                deduplicated: result.deduplicated,
            };
            if let Err(e) = publisher.publish(ctx, event).await {
                warn!(error = ?e, message_id = %result.message_id, "ACK publish failed");
            }
        }
        Ok(())
    }

    /// 单条一致性流程：幂等 → 持久化 → WAL 清理 → ACK
    pub async fn ensure_consistency(
        &self,
        ctx: &Ctx,
        prepared: PreparedMessage,
    ) -> Result<PersistenceResult> {
        let is_new = self.check_idempotency(ctx, &prepared).await?;
        if !is_new {
            return Ok(PersistenceResult {
                message_id: prepared.message_id.clone(),
                conversation_id: prepared.conversation_id.clone(),
                timeline: prepared.timeline.clone(),
                deduplicated: true,
            });
        }

        let message_id = prepared.message_id.clone();
        let conversation_id = prepared.conversation_id.clone();
        let timeline = prepared.timeline.clone();

        self.persist_message(ctx, prepared).await?;
        self.cleanup_wal(ctx, &message_id).await?;

        let result = PersistenceResult {
            message_id,
            conversation_id,
            timeline,
            deduplicated: false,
        };
        self.publish_ack(ctx, &result).await?;
        Ok(result)
    }

    /// 批量一致性流程
    pub async fn ensure_batch_consistency(
        &self,
        ctx: &Ctx,
        prepared: Vec<PreparedMessage>,
    ) -> Result<Vec<PersistenceResult>> {
        if prepared.is_empty() {
            return Ok(vec![]);
        }

        let mut new_messages = Vec::new();
        let mut results = Vec::new();

        for msg in prepared {
            let is_new = self.check_idempotency(ctx, &msg).await?;
            if is_new {
                new_messages.push(msg);
            } else {
                results.push(PersistenceResult {
                    message_id: msg.message_id.clone(),
                    conversation_id: msg.conversation_id.clone(),
                    timeline: msg.timeline.clone(),
                    deduplicated: true,
                });
            }
        }

        if new_messages.is_empty() {
            return Ok(results);
        }

        self.persist_batch(ctx, new_messages.clone()).await?;
        for msg in &new_messages {
            self.cleanup_wal(ctx, &msg.message_id).await?;
        }
        for msg in new_messages {
            let result = PersistenceResult {
                message_id: msg.message_id.clone(),
                conversation_id: msg.conversation_id.clone(),
                timeline: msg.timeline.clone(),
                deduplicated: false,
            };
            self.publish_ack(ctx, &result).await?;
            results.push(result);
        }
        Ok(results)
    }
}

/// 从已持久化消息构建 EVENT_MESSAGE，供事件流写入（Sync 按 last_seq 拉取）
fn build_event_message(message: &crate::domain::model::Message) -> Event {
    let tenant_id = message
        .extra
        .get("tenant_id")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("default");
    let now = chrono::Utc::now();
    Event {
        tenant_id: tenant_id.to_string(),
        conversation_id: message.conversation_id.clone(),
        seq: message.seq,
        r#type: EventType::Message,
        created_at: Some(datetime_to_timestamp(now)),
        operator_id: String::new(),
        event_seq: None,
        request_id: None,
        payload: Some(EventPayload::Message(message.clone())),
    }
}
