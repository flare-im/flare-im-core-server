//! 消息持久化领域服务
//!
//! 单一职责：消息与操作消息的存储。不负责会话更新、未读数、游标、媒体校验等（由会话服务/编排层负责）。

use std::sync::Arc;

use anyhow::{Result, anyhow};
use flare_im_core::utils::{current_millis, extract_timeline_from_extra};
use flare_proto::common::Message;
use tracing::{instrument, warn};

use crate::domain::events::{AckEvent, AckStatus};
use crate::domain::model::{PersistenceResult, PreparedMessage};
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, HotCacheRepository, MessageIdempotencyRepository,
    WalCleanupRepository,
};

/// 消息持久化领域服务
///
/// 只做：幂等 → 热缓存(可选) → 归档(PostgreSQL) → WAL 清理(可选) → ACK 发布(可选)
pub struct MessagePersistenceDomainService {
    idempotency_repo: Option<Arc<dyn MessageIdempotencyRepository + Send + Sync>>,
    hot_cache_repo: Option<Arc<dyn HotCacheRepository + Send + Sync>>,
    archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>>,
    wal_cleanup_repo: Option<Arc<dyn WalCleanupRepository + Send + Sync>>,
    ack_publisher: Option<Arc<dyn AckPublisher + Send + Sync>>,
}

impl MessagePersistenceDomainService {
    pub fn new(
        idempotency_repo: Option<Arc<dyn MessageIdempotencyRepository + Send + Sync>>,
        hot_cache_repo: Option<Arc<dyn HotCacheRepository + Send + Sync>>,
        archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>>,
        wal_cleanup_repo: Option<Arc<dyn WalCleanupRepository + Send + Sync>>,
        ack_publisher: Option<Arc<dyn AckPublisher + Send + Sync>>,
    ) -> Self {
        Self {
            idempotency_repo,
            hot_cache_repo,
            archive_repo,
            wal_cleanup_repo,
            ack_publisher,
        }
    }

    /// 准备消息（从 Kafka 请求中提取并补全字段）
    pub fn prepare_message(
        &self,
        request: crate::application::commands::StoreMessageCommandInternal,
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

        if message.conversation_id.is_empty() {
            message.conversation_id = conversation_id.clone();
        }
        if message.server_id.is_empty() {
            return Err(anyhow!("Message server_id cannot be empty"));
        }

        use flare_proto::common::MessageStatus;
        if message.status == MessageStatus::Created as i32 || message.status == 0 {
            message.status = MessageStatus::Sent as i32;
        }

        let mut timeline = extract_timeline_from_extra(&message.extra, current_millis());
        timeline.persisted_ts = Some(current_millis());
        flare_im_core::utils::embed_timeline_in_extra(&mut message, &timeline);

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
    pub async fn check_idempotency(&self, prepared: &PreparedMessage) -> Result<bool> {
        match &self.idempotency_repo {
            Some(repo) => {
                if !prepared.message.client_msg_id.is_empty() {
                    match repo
                        .is_new_by_client_msg_id(
                            &prepared.message.client_msg_id,
                            Some(&prepared.message.sender_id),
                        )
                        .await
                    {
                        Ok(v) => Ok(v),
                        Err(e) => {
                            warn!(error = ?e, "idempotency by client_msg_id failed, fallback to message_id");
                            repo.is_new(&prepared.message_id).await
                        }
                    }
                } else {
                    repo.is_new(&prepared.message_id).await
                }
            }
            None => Ok(true),
        }
    }

    /// 持久化单条：热缓存(可选) + 归档
    #[instrument(skip(self), fields(message_id = %prepared.message_id))]
    pub async fn persist_message(
        &self,
        _ctx: &flare_server_core::context::Context,
        prepared: PreparedMessage,
    ) -> Result<()> {
        if let Some(repo) = &self.hot_cache_repo {
            repo.store_hot(&prepared.message).await?;
        }
        if let Some(repo) = &self.archive_repo {
            repo.store_archive(&prepared.message).await?;
        }
        Ok(())
    }

    /// 批量持久化
    #[instrument(skip(self), fields(batch_size = prepared.len()))]
    pub async fn persist_batch(
        &self,
        _ctx: &flare_server_core::context::Context,
        prepared: Vec<PreparedMessage>,
    ) -> Result<()> {
        if prepared.is_empty() {
            return Ok(());
        }
        let messages: Vec<Message> = prepared.iter().map(|p| p.message.clone()).collect();
        if let Some(repo) = &self.hot_cache_repo {
            repo.store_hot_batch(&messages).await?;
        }
        if let Some(repo) = &self.archive_repo {
            repo.store_archive_batch(&messages).await?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn cleanup_wal(&self, message_id: &str) -> Result<()> {
        if let Some(repo) = &self.wal_cleanup_repo {
            if let Err(e) = repo.remove(message_id).await {
                warn!(error = ?e, message_id = %message_id, "WAL cleanup failed");
            }
        }
        Ok(())
    }

    pub async fn publish_ack(&self, result: &PersistenceResult) -> Result<()> {
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
            if let Err(e) = publisher.publish(event).await {
                warn!(error = ?e, message_id = %result.message_id, "ACK publish failed");
            }
        }
        Ok(())
    }

    /// 单条一致性流程：幂等 → 持久化 → WAL 清理 → ACK
    pub async fn ensure_consistency(
        &self,
        ctx: &flare_server_core::context::Context,
        prepared: PreparedMessage,
    ) -> Result<PersistenceResult> {
        let is_new = self.check_idempotency(&prepared).await?;
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
        self.cleanup_wal(&message_id).await?;

        let result = PersistenceResult {
            message_id,
            conversation_id,
            timeline,
            deduplicated: false,
        };
        self.publish_ack(&result).await?;
        Ok(result)
    }

    /// 批量一致性流程
    pub async fn ensure_batch_consistency(
        &self,
        ctx: &flare_server_core::context::Context,
        prepared: Vec<PreparedMessage>,
    ) -> Result<Vec<PersistenceResult>> {
        if prepared.is_empty() {
            return Ok(vec![]);
        }

        let mut new_messages = Vec::new();
        let mut results = Vec::new();

        for msg in prepared {
            let is_new = self.check_idempotency(&msg).await?;
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
            self.cleanup_wal(&msg.message_id).await?;
        }
        for msg in new_messages {
            let result = PersistenceResult {
                message_id: msg.message_id.clone(),
                conversation_id: msg.conversation_id.clone(),
                timeline: msg.timeline.clone(),
                deduplicated: false,
            };
            self.publish_ack(&result).await?;
            results.push(result);
        }
        Ok(results)
    }
}
