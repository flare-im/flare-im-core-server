//! 命令处理器（编排层）- 轻量级，只负责编排领域服务

use anyhow::Result;
use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::context::Ctx;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

use crate::application::commands::ProcessStoreMessageCommand;
use crate::domain::model::{PersistenceResult, PreparedMessage};
use crate::domain::service::MessagePersistenceDomainService;

/// 普通消息持久化命令处理器（编排层）
///
/// 使用泛型参数而非 trait objects，符合 Rust 2024 规范
pub struct MessagePersistenceCommandHandler<I, H, A, E, W, P>
where
    I: crate::domain::repository::MessageIdempotencyRepository + Send + Sync,
    H: crate::domain::repository::HotCacheRepository + Send + Sync,
    A: crate::domain::repository::ArchiveStoreRepository + Send + Sync,
    E: crate::domain::repository::EventStreamRepository + Send + Sync,
    W: crate::domain::repository::WalCleanupRepository + Send + Sync,
    P: crate::domain::repository::AckPublisher + Send + Sync,
{
    domain_service: Arc<MessagePersistenceDomainService<I, H, A, E, W, P>>,
    metrics: Arc<StorageWriterMetrics>,
}

impl<I, H, A, E, W, P> MessagePersistenceCommandHandler<I, H, A, E, W, P>
where
    I: crate::domain::repository::MessageIdempotencyRepository + Send + Sync,
    H: crate::domain::repository::HotCacheRepository + Send + Sync,
    A: crate::domain::repository::ArchiveStoreRepository + Send + Sync,
    E: crate::domain::repository::EventStreamRepository + Send + Sync,
    W: crate::domain::repository::WalCleanupRepository + Send + Sync,
    P: crate::domain::repository::AckPublisher + Send + Sync,
{
    pub fn new(
        domain_service: Arc<MessagePersistenceDomainService<I, H, A, E, W, P>>,
        metrics: Arc<StorageWriterMetrics>,
    ) -> Self {
        Self {
            domain_service,
            metrics,
        }
    }

    /// 处理存储消息命令 - 只处理普通消息，如果是操作消息则返回 None
    #[instrument(skip(self), fields(tenant_id, message_id))]
    pub async fn handle(&self, ctx: &Ctx, command: ProcessStoreMessageCommand) -> Result<Option<PersistenceResult>> {
        let start = Instant::now();

        let message_id_for_error = command.message.as_ref().map(|m| m.server_id.clone());

        // 提取租户ID用于指标
        let tenant_id = command
            .tenant
            .as_ref()
            .map(|t| t.tenant_id.as_str())
            .unwrap_or("unknown")
            .to_string();

        let prepared = match self.domain_service.prepare_message(command.clone()) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    message_id = ?message_id_for_error,
                    "Failed to prepare message"
                );
                return Err(e);
            }
        };

        // 操作类由 Event 驱动，由 OperationMessageConsumer + EventApplicationService 处理
        // 保存必要信息用于后续操作（因为 prepared 会被移动到 persist_message）
        let message_id = prepared.message_id.clone();
        let conversation_id = prepared.conversation_id.clone();
        let timeline = prepared.timeline.clone();

        // 检查幂等性
        let is_new = match self.domain_service.check_idempotency(ctx, &prepared).await {
            Ok(new) => new,
            Err(e) => {
                tracing::error!(error = %e, message_id = %message_id, "Failed to check idempotency");
                return Err(e);
            }
        };
        let deduplicated = !is_new;

        // 记录去重统计（应用层关注点）
        if deduplicated {
            self.metrics.messages_duplicate_total.inc();
            tracing::debug!(message_id = %message_id, "Message is duplicate, skipping persistence");
        }

        if is_new {
            // 数据库写入
            let db_start = Instant::now();
            match self.domain_service.persist_message(ctx, prepared).await {
                Ok(_) => {
                    let db_duration = db_start.elapsed();

                    // 记录数据库写入耗时（应用层关注点）
                    self.metrics
                        .db_write_duration_seconds
                        .observe(db_duration.as_secs_f64());

                    // 记录总耗时和持久化计数（应用层关注点）
                    let total_duration = start.elapsed();
                    self.metrics
                        .messages_persisted_duration_seconds
                        .observe(total_duration.as_secs_f64());
                    self.metrics
                        .messages_persisted_total
                        .with_label_values(&[tenant_id.as_str()])
                        .inc();

                    tracing::info!(
                        message_id = %message_id,
                        conversation_id = %conversation_id,
                        duration_ms = total_duration.as_millis(),
                        "Message persisted successfully"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        message_id = %message_id,
                        conversation_id = %conversation_id,
                        "Failed to persist message to database"
                    );
                    return Err(e);
                }
            }
        }

        // 清理 WAL（即使失败也不影响消息持久化，只记录警告）
        if let Err(e) = self.domain_service.cleanup_wal(ctx, &message_id).await {
            tracing::warn!(error = %e, message_id = %message_id, "Failed to cleanup WAL, but message is already persisted");
        }

        // 构建结果（使用已保存的信息）
        let result = PersistenceResult {
            conversation_id,
            message_id,
            timeline,
            deduplicated,
        };

        // 发布 ACK 事件（即使失败也不影响消息持久化，只记录警告）
        if let Err(e) = self.domain_service.publish_ack(ctx, &result).await {
            tracing::warn!(error = %e, message_id = %result.message_id, "Failed to publish ACK, but message is already persisted");
        }

        Ok(Some(result))
    }

    /// 批量处理存储消息命令（优化性能）
    #[instrument(skip(self), fields(batch_size = commands.len()))]
    pub async fn handle_batch(
        &self,
        ctx: &Ctx,
        commands: Vec<ProcessStoreMessageCommand>,
    ) -> Result<Vec<PersistenceResult>> {
        let start = Instant::now();

        // 1. 批量准备消息
        let mut prepared_messages = Vec::with_capacity(commands.len());
        for command in &commands {
            match self.domain_service.prepare_message(command.clone()) {
                Ok(prepared) => {
                    prepared_messages.push(prepared);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to prepare message in batch");
                    // 继续处理其他消息
                }
            }
        }

        if prepared_messages.is_empty() {
            return Ok(Vec::new());
        }

        // 2. 批量检查幂等性
        let mut new_messages: Vec<PreparedMessage> = Vec::new();
        let mut deduplicated_count = 0;

        for prepared in &prepared_messages {
            match self.domain_service.check_idempotency(ctx, prepared).await {
                Ok(true) => {
                    new_messages.push(PreparedMessage::clone(prepared));
                }
                Ok(false) => {
                    deduplicated_count += 1;
                    self.metrics.messages_duplicate_total.inc();
                }
                Err(e) => {
                    tracing::warn!(error = %e, message_id = %prepared.message_id, "Idempotency check failed, treating as new");
                    new_messages.push(PreparedMessage::clone(prepared));
                }
            }
        }

        // 3. 批量持久化新消息
        if !new_messages.is_empty() {
            let db_start = Instant::now();
            match self
                .domain_service
                .persist_batch(ctx, new_messages.clone())
                .await
            {
                Ok(_) => {
                    let db_duration = db_start.elapsed();
                    self.metrics
                        .db_write_duration_seconds
                        .observe(db_duration.as_secs_f64());

                    let total_duration = start.elapsed();
                    self.metrics
                        .messages_persisted_duration_seconds
                        .observe(total_duration.as_secs_f64());
                    self.metrics
                        .messages_persisted_total
                        .with_label_values(&["batch"])
                        .inc_by(new_messages.len() as u64);

                    tracing::info!(
                        batch_size = new_messages.len(),
                        duration_ms = total_duration.as_millis(),
                        "Batch messages persisted successfully"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to persist batch messages");
                    return Err(e);
                }
            }
        }

        // 4. 批量清理 WAL 和发布 ACK
        let mut results = Vec::new();
        for prepared in &prepared_messages {
            let deduplicated = deduplicated_count > 0
                && !new_messages
                    .iter()
                    .any(|m| m.message_id == prepared.message_id);

            // 清理 WAL
            if let Err(e) = self.domain_service.cleanup_wal(ctx, &prepared.message_id).await {
                tracing::warn!(error = %e, message_id = %prepared.message_id, "Failed to cleanup WAL");
            }

            // 构建结果
            let result = PersistenceResult {
                conversation_id: prepared.conversation_id.clone(),
                message_id: prepared.message_id.clone(),
                timeline: prepared.timeline.clone(),
                deduplicated,
            };

            // 发布 ACK
            if let Err(e) = self.domain_service.publish_ack(ctx, &result).await {
                tracing::warn!(error = %e, message_id = %result.message_id, "Failed to publish ACK");
            }

            results.push(result);
        }

        Ok(results)
    }
}