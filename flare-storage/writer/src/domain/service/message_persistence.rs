//! 消息持久化领域服务
//!
//! 单一职责：消息与操作消息的存储。不负责会话更新、未读数、游标、媒体校验等（由会话服务/编排层负责）。

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::{
    current_millis, extract_timeline_from_extra, normalize_optional_tenant_id, normalize_tenant_id,
};
use flare_im_service_kit::metrics::StorageWriterMetrics;
use flare_im_usage::UsageRecorder;
use flare_proto::common::RetentionMode;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use tracing::{instrument, warn};

use flare_im_contracts::utils::datetime_to_timestamp;

use crate::domain::events::{AckEvent, AckStatus};
use crate::domain::model::{Event, EventPayload, EventType, PersistenceResult, PreparedMessage};
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
    MessageIdempotencyRepository, MessageWriteLedgerRepository, MessageWriteStage,
    PluginEntityBindingRepository, WalCleanupRepository,
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
    write_ledger_repo: Option<Arc<dyn MessageWriteLedgerRepository>>,
    metrics: Option<Arc<StorageWriterMetrics>>,
    /// 用量计数点（`messages.persisted`）：新消息提交后按租户计；去重命中的不计。
    usage: UsageRecorder,
    /// 插件卡片的实体绑定登记（归档之后、确认之前）；没有时插件卡片不绑定实体。
    plugin_entity_bindings: Option<Arc<dyn PluginEntityBindingRepository>>,
}

#[derive(Debug, Clone)]
enum IdempotencyReservation {
    None,
    ServerMessageId(String),
    ClientMessageId {
        client_msg_id: String,
        sender_id: Option<String>,
        conversation_id: Option<String>,
    },
}

#[derive(Debug, Clone)]
struct IdempotencyDecision {
    is_new: bool,
    reservation: IdempotencyReservation,
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
            write_ledger_repo: None,
            metrics: None,
            usage: UsageRecorder::disabled(),
            plugin_entity_bindings: None,
        }
    }

    pub fn with_plugin_entity_bindings(
        mut self,
        plugin_entity_bindings: Option<Arc<dyn PluginEntityBindingRepository>>,
    ) -> Self {
        self.plugin_entity_bindings = plugin_entity_bindings;
        self
    }

    /// 登记已归档消息里插件卡片的实体绑定。只是插件状态发布的前提，不影响消息本身：失败只记 WARN
    /// 与计数（`storage_writer_persist_total{path="plugin_entity_binding",result="error"}`），消息照常确认；
    /// 该实体此后的共享状态发布会因未绑定被拒绝（插件调用本身不受影响）。
    async fn register_plugin_entity_bindings(
        &self,
        ctx: &Ctx,
        messages: &[crate::domain::model::Message],
    ) {
        let Some(bindings) = &self.plugin_entity_bindings else {
            return;
        };
        let mut by_tenant: std::collections::BTreeMap<String, Vec<crate::domain::model::Message>> =
            Default::default();
        for message in messages {
            if let Ok(tenant_id) = prepared_tenant_id(message) {
                by_tenant
                    .entry(tenant_id)
                    .or_default()
                    .push(message.clone());
            }
        }
        for (tenant_id, messages) in by_tenant {
            match bindings.register(ctx, &tenant_id, &messages).await {
                Ok(0) => {}
                Ok(registered) => {
                    tracing::debug!(tenant_id = %tenant_id, registered, "plugin entity bindings registered");
                    if let Some(metrics) = &self.metrics {
                        metrics.record_storage_persist("plugin_entity_binding", "ok");
                    }
                }
                Err(error) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.record_storage_persist("plugin_entity_binding", "error");
                    }
                    warn!(
                        error = ?error,
                        tenant_id = %tenant_id,
                        batch_size = messages.len(),
                        "plugin entity binding registration failed after durable message write"
                    );
                }
            }
        }
    }

    pub fn with_write_ledger_repo(
        mut self,
        write_ledger_repo: Option<Arc<dyn MessageWriteLedgerRepository>>,
    ) -> Self {
        self.write_ledger_repo = write_ledger_repo;
        self
    }

    pub fn with_usage(mut self, usage: UsageRecorder) -> Self {
        self.usage = usage;
        self
    }

    pub fn with_metrics(mut self, metrics: Option<Arc<StorageWriterMetrics>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// 准备消息（从 MqEnvelope 解析后的命令）
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
            .ok_or_else(|| flare_err!(ErrorCode::InvalidParameter, "missing message payload"))?;

        // 落库租户只取命令上的租户（消费者按生产方写的消息头填）；消息体 attributes（即
        // `extra`）里的租户字段是发送方提交的数据，不作租户来源，也不再兜底成 "0"。
        let tenant_id = request
            .tenant
            .as_ref()
            .and_then(|tenant| normalize_optional_tenant_id(Some(&tenant.tenant_id)))
            .ok_or_else(|| {
                flare_err!(
                    ErrorCode::InvalidParameter,
                    "stored message has no tenant from its envelope headers"
                )
            })?;
        if message.conversation_id.is_empty() {
            message.conversation_id = conversation_id.clone();
        }
        pin_message_tenant(&mut message, &tenant_id);
        if message.server_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "Message server_id cannot be empty"
            ));
        }

        // 与 proto MessageStatus 语义对齐：Created=1, Sent=2
        if message.status == 1 || message.status == 0 {
            message.status = 2;
        }
        if let Some(policy) = message.retention_policy.as_ref() {
            let mode = RetentionMode::try_from(policy.mode).unwrap_or(RetentionMode::Unspecified);
            if mode != RetentionMode::None
                && policy
                    .expire_after_seconds
                    .is_some_and(|seconds| seconds <= 0)
            {
                return Err(flare_err!(
                    ErrorCode::InvalidParameter,
                    "expire_after_seconds must be positive when retention is enabled"
                ));
            }
        }

        let mut timeline = extract_timeline_from_extra(&request.metadata, current_millis());
        timeline.persisted_ts = Some(current_millis());

        Ok(PreparedMessage {
            conversation_id,
            message_id: message.server_id.clone(),
            message,
            timeline,
            sync: request.sync,
        })
    }

    /// 幂等性检查（client_msg_id 优先，否则 server_id）
    #[instrument(skip(self, ctx, prepared), fields(message_id = %prepared.message_id))]
    pub async fn check_idempotency(&self, ctx: &Ctx, prepared: &PreparedMessage) -> Result<bool> {
        Ok(self.reserve_idempotency(ctx, prepared).await?.is_new)
    }

    async fn reserve_idempotency(
        &self,
        ctx: &Ctx,
        prepared: &PreparedMessage,
    ) -> Result<IdempotencyDecision> {
        match &self.idempotency_repo {
            Some(repo) => {
                if !prepared.message.client_msg_id.is_empty() {
                    match repo
                        .is_new_by_client_msg_id(
                            ctx,
                            &prepared.message.client_msg_id,
                            Some(&prepared.message.sender_id),
                            Some(&prepared.conversation_id),
                        )
                        .await
                    {
                        Ok(true) => Ok(IdempotencyDecision {
                            is_new: true,
                            reservation: IdempotencyReservation::ClientMessageId {
                                client_msg_id: prepared.message.client_msg_id.clone(),
                                sender_id: (!prepared.message.sender_id.is_empty())
                                    .then(|| prepared.message.sender_id.clone()),
                                conversation_id: (!prepared.conversation_id.is_empty())
                                    .then(|| prepared.conversation_id.clone()),
                            },
                        }),
                        Ok(false) => Ok(IdempotencyDecision {
                            is_new: false,
                            reservation: IdempotencyReservation::None,
                        }),
                        Err(e) => {
                            warn!(error = ?e, "idempotency by client_msg_id failed, fallback to message_id");
                            match repo.is_new(ctx, &prepared.message_id).await {
                                Ok(true) => Ok(IdempotencyDecision {
                                    is_new: true,
                                    reservation: IdempotencyReservation::ServerMessageId(
                                        prepared.message_id.clone(),
                                    ),
                                }),
                                Ok(false) => Ok(IdempotencyDecision {
                                    is_new: false,
                                    reservation: IdempotencyReservation::None,
                                }),
                                Err(e) => Err(map_infra_error(
                                    e,
                                    ErrorCode::DatabaseError,
                                    "Failed to check idempotency",
                                )),
                            }
                        }
                    }
                } else {
                    match repo.is_new(ctx, &prepared.message_id).await {
                        Ok(true) => Ok(IdempotencyDecision {
                            is_new: true,
                            reservation: IdempotencyReservation::ServerMessageId(
                                prepared.message_id.clone(),
                            ),
                        }),
                        Ok(false) => Ok(IdempotencyDecision {
                            is_new: false,
                            reservation: IdempotencyReservation::None,
                        }),
                        Err(e) => Err(map_infra_error(
                            e,
                            ErrorCode::DatabaseError,
                            "Failed to check idempotency",
                        )),
                    }
                }
            }
            None => Ok(IdempotencyDecision {
                is_new: true,
                reservation: IdempotencyReservation::None,
            }),
        }
    }

    async fn release_idempotency_reservation(
        &self,
        ctx: &Ctx,
        reservation: &IdempotencyReservation,
    ) {
        let Some(repo) = &self.idempotency_repo else {
            return;
        };

        let release_result = match reservation {
            IdempotencyReservation::None => Ok(()),
            IdempotencyReservation::ServerMessageId(message_id) => {
                repo.release(ctx, message_id).await
            }
            IdempotencyReservation::ClientMessageId {
                client_msg_id,
                sender_id,
                conversation_id,
            } => {
                repo.release_by_client_msg_id(
                    ctx,
                    client_msg_id,
                    sender_id.as_deref(),
                    conversation_id.as_deref(),
                )
                .await
            }
        };

        if let Err(e) = release_result {
            warn!(
                error = ?e,
                reservation = ?reservation,
                "Failed to release idempotency reservation after durable write failure"
            );
        }
    }

    fn ledger_tenant_id(prepared: &PreparedMessage) -> Result<String> {
        prepared_tenant_id(&prepared.message)
    }

    async fn record_write_stage_owned(
        repo: Arc<dyn MessageWriteLedgerRepository>,
        metrics: Option<Arc<StorageWriterMetrics>>,
        ctx: Ctx,
        tenant_id: String,
        message_id: String,
        stage: MessageWriteStage,
        error: Option<String>,
    ) {
        if let Err(err) = repo
            .mark_stage(&ctx, &tenant_id, &message_id, stage, error.as_deref())
            .await
        {
            if let Some(metrics) = &metrics {
                metrics.record_ledger_transition(stage.as_str(), "error");
            }
            warn!(
                error = ?err,
                tenant_id = %tenant_id,
                message_id = %message_id,
                stage = %stage.as_str(),
                "Message write ledger stage update failed"
            );
        } else if let Some(metrics) = &metrics {
            metrics.record_ledger_transition(stage.as_str(), "success");
        }
    }

    async fn record_write_stage(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        stage: MessageWriteStage,
        error: Option<&str>,
    ) {
        let Some(repo) = &self.write_ledger_repo else {
            return;
        };

        Self::record_write_stage_owned(
            Arc::clone(repo),
            self.metrics.clone(),
            Arc::clone(ctx),
            tenant_id.to_string(),
            message_id.to_string(),
            stage,
            error.map(ToOwned::to_owned),
        )
        .await;
    }

    fn record_write_stage_best_effort(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        stage: MessageWriteStage,
    ) {
        let _ = (ctx, tenant_id, message_id);
        if let Some(metrics) = &self.metrics {
            metrics.record_ledger_transition(stage.as_str(), "skipped_hot_path");
        }
    }

    fn record_hot_cache_failure(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_storage_persist("hot_cache", "error");
        }
    }

    /// 持久化单条：归档 + 事件流(供 Sync) + 热缓存投影(可选)
    #[instrument(skip(self, ctx, prepared), fields(message_id = %prepared.message_id))]
    pub async fn persist_message(&self, ctx: &Ctx, prepared: PreparedMessage) -> Result<()> {
        if let Some(repo) = &self.archive_repo {
            repo.store_archive(ctx, &prepared.message)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "Failed to store archive")
                })?;
            self.register_plugin_entity_bindings(ctx, std::slice::from_ref(&prepared.message))
                .await;
        }
        let repo = self.event_stream_repo.as_ref().ok_or_else(|| {
            flare_err!(
                ErrorCode::InternalError,
                "Event stream repository not configured"
            )
        })?;
        let event = build_event_message(&prepared.message)?;
        repo.append_event_to_stream(ctx, &event)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to append message event to durable event stream",
                )
            })?;
        // 热缓存只是投影：写失败只记 WARN 与计数（`storage_writer_persist_total{path="hot_cache",
        // result="error"}`），不影响确认；Redis 不可达时连接上限让这一步在几秒内结束。
        if let Some(repo) = &self.hot_cache_repo
            && let Err(e) = repo.store_hot(ctx, &prepared.message).await
        {
            self.record_hot_cache_failure();
            warn!(
                error = ?e,
                message_id = %prepared.message_id,
                "Hot cache projection failed after durable message write"
            );
        }
        Ok(())
    }

    /// 批量持久化
    #[instrument(skip(self, ctx, prepared), fields(batch_size = prepared.len()))]
    pub async fn persist_batch(&self, ctx: &Ctx, prepared: Vec<PreparedMessage>) -> Result<()> {
        // batch_size 这个指标以前只有声明没有写入路径：注册进了 Prometheus，
        // /metrics 里永远是 0，看的人会以为批量落库从没发生过。
        if let Some(m) = &self.metrics {
            m.observe_batch_size(prepared.len());
        }
        if prepared.is_empty() {
            return Ok(());
        }
        let messages: Vec<crate::domain::model::Message> =
            prepared.iter().map(|p| p.message.clone()).collect();
        if let Some(repo) = &self.archive_repo {
            repo.store_archive_batch(ctx, &messages)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "Failed to store archive batch")
                })?;
            self.register_plugin_entity_bindings(ctx, &messages).await;
        }
        let repo = self.event_stream_repo.as_ref().ok_or_else(|| {
            flare_err!(
                ErrorCode::InternalError,
                "Event stream repository not configured"
            )
        })?;
        let events: Vec<Event> = prepared
            .iter()
            .map(|msg| build_event_message(&msg.message))
            .collect::<Result<_>>()?;
        repo.append_events_to_stream(ctx, &events)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to append message events to durable event stream",
                )
            })?;
        if let Some(repo) = &self.hot_cache_repo
            && let Err(e) = repo.store_hot_batch(ctx, &messages).await
        {
            self.record_hot_cache_failure();
            warn!(
                error = ?e,
                batch_size = messages.len(),
                "Hot cache batch projection failed after durable message writes"
            );
        }
        Ok(())
    }

    #[instrument(skip(self, ctx), fields(message_id = %message_id))]
    pub async fn cleanup_wal(&self, ctx: &Ctx, message_id: &str) -> Result<Option<bool>> {
        let Some(repo) = &self.wal_cleanup_repo else {
            return Ok(None);
        };

        if let Err(e) = repo.remove(ctx, message_id).await {
            warn!(error = ?e, message_id = %message_id, "WAL cleanup failed");
            return Ok(Some(false));
        }

        Ok(Some(true))
    }

    pub async fn publish_ack(&self, ctx: &Ctx, result: &PersistenceResult) -> Result<()> {
        let publisher = self
            .ack_publisher
            .as_ref()
            .ok_or_else(|| flare_err!(ErrorCode::InternalError, "ACK publisher not configured"))?;
        let persisted_ts = result.timeline.persisted_ts.unwrap_or_else(current_millis);
        let event = AckEvent {
            message_id: &result.message_id,
            conversation_id: &result.conversation_id,
            status: AckStatus::from_deduplicated(result.deduplicated),
            ingestion_ts: result.timeline.ingestion_ts,
            persisted_ts,
            deduplicated: result.deduplicated,
        };
        publisher
            .publish(ctx, event)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to publish ACK"))?;
        Ok(())
    }

    async fn publish_ack_and_record(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        result: &PersistenceResult,
    ) -> Result<()> {
        match self.publish_ack(ctx, result).await {
            Ok(()) => {
                self.record_write_stage_best_effort(
                    ctx,
                    tenant_id,
                    &result.message_id,
                    MessageWriteStage::AckPublished,
                );
                Ok(())
            }
            Err(err) => {
                let error = err.to_string();
                self.record_write_stage(
                    ctx,
                    tenant_id,
                    &result.message_id,
                    MessageWriteStage::AckPublishFailed,
                    Some(&error),
                )
                .await;
                Err(err)
            }
        }
    }

    /// 单条一致性流程：幂等 → 持久化 → ACK → WAL 清理
    pub async fn ensure_consistency(
        &self,
        ctx: &Ctx,
        prepared: PreparedMessage,
    ) -> Result<PersistenceResult> {
        let tenant_id = Self::ledger_tenant_id(&prepared)?;
        let idempotency = self.reserve_idempotency(ctx, &prepared).await?;
        if !idempotency.is_new {
            let result = PersistenceResult {
                message_id: prepared.message_id.clone(),
                conversation_id: prepared.conversation_id.clone(),
                timeline: prepared.timeline.clone(),
                deduplicated: true,
            };
            self.publish_ack_and_record(ctx, &tenant_id, &result)
                .await?;
            return Ok(result);
        }

        let message_id = prepared.message_id.clone();
        let conversation_id = prepared.conversation_id.clone();
        let timeline = prepared.timeline.clone();

        if let Err(err) = self.persist_message(ctx, prepared).await {
            self.release_idempotency_reservation(ctx, &idempotency.reservation)
                .await;
            return Err(err);
        }
        self.record_write_stage_best_effort(
            ctx,
            &tenant_id,
            &message_id,
            MessageWriteStage::StoragePersisted,
        );
        self.usage.messages_persisted(&tenant_id, 1);

        let result = PersistenceResult {
            message_id,
            conversation_id,
            timeline,
            deduplicated: false,
        };
        self.publish_ack_and_record(ctx, &tenant_id, &result)
            .await?;

        match self.cleanup_wal(ctx, &result.message_id).await? {
            Some(true) => {
                self.record_write_stage_best_effort(
                    ctx,
                    &tenant_id,
                    &result.message_id,
                    MessageWriteStage::WalCleaned,
                );
            }
            Some(false) => {
                self.record_write_stage(
                    ctx,
                    &tenant_id,
                    &result.message_id,
                    MessageWriteStage::WalCleanupFailed,
                    Some("wal cleanup failed"),
                )
                .await;
            }
            None => {}
        }
        Ok(result)
    }

    /// 批量一致性流程：幂等 → 批量持久化 → ACK → WAL 清理
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
            let tenant_id = Self::ledger_tenant_id(&msg)?;
            let idempotency = self.reserve_idempotency(ctx, &msg).await?;
            if idempotency.is_new {
                new_messages.push((msg, idempotency.reservation, tenant_id));
            } else {
                let result = PersistenceResult {
                    message_id: msg.message_id.clone(),
                    conversation_id: msg.conversation_id.clone(),
                    timeline: msg.timeline.clone(),
                    deduplicated: true,
                };
                self.publish_ack_and_record(ctx, &tenant_id, &result)
                    .await?;
                results.push(result);
            }
        }

        if new_messages.is_empty() {
            return Ok(results);
        }

        let messages_to_persist: Vec<PreparedMessage> = new_messages
            .iter()
            .map(|(message, _, _)| message.clone())
            .collect();
        if let Err(err) = self.persist_batch(ctx, messages_to_persist).await {
            for (_, reservation, _) in &new_messages {
                self.release_idempotency_reservation(ctx, reservation).await;
            }
            return Err(err);
        }
        // 整批已落库：先把每条都记为已持久化并计入用量，再逐条 ACK。ACK 中途失败会提前
        // 返回，而后面几条的幂等占位已经生效，重投时走去重分支不会再计，所以计数不能放在
        // ACK 循环里。
        for (msg, _, tenant_id) in &new_messages {
            self.record_write_stage_best_effort(
                ctx,
                tenant_id,
                &msg.message_id,
                MessageWriteStage::StoragePersisted,
            );
            self.usage.messages_persisted(tenant_id, 1);
        }
        for (msg, _, tenant_id) in &new_messages {
            let result = PersistenceResult {
                message_id: msg.message_id.clone(),
                conversation_id: msg.conversation_id.clone(),
                timeline: msg.timeline.clone(),
                deduplicated: false,
            };
            self.publish_ack_and_record(ctx, tenant_id, &result).await?;
            results.push(result);
        }
        for (msg, _, tenant_id) in &new_messages {
            match self.cleanup_wal(ctx, &msg.message_id).await? {
                Some(true) => {
                    self.record_write_stage_best_effort(
                        ctx,
                        tenant_id,
                        &msg.message_id,
                        MessageWriteStage::WalCleaned,
                    );
                }
                Some(false) => {
                    self.record_write_stage(
                        ctx,
                        tenant_id,
                        &msg.message_id,
                        MessageWriteStage::WalCleanupFailed,
                        Some("wal cleanup failed"),
                    )
                    .await;
                }
                None => {}
            }
        }
        Ok(results)
    }
}

/// 消息体 attributes 中声明租户的键：发送方可以提交它们，落库前一律改成命令租户。
const MESSAGE_TENANT_ATTRIBUTES: [&str; 2] = ["tenant_id", "x-tenant-id"];

/// 把消息体里的租户字段固定为落库租户（`prepare_message` 已从消息头取得）。归档行的
/// `extra`、事件流与写入台账都读 `extra["tenant_id"]`，这里保证它们与消息头一致。
fn pin_message_tenant(message: &mut crate::domain::model::Message, tenant_id: &str) {
    for key in MESSAGE_TENANT_ATTRIBUTES {
        let declared = message.extra.get(key).map(String::as_str);
        if declared.is_some_and(|declared| normalize_tenant_id(declared) != tenant_id) {
            warn!(
                message_id = %message.server_id,
                attribute = key,
                tenant_id = %tenant_id,
                "Message attribute declares another tenant; storing under the envelope tenant"
            );
        }
        if key == "tenant_id" || declared.is_some() {
            message.extra.insert(key.to_string(), tenant_id.to_string());
        }
    }
}

/// `prepare_message` 固定下来的落库租户；缺失说明消息没有经过它，拒绝而不是写进租户 "0"。
fn prepared_tenant_id(message: &crate::domain::model::Message) -> Result<String> {
    normalize_optional_tenant_id(message.extra.get("tenant_id").map(String::as_str)).ok_or_else(
        || {
            flare_err!(
                ErrorCode::InvalidParameter,
                "prepared message has no tenant"
            )
        },
    )
}

/// 从已持久化消息构建 EVENT_MESSAGE，供事件流写入（Sync 按 last_seq 拉取）
fn build_event_message(message: &crate::domain::model::Message) -> Result<Event> {
    let tenant_id = prepared_tenant_id(message)?;
    let now = chrono::Utc::now();
    Ok(Event {
        tenant_id,
        conversation_id: message.conversation_id.clone(),
        seq: message.conversation_seq,
        r#type: EventType::Message,
        created_at: Some(datetime_to_timestamp(now)),
        operator_id: String::new(),
        event_seq: None,
        request_id: None,
        payload: Some(EventPayload::Message(Box::new(message.clone()))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::events::AckEvent;
    use crate::domain::repository::{
        AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
        MessageIdempotencyRepository, MessageWriteLedgerRepository, MessageWriteStage,
        WalCleanupRepository,
    };
    use flare_im_contracts::message::Message;
    use flare_im_contracts::utils::{Context, TimelineMetadata};
    use flare_server_core::error::Result as AnyhowResult;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct NoopRepository;

    #[derive(Debug, Clone)]
    struct RecordedLedgerStage {
        message_id: String,
        stage: MessageWriteStage,
        error: Option<String>,
    }

    struct RecordingWriteLedgerRepository {
        stages: Arc<Mutex<Vec<RecordedLedgerStage>>>,
    }

    impl MessageWriteLedgerRepository for RecordingWriteLedgerRepository {
        fn mark_stage<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _tenant_id: &'a str,
            message_id: &'a str,
            stage: MessageWriteStage,
            error: Option<&'a str>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AnyhowResult<()>> + Send + 'a>>
        {
            let stages = self.stages.clone();
            let message_id = message_id.to_string();
            let error = error.map(ToString::to_string);
            Box::pin(async move {
                stages
                    .lock()
                    .expect("recorded ledger lock")
                    .push(RecordedLedgerStage {
                        message_id,
                        stage,
                        error,
                    });
                Ok(())
            })
        }
    }

    impl MessageIdempotencyRepository for NoopRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Ok(true)
        }
    }

    /// 一次幂等写入的捕获记录：(key, client_msg_id, device_id)。
    type IdempotencyObservation = (String, Option<String>, Option<String>);

    struct CapturingClientIdempotencyRepository {
        observed: Arc<Mutex<Vec<IdempotencyObservation>>>,
    }

    impl MessageIdempotencyRepository for CapturingClientIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Ok(true)
        }

        async fn is_new_by_client_msg_id(
            &self,
            _ctx: &Ctx,
            client_msg_id: &str,
            sender_id: Option<&str>,
            conversation_id: Option<&str>,
        ) -> AnyhowResult<bool> {
            self.observed
                .lock()
                .expect("observed idempotency lock")
                .push((
                    client_msg_id.to_string(),
                    sender_id.map(ToString::to_string),
                    conversation_id.map(ToString::to_string),
                ));
            Ok(true)
        }
    }

    impl HotCacheRepository for NoopRepository {
        async fn store_hot(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            Ok(())
        }
    }

    impl ArchiveStoreRepository for NoopRepository {
        async fn store_archive(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            Ok(())
        }
    }

    impl WalCleanupRepository for NoopRepository {
        async fn remove(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<()> {
            Ok(())
        }
    }

    impl AckPublisher for NoopRepository {
        async fn publish(&self, _ctx: &Ctx, _event: AckEvent<'_>) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct FailingEventStreamRepository;

    impl EventStreamRepository for FailingEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            Err(flare_server_core::error::FlareError::system(
                "event stream unavailable".to_string(),
            ))
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    struct FailOnceEventStreamRepository {
        attempts: Arc<AtomicUsize>,
    }

    impl EventStreamRepository for FailOnceEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(flare_server_core::error::FlareError::system(
                    "event stream unavailable on first attempt".to_string(),
                ))
            } else {
                Ok(())
            }
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    /// 按 message_id 逐条占位：同一条第二次进来就是去重命中。
    struct PerMessageIdempotencyRepository {
        reserved: Arc<Mutex<std::collections::HashSet<String>>>,
    }

    impl MessageIdempotencyRepository for PerMessageIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, message_id: &str) -> AnyhowResult<bool> {
            Ok(self
                .reserved
                .lock()
                .expect("reserved idempotency lock")
                .insert(message_id.to_string()))
        }

        async fn release(&self, _ctx: &Ctx, message_id: &str) -> AnyhowResult<()> {
            self.reserved
                .lock()
                .expect("reserved idempotency lock")
                .remove(message_id);
            Ok(())
        }
    }

    struct ReservingIdempotencyRepository {
        reserved: Arc<AtomicBool>,
    }

    impl MessageIdempotencyRepository for ReservingIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Ok(!self.reserved.swap(true, Ordering::SeqCst))
        }

        async fn release(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<()> {
            self.reserved.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    struct DuplicateIdempotencyRepository;

    impl MessageIdempotencyRepository for DuplicateIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Ok(false)
        }
    }

    struct FailingHotCacheRepository;

    impl HotCacheRepository for FailingHotCacheRepository {
        async fn store_hot(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            Err(flare_server_core::error::FlareError::system(
                "hot cache unavailable".to_string(),
            ))
        }
    }

    struct CountingHotCacheRepository {
        writes: Arc<AtomicUsize>,
    }

    impl HotCacheRepository for CountingHotCacheRepository {
        async fn store_hot(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct CountingArchiveRepository {
        writes: Arc<AtomicUsize>,
    }

    impl ArchiveStoreRepository for CountingArchiveRepository {
        async fn store_archive(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingArchiveRepository;

    impl ArchiveStoreRepository for FailingArchiveRepository {
        async fn store_archive(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            Err(flare_server_core::error::FlareError::system(
                "archive unavailable".to_string(),
            ))
        }
    }

    struct CountingEventStreamRepository {
        writes: Arc<AtomicUsize>,
    }

    impl EventStreamRepository for CountingEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    struct BatchCountingEventStreamRepository {
        batch_calls: Arc<AtomicUsize>,
        event_count: Arc<AtomicUsize>,
        single_writes: Arc<AtomicUsize>,
    }

    impl EventStreamRepository for BatchCountingEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            self.single_writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn append_events_to_stream(&self, _ctx: &Ctx, events: &[Event]) -> AnyhowResult<()> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            self.event_count.fetch_add(events.len(), Ordering::SeqCst);
            Ok(())
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    struct CountingAckPublisher {
        duplicate_acks: Arc<AtomicUsize>,
    }

    impl AckPublisher for CountingAckPublisher {
        async fn publish(&self, _ctx: &Ctx, event: AckEvent<'_>) -> AnyhowResult<()> {
            if event.deduplicated {
                self.duplicate_acks.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    /// 第 `fail_at` 次（从 0 起）发布失败，其余成功。
    struct FailNthAckPublisher {
        attempts: Arc<AtomicUsize>,
        fail_at: usize,
    }

    impl AckPublisher for FailNthAckPublisher {
        async fn publish(&self, _ctx: &Ctx, _event: AckEvent<'_>) -> AnyhowResult<()> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == self.fail_at {
                Err(flare_server_core::error::FlareError::system(
                    "ack publisher unavailable".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    struct FailOnceAckPublisher {
        attempts: Arc<AtomicUsize>,
    }

    impl AckPublisher for FailOnceAckPublisher {
        async fn publish(&self, _ctx: &Ctx, _event: AckEvent<'_>) -> AnyhowResult<()> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(flare_server_core::error::FlareError::system(
                    "ack publisher unavailable on first attempt".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    type TestPersistenceService = MessagePersistenceDomainService<
        NoopRepository,
        NoopRepository,
        NoopRepository,
        FailingEventStreamRepository,
        NoopRepository,
        NoopRepository,
    >;

    fn service_with_failing_event_stream() -> TestPersistenceService {
        MessagePersistenceDomainService::new(
            None,
            None,
            None,
            Some(Arc::new(FailingEventStreamRepository)),
            None,
            None,
        )
    }

    fn test_ctx() -> Ctx {
        Arc::new(Context::with_request_id("req-event-stream-test").with_tenant_id("tenant-a"))
    }

    fn prepared_message(message_id: &str, seq: u64) -> PreparedMessage {
        let mut extra = std::collections::HashMap::new();
        extra.insert("tenant_id".to_string(), "tenant-a".to_string());
        PreparedMessage {
            conversation_id: "conversation-a".to_string(),
            message_id: message_id.to_string(),
            message: Message {
                server_id: message_id.to_string(),
                conversation_id: "conversation-a".to_string(),
                sender_id: "sender-a".to_string(),
                conversation_seq: seq,
                extra,
                ..Message::default()
            },
            timeline: TimelineMetadata {
                ingestion_ts: 1,
                ..TimelineMetadata::default()
            },
            sync: true,
        }
    }

    fn store_command(
        tenant: Option<&str>,
        attributes: &[(&str, &str)],
    ) -> crate::application::commands::ProcessStoreMessageCommand {
        let extra = attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        crate::application::commands::ProcessStoreMessageCommand {
            conversation_id: "conversation-a".to_string(),
            message: Some(Message {
                server_id: "message-a".to_string(),
                conversation_id: "conversation-a".to_string(),
                conversation_seq: 1,
                extra,
                ..Message::default()
            }),
            sync: false,
            context: None,
            tenant: tenant.map(|tenant_id| crate::domain::model::TenantContext {
                tenant_id: tenant_id.to_string(),
                user_id: None,
            }),
            tags: Default::default(),
            // 消费者把消息体 attributes 与消息头一起并进 metadata；这里不该再从中取租户。
            metadata: [("tenant_id".to_string(), "tenant-b".to_string())]
                .into_iter()
                .collect(),
        }
    }

    /// 落库租户只取命令租户（消息头），缺失即拒绝，不再兜底成 "0"；消息体里声明的
    /// 租户字段被改成命令租户，事件流与写入台账随之一致（W11-T）。
    #[test]
    fn prepare_message_requires_the_command_tenant_and_pins_tenant_attributes() {
        let service = service_with_failing_event_stream();
        for tenant in [None, Some(""), Some("  ")] {
            let error = service
                .prepare_message(store_command(tenant, &[("tenant_id", "tenant-b")]))
                .expect_err("a message without a command tenant must not be prepared");
            assert_eq!(
                error.code(),
                Some(ErrorCode::InvalidParameter),
                "{tenant:?}"
            );
        }

        let prepared = service
            .prepare_message(store_command(
                Some("tenant-a"),
                &[("tenant_id", "tenant-b"), ("x-tenant-id", "tenant-c")],
            ))
            .expect("prepares");
        assert_eq!(prepared.message.extra["tenant_id"], "tenant-a");
        assert_eq!(prepared.message.extra["x-tenant-id"], "tenant-a");
        let event = build_event_message(&prepared.message).expect("event");
        assert_eq!(event.tenant_id, "tenant-a");
        assert_eq!(
            TestPersistenceService::ledger_tenant_id(&prepared).expect("ledger"),
            "tenant-a"
        );

        let zero = service
            .prepare_message(store_command(Some("0"), &[]))
            .expect("explicit tenant 0");
        assert_eq!(zero.message.extra["tenant_id"], "0");
        assert!(!zero.message.extra.contains_key("x-tenant-id"));

        let mut unprepared = prepared.message.clone();
        unprepared.extra.remove("tenant_id");
        assert!(build_event_message(&unprepared).is_err());
    }

    #[tokio::test]
    async fn persist_message_returns_error_when_event_stream_append_fails() {
        let service = service_with_failing_event_stream();
        let err = service
            .persist_message(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect_err("event stream failure must fail the durable write path");

        assert!(
            err.to_string()
                .contains("Failed to append message event to durable event stream"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn persist_message_returns_error_when_event_stream_repository_is_missing() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            None,
            None,
            None,
        );

        let err = service
            .persist_message(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect_err("durable event stream must be required after archive write");

        assert!(
            err.to_string()
                .contains("Event stream repository not configured"),
            "unexpected error: {err}"
        );
        assert_eq!(archive_writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn persist_batch_returns_error_when_event_stream_repository_is_missing() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            None,
            None,
            None,
        );

        let err = service
            .persist_batch(
                &test_ctx(),
                vec![
                    prepared_message("message-a", 1),
                    prepared_message("message-b", 2),
                ],
            )
            .await
            .expect_err("durable event stream must be required after archive batch write");

        assert!(
            err.to_string()
                .contains("Event stream repository not configured"),
            "unexpected error: {err}"
        );
        assert_eq!(archive_writes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn persist_message_does_not_fail_when_hot_cache_write_fails_after_durable_write() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            FailingHotCacheRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            Some(Arc::new(FailingHotCacheRepository)),
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes.clone(),
            })),
            None,
            None,
        );

        service
            .persist_message(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect("hot cache failures must not fail the durable write path");

        assert_eq!(archive_writes.load(Ordering::SeqCst), 1);
        assert_eq!(stream_writes.load(Ordering::SeqCst), 1);
    }

    /// Redis 不可达时持久写之后的确认不被热缓存卡住（W14-R (a)）：用真实的热缓存仓储指向关闭的
    /// 端口，`persist_message` 在连接上限内返回成功，失败计入
    /// `storage_writer_persist_total{path="hot_cache",result="error"}`。修复前每次写都新建连接、用
    /// redis 的默认退避，这一步 60 s 仍未返回。
    #[tokio::test]
    async fn an_unreachable_redis_does_not_hold_the_ack_after_the_durable_write() {
        use crate::infrastructure::persistence::repository::redis_cache::RedisHotCacheRepository;
        use flare_im_service_kit::redis_connection::{
            RedisConnectionLimits, SharedRedisConnection,
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let closed = listener.local_addr().expect("address");
        drop(listener);
        let limits = RedisConnectionLimits::default();
        let redis = Arc::new(SharedRedisConnection::new(
            redis::Client::open(format!("redis://{closed}/")).expect("client"),
            limits,
        ));
        let metrics = Arc::new(StorageWriterMetrics::new());
        let failures = || {
            metrics
                .storage_persist_total
                .with_label_values(&["hot_cache", "error"])
                .get()
        };
        let before = failures();
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            RedisHotCacheRepository,
            NoopRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            Some(Arc::new(RedisHotCacheRepository::from_parts(redis, 60, 10))),
            Some(Arc::new(NoopRepository)),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes.clone(),
            })),
            None,
            None,
        )
        .with_metrics(Some(metrics.clone()));

        tokio::time::timeout(
            limits.connect_budget() + std::time::Duration::from_secs(2),
            service.persist_message(&test_ctx(), prepared_message("message-a", 1)),
        )
        .await
        .expect("the hot cache projection must not hold the ack when Redis is unreachable")
        .expect("hot cache failures must not fail the durable write path");

        assert_eq!(stream_writes.load(Ordering::SeqCst), 1);
        assert_eq!(failures(), before + 1);
    }

    #[tokio::test]
    async fn persist_message_does_not_write_hot_cache_when_archive_write_fails() {
        let cache_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            CountingHotCacheRepository,
            FailingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            Some(Arc::new(CountingHotCacheRepository {
                writes: cache_writes.clone(),
            })),
            Some(Arc::new(FailingArchiveRepository)),
            None,
            None,
            None,
        );

        let err = service
            .persist_message(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect_err("archive failures must fail the durable write path");

        assert!(
            err.to_string().contains("Failed to store archive"),
            "unexpected error: {err}"
        );
        assert_eq!(
            cache_writes.load(Ordering::SeqCst),
            0,
            "hot cache must not be written before archive is durable"
        );
    }

    #[tokio::test]
    async fn persist_batch_does_not_fail_when_hot_cache_write_fails_after_durable_writes() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            FailingHotCacheRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            Some(Arc::new(FailingHotCacheRepository)),
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes.clone(),
            })),
            None,
            None,
        );

        service
            .persist_batch(
                &test_ctx(),
                vec![
                    prepared_message("message-a", 1),
                    prepared_message("message-b", 2),
                ],
            )
            .await
            .expect("hot cache batch failures must not fail the durable write path");

        assert_eq!(archive_writes.load(Ordering::SeqCst), 2);
        assert_eq!(stream_writes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn persist_batch_uses_batch_event_stream_append() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let batch_calls = Arc::new(AtomicUsize::new(0));
        let event_count = Arc::new(AtomicUsize::new(0));
        let single_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            BatchCountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(BatchCountingEventStreamRepository {
                batch_calls: batch_calls.clone(),
                event_count: event_count.clone(),
                single_writes: single_writes.clone(),
            })),
            None,
            None,
        );

        service
            .persist_batch(
                &test_ctx(),
                vec![
                    prepared_message("message-a", 1),
                    prepared_message("message-b", 2),
                    prepared_message("message-c", 3),
                ],
            )
            .await
            .expect("batch event stream append should succeed");

        assert_eq!(archive_writes.load(Ordering::SeqCst), 3);
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(event_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            single_writes.load(Ordering::SeqCst),
            0,
            "batch path must avoid per-event append calls"
        );
    }

    #[tokio::test]
    async fn ensure_consistency_releases_idempotency_reservation_after_durable_write_failure() {
        let reserved = Arc::new(AtomicBool::new(false));
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_attempts = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            ReservingIdempotencyRepository,
            NoopRepository,
            CountingArchiveRepository,
            FailOnceEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            Some(Arc::new(ReservingIdempotencyRepository {
                reserved: reserved.clone(),
            })),
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(FailOnceEventStreamRepository {
                attempts: stream_attempts.clone(),
            })),
            None,
            Some(Arc::new(NoopRepository)),
        )
        .with_usage(flare_im_usage::UsageRecorder::unpublished(
            "storage-writer-test",
        ));
        let usage = service.usage.clone();
        let persisted = || {
            usage
                .live()
                .into_iter()
                .filter(|reading| reading.meter == "messages.persisted")
                .map(|reading| (reading.app, reading.value))
                .collect::<Vec<_>>()
        };
        let ctx = test_ctx();

        service
            .ensure_consistency(&ctx, prepared_message("message-a", 1))
            .await
            .expect_err("first attempt should fail at durable event stream append");
        // Usage counts committed messages only.
        assert!(persisted().is_empty());

        let retry = service
            .ensure_consistency(&ctx, prepared_message("message-a", 1))
            .await
            .expect("retry after a failed durable write must be allowed");

        assert!(
            !retry.deduplicated,
            "retry after a failed durable write must not be reported as duplicate"
        );
        assert_eq!(
            archive_writes.load(Ordering::SeqCst),
            2,
            "retry must re-enter durable archive path"
        );
        assert_eq!(
            stream_attempts.load(Ordering::SeqCst),
            2,
            "retry must re-enter durable event stream path"
        );
        assert_eq!(persisted(), [("tenant-a".to_string(), 1)]);
    }

    /// 批量路径：整批落库后第二条 ACK 失败，第三条已经占位，重投时走去重分支。用量仍按
    /// 落库条数计，重投不再重复计。
    #[tokio::test]
    async fn ensure_batch_consistency_counts_every_persisted_message_when_an_ack_fails() {
        let reserved = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let ack_attempts = Arc::new(AtomicUsize::new(0));
        let batch_calls = Arc::new(AtomicUsize::new(0));
        let event_count = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            PerMessageIdempotencyRepository,
            NoopRepository,
            NoopRepository,
            BatchCountingEventStreamRepository,
            NoopRepository,
            FailNthAckPublisher,
        > = MessagePersistenceDomainService::new(
            Some(Arc::new(PerMessageIdempotencyRepository {
                reserved: reserved.clone(),
            })),
            None,
            Some(Arc::new(NoopRepository)),
            Some(Arc::new(BatchCountingEventStreamRepository {
                batch_calls: batch_calls.clone(),
                event_count: event_count.clone(),
                single_writes: Arc::new(AtomicUsize::new(0)),
            })),
            None,
            Some(Arc::new(FailNthAckPublisher {
                attempts: ack_attempts.clone(),
                fail_at: 1,
            })),
        )
        .with_usage(flare_im_usage::UsageRecorder::unpublished(
            "storage-writer-test",
        ));
        let usage = service.usage.clone();
        let persisted = || {
            usage
                .live()
                .into_iter()
                .filter(|reading| reading.meter == "messages.persisted")
                .map(|reading| (reading.app, reading.value))
                .collect::<Vec<_>>()
        };
        let batch = || {
            vec![
                prepared_message("message-a", 1),
                prepared_message("message-b", 2),
                prepared_message("message-c", 3),
            ]
        };
        let ctx = test_ctx();

        service
            .ensure_batch_consistency(&ctx, batch())
            .await
            .expect_err("the second ACK fails");
        assert_eq!(batch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(event_count.load(Ordering::SeqCst), 3);
        assert_eq!(ack_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(persisted(), [("tenant-a".to_string(), 3)]);

        let redelivered = service
            .ensure_batch_consistency(&ctx, batch())
            .await
            .expect("redelivery acknowledges the stored messages");
        assert!(redelivered.iter().all(|result| result.deduplicated));
        assert_eq!(
            batch_calls.load(Ordering::SeqCst),
            1,
            "nothing is rewritten"
        );
        assert_eq!(persisted(), [("tenant-a".to_string(), 3)]);
    }

    #[tokio::test]
    async fn ensure_consistency_scopes_client_idempotency_by_conversation() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            CapturingClientIdempotencyRepository,
            NoopRepository,
            NoopRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            Some(Arc::new(CapturingClientIdempotencyRepository {
                observed: observed.clone(),
            })),
            None,
            Some(Arc::new(NoopRepository)),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes,
            })),
            None,
            Some(Arc::new(NoopRepository)),
        );
        let mut prepared = prepared_message("message-a", 1);
        prepared.message.client_msg_id = "client-a".to_string();

        service
            .ensure_consistency(&test_ctx(), prepared)
            .await
            .expect("message should persist");

        assert_eq!(
            observed
                .lock()
                .expect("observed idempotency lock")
                .as_slice(),
            &[(
                "client-a".to_string(),
                Some("sender-a".to_string()),
                Some("conversation-a".to_string()),
            )]
        );
    }

    #[tokio::test]
    async fn ensure_consistency_skips_write_ledger_success_stages_on_hot_path() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let stages = Arc::new(Mutex::new(Vec::new()));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes,
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes,
            })),
            Some(Arc::new(NoopRepository)),
            Some(Arc::new(NoopRepository)),
        )
        .with_write_ledger_repo(Some(Arc::new(RecordingWriteLedgerRepository {
            stages: stages.clone(),
        })));

        service
            .ensure_consistency(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect("message should persist and publish ack");

        let recorded = stages.lock().expect("recorded ledger lock").clone();
        assert!(
            recorded.is_empty(),
            "successful ledger stages must not write on the ACK hot path: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn ensure_consistency_publishes_ack_for_duplicate_message() {
        let duplicate_acks = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            DuplicateIdempotencyRepository,
            NoopRepository,
            NoopRepository,
            CountingEventStreamRepository,
            NoopRepository,
            CountingAckPublisher,
        > = MessagePersistenceDomainService::new(
            Some(Arc::new(DuplicateIdempotencyRepository)),
            None,
            None,
            None,
            None,
            Some(Arc::new(CountingAckPublisher {
                duplicate_acks: duplicate_acks.clone(),
            })),
        )
        .with_usage(flare_im_usage::UsageRecorder::unpublished(
            "storage-writer-test",
        ));

        let result = service
            .ensure_consistency(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect("duplicate message should still produce a result");

        assert!(result.deduplicated);
        // A redelivered duplicate was counted when it was first persisted.
        assert!(service.usage.live().is_empty());
        assert_eq!(
            duplicate_acks.load(Ordering::SeqCst),
            1,
            "duplicate messages must still publish duplicate ACKs"
        );
    }

    #[tokio::test]
    async fn ensure_consistency_returns_error_when_ack_publisher_is_missing_after_durable_write() {
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            NoopRepository,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes.clone(),
            })),
            None,
            None,
        );

        let err = service
            .ensure_consistency(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect_err("ACK publisher must be required after durable write");

        assert!(
            err.to_string().contains("ACK publisher not configured"),
            "unexpected error: {err}"
        );
        assert_eq!(archive_writes.load(Ordering::SeqCst), 1);
        assert_eq!(stream_writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ensure_consistency_returns_error_when_ack_publish_fails_and_retries_duplicate_ack() {
        let reserved = Arc::new(AtomicBool::new(false));
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_writes = Arc::new(AtomicUsize::new(0));
        let ack_attempts = Arc::new(AtomicUsize::new(0));
        let service: MessagePersistenceDomainService<
            ReservingIdempotencyRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            FailOnceAckPublisher,
        > = MessagePersistenceDomainService::new(
            Some(Arc::new(ReservingIdempotencyRepository { reserved })),
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: archive_writes.clone(),
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: stream_writes.clone(),
            })),
            None,
            Some(Arc::new(FailOnceAckPublisher {
                attempts: ack_attempts.clone(),
            })),
        );
        let ctx = test_ctx();

        service
            .ensure_consistency(&ctx, prepared_message("message-a", 1))
            .await
            .expect_err("ACK publish failure must ask the consumer to retry");

        let retry = service
            .ensure_consistency(&ctx, prepared_message("message-a", 1))
            .await
            .expect("retry should publish a duplicate ACK");

        assert!(retry.deduplicated);
        assert_eq!(
            archive_writes.load(Ordering::SeqCst),
            1,
            "ACK retry must not re-enter archive path"
        );
        assert_eq!(
            stream_writes.load(Ordering::SeqCst),
            1,
            "ACK retry must not re-enter event stream path"
        );
        assert_eq!(ack_attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ensure_consistency_records_write_ledger_ack_failure_stage() {
        let ack_attempts = Arc::new(AtomicUsize::new(0));
        let stages = Arc::new(Mutex::new(Vec::new()));
        let service: MessagePersistenceDomainService<
            NoopRepository,
            NoopRepository,
            CountingArchiveRepository,
            CountingEventStreamRepository,
            NoopRepository,
            FailOnceAckPublisher,
        > = MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(CountingArchiveRepository {
                writes: Arc::new(AtomicUsize::new(0)),
            })),
            Some(Arc::new(CountingEventStreamRepository {
                writes: Arc::new(AtomicUsize::new(0)),
            })),
            Some(Arc::new(NoopRepository)),
            Some(Arc::new(FailOnceAckPublisher {
                attempts: ack_attempts,
            })),
        )
        .with_write_ledger_repo(Some(Arc::new(RecordingWriteLedgerRepository {
            stages: stages.clone(),
        })));

        let err = service
            .ensure_consistency(&test_ctx(), prepared_message("message-a", 1))
            .await
            .expect_err("ACK publish failure must fail the consistency flow");

        assert!(
            err.to_string().contains("Failed to publish ACK"),
            "unexpected error: {err}"
        );

        let recorded = stages.lock().expect("recorded ledger lock").clone();
        let failed = recorded
            .iter()
            .find(|record| record.stage == MessageWriteStage::AckPublishFailed)
            .expect("ack failure stage should be recorded");

        assert_eq!(failed.message_id, "message-a");
        assert!(
            failed
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Failed to publish ACK"),
            "unexpected ledger error: {:?}",
            failed.error
        );
    }
}
