//! 同步编排应用服务：组合 Conversation + Storage +（可选）事件读端口，落实初始化/离线/增量策略。
//!
//! 对外统一为 `flare.common.v1.Sync` / `SyncRes`（gRPC `ExecuteSync` 与 DATA 信道一致；`SyncRes` 仅承载 `payload`，错误走 gRPC `Status`）。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ListConversationParticipantsRequest,
    UpdateConversationUserSettingsRequest, UpdateCursorRequest,
};
use flare_im_contracts::Ctx;
use flare_proto::Message;
use flare_proto::common::SyncRecoveryHint;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::sync_slice_item::Payload as SyncSlicePayload;
use flare_proto::common::{
    ConversationDetailSync, ConversationDetailSyncRes, ConversationParticipant,
    ConversationParticipantsSync, ConversationParticipantsSyncRes, ConversationSummary,
    ConversationType as ProtoConversationType, ConversationUserSettingsSync,
    ConversationUserSettingsSyncRes, ConversationVersion, ConversationsSync, ConversationsSyncRes,
    EnsureConversationSync, EnsureConversationSyncRes, EventEnvelope, EventEnvelopeDeliveryMode,
    EventReplayPreset, EventStreamAckSyncRes, GetSyncCursorSync, GetSyncCursorSyncRes,
    MessagePreview, MultiConversationSync, MultiConversationSyncRes, MultiDeviceCursor,
    QueryEventsSync, QueryEventsSyncRes, SingleConversationSync, SingleConversationSyncRes,
    SnapshotConversationRow, SyncRes, SyncSessionHints, SyncSkipItem, SyncSliceItem,
    SyncSnapshotSync, SyncSnapshotSyncRes, SyncStaleContext, SyncTombstoneItem,
    UpdateSyncCursorSync, UpdateSyncCursorSyncRes,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tracing::{debug, trace, warn};

use crate::application::error::require_nonempty_conversation_id;
use crate::application::ports::{
    BootstrapPageCache, ConversationEventReadPort, ConversationSyncPort,
    ConversationVersionIndexPort, MemorySyncCursorCache, StorageReadPort, SyncCursorCachePort,
};
use crate::domain::model::{
    SyncIntent, clamp_messages_per_conversation, clamp_query_events_limit,
    normalize_query_event_types,
};
use crate::domain::service::{
    build_snapshot_cursor, max_seq_from_events, parse_snapshot_cursor, snapshot_global_seq,
};
use futures::stream::{StreamExt, TryStreamExt};

/// 批内/页内会话级存储查询的保序有界并发（J1）：延迟从 Σ(单查) 降到 ≈max(单查)×⌈N/并发⌉，
/// 并发上限护住存储连接池。`multi_conversation_sync` 与 `get_sync_snapshot` 共用。
const MULTI_SYNC_QUERY_CONCURRENCY: usize = 8;

/// 快照分页的 bootstrap 拉取上限：分页在编排层完成，须一次拿到大账号全集
/// （conversation 服务默认保守截 100 会让 >100 会话账号的其余会话永远不经列表同步下发）；
/// 受 conversation 侧硬上限共同钳制。
const SNAPSHOT_BOOTSTRAP_MAX_CONVERSATIONS: i32 = 5_000;

/// 与 `SyncSnapshotSyncRes.conversations` 逐行对齐，来自 ConversationBootstrap 摘要（单聊 `channel_id` 等对端路由）
#[derive(Clone, Default)]
struct ConversationSyncRoutingHint {
    channel_id: String,
    conversation_type: i32,
    peer_read_seq: u64,
    participant_version: u64,
    member_preview: Vec<ConversationParticipant>,
    is_muted: bool,
    is_pinned: bool,
    is_archived: bool,
    user_settings_version: u64,
    draft: String,
    visible_after_conversation_seq: u64,
}

pub struct SyncSnapshotOutcome {
    pub res: SyncSnapshotSyncRes,
    routing: Vec<ConversationSyncRoutingHint>,
}

struct MergedSnapshotRow {
    row: SnapshotConversationRow,
    bootstrap: ConversationSummary,
}

pub struct SyncOrchestrationHandler<I>
where
    I: ConversationSyncPort
        + StorageReadPort
        + ConversationEventReadPort
        + ConversationVersionIndexPort
        + Send
        + Sync,
{
    infra: Arc<I>,
    cursor_cache: Arc<MemorySyncCursorCache>,
    /// 分页续拉专用 bootstrap 快照缓存（见 [`BootstrapPageCache`]）。
    bootstrap_page_cache: BootstrapPageCache,
}

impl<I> SyncOrchestrationHandler<I>
where
    I: ConversationSyncPort
        + StorageReadPort
        + ConversationEventReadPort
        + ConversationVersionIndexPort
        + Send
        + Sync,
{
    pub fn new(infra: Arc<I>, cursor_cache: Arc<MemorySyncCursorCache>) -> Self {
        Self {
            infra,
            cursor_cache,
            bootstrap_page_cache: BootstrapPageCache::new(),
        }
    }

    /// 统一入口：`Sync` → 编排逻辑 → `SyncRes`。失败返回 `FlareError`；gRPC 层用 `IntoGrpc` 转为 `tonic::Status`（与 media 等服务一致）。
    pub async fn execute_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        mut sync: flare_proto::common::Sync,
    ) -> Result<SyncRes, FlareError> {
        let Some(payload) = sync.payload.take() else {
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "sync payload is required")
                    .build_error(),
            );
        };

        match payload {
            SyncPayload::SingleConversation(req) => {
                let v = self.single_conversation_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::SingleConversation(v)),
                })
            }
            SyncPayload::MultiConversation(req) => {
                let v = self.multi_conversation_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::MultiConversation(v)),
                })
            }
            SyncPayload::Conversations(req) => {
                let v = self.conversations_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::Conversations(v)),
                })
            }
            SyncPayload::ConversationDetail(req) => {
                let v = self.conversation_detail_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationDetail(v)),
                })
            }
            SyncPayload::QueryEvents(req) => {
                let v = self.query_events_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::QueryEvents(v)),
                })
            }
            SyncPayload::GetSyncCursor(req) => {
                let v = self.get_sync_cursor_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::GetSyncCursor(v)),
                })
            }
            SyncPayload::UpdateSyncCursor(req) => {
                let v = self.update_sync_cursor_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::UpdateSyncCursor(v)),
                })
            }
            SyncPayload::ConversationUserSettings(req) => {
                let v = self
                    .conversation_user_settings_sync(ctx, user_id, req)
                    .await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationUserSettings(v)),
                })
            }
            SyncPayload::EventStreamAck(_) => Ok(SyncRes {
                payload: Some(SyncResPayload::EventStreamAckRes(EventStreamAckSyncRes {})),
            }),
            SyncPayload::SyncSnapshot(req) => {
                let outcome = self.get_sync_snapshot(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::SyncSnapshotRes(outcome.res)),
                })
            }
            SyncPayload::ConversationParticipants(req) => {
                let v = self.conversation_participants_sync(ctx, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationParticipants(v)),
                })
            }
            SyncPayload::EnsureConversation(req) => {
                let v = self.ensure_conversation_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::EnsureConversation(v)),
                })
            }
            SyncPayload::ConversationsIncremental(_)
            | SyncPayload::ConversationsAll(_)
            | SyncPayload::ConversationMaxSeq(_) => Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "sync payload is not supported by orchestrator",
            )
            .build_error()),
        }
    }

    pub async fn get_sync_snapshot(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: SyncSnapshotSync,
    ) -> Result<SyncSnapshotOutcome, FlareError> {
        let started = std::time::Instant::now();
        let message_limit = clamp_messages_per_conversation(req.messages_per_conversation);
        debug!(
            user_id = %user_id,
            include_conversations = req.include_conversations,
            include_deleted = req.include_deleted,
            requested_conversation_count = req.conversation_ids.len(),
            messages_per_conversation = message_limit,
            snapshot_cursor = %req.snapshot_cursor,
            intent = ?SyncIntent::InitialBootstrap,
            "get_sync_snapshot (初始化/分页快照)"
        );

        // 分页游标只解析一次（过滤边界与分页共用同一份）。
        let page_cursor = parse_snapshot_cursor(&req.snapshot_cursor);
        // 存储层增量过滤：仅 **ASC 列表续拉**（warm 增量，cursor 非空）传边界——
        // DESC（冷启 bundle）与定向 conversation_ids 快照需要全集，不过滤。
        // 边界取 cursor_ms - 1：分页元组过滤是 (ms,cid) > (cursor_ms,cid)，同毫秒不同 cid
        // 的行仍可入页，-1 保证存储层返回的是元组过滤结果的超集（绝不欠拉）。
        let updated_after_ms = (req.conversation_ids.is_empty() && !req.newest_first)
            .then(|| {
                page_cursor
                    .as_ref()
                    .map(|(ms, _)| ms.saturating_sub(1).max(0))
            })
            .flatten()
            .unwrap_or(0);

        // 续拉页（cursor 非空）在 TTL 内复用同一份 bootstrap 快照：
        // 消灭"每页全量 bootstrap"的 DB 放大，并让同一分页序列看到一致数据集。
        // 第 1 页（cursor 为空）恒新鲜并回填缓存。缓存按"超集规则"服务（见 BootstrapPageCache）。
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        let is_continuation_page = !req.snapshot_cursor.trim().is_empty();
        let cached = if is_continuation_page {
            self.bootstrap_page_cache
                .get(&tenant_id, user_id, updated_after_ms)
        } else {
            None
        };
        let from_page_cache = cached.is_some();
        let conv_resp = match cached {
            Some(cached) => cached,
            None => {
                let resp = std::sync::Arc::new(
                    self.infra
                        .conversation_bootstrap(
                            ctx,
                            ConversationBootstrapRequest {
                                client_cursor_map: Default::default(),
                                include_recent_messages: false,
                                recent_message_limit: 0,
                                device_id: String::new(),
                                device_platform: String::new(),
                                updated_after_ms,
                                max_conversations: SNAPSHOT_BOOTSTRAP_MAX_CONVERSATIONS,
                            },
                        )
                        .await?,
                );
                self.bootstrap_page_cache
                    .put(&tenant_id, user_id, updated_after_ms, resp.clone());
                resp
            }
        };

        debug!(
            user_id = %user_id,
            conversation_bootstrap_count = conv_resp.conversations.len(),
            from_page_cache,
            "conversation bootstrap returned"
        );

        let filter_set: Option<HashSet<&str>> = if req.conversation_ids.is_empty() {
            None
        } else {
            Some(req.conversation_ids.iter().map(String::as_str).collect())
        };

        // 引用级筛选/去重/排序/分页——bootstrap 摘要（draft/preview/成员预览等大字段）
        // 只在**最终页**克隆一次；缓存命中时续拉页不再为全账号做 K×N 克隆 + K 次全量排序开销以外的复制。
        let mut candidate_index: HashMap<&str, &_> = HashMap::new();
        let mut filtered_out = 0usize;
        for bootstrap in conv_resp.conversations.iter() {
            if !valid_sync_conversation_id(&bootstrap.conversation_id) {
                filtered_out += 1;
                warn!(
                    conversation_id = %bootstrap.conversation_id,
                    "drop invalid conversation summary during sync snapshot"
                );
                continue;
            }
            if let Some(set) = &filter_set
                && !set.contains(bootstrap.conversation_id.as_str())
            {
                filtered_out += 1;
                continue;
            }
            candidate_index.insert(bootstrap.conversation_id.as_str(), bootstrap);
        }

        // 页大小与每会话消息数正交（旧实现复用 messages_per_conversation 双重语义）。
        // conversation_page_limit=0 → 沿用 messages_per_conversation 派生的旧默认。
        let page_limit = if req.conversation_page_limit > 0 {
            req.conversation_page_limit
        } else {
            req.messages_per_conversation
                .max(crate::domain::model::MIN_SNAPSHOT_PAGE_SIZE)
        }
        .max(crate::domain::model::MIN_SNAPSHOT_PAGE_SIZE) as usize;
        debug!(
            user_id = %user_id,
            merged_conversation_count = candidate_index.len(),
            filtered_out,
            page_limit,
            parsed_cursor = ?page_cursor,
            "snapshot merge completed"
        );

        let mut sorted: Vec<(i64, &str)> = candidate_index
            .values()
            .map(|b| (b.updated_at, b.conversation_id.as_str()))
            .collect();
        if req.newest_first {
            // I6 冷启 bundle：按活跃度降序，首屏 top-N 先到。
            sorted.sort_by(|a, b| b.cmp(a));
        } else {
            sorted.sort();
        }

        let page_keys: Vec<(i64, &str)> = match &page_cursor {
            Some((cursor_ms, cursor_cid)) => {
                let boundary = (*cursor_ms, cursor_cid.as_str());
                sorted
                    .into_iter()
                    .filter(|key| {
                        if req.newest_first {
                            *key < boundary
                        } else {
                            *key > boundary
                        }
                    })
                    .collect()
            }
            None => sorted,
        };

        let has_more = page_keys.len() > page_limit;
        // I6：只要本页有行就返回行水位游标（旧行为 !has_more 时返回空 → 上层回显旧游标 →
        // 客户端游标永不前进 → 每次热启/重连全量拉列表）。页空时保持空串，上层回显客户端游标。
        let next_cursor = page_keys
            .iter()
            .take(page_limit)
            .next_back()
            .map(|(ms, cid)| build_snapshot_cursor(*ms, cid))
            .unwrap_or_default();

        // 只克隆最终页的 bootstrap 行。
        let mut page: Vec<MergedSnapshotRow> = page_keys
            .into_iter()
            .take(page_limit)
            .filter_map(|(_, cid)| candidate_index.get(cid).copied())
            .map(|bootstrap| {
                let max_seq = bootstrap.max_conversation_seq as i64;
                MergedSnapshotRow {
                    row: SnapshotConversationRow {
                        conversation_id: bootstrap.conversation_id.clone(),
                        messages: Vec::new(),
                        last_conversation_seq: max_seq.max(0) as u64,
                        last_message_at: bootstrap.updated_at,
                        unread_count: (bootstrap.unread_count as i32).max(0),
                        last_read_seq: bootstrap.last_read_seq,
                        summary: None,
                    },
                    bootstrap: bootstrap.clone(),
                }
            })
            .collect();

        // 消息只为最终页内会话查询（分页裁剪后）：批量窗口一次 RPC 取整页
        //（替代逐会话 buffered(8) 的 N 次存储往返）。
        if message_limit > 0 {
            let mut index_by_id: HashMap<String, usize> = page
                .iter()
                .enumerate()
                .map(|(idx, m)| (m.row.conversation_id.clone(), idx))
                .collect();
            let query_targets: Vec<(String, i64)> = page
                .iter()
                .filter(|m| m.bootstrap.max_conversation_seq as i64 > 0)
                .map(|m| (m.row.conversation_id.clone(), 0))
                .collect();
            if !query_targets.is_empty() {
                trace!(
                    user_id = %user_id,
                    conversations = query_targets.len(),
                    limit = message_limit,
                    "querying message windows for snapshot page"
                );
                let windows = self
                    .infra
                    .query_conversations_message_windows(
                        ctx,
                        &query_targets,
                        message_limit,
                        true,
                        user_id,
                    )
                    .await?;
                for (conversation_id, messages, last_seq) in windows {
                    let Some(idx) = index_by_id.remove(&conversation_id) else {
                        continue;
                    };
                    let m = &mut page[idx];
                    m.row.messages = messages;
                    if last_seq > 0 {
                        m.row.last_conversation_seq = last_seq as u64;
                    }
                    if m.row.last_message_at <= 0 {
                        m.row.last_message_at = m
                            .row
                            .messages
                            .iter()
                            .map(|msg| msg.created_at)
                            .max()
                            .unwrap_or_default();
                    }
                }
            }
        }

        let mut routing = Vec::with_capacity(page.len());
        let conversations: Vec<SnapshotConversationRow> = page
            .into_iter()
            .map(|m| {
                let hint = ConversationSyncRoutingHint {
                    channel_id: m.bootstrap.channel_id.clone(),
                    conversation_type: conversation_type_from_summary(
                        &m.bootstrap.conversation_type,
                    ),
                    peer_read_seq: m
                        .bootstrap
                        .attributes
                        .get("peer_read_seq")
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or_default(),
                    participant_version: m.bootstrap.participant_version,
                    member_preview: m.bootstrap.member_preview.clone(),
                    is_muted: m.bootstrap.is_muted,
                    is_pinned: m.bootstrap.is_pinned,
                    is_archived: m.bootstrap.is_archived,
                    user_settings_version: m.bootstrap.user_settings_version,
                    draft: m.bootstrap.draft.clone(),
                    visible_after_conversation_seq: m.bootstrap.visible_after_conversation_seq,
                };
                let mut row = m.row;
                if req.include_conversations {
                    // I6 冷启 bundle：行内内嵌完整摘要，客户端一次 RPC 同得摘要+首页消息。
                    row.summary = Some(snapshot_row_to_summary(&row, &hint));
                }
                routing.push(hint);
                row
            })
            .collect();

        let snapshot_version = snapshot_global_seq(&conversations);
        let snapshot_at = conversations
            .iter()
            .map(|i| i.last_message_at)
            .max()
            .unwrap_or_default();

        debug!(
            user_id = %user_id,
            page_conversation_count = conversations.len(),
            snapshot_version,
            has_more,
            next_cursor = %next_cursor,
            elapsed_ms = started.elapsed().as_millis(),
            "sync snapshot response prepared"
        );

        Ok(SyncSnapshotOutcome {
            res: SyncSnapshotSyncRes {
                conversations,
                snapshot_version: snapshot_version.max(0) as u64,
                snapshot_at,
                next_cursor,
                has_more,
            },
            routing,
        })
    }

    async fn single_conversation_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: SingleConversationSync,
    ) -> Result<SingleConversationSyncRes, FlareError> {
        let conversation_id = req.conversation_id;
        require_nonempty_conversation_id(&conversation_id)?;
        let limit = req.limit.clamp(1, 500);
        let requested_after_seq = req.after_conversation_seq;
        // 历史回溯：方向由 proto 显式字段表达（替代旧 "before:" 字符串游标约定）。
        if req.before_conversation_seq > 0 {
            let before_seq = req.before_conversation_seq;
            let (mut messages, _storage_last_seq) = self
                .infra
                .query_messages_by_seq(
                    ctx,
                    &conversation_id,
                    0,
                    before_seq as i64,
                    limit + 1,
                    user_id,
                )
                .await?;
            let has_more = messages.len() as i32 > limit;
            if has_more {
                let limit = limit as usize;
                let overflow = messages.len().saturating_sub(limit);
                messages.drain(0..overflow);
            }
            let page = build_backfill_sync_items(messages, has_more)?;
            return Ok(SingleConversationSyncRes {
                conversation_id,
                items: page.items,
                max_conversation_seq: page.max_seq,
                next_cursor: page.next_cursor,
                has_more: page.has_more,
                hints: None,
                stale: None,
            });
        }
        let head_max_seq = self
            .conversation_head_max_seq(ctx, &conversation_id, requested_after_seq as i64)
            .await;
        let cold_start_tail_after_seq = cold_start_tail_after_seq(
            requested_after_seq,
            req.cursor.as_str(),
            head_max_seq,
            limit,
        );
        let query_after_seq = cold_start_tail_after_seq.unwrap_or(requested_after_seq);
        // `after_conversation_seq`：客户端本地已应用的最后 conversation_seq。
        let (messages, _storage_last_seq) = self
            .infra
            .query_messages_by_seq(
                ctx,
                &conversation_id,
                query_after_seq as i64,
                0,
                limit + 1,
                user_id,
            )
            .await?;
        let page = if cold_start_tail_after_seq.is_some() {
            build_tail_sync_items(messages, head_max_seq as u64)?
        } else {
            build_contiguous_sync_items(
                &conversation_id,
                requested_after_seq,
                limit as usize,
                messages,
                head_max_seq as u64,
            )?
        };
        Ok(SingleConversationSyncRes {
            conversation_id,
            items: page.items,
            max_conversation_seq: page.max_seq,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
            hints: None,
            stale: None,
        })
    }

    async fn conversation_head_max_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        fallback_seq: i64,
    ) -> i64 {
        match self
            .infra
            .get_conversation_message_head(ctx, conversation_id)
            .await
        {
            Ok(head) => head.max_seq.max(fallback_seq).max(0),
            Err(error) => {
                warn!(
                    conversation_id = %conversation_id,
                    error = %error,
                    "failed to load conversation message head; fallback to requested seq"
                );
                fallback_seq.max(0)
            }
        }
    }

    async fn multi_conversation_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: MultiConversationSync,
    ) -> Result<MultiConversationSyncRes, FlareError> {
        let limit = req.limit_per_conversation.clamp(1, 500);
        let targets: Vec<(String, i64)> = req
            .conversation_ids
            .iter()
            .filter(|cid| !cid.trim().is_empty())
            .map(|cid| {
                let after = req
                    .last_conversation_seq_per_conversation
                    .get(cid)
                    .copied()
                    .unwrap_or(0) as i64;
                (cid.clone(), after)
            })
            .collect();
        // 消息批量窗口：一次存储 RPC 取全部会话增量页（limit+1 探测 has_more）；
        // 水位 head 仍逐会话（单行点查），保序有界并发。
        let windows = self
            .infra
            .query_conversations_message_windows(ctx, &targets, limit + 1, false, user_id)
            .await?;
        let after_by_id: HashMap<&str, i64> = targets
            .iter()
            .map(|(cid, after)| (cid.as_str(), *after))
            .collect();
        let query_results: Vec<(String, ContiguousSyncPage)> =
            futures::stream::iter(windows.into_iter().map(|(cid, messages, _last_seq)| {
                let after = after_by_id.get(cid.as_str()).copied().unwrap_or(0);
                async move {
                    let head_max_seq = self.conversation_head_max_seq(ctx, &cid, after).await;
                    let page = build_contiguous_sync_items(
                        &cid,
                        after as u64,
                        limit as usize,
                        messages,
                        head_max_seq as u64,
                    )?;
                    Ok::<_, FlareError>((cid, page))
                }
            }))
            .buffered(MULTI_SYNC_QUERY_CONCURRENCY)
            .try_collect()
            .await?;

        let mut slices = Vec::with_capacity(query_results.len());
        let mut max_seq_per_conversation = HashMap::new();
        let mut has_more = false;
        for (cid, page) in query_results {
            let slice_has_more = page.has_more;
            if page.has_more {
                has_more = true;
            }
            let max_seq = page.max_seq;
            max_seq_per_conversation.insert(cid.clone(), max_seq);
            slices.push(flare_proto::common::ConversationSyncSlice {
                conversation_id: cid,
                items: page.items,
                max_conversation_seq: max_seq,
                next_cursor: page.next_cursor,
                has_more: slice_has_more,
            });
        }

        Ok(MultiConversationSyncRes {
            slices,
            max_conversation_seq_per_conversation: max_seq_per_conversation,
            has_more,
            hints: None,
        })
    }

    async fn conversations_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationsSync,
    ) -> Result<ConversationsSyncRes, FlareError> {
        let limit = req.limit.max(1);
        let client_cursor = req.cursor.trim().to_string();
        let is_cold_start = client_cursor.is_empty();
        let snap_req = SyncSnapshotSync {
            conversation_ids: Vec::new(),
            // 列表同步只需最新 1 条做预览/身份合并——旧值 `limit`(列表页大小) 会对页内每会话
            // 拉整页消息再丢弃（J1 消灭的 DB 放大之一）。
            messages_per_conversation: 1,
            include_deleted: req.include_deleted,
            include_conversations: true,
            snapshot_cursor: if is_cold_start {
                String::new()
            } else {
                client_cursor.clone()
            },
            newest_first: false,
            conversation_page_limit: limit,
        };
        let outcome = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let response = outcome.res;
        let conversations = response
            .conversations
            .iter()
            .zip(outcome.routing.iter())
            .filter(|(c, _)| valid_sync_conversation_id(&c.conversation_id))
            .map(|(c, hint)| snapshot_row_to_summary(c, hint))
            .collect::<Vec<_>>();
        let next_cursor = if !response.next_cursor.is_empty() {
            response.next_cursor
        } else if is_cold_start {
            String::new()
        } else {
            client_cursor
        };

        Ok(ConversationsSyncRes {
            conversations,
            next_cursor,
            has_more: response.has_more,
            hints: None,
        })
    }

    async fn conversation_participants_sync(
        &self,
        ctx: &Ctx,
        req: ConversationParticipantsSync,
    ) -> Result<ConversationParticipantsSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        let cursor_empty = req.cursor.trim().is_empty();
        let page = self
            .infra
            .list_conversation_participants(
                ctx,
                ListConversationParticipantsRequest {
                    conversation_id: req.conversation_id.clone(),
                    cursor: req.cursor,
                    limit: req.limit,
                    include_removed: req.include_removed,
                    ..Default::default()
                },
            )
            .await?;
        if req.known_participant_version > 0
            && req.known_participant_version == page.participant_version
            && cursor_empty
        {
            return Ok(ConversationParticipantsSyncRes {
                conversation_id: req.conversation_id,
                participant_version: page.participant_version,
                member_count: page.member_count,
                ..Default::default()
            });
        }
        Ok(ConversationParticipantsSyncRes {
            conversation_id: req.conversation_id,
            participants: page.participants,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
            participant_version: page.participant_version,
            member_count: page.member_count,
        })
    }

    /// 显式建群：客户端 get_group_by_user_ids 一次性把成员表交给服务端建群(幂等,按 attributes 携带的
    /// conversation_id 建)。建成后消息不再携带整张成员表——超大群建群不再受 NATS 单消息上限约束。
    async fn ensure_conversation_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: EnsureConversationSync,
    ) -> Result<EnsureConversationSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;

        // 成员去重 + 确保发起者在内。
        let mut members: Vec<String> = req
            .member_ids
            .into_iter()
            .filter(|m| !m.trim().is_empty())
            .collect();
        if !user_id.trim().is_empty() {
            members.push(user_id.to_string());
        }
        members.sort();
        members.dedup();

        let member_count = members.len() as u64;
        // 建群约定（attributes 携带 conversation_id 等）唯一实现在 flare-grpc-proto。
        let request = flare_grpc_proto::ensure_conversation_request(
            &req.conversation_id,
            req.conversation_type,
            req.business_type,
            members,
            req.channel_id,
        );

        match self.infra.create_conversation(ctx, request).await {
            Ok(_) => Ok(EnsureConversationSyncRes {
                conversation_id: req.conversation_id,
                ok: true,
                error: String::new(),
                member_count,
            }),
            // 失败返回 ok=false(非 Err):客户端据此回退到"首条消息携带成员表"的兜底建群。
            Err(error) => {
                warn!(conversation_id = %req.conversation_id, %error, "ensure_conversation_sync failed");
                Ok(EnsureConversationSyncRes {
                    conversation_id: req.conversation_id,
                    ok: false,
                    error: error.to_string(),
                    member_count: 0,
                })
            }
        }
    }

    async fn conversation_detail_sync(
        &self,
        ctx: &Ctx,
        _user_id: &str,
        req: ConversationDetailSync,
    ) -> Result<ConversationDetailSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        let detail = self
            .infra
            .conversation_detail(
                ctx,
                flare_grpc_proto::conversation::GetConversationDetailRequest {
                    conversation_id: req.conversation_id,
                },
            )
            .await?;
        Ok(ConversationDetailSyncRes {
            detail: detail.detail,
            ..Default::default()
        })
    }

    async fn query_events_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        mut req: QueryEventsSync,
    ) -> Result<QueryEventsSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        let unfiltered_replay_requested = req.event_types.is_empty()
            && req.replay_preset == EventReplayPreset::AllPersisted as i32;
        if !unfiltered_replay_requested {
            normalize_query_event_types(&mut req.event_types);
        }
        let limit = clamp_query_events_limit(req.limit);
        let max_seq_hint = None;
        let intent = SyncIntent::from_event_anchor(req.after_conversation_seq as i64, max_seq_hint);
        debug!(
            user_id = %user_id,
            conversation_id = %req.conversation_id,
            after_seq = req.after_conversation_seq,
            before_seq = req.before_conversation_seq,
            limit,
            event_type_count = req.event_types.len(),
            include_deleted = req.include_deleted,
            ?intent,
            "query_events (离线追赶 / 增量关键事件)"
        );

        let page = self
            .infra
            .query_events_page(
                ctx,
                &req.conversation_id,
                req.after_conversation_seq as i64,
                req.before_conversation_seq as i64,
                limit,
                &req.event_types,
                req.include_deleted,
            )
            .await?;

        let mut events = page.events;
        filter_events_by_types(&mut events, &req.event_types);

        let last_seq = if page.last_seq > 0 {
            page.last_seq
        } else {
            max_seq_from_events(&events)
        };
        let min_seq = events
            .iter()
            .map(|event| event.conversation_seq)
            .min()
            .unwrap_or(0);
        if unfiltered_replay_requested
            && let Some(gap) = detect_event_replay_gap(req.after_conversation_seq, &events)
        {
            let server_replay_max_seq = last_seq.max(gap.observed_seq as i64).max(0) as u64;
            let (hints, stale) = event_replay_gap_repair_context(
                &req.conversation_id,
                req.after_conversation_seq,
                server_replay_max_seq,
                gap,
            );
            warn!(
                conversation_id = %req.conversation_id,
                after_seq = req.after_conversation_seq,
                expected_seq = gap.expected_seq,
                observed_seq = gap.observed_seq,
                server_replay_max_seq,
                "event replay gap detected; returning stale context for conversation resync"
            );
            return Ok(QueryEventsSyncRes {
                envelope: Some(EventEnvelope {
                    events: Vec::new(),
                    max_conversation_seq: req.after_conversation_seq,
                    has_more: false,
                    next_cursor: String::new(),
                    window_id: String::new(),
                    delivery_mode: EventEnvelopeDeliveryMode::Inline as i32,
                    conversation_id: req.conversation_id,
                    min_conversation_seq: req.after_conversation_seq,
                    inline_events_truncated: false,
                    attributes: Default::default(),
                }),
                hints: Some(hints),
                stale: Some(stale),
            });
        }

        Ok(QueryEventsSyncRes {
            envelope: Some(EventEnvelope {
                events,
                max_conversation_seq: last_seq.max(0) as u64,
                has_more: page.has_more,
                next_cursor: page.next_cursor,
                window_id: String::new(),
                delivery_mode: EventEnvelopeDeliveryMode::Inline as i32,
                conversation_id: req.conversation_id,
                min_conversation_seq: min_seq,
                inline_events_truncated: false,
                attributes: Default::default(),
            }),
            hints: None,
            stale: None,
        })
    }

    async fn get_sync_cursor_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: GetSyncCursorSync,
    ) -> Result<GetSyncCursorSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        let hints = self
            .conversation_version_hints(ctx, user_id, &req.known_conversation_versions)
            .await;
        if let Some(cursor) = self.cursor_cache.get(user_id, &req.conversation_id).await {
            debug!(
                user_id = %user_id,
                conversation_id = %req.conversation_id,
                "get sync cursor hit (L1)"
            );
            return Ok(GetSyncCursorSyncRes {
                cursor: Some(cursor),
                hints,
            });
        }
        debug!(
            user_id = %user_id,
            conversation_id = %req.conversation_id,
            "get sync cursor miss (L1)"
        );

        let conv_resp = self
            .infra
            .conversation_bootstrap(
                ctx,
                ConversationBootstrapRequest {
                    client_cursor_map: Default::default(),
                    include_recent_messages: false,
                    recent_message_limit: 0,
                    device_id: req.device_id.clone(),
                    device_platform: String::new(),
                    // 游标回填需要目标会话必在集合内 → 全量、放开截断上限。
                    updated_after_ms: 0,
                    max_conversations: SNAPSHOT_BOOTSTRAP_MAX_CONVERSATIONS,
                },
            )
            .await?;
        let summary = conv_resp
            .conversations
            .iter()
            .find(|c| c.conversation_id == req.conversation_id);
        let last_sync_seq = conv_resp
            .server_cursor_map
            .get(&req.conversation_id)
            .copied()
            .or_else(|| summary.map(|s| s.max_conversation_seq as i64))
            .filter(|seq| *seq > 0)
            .map(|seq| seq.max(0) as u64);
        let Some(last_sync_seq) = last_sync_seq else {
            return Ok(GetSyncCursorSyncRes {
                cursor: None,
                hints,
            });
        };
        let last_read_seq = summary
            .map(|s| {
                normalize_participant_read_seq(
                    s.max_conversation_seq,
                    s.unread_count,
                    s.last_read_seq,
                )
            })
            .unwrap_or(0);
        let cursor = MultiDeviceCursor {
            device_id: req.device_id,
            conversation_id: req.conversation_id,
            last_conversation_seq: last_sync_seq,
            last_sync_at: summary.map(|s| s.updated_at).unwrap_or_default(),
            last_read_seq,
            last_message_seq: 0,
        };
        self.cursor_cache.put(user_id, cursor.clone()).await;
        debug!(
            user_id = %user_id,
            conversation_id = %cursor.conversation_id,
            last_sync_seq = cursor.last_conversation_seq,
            last_read_seq = cursor.last_read_seq,
            "get sync cursor hit (persistent)"
        );
        Ok(GetSyncCursorSyncRes {
            cursor: Some(cursor),
            hints,
        })
    }

    async fn conversation_version_hints(
        &self,
        ctx: &Ctx,
        user_id: &str,
        known_versions: &[ConversationVersion],
    ) -> Option<SyncSessionHints> {
        let known = known_versions
            .iter()
            .map(|version| (version.conversation_id.clone(), version.version))
            .filter(|(conversation_id, _)| !conversation_id.trim().is_empty())
            .collect::<Vec<_>>();
        if known.is_empty() {
            return None;
        }

        match self
            .infra
            .diff_known_conversation_versions(ctx, &known)
            .await
        {
            Ok(changes) if changes.is_empty() => None,
            Ok(changes) => Some(SyncSessionHints {
                conversation_versions: changes
                    .into_iter()
                    .map(|change| ConversationVersion {
                        conversation_id: change.conversation_id,
                        version: change.version,
                        max_conversation_seq: change.max_conversation_seq,
                        updated_at: change.updated_at_ms,
                    })
                    .collect(),
                ..Default::default()
            }),
            Err(error) => {
                warn!(
                    user_id = %user_id,
                    known_version_count = known.len(),
                    error = %error,
                    "failed to build conversation version sync hints"
                );
                None
            }
        }
    }

    async fn update_sync_cursor_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: UpdateSyncCursorSync,
    ) -> Result<UpdateSyncCursorSyncRes, FlareError> {
        let cursor = req.cursor.as_ref().ok_or_else(|| {
            FlareError::localized(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "cursor is required",
            )
        })?;
        require_nonempty_conversation_id(&cursor.conversation_id)?;
        debug!(
            user_id = %user_id,
            conversation_id = %cursor.conversation_id,
            last_sync_seq = cursor.last_conversation_seq,
            "update_sync_cursor (命令)"
        );

        let previous_cursor = self
            .cursor_cache
            .get(user_id, &cursor.conversation_id)
            .await;
        let merged_seq = crate::domain::service::merge_cursor_monotonic(
            previous_cursor
                .as_ref()
                .map(|cursor| cursor.last_conversation_seq as i64),
            cursor.last_conversation_seq as i64,
        ) as u64;
        let merged_read_seq = cursor.last_read_seq.max(
            previous_cursor
                .as_ref()
                .map(|cursor| cursor.last_read_seq)
                .unwrap_or(0),
        );
        let merged_message_seq = cursor.last_message_seq.max(
            previous_cursor
                .as_ref()
                .map(|cursor| cursor.last_message_seq)
                .unwrap_or(0),
        );
        let merged_sync_at = cursor.last_sync_at.max(
            previous_cursor
                .as_ref()
                .map(|cursor| cursor.last_sync_at)
                .unwrap_or(0),
        );

        self.infra
            .update_sync_cursor(
                ctx,
                UpdateCursorRequest {
                    conversation_id: cursor.conversation_id.clone(),
                    sync_seq: merged_seq as i64,
                    device_id: cursor.device_id.clone(),
                },
            )
            .await?;

        let out = MultiDeviceCursor {
            device_id: cursor.device_id.clone(),
            conversation_id: cursor.conversation_id.clone(),
            last_conversation_seq: merged_seq,
            last_sync_at: merged_sync_at,
            last_read_seq: merged_read_seq,
            last_message_seq: merged_message_seq,
        };
        self.cursor_cache.put(user_id, out.clone()).await;

        Ok(UpdateSyncCursorSyncRes {
            cursor: Some(out),
            hints: None,
        })
    }

    async fn conversation_user_settings_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationUserSettingsSync,
    ) -> Result<ConversationUserSettingsSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        debug!(
            user_id = %user_id,
            conversation_id = %req.conversation_id,
            base_settings_version = req.base_settings_version,
            "conversation_user_settings sync"
        );
        let resp = self
            .infra
            .update_conversation_user_settings(
                ctx,
                UpdateConversationUserSettingsRequest {
                    conversation_id: req.conversation_id,
                    is_pinned: req.is_pinned,
                    is_muted: req.is_muted,
                    is_archived: req.is_archived,
                    draft: req.draft,
                    base_settings_version: req.base_settings_version,
                },
            )
            .await?;
        Ok(ConversationUserSettingsSyncRes {
            settings: resp.settings,
        })
    }
}

fn message_to_sync_item(message: &Message) -> Result<SyncSliceItem, FlareError> {
    let skip_reason = message
        .attributes
        .get("__sync_skip")
        .cloned()
        .unwrap_or_default();
    if !skip_reason.trim().is_empty() {
        return Ok(SyncSliceItem {
            conversation_seq: message.conversation_seq,
            created_at: message.created_at,
            payload: Some(SyncSlicePayload::Skip(SyncSkipItem {
                reason: skip_reason,
            })),
        });
    }

    Ok(SyncSliceItem {
        conversation_seq: message.conversation_seq,
        created_at: message.created_at,
        payload: Some(SyncSlicePayload::Message(message.clone())),
    })
}

struct ContiguousSyncPage {
    items: Vec<SyncSliceItem>,
    max_seq: u64,
    next_cursor: String,
    has_more: bool,
}

fn cold_start_tail_after_seq(
    requested_after_seq: u64,
    cursor: &str,
    remote_max_seq: i64,
    limit: i32,
) -> Option<u64> {
    if requested_after_seq != 0 || !cursor.trim().is_empty() {
        return None;
    }
    let limit = limit.max(1) as u64;
    let remote_max_seq = remote_max_seq.max(0) as u64;
    if remote_max_seq <= limit {
        return None;
    }
    Some(remote_max_seq.saturating_sub(limit))
}

fn build_backfill_sync_items(
    messages: Vec<Message>,
    has_more: bool,
) -> Result<ContiguousSyncPage, FlareError> {
    let mut items = Vec::with_capacity(messages.len());
    for message in messages {
        items.push(message_to_sync_item(&message)?);
    }
    // 回溯分页由客户端驱动（下一页 before = 本地已加载的最小 seq），无需服务端续拉游标。
    Ok(ContiguousSyncPage {
        items,
        max_seq: 0,
        next_cursor: String::new(),
        has_more,
    })
}

fn build_tail_sync_items(
    messages: Vec<Message>,
    remote_max_seq: u64,
) -> Result<ContiguousSyncPage, FlareError> {
    let mut items = Vec::with_capacity(messages.len());
    let mut max_message_seq = 0_u64;
    let mut last_real_message_id = String::new();
    for message in messages {
        max_message_seq = max_message_seq.max(message.conversation_seq);
        last_real_message_id = message.server_id.clone();
        items.push(message_to_sync_item(&message)?);
    }

    let max_seq = remote_max_seq.max(max_message_seq);
    let next_cursor = if max_seq > 0 {
        if last_real_message_id.is_empty() {
            format!("seq:{max_seq}")
        } else {
            format!("seq:{max_seq}:{last_real_message_id}")
        }
    } else {
        String::new()
    };

    Ok(ContiguousSyncPage {
        items,
        max_seq,
        next_cursor,
        has_more: false,
    })
}

fn build_contiguous_sync_items(
    conversation_id: &str,
    after_seq: u64,
    limit: usize,
    messages: Vec<Message>,
    remote_max_seq: u64,
) -> Result<ContiguousSyncPage, FlareError> {
    let limit = limit.max(1);
    let max_message_seq = messages
        .iter()
        .map(|message| message.conversation_seq)
        .max()
        .unwrap_or(after_seq);
    let authoritative_max_seq = remote_max_seq.max(max_message_seq).max(after_seq);
    if authoritative_max_seq <= after_seq {
        return Ok(ContiguousSyncPage {
            items: Vec::new(),
            max_seq: after_seq,
            next_cursor: String::new(),
            has_more: false,
        });
    }

    let page_end_seq = authoritative_max_seq.min(after_seq.saturating_add(limit as u64));
    let mut message_by_seq = BTreeMap::new();
    for message in messages {
        if message.conversation_seq <= after_seq || message.conversation_seq > page_end_seq {
            continue;
        }
        let seq = message.conversation_seq;
        if message_by_seq.insert(seq, message).is_some() {
            warn!(
                conversation_id = %conversation_id,
                seq,
                "duplicate message seq found while building sync page; latest row kept"
            );
        }
    }

    let mut items = Vec::with_capacity((page_end_seq - after_seq) as usize);
    let mut last_real_message_id = String::new();
    let mut missing_count = 0_u64;
    let mut first_missing_seq = 0_u64;
    let mut last_missing_seq = 0_u64;
    for seq in (after_seq + 1)..=page_end_seq {
        if let Some(message) = message_by_seq.remove(&seq) {
            last_real_message_id = message.server_id.clone();
            items.push(message_to_sync_item(&message)?);
        } else {
            missing_count += 1;
            if first_missing_seq == 0 {
                first_missing_seq = seq;
            }
            last_missing_seq = seq;
            items.push(SyncSliceItem {
                conversation_seq: seq,
                created_at: 0,
                payload: Some(SyncSlicePayload::Tombstone(SyncTombstoneItem {
                    tombstone_id: format!("{conversation_id}:{seq}"),
                    reason: Some("storage_missing_committed_seq".to_string()),
                })),
            });
        }
    }
    if missing_count > 0 {
        warn!(
            conversation_id = %conversation_id,
            after_seq,
            page_end_seq,
            remote_max_seq = authoritative_max_seq,
            missing_count,
            first_missing_seq,
            last_missing_seq,
            "message rows missing inside committed seq range; emitted tombstones to keep sync cursor continuous"
        );
    }

    let next_cursor = if page_end_seq > after_seq {
        if last_real_message_id.is_empty() {
            format!("seq:{page_end_seq}")
        } else {
            format!("seq:{page_end_seq}:{last_real_message_id}")
        }
    } else {
        String::new()
    };

    Ok(ContiguousSyncPage {
        items,
        max_seq: page_end_seq,
        next_cursor,
        has_more: page_end_seq < authoritative_max_seq,
    })
}

fn latest_message(item: &SnapshotConversationRow) -> Option<&Message> {
    item.messages.iter().max_by_key(|m| m.conversation_seq)
}

fn conversation_type_int(message: Option<&Message>) -> i32 {
    let Some(msg) = message else {
        return 0;
    };
    msg.conversation_type
}

fn conversation_type_from_summary(value: &str) -> i32 {
    match value.trim() {
        "single" => ProtoConversationType::Single as i32,
        "group" => ProtoConversationType::Group as i32,
        "ai" => ProtoConversationType::Ai as i32,
        "system" => ProtoConversationType::System as i32,
        "customer" => ProtoConversationType::Customer as i32,
        "temp" => ProtoConversationType::Temp as i32,
        "channel" => ProtoConversationType::Channel as i32,
        "broadcast" => ProtoConversationType::Broadcast as i32,
        "unspecified" | "" => ProtoConversationType::Unspecified as i32,
        _ => ProtoConversationType::Unspecified as i32,
    }
}

fn conversation_type_label(value: i32) -> &'static str {
    match ProtoConversationType::try_from(value).ok() {
        Some(ProtoConversationType::Single) => "single",
        Some(ProtoConversationType::Group) => "group",
        Some(ProtoConversationType::Ai) => "ai",
        Some(ProtoConversationType::System) => "system",
        Some(ProtoConversationType::Customer) => "customer",
        Some(ProtoConversationType::Temp) => "temp",
        Some(ProtoConversationType::Channel) => "channel",
        Some(ProtoConversationType::Broadcast) => "broadcast",
        _ => "unspecified",
    }
}

/// 同步补丁摘要 `channel_id`。
///
/// 单聊的对端路由以 Conversation Bootstrap 从成员表解析出的 channel_id 为准；
/// 最新消息体里的 channel_id 可能来自历史坏数据或旧客户端，不能覆盖它。
fn merge_sync_summary_channel_id(
    conversation_type: i32,
    from_message: &str,
    hint: &ConversationSyncRoutingHint,
) -> String {
    if conversation_type == flare_proto::common::ConversationType::Single as i32
        && !hint.channel_id.is_empty()
    {
        return hint.channel_id.clone();
    }
    if !from_message.is_empty() {
        return from_message.to_string();
    }
    hint.channel_id.clone()
}

fn merge_sync_summary_conversation_type(from_message: i32, hint: i32) -> i32 {
    if from_message > 0 {
        return from_message;
    }
    hint
}

/// 从消息 content 派生会话列表预览文本:文本类取正文,媒体/卡片等取占位标签。
/// 供 `message_preview` 在 `attributes["text_preview"]` 缺失时兜底(历史消息普遍未写该属性)。
fn preview_text_from_content(content: Option<&flare_proto::common::MessageContent>) -> String {
    use flare_proto::common::message_content::Content;
    let Some(mc) = content else {
        return String::new();
    };
    match &mc.content {
        Some(Content::Text(t)) => t.text.clone(),
        Some(Content::RichText(_)) => "[富文本]".to_string(),
        Some(Content::Image(_)) | Some(Content::ImageGroup(_)) => "[图片]".to_string(),
        Some(Content::Video(_)) => "[视频]".to_string(),
        Some(Content::Audio(_)) => "[语音]".to_string(),
        Some(Content::File(_)) => "[文件]".to_string(),
        Some(Content::Location(_)) => "[位置]".to_string(),
        Some(Content::Sticker(_)) | Some(Content::Emoji(_)) => "[表情]".to_string(),
        Some(Content::Card(_)) | Some(Content::AppCard(_)) | Some(Content::LinkCard(_)) => {
            "[卡片]".to_string()
        }
        Some(Content::Quote(_)) => "[引用]".to_string(),
        Some(Content::Forward(_)) => "[转发]".to_string(),
        Some(Content::Notification(_)) | Some(Content::System(_)) => "[系统消息]".to_string(),
        Some(_) => "[消息]".to_string(),
        None => String::new(),
    }
}

fn message_preview(message: Option<&Message>, sent_at: i64) -> Option<MessagePreview> {
    message.map(|m| {
        // 优先 attributes["text_preview"](写入方已算好);缺失/空时从 content 派生。
        let text = m
            .attributes
            .get("text_preview")
            .filter(|t| !t.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| preview_text_from_content(m.content.as_ref()));
        MessagePreview {
            message_id: m.server_id.clone(),
            sender_id: m.sender_id.clone(),
            r#type: m.message_type,
            text,
            created_at: sent_at,
        }
    })
}

fn normalize_participant_read_seq(_max_seq: u64, _unread_count: u32, last_read_seq: u64) -> u64 {
    last_read_seq
}

fn snapshot_row_to_summary(
    item: &SnapshotConversationRow,
    hint: &ConversationSyncRoutingHint,
) -> ConversationSummary {
    let latest = latest_message(item);
    let mut ext = HashMap::new();
    if let Some(msg) = latest {
        ext.extend(msg.attributes.clone());
    }
    let max_seq = item.last_conversation_seq;
    let peer_read_seq = if hint.peer_read_seq <= max_seq {
        hint.peer_read_seq
    } else {
        tracing::warn!(
            conversation_id = %item.conversation_id,
            peer_read_seq = hint.peer_read_seq,
            max_seq,
            "drop impossible peer_read_seq from conversation summary"
        );
        0
    };
    ext.insert("peer_read_seq".to_string(), peer_read_seq.to_string());
    let display_name = ext.get("display_name").cloned().unwrap_or_default();
    let avatar_url = ext.get("avatar_url").cloned().unwrap_or_default();
    let type_from_msg = conversation_type_int(latest);
    let conversation_type =
        merge_sync_summary_conversation_type(type_from_msg, hint.conversation_type);
    let channel_from_msg = latest.map(|m| m.channel_id.as_str()).unwrap_or_default();
    let channel_id = merge_sync_summary_channel_id(conversation_type, channel_from_msg, hint);
    let visible_after_seq = hint.visible_after_conversation_seq;
    let last_read_seq = normalize_participant_read_seq(
        max_seq,
        item.unread_count.max(0) as u32,
        item.last_read_seq,
    )
    .max(visible_after_seq);
    let last_message = if visible_after_seq > 0 && max_seq <= visible_after_seq {
        None
    } else {
        message_preview(latest, item.last_message_at)
    };
    let unread_count = if visible_after_seq > 0 && max_seq <= visible_after_seq {
        0
    } else {
        item.unread_count as u32
    };
    ConversationSummary {
        conversation_id: item.conversation_id.clone(),
        conversation_type: conversation_type_label(conversation_type).to_string(),
        display_name,
        avatar_url,
        last_message,
        unread_count,
        max_conversation_seq: max_seq,
        last_read_seq,
        is_muted: hint.is_muted,
        is_pinned: hint.is_pinned,
        mute_until: None,
        is_archived: hint.is_archived,
        user_settings_version: hint.user_settings_version,
        draft: hint.draft.clone(),
        visible_after_conversation_seq: visible_after_seq,
        updated_at: item.last_message_at,
        created_at: 0,
        labels: Vec::new(),
        member_count: ext
            .get("member_count")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0),
        channel_id,
        participant_version: hint.participant_version,
        member_preview: if conversation_type == flare_proto::common::ConversationType::Single as i32
        {
            Vec::new()
        } else {
            hint.member_preview.clone()
        },
        attributes: ext,
    }
}

fn filter_events_by_types(events: &mut Vec<flare_proto::common::Event>, allowed: &[i32]) {
    if allowed.is_empty() {
        return;
    }
    let set: HashSet<i32> = allowed.iter().copied().collect();
    events.retain(|e| set.contains(&e.r#type));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EventReplayGap {
    expected_seq: u64,
    observed_seq: u64,
}

fn detect_event_replay_gap(
    after_seq: u64,
    events: &[flare_proto::common::Event],
) -> Option<EventReplayGap> {
    let mut expected_seq = after_seq.saturating_add(1);
    for event in events {
        let observed_seq = event.conversation_seq;
        if observed_seq <= after_seq || observed_seq < expected_seq {
            continue;
        }
        if observed_seq > expected_seq {
            return Some(EventReplayGap {
                expected_seq,
                observed_seq,
            });
        }
        expected_seq = expected_seq.saturating_add(1);
    }
    None
}

fn event_replay_gap_repair_context(
    conversation_id: &str,
    after_seq: u64,
    server_replay_max_seq: u64,
    gap: EventReplayGap,
) -> (SyncSessionHints, SyncStaleContext) {
    let mut localization_params = HashMap::new();
    localization_params.insert("expected_seq".to_string(), gap.expected_seq.to_string());
    localization_params.insert("observed_seq".to_string(), gap.observed_seq.to_string());

    (
        SyncSessionHints {
            recovery_hint: SyncRecoveryHint::ResyncConversation as i32,
            localization_key: "sync.events.gap_detected".to_string(),
            localization_params,
            server_replay_max_conversation_seq: server_replay_max_seq,
            ..Default::default()
        },
        SyncStaleContext {
            conversation_id: conversation_id.to_string(),
            client_reported_conversation_seq: after_seq,
            server_earliest_available_conversation_seq: gap.observed_seq,
            recommended_action: SyncRecoveryHint::ResyncConversation as i32,
        },
    )
}

fn valid_sync_conversation_id(conversation_id: &str) -> bool {
    let conversation_id = conversation_id.trim();
    !conversation_id.is_empty() && !conversation_id.starts_with("sync:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::conversation::{
        ConversationBootstrapResponse, UpdateConversationUserSettingsRequest,
    };
    use flare_server_core::context::Context;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockInfra {
        bootstrap: ConversationBootstrapResponse,
        messages: Vec<Message>,
        message_head: crate::application::ports::StorageConversationMessageHead,
        message_queries: Mutex<Vec<MessageQuery>>,
        events_page: crate::application::ports::QueryEventsPage,
        updates: Mutex<Vec<UpdateCursorRequest>>,
        settings_updates: Mutex<Vec<UpdateConversationUserSettingsRequest>>,
        version_changes: Vec<crate::application::ports::ConversationVersionChange>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct MessageQuery {
        conversation_id: String,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        user_id: String,
    }

    impl ConversationSyncPort for MockInfra {
        async fn conversation_bootstrap(
            &self,
            _ctx: &Ctx,
            _req: ConversationBootstrapRequest,
        ) -> Result<ConversationBootstrapResponse, FlareError> {
            Ok(self.bootstrap.clone())
        }

        async fn update_sync_cursor(
            &self,
            _ctx: &Ctx,
            req: UpdateCursorRequest,
        ) -> Result<(), FlareError> {
            self.updates.lock().expect("updates lock").push(req);
            Ok(())
        }

        async fn conversation_detail(
            &self,
            _ctx: &Ctx,
            req: flare_grpc_proto::conversation::GetConversationDetailRequest,
        ) -> Result<flare_grpc_proto::conversation::GetConversationDetailResponse, FlareError>
        {
            Ok(
                flare_grpc_proto::conversation::GetConversationDetailResponse {
                    detail: Some(flare_proto::common::ConversationDetail {
                        conversation_id: req.conversation_id,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        }

        async fn list_conversation_participants(
            &self,
            _ctx: &Ctx,
            req: ListConversationParticipantsRequest,
        ) -> Result<flare_grpc_proto::conversation::ListConversationParticipantsResponse, FlareError>
        {
            Ok(
                flare_grpc_proto::conversation::ListConversationParticipantsResponse {
                    conversation_id: req.conversation_id,
                    ..Default::default()
                },
            )
        }

        async fn update_conversation_user_settings(
            &self,
            _ctx: &Ctx,
            req: UpdateConversationUserSettingsRequest,
        ) -> Result<
            flare_grpc_proto::conversation::UpdateConversationUserSettingsResponse,
            FlareError,
        > {
            self.settings_updates
                .lock()
                .expect("settings updates lock")
                .push(req.clone());
            Ok(
                flare_grpc_proto::conversation::UpdateConversationUserSettingsResponse {
                    settings: Some(flare_proto::common::ConversationUserSettings {
                        is_pinned: req.is_pinned.unwrap_or_default(),
                        is_muted: req.is_muted.unwrap_or_default(),
                        is_archived: req.is_archived.unwrap_or_default(),
                        draft: req.draft.unwrap_or_default(),
                        settings_version: req.base_settings_version.saturating_add(1),
                        ..Default::default()
                    }),
                },
            )
        }

        async fn create_conversation(
            &self,
            _ctx: &Ctx,
            _req: flare_grpc_proto::conversation::CreateConversationRequest,
        ) -> Result<flare_grpc_proto::conversation::CreateConversationResponse, FlareError>
        {
            Ok(flare_grpc_proto::conversation::CreateConversationResponse { conversation: None })
        }
    }

    impl StorageReadPort for MockInfra {
        async fn query_messages_by_seq(
            &self,
            _ctx: &Ctx,
            conversation_id: &str,
            after_seq: i64,
            before_seq: i64,
            limit: i32,
            user_id: &str,
        ) -> Result<(Vec<Message>, i64), FlareError> {
            self.message_queries
                .lock()
                .expect("message queries lock")
                .push(MessageQuery {
                    conversation_id: conversation_id.to_string(),
                    after_seq,
                    before_seq,
                    limit,
                    user_id: user_id.to_string(),
                });
            let messages = self
                .messages
                .iter()
                .filter(|message| message.conversation_id == conversation_id)
                .filter(|message| message.conversation_seq as i64 > after_seq)
                .filter(|message| before_seq <= 0 || (message.conversation_seq as i64) < before_seq)
                .take(limit.max(0) as usize)
                .cloned()
                .collect::<Vec<_>>();
            let last_seq = messages
                .last()
                .map(|message| message.conversation_seq as i64)
                .unwrap_or(after_seq);
            Ok((messages, last_seq))
        }

        async fn get_conversation_message_head(
            &self,
            _ctx: &Ctx,
            _conversation_id: &str,
        ) -> Result<crate::application::ports::StorageConversationMessageHead, FlareError> {
            Ok(self.message_head.clone())
        }

        /// 测试替身的批量窗口显式实现（生产语义见 Postgres/LATERAL）：
        /// 逐会话循环的成本与语义在 mock 层可见，而不是藏在 trait 默认实现后面。
        async fn query_conversations_message_windows(
            &self,
            ctx: &Ctx,
            targets: &[(String, i64)],
            per_conversation_limit: i32,
            newest_window: bool,
            user_id: &str,
        ) -> Result<Vec<(String, Vec<Message>, i64)>, FlareError> {
            let mut windows = Vec::with_capacity(targets.len());
            for (conversation_id, after_seq) in targets {
                let after = if newest_window {
                    // mock 消息量小：最新窗口用「总量减 limit」近似 tail 截断。
                    let total = self
                        .messages
                        .iter()
                        .filter(|m| m.conversation_id == *conversation_id)
                        .count() as i64;
                    (total - per_conversation_limit as i64).max(0)
                } else {
                    *after_seq
                };
                let (messages, last_seq) = self
                    .query_messages_by_seq(
                        ctx,
                        conversation_id,
                        after,
                        0,
                        per_conversation_limit,
                        user_id,
                    )
                    .await?;
                windows.push((conversation_id.clone(), messages, last_seq));
            }
            Ok(windows)
        }
    }

    impl ConversationEventReadPort for MockInfra {
        async fn query_events_page(
            &self,
            _ctx: &Ctx,
            _conversation_id: &str,
            _after_seq: i64,
            _before_seq: i64,
            _limit: i32,
            _event_types: &[i32],
            _include_deleted: bool,
        ) -> Result<crate::application::ports::QueryEventsPage, FlareError> {
            Ok(self.events_page.clone())
        }
    }

    impl ConversationVersionIndexPort for MockInfra {
        async fn diff_known_conversation_versions(
            &self,
            _ctx: &Ctx,
            known: &[(String, u64)],
        ) -> Result<Vec<crate::application::ports::ConversationVersionChange>, FlareError> {
            Ok(self
                .version_changes
                .iter()
                .filter(|change| {
                    known
                        .iter()
                        .find(|(conversation_id, _)| conversation_id == &change.conversation_id)
                        .map(|(_, known_version)| change.version > *known_version)
                        .unwrap_or(false)
                })
                .cloned()
                .collect())
        }
    }

    fn ctx() -> Ctx {
        Arc::new(
            Context::root()
                .with_tenant_id("0")
                .with_user_id("22")
                .with_trace_id("test-trace"),
        )
    }

    fn sync_event(conversation_id: &str, seq: u64) -> flare_proto::common::Event {
        flare_proto::common::Event {
            conversation_id: conversation_id.to_string(),
            conversation_seq: seq,
            event_id: format!("{conversation_id}:{seq}"),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn single_conversation_cold_start_returns_sparse_tail_message() {
        let infra = Arc::new(MockInfra {
            messages: vec![Message {
                server_id: "m-tail".to_string(),
                conversation_id: "c1".to_string(),
                conversation_seq: 1_211_546,
                ..Default::default()
            }],
            message_head: crate::application::ports::StorageConversationMessageHead {
                max_seq: 1_211_546,
                last_message_id: "m-tail".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });
        let handler =
            SyncOrchestrationHandler::new(infra.clone(), Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .single_conversation_sync(
                &ctx(),
                "22",
                SingleConversationSync {
                    conversation_id: "c1".to_string(),
                    after_conversation_seq: 0,
                    limit: 200,
                    ..Default::default()
                },
            )
            .await
            .expect("single conversation sync");

        assert_eq!(response.max_conversation_seq, 1_211_546);
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].conversation_seq, 1_211_546);
        assert!(matches!(
            response.items[0].payload,
            Some(SyncSlicePayload::Message(_))
        ));
        let queries = infra.message_queries.lock().expect("message queries lock");
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].after_seq, 1_211_346);
        assert_eq!(queries[0].limit, 201);
    }

    #[test]
    fn cold_start_tail_after_seq_requires_blank_initial_cursor() {
        assert_eq!(cold_start_tail_after_seq(0, "", 1_000, 200), Some(800));
        assert_eq!(cold_start_tail_after_seq(0, "seq:200", 1_000, 200), None);
        assert_eq!(cold_start_tail_after_seq(20, "", 1_000, 200), None);
        assert_eq!(cold_start_tail_after_seq(0, "", 199, 200), None);
    }

    #[test]
    fn event_replay_gap_detection_requires_contiguous_unfiltered_events() {
        assert_eq!(
            detect_event_replay_gap(1, &[sync_event("c1", 2), sync_event("c1", 3)]),
            None
        );
        assert_eq!(
            detect_event_replay_gap(1, &[sync_event("c1", 3), sync_event("c1", 4)]),
            Some(EventReplayGap {
                expected_seq: 2,
                observed_seq: 3,
            })
        );
    }

    #[tokio::test]
    async fn query_events_sync_returns_stale_context_on_event_gap() {
        let infra = Arc::new(MockInfra {
            events_page: crate::application::ports::QueryEventsPage {
                events: vec![sync_event("c1", 3), sync_event("c1", 4)],
                last_seq: 4,
                has_more: false,
                next_cursor: "evt:4".to_string(),
            },
            ..Default::default()
        });
        let handler =
            SyncOrchestrationHandler::new(infra.clone(), Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .query_events_sync(
                &ctx(),
                "22",
                QueryEventsSync {
                    conversation_id: "c1".to_string(),
                    after_conversation_seq: 1,
                    limit: 100,
                    replay_preset: EventReplayPreset::AllPersisted as i32,
                    ..Default::default()
                },
            )
            .await
            .expect("query events sync");

        let stale = response.stale.expect("gap should return stale context");
        assert_eq!(stale.conversation_id, "c1");
        assert_eq!(stale.client_reported_conversation_seq, 1);
        assert_eq!(stale.server_earliest_available_conversation_seq, 3);
        assert_eq!(
            stale.recommended_action,
            SyncRecoveryHint::ResyncConversation as i32
        );
        let hints = response.hints.expect("gap should return recovery hints");
        assert_eq!(
            hints.recovery_hint,
            SyncRecoveryHint::ResyncConversation as i32
        );
        assert_eq!(hints.server_replay_max_conversation_seq, 4);
        assert!(
            response
                .envelope
                .expect("gap response still carries empty envelope")
                .events
                .is_empty()
        );
    }

    #[tokio::test]
    async fn conversation_user_settings_sync_forwards_to_conversation_port() {
        let infra = Arc::new(MockInfra::default());
        let handler =
            SyncOrchestrationHandler::new(infra.clone(), Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .execute_sync(
                &ctx(),
                "22",
                flare_proto::common::Sync {
                    device_id: "device-a".to_string(),
                    payload: Some(SyncPayload::ConversationUserSettings(
                        ConversationUserSettingsSync {
                            conversation_id: "c1".to_string(),
                            is_pinned: Some(true),
                            is_muted: Some(false),
                            is_archived: Some(true),
                            draft: Some("draft text".to_string()),
                            base_settings_version: 7,
                        },
                    )),
                },
            )
            .await
            .expect("conversation user settings sync");

        let Some(SyncResPayload::ConversationUserSettings(res)) = response.payload else {
            panic!("expected conversation user settings response");
        };
        let settings = res.settings.expect("settings");
        assert!(settings.is_pinned);
        assert!(!settings.is_muted);
        assert!(settings.is_archived);
        assert_eq!(settings.draft, "draft text");
        assert_eq!(settings.settings_version, 8);

        let updates = infra
            .settings_updates
            .lock()
            .expect("settings updates lock");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].conversation_id, "c1");
        assert_eq!(updates[0].base_settings_version, 7);
    }

    #[test]
    fn normalize_read_seq_preserves_participant_cursor_when_unread_zero() {
        assert_eq!(normalize_participant_read_seq(100, 0, 80), 80);
        assert_eq!(normalize_participant_read_seq(100, 1, 99), 99);
    }

    #[test]
    fn snapshot_summary_uses_authoritative_peer_read_seq_hint() {
        let mut message = Message {
            server_id: "m1".to_string(),
            conversation_seq: 10,
            ..Default::default()
        };
        message
            .attributes
            .insert("peer_read_seq".to_string(), "999999".to_string());
        let item = SnapshotConversationRow {
            conversation_id: "c1".to_string(),
            messages: vec![message],
            last_conversation_seq: 10,
            ..Default::default()
        };
        let hint = ConversationSyncRoutingHint {
            peer_read_seq: 3,
            ..Default::default()
        };

        let summary = snapshot_row_to_summary(&item, &hint);

        assert_eq!(
            summary.attributes.get("peer_read_seq").map(String::as_str),
            Some("3")
        );
    }

    #[test]
    fn snapshot_summary_single_chat_channel_uses_bootstrap_peer_hint() {
        let message = Message {
            server_id: "m1".to_string(),
            conversation_seq: 10,
            conversation_type: flare_proto::common::ConversationType::Single as i32,
            channel_id: "22".to_string(),
            ..Default::default()
        };
        let item = SnapshotConversationRow {
            conversation_id: "c1".to_string(),
            messages: vec![message],
            last_conversation_seq: 10,
            ..Default::default()
        };
        let hint = ConversationSyncRoutingHint {
            channel_id: "hugo1".to_string(),
            conversation_type: flare_proto::common::ConversationType::Single as i32,
            ..Default::default()
        };

        let summary = snapshot_row_to_summary(&item, &hint);

        assert_eq!(summary.channel_id, "hugo1");
    }

    #[test]
    fn conversation_summary_type_parser_accepts_only_canonical_names() {
        assert_eq!(
            conversation_type_from_summary("single"),
            flare_proto::common::ConversationType::Single as i32
        );
        assert_eq!(
            conversation_type_from_summary("channel"),
            flare_proto::common::ConversationType::Channel as i32
        );
        assert_eq!(
            conversation_type_from_summary("1"),
            flare_proto::common::ConversationType::Unspecified as i32
        );
        assert_eq!(
            conversation_type_from_summary("conversation_type_single"),
            flare_proto::common::ConversationType::Unspecified as i32
        );
    }

    #[test]
    fn snapshot_summary_emits_canonical_conversation_type_name() {
        let message = Message {
            server_id: "m1".to_string(),
            conversation_seq: 10,
            conversation_type: flare_proto::common::ConversationType::Single as i32,
            ..Default::default()
        };
        let item = SnapshotConversationRow {
            conversation_id: "c1".to_string(),
            messages: vec![message],
            last_conversation_seq: 10,
            ..Default::default()
        };

        let summary = snapshot_row_to_summary(&item, &ConversationSyncRoutingHint::default());

        assert_eq!(summary.conversation_type, "single");
    }

    #[test]
    fn snapshot_summary_drops_impossible_peer_read_seq_hint() {
        let item = SnapshotConversationRow {
            conversation_id: "c1".to_string(),
            last_conversation_seq: 10,
            ..Default::default()
        };
        let hint = ConversationSyncRoutingHint {
            peer_read_seq: 11,
            ..Default::default()
        };

        let summary = snapshot_row_to_summary(&item, &hint);

        assert_eq!(
            summary.attributes.get("peer_read_seq").map(String::as_str),
            Some("0")
        );
    }

    #[tokio::test]
    async fn conversations_sync_uses_participant_last_read_seq() {
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![ConversationSummary {
                    conversation_id: "c1".to_string(),
                    max_conversation_seq: 100,
                    unread_count: 39,
                    last_read_seq: 99,
                    updated_at: 1_000,
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .conversations_sync(
                &ctx(),
                "22",
                ConversationsSync {
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .expect("conversations sync");

        assert_eq!(response.conversations.len(), 1);
        assert_eq!(response.conversations[0].last_read_seq, 99);
        assert_eq!(response.conversations[0].unread_count, 39);
    }

    #[tokio::test]
    async fn conversations_sync_drops_invalid_and_internal_conversation_ids() {
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![
                    ConversationSummary {
                        conversation_id: String::new(),
                        max_conversation_seq: 100,
                        updated_at: 3_000,
                        ..Default::default()
                    },
                    ConversationSummary {
                        conversation_id: "sync:internal".to_string(),
                        max_conversation_seq: 100,
                        updated_at: 2_000,
                        ..Default::default()
                    },
                    ConversationSummary {
                        conversation_id: "c1".to_string(),
                        max_conversation_seq: 100,
                        updated_at: 1_000,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .conversations_sync(
                &ctx(),
                "22",
                ConversationsSync {
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .expect("conversations sync");

        assert_eq!(response.conversations.len(), 1);
        assert_eq!(response.conversations[0].conversation_id, "c1");
    }

    #[tokio::test]
    async fn conversations_sync_echoes_client_cursor_when_no_changes() {
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![ConversationSummary {
                    conversation_id: "c1".to_string(),
                    max_conversation_seq: 10,
                    updated_at: 1_000,
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .conversations_sync(
                &ctx(),
                "22",
                ConversationsSync {
                    cursor: build_snapshot_cursor(2_000, ""),
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .expect("conversations sync");

        assert!(response.conversations.is_empty());
        assert_eq!(response.next_cursor, build_snapshot_cursor(2_000, ""));
        assert!(!response.has_more);
    }

    #[tokio::test]
    async fn get_sync_cursor_returns_participant_last_read_seq() {
        let mut server_cursor_map = HashMap::new();
        server_cursor_map.insert("c1".to_string(), 100);
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![ConversationSummary {
                    conversation_id: "c1".to_string(),
                    max_conversation_seq: 100,
                    unread_count: 1,
                    last_read_seq: 99,
                    updated_at: 1_000,
                    ..Default::default()
                }],
                server_cursor_map,
                ..Default::default()
            },
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .get_sync_cursor_sync(
                &ctx(),
                "22",
                GetSyncCursorSync {
                    device_id: "sdk-22".to_string(),
                    conversation_id: "c1".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("get cursor");

        let cursor = response.cursor.expect("cursor");
        assert_eq!(cursor.conversation_id, "c1");
        assert_eq!(cursor.last_conversation_seq, 100);
        assert_eq!(cursor.last_read_seq, 99);
    }

    #[tokio::test]
    async fn get_sync_cursor_falls_back_to_persistent_bootstrap_cursor() {
        let mut server_cursor_map = HashMap::new();
        server_cursor_map.insert("__conversations__".to_string(), 1_778_673_857_000);
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                server_cursor_map,
                ..Default::default()
            },
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .get_sync_cursor_sync(
                &ctx(),
                "22",
                GetSyncCursorSync {
                    device_id: "sdk-22-9357".to_string(),
                    conversation_id: "__conversations__".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("get cursor");

        let cursor = response.cursor.expect("cursor");
        assert_eq!(cursor.conversation_id, "__conversations__");
        assert_eq!(cursor.last_conversation_seq, 1_778_673_857_000);
    }

    #[tokio::test]
    async fn get_sync_cursor_returns_conversation_version_hints() {
        let mut server_cursor_map = HashMap::new();
        server_cursor_map.insert("c1".to_string(), 100);
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![ConversationSummary {
                    conversation_id: "c1".to_string(),
                    max_conversation_seq: 100,
                    updated_at: 1_000,
                    ..Default::default()
                }],
                server_cursor_map,
                ..Default::default()
            },
            version_changes: vec![crate::application::ports::ConversationVersionChange {
                conversation_id: "c-large".to_string(),
                version: 7,
                max_conversation_seq: 900,
                updated_at_ms: 1_700,
            }],
            ..Default::default()
        });
        let handler = SyncOrchestrationHandler::new(infra, Arc::new(MemorySyncCursorCache::new()));

        let response = handler
            .get_sync_cursor_sync(
                &ctx(),
                "22",
                GetSyncCursorSync {
                    device_id: "sdk-22".to_string(),
                    conversation_id: "c1".to_string(),
                    known_conversation_versions: vec![ConversationVersion {
                        conversation_id: "c-large".to_string(),
                        version: 6,
                        ..Default::default()
                    }],
                },
            )
            .await
            .expect("get cursor");

        let hints = response.hints.expect("version hints");
        assert_eq!(hints.conversation_versions.len(), 1);
        assert_eq!(hints.conversation_versions[0].conversation_id, "c-large");
        assert_eq!(hints.conversation_versions[0].version, 7);
        assert_eq!(hints.conversation_versions[0].max_conversation_seq, 900);
        assert_eq!(hints.conversation_versions[0].updated_at, 1_700);
    }

    #[tokio::test]
    async fn update_sync_cursor_persists_cursor_and_updates_cache() {
        let infra = Arc::new(MockInfra::default());
        let cache = Arc::new(MemorySyncCursorCache::new());
        let handler = SyncOrchestrationHandler::new(infra.clone(), cache.clone());

        let response = handler
            .update_sync_cursor_sync(
                &ctx(),
                "22",
                UpdateSyncCursorSync {
                    cursor: Some(MultiDeviceCursor {
                        device_id: "sdk-22".to_string(),
                        conversation_id: "c1".to_string(),
                        last_conversation_seq: 42,
                        last_sync_at: 1_000,
                        last_read_seq: 40,
                        last_message_seq: 42,
                    }),
                },
            )
            .await
            .expect("update cursor");

        let cursor = response.cursor.expect("cursor");
        assert_eq!(cursor.conversation_id, "c1");
        assert_eq!(cursor.last_conversation_seq, 42);
        assert_eq!(cursor.last_read_seq, 40);
        assert_eq!(cursor.last_message_seq, 42);

        {
            let updates = infra.updates.lock().expect("updates lock");
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].conversation_id, "c1");
            assert_eq!(updates[0].sync_seq, 42);
            assert_eq!(updates[0].device_id, "sdk-22");
        }

        let cached = cache.get("22", "c1").await.expect("cached cursor");
        assert_eq!(cached.last_conversation_seq, 42);
        assert_eq!(cached.last_read_seq, 40);
    }

    #[tokio::test]
    async fn update_sync_cursor_keeps_high_water_for_stale_device_report() {
        let infra = Arc::new(MockInfra::default());
        let cache = Arc::new(MemorySyncCursorCache::new());
        cache
            .put(
                "22",
                MultiDeviceCursor {
                    device_id: "sdk-new".to_string(),
                    conversation_id: "c1".to_string(),
                    last_conversation_seq: 120,
                    last_sync_at: 2_000,
                    last_read_seq: 118,
                    last_message_seq: 121,
                },
            )
            .await;
        let handler = SyncOrchestrationHandler::new(infra.clone(), cache.clone());

        let response = handler
            .update_sync_cursor_sync(
                &ctx(),
                "22",
                UpdateSyncCursorSync {
                    cursor: Some(MultiDeviceCursor {
                        device_id: "sdk-old".to_string(),
                        conversation_id: "c1".to_string(),
                        last_conversation_seq: 90,
                        last_sync_at: 1_000,
                        last_read_seq: 80,
                        last_message_seq: 91,
                    }),
                },
            )
            .await
            .expect("update cursor");

        let cursor = response.cursor.expect("cursor");
        assert_eq!(cursor.last_conversation_seq, 120);
        assert_eq!(cursor.last_read_seq, 118);
        assert_eq!(cursor.last_message_seq, 121);
        assert_eq!(cursor.last_sync_at, 2_000);

        {
            let updates = infra.updates.lock().expect("updates lock");
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].conversation_id, "c1");
            assert_eq!(updates[0].sync_seq, 120);
        }

        let cached = cache.get("22", "c1").await.expect("cached cursor");
        assert_eq!(cached.last_conversation_seq, 120);
        assert_eq!(cached.last_read_seq, 118);
        assert_eq!(cached.last_message_seq, 121);
        assert_eq!(cached.last_sync_at, 2_000);
    }

    #[test]
    fn message_preview_derives_text_from_content_when_attribute_missing() {
        use flare_proto::common::{
            ImageContent, MessageContent, TextContent, message_content::Content,
        };

        // 无 text_preview 属性 → 从 content 取正文(修复会话列表恒显'暂无消息')。
        let mut text_msg = Message {
            server_id: "s1".to_string(),
            sender_id: "u1".to_string(),
            message_type: 1,
            ..Default::default()
        };
        text_msg.content = Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "hello list".to_string(),
                mentions: vec![],
            })),
        });
        assert_eq!(message_preview(Some(&text_msg), 123).unwrap().text, "hello list");

        // 媒体 → 占位标签。
        let mut image_msg = Message::default();
        image_msg.content = Some(MessageContent {
            content: Some(Content::Image(ImageContent::default())),
        });
        assert_eq!(message_preview(Some(&image_msg), 1).unwrap().text, "[图片]");

        // attributes["text_preview"] 优先于 content。
        let mut attr_msg = Message::default();
        attr_msg
            .attributes
            .insert("text_preview".to_string(), "attr wins".to_string());
        attr_msg.content = Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "ignored".to_string(),
                mentions: vec![],
            })),
        });
        assert_eq!(message_preview(Some(&attr_msg), 1).unwrap().text, "attr wins");
    }
}
