//! 同步编排应用服务：组合 Conversation + Storage +（可选）事件读端口，落实初始化/离线/增量策略。
//!
//! 对外统一为 `flare.common.v1.Sync` / `SyncRes`（gRPC `ExecuteSync` 与 DATA 信道一致；`SyncRes` 仅承载 `payload`，错误走 gRPC `Status`）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flare_grpc_proto::conversation::{ConversationBootstrapRequest, UpdateCursorRequest};
use flare_im_core::Ctx;
use flare_im_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_proto::Message;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::{
    ConversationDetail, ConversationDetailSync, ConversationDetailSyncRes, ConversationLight,
    ConversationMaxSeqSync, ConversationMaxSeqSyncRes, ConversationPatch, ConversationPatchType,
    ConversationSummary, ConversationsAllSync, ConversationsAllSyncRes,
    ConversationsIncrementalSync, ConversationsIncrementalSyncRes, EventEnvelope,
    EventStreamAckSyncRes, GetSyncCursorSync, GetSyncCursorSyncRes, MessagePreview,
    MultiConversationSync, MultiConversationSyncRes, MultiDeviceCursor, QueryEventsSync,
    QueryEventsSyncRes, SingleConversationSync, SingleConversationSyncRes, SnapshotConversationRow,
    SyncKind, SyncRes, SyncSliceItem, SyncSnapshotSync, SyncSnapshotSyncRes, UpdateSyncCursorSync,
    UpdateSyncCursorSyncRes,
};
use prost::Message as ProstMessage;
use tracing::{debug, trace};

use crate::application::error::require_nonempty_conversation_id;
use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, MemorySyncCursorCache, StorageReadPort,
    SyncCursorCachePort,
};
use crate::domain::model::{
    SyncIntent, clamp_messages_per_conversation, clamp_query_events_limit,
    normalize_query_event_types,
};
use crate::domain::service::{
    build_snapshot_cursor, ensure_cursor_monotonic, max_seq_from_events, parse_snapshot_cursor,
    snapshot_global_seq, ts_millis,
};

/// 与 `SyncSnapshotSyncRes.conversations` 逐行对齐，来自 ConversationBootstrap 摘要（单聊 `channel_id` 等对端路由）
#[derive(Clone, Default)]
struct ConversationSyncRoutingHint {
    channel_id: String,
    conversation_type: i32,
    peer_read_seq: u64,
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
    I: ConversationSyncPort + StorageReadPort + ConversationEventReadPort + Send + Sync,
{
    infra: Arc<I>,
    cursor_cache: Arc<MemorySyncCursorCache>,
}

impl<I> SyncOrchestrationHandler<I>
where
    I: ConversationSyncPort + StorageReadPort + ConversationEventReadPort + Send + Sync,
{
    pub fn new(infra: Arc<I>, cursor_cache: Arc<MemorySyncCursorCache>) -> Self {
        Self {
            infra,
            cursor_cache,
        }
    }

    /// 统一入口：`Sync` → 编排逻辑 → `SyncRes`。失败返回 `FlareError`；gRPC 层用 `IntoGrpc` 转为 `tonic::Status`（与 media 等服务一致）。
    pub async fn execute_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        mut sync: flare_proto::common::Sync,
    ) -> Result<SyncRes, FlareError> {
        let kind = decode_sync_kind(sync.kind);
        if kind == SyncKind::Unspecified {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "sync kind must not be SYNC_KIND_UNSPECIFIED",
            )
            .build_error());
        }
        let Some(payload) = sync.payload.take() else {
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "sync payload is required")
                    .build_error(),
            );
        };

        match (kind, payload) {
            (SyncKind::SingleConversation, SyncPayload::SingleConversation(req)) => {
                let v = self.single_conversation_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::SingleConversation(v)),
                })
            }
            (SyncKind::MultiConversation, SyncPayload::MultiConversation(req)) => {
                let v = self.multi_conversation_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::MultiConversation(v)),
                })
            }
            (SyncKind::ConversationsIncremental, SyncPayload::ConversationsIncremental(req)) => {
                let v = self
                    .conversations_incremental_sync(ctx, user_id, req)
                    .await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationsIncremental(v)),
                })
            }
            (SyncKind::ConversationsAll, SyncPayload::ConversationsAll(req)) => {
                let v = self.conversations_all_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationsAll(v)),
                })
            }
            (SyncKind::ConversationDetail, SyncPayload::ConversationDetail(req)) => {
                let v = self.conversation_detail_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationDetail(v)),
                })
            }
            (SyncKind::QueryEvents, SyncPayload::QueryEvents(req)) => {
                let v = self.query_events_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::QueryEvents(v)),
                })
            }
            (SyncKind::GetSyncCursor, SyncPayload::GetSyncCursor(req)) => {
                let v = self.get_sync_cursor_sync(user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::GetSyncCursor(v)),
                })
            }
            (SyncKind::UpdateSyncCursor, SyncPayload::UpdateSyncCursor(req)) => {
                let v = self.update_sync_cursor_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::UpdateSyncCursor(v)),
                })
            }
            (SyncKind::EventStreamAck, SyncPayload::EventStreamAck(_)) => Ok(SyncRes {
                payload: Some(SyncResPayload::EventStreamAckRes(EventStreamAckSyncRes {})),
            }),
            (SyncKind::SyncSnapshot, SyncPayload::SyncSnapshot(req)) => {
                let outcome = self.get_sync_snapshot(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::SyncSnapshotRes(outcome.res)),
                })
            }
            (SyncKind::ConversationMaxSeq, SyncPayload::ConversationMaxSeq(req)) => {
                let v = self.conversation_max_seq_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationMaxSeqRes(v)),
                })
            }
            _ => Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "sync kind does not match payload oneof",
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

        let conv_resp = self
            .infra
            .conversation_bootstrap(
                ctx,
                ConversationBootstrapRequest {
                    client_cursor_map: Default::default(),
                    include_recent_messages: false,
                    recent_message_limit: 0,
                    device_id: String::new(),
                    device_platform: String::new(),
                },
            )
            .await?;

        debug!(
            user_id = %user_id,
            conversation_bootstrap_count = conv_resp.conversations.len(),
            "conversation bootstrap returned"
        );

        let filter_set: Option<HashSet<&str>> = if req.conversation_ids.is_empty() {
            None
        } else {
            Some(req.conversation_ids.iter().map(String::as_str).collect())
        };

        let mut merged: HashMap<String, MergedSnapshotRow> = HashMap::new();
        let mut filtered_out = 0usize;

        for bootstrap in conv_resp.conversations {
            if let Some(set) = &filter_set {
                if !set.contains(bootstrap.conversation_id.as_str()) {
                    filtered_out += 1;
                    continue;
                }
            }
            let conversation_id = bootstrap.conversation_id.clone();
            let max_seq = bootstrap.max_seq as i64;
            let mut item = SnapshotConversationRow {
                conversation_id: conversation_id.clone(),
                messages: Vec::new(),
                last_seq: max_seq.max(0),
                last_timestamp: bootstrap.updated_at.clone(),
                unread_count: (bootstrap.unread_count as i32).max(0),
            };

            if message_limit > 0 && max_seq > 0 {
                let after_seq = (max_seq - message_limit as i64).max(0);
                trace!(
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    max_seq,
                    after_seq,
                    limit = message_limit,
                    "querying messages for snapshot item"
                );
                let (messages, last_seq) = self
                    .infra
                    .query_messages_by_seq(
                        ctx,
                        &conversation_id,
                        after_seq,
                        0,
                        message_limit,
                        user_id,
                    )
                    .await?;

                item.messages = messages;
                if last_seq > 0 {
                    item.last_seq = last_seq;
                }
                if item.last_timestamp.is_none() {
                    item.last_timestamp = item
                        .messages
                        .iter()
                        .filter_map(|m| m.timestamp.clone())
                        .max_by_key(|ts| (ts.seconds, ts.nanos));
                }
            }

            merged.insert(
                conversation_id,
                MergedSnapshotRow {
                    row: item,
                    bootstrap,
                },
            );
        }

        let page_limit = req
            .messages_per_conversation
            .max(crate::domain::model::MIN_SNAPSHOT_PAGE_SIZE) as usize;
        let page_cursor = parse_snapshot_cursor(&req.snapshot_cursor);
        debug!(
            user_id = %user_id,
            merged_conversation_count = merged.len(),
            filtered_out,
            page_limit,
            parsed_cursor = ?page_cursor,
            "snapshot merge completed"
        );

        let mut sorted = merged
            .into_values()
            .map(|m| {
                let patched_ms = ts_millis(m.row.last_timestamp.as_ref());
                (patched_ms, m.row.conversation_id.clone(), m)
            })
            .collect::<Vec<_>>();
        sorted.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

        let filtered = if let Some((cursor_ms, cursor_cid)) = page_cursor {
            sorted
                .into_iter()
                .filter(|(ms, cid, _)| (*ms, cid.clone()) > (cursor_ms, cursor_cid.clone()))
                .collect::<Vec<_>>()
        } else {
            sorted
        };

        let has_more = filtered.len() > page_limit;
        let page = filtered.into_iter().take(page_limit).collect::<Vec<_>>();
        let next_cursor = if has_more {
            page.last()
                .map(|(ms, cid, _)| build_snapshot_cursor(*ms, cid))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let mut routing = Vec::with_capacity(page.len());
        let conversations: Vec<SnapshotConversationRow> = page
            .into_iter()
            .map(|(_, _, m)| {
                routing.push(ConversationSyncRoutingHint {
                    channel_id: m.bootstrap.channel_id.clone(),
                    conversation_type: m.bootstrap.conversation_type.parse::<i32>().unwrap_or(0),
                    peer_read_seq: m
                        .bootstrap
                        .ext
                        .get("peer_read_seq")
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or_default(),
                });
                m.row
            })
            .collect();

        let snapshot_seq = snapshot_global_seq(&conversations);
        let snapshot_timestamp = conversations
            .iter()
            .filter_map(|i| i.last_timestamp.as_ref())
            .max_by_key(|ts| (ts.seconds, ts.nanos))
            .cloned();

        debug!(
            user_id = %user_id,
            page_conversation_count = conversations.len(),
            snapshot_seq,
            has_more,
            next_cursor = %next_cursor,
            elapsed_ms = started.elapsed().as_millis(),
            "sync snapshot response prepared"
        );

        Ok(SyncSnapshotOutcome {
            res: SyncSnapshotSyncRes {
                conversations,
                snapshot_seq,
                snapshot_timestamp,
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
        require_nonempty_conversation_id(&req.conversation_id)?;
        let limit = req.limit.max(1).min(500);
        // `SingleConversationSync.max_seq`：客户端本地已知的最后消息 seq，语义同 `QueryMessagesBySeq.after_seq`（严格大于）。
        let (messages, last_seq) = self
            .infra
            .query_messages_by_seq(
                ctx,
                &req.conversation_id,
                req.max_seq as i64,
                0,
                limit,
                user_id,
            )
            .await?;
        let has_more = messages.len() as i32 >= limit;
        let next_cursor = messages
            .last()
            .and_then(|m| {
                m.extra
                    .get("seq")
                    .map(|s| format!("seq:{}:{}", s, m.server_id))
            })
            .unwrap_or_default();
        let mut items = Vec::with_capacity(messages.len());
        for m in &messages {
            items.push(message_to_sync_item(m)?);
        }
        Ok(SingleConversationSyncRes {
            conversation_id: req.conversation_id,
            items,
            max_seq: last_seq.max(0) as u64,
            next_cursor,
            has_more,
            hints: None,
            stale: None,
        })
    }

    async fn multi_conversation_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: MultiConversationSync,
    ) -> Result<MultiConversationSyncRes, FlareError> {
        let limit = req.limit_per_conversation.max(1).min(500);
        let mut slices = Vec::new();
        let mut max_seq_per_conversation = HashMap::new();
        let mut has_more = false;

        for cid in &req.conversation_ids {
            if cid.trim().is_empty() {
                continue;
            }
            let after = req.last_seq_per_conversation.get(cid).copied().unwrap_or(0) as i64;
            let (messages, last_seq) = self
                .infra
                .query_messages_by_seq(ctx, cid, after, 0, limit, user_id)
                .await?;
            let slice_has_more = messages.len() as i32 >= limit;
            if slice_has_more {
                has_more = true;
            }
            let next_cursor = messages
                .last()
                .and_then(|m| {
                    m.extra
                        .get("seq")
                        .map(|s| format!("seq:{}:{}", s, m.server_id))
                })
                .unwrap_or_default();
            let mut items = Vec::with_capacity(messages.len());
            for m in &messages {
                items.push(message_to_sync_item(m)?);
            }
            let max_seq = last_seq.max(0) as u64;
            max_seq_per_conversation.insert(cid.clone(), max_seq);
            slices.push(flare_proto::common::ConversationSyncSlice {
                conversation_id: cid.clone(),
                items,
                max_seq,
                next_cursor,
                has_more: slice_has_more,
            });
        }

        Ok(MultiConversationSyncRes {
            slices,
            max_seq_per_conversation,
            has_more,
            hints: None,
        })
    }

    async fn conversations_incremental_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationsIncrementalSync,
    ) -> Result<ConversationsIncrementalSyncRes, FlareError> {
        let snap_req = SyncSnapshotSync {
            conversation_ids: Vec::new(),
            messages_per_conversation: req.limit.max(1),
            include_deleted: false,
            include_conversations: true,
            snapshot_cursor: build_snapshot_cursor_from_ts(req.client_conversation_cursor.as_ref()),
        };
        let outcome = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let response = outcome.res;
        let fallback_patched_at = response.snapshot_timestamp.clone();
        let patches = response
            .conversations
            .iter()
            .zip(outcome.routing.iter())
            .filter_map(|(c, hint)| {
                let patched_at = c
                    .last_timestamp
                    .clone()
                    .or_else(|| fallback_patched_at.clone());
                let summary = snapshot_row_to_summary(c, hint);
                Some(ConversationPatch {
                    conversation_id: c.conversation_id.clone(),
                    patch_type: ConversationPatchType::ConversationPatchSummary as i32,
                    light: Some(summary_to_light(&summary)),
                    summary: Some(summary),
                    patched_at,
                })
            })
            .collect::<Vec<_>>();

        Ok(ConversationsIncrementalSyncRes {
            patches,
            server_conversation_cursor: snapshot_cursor_to_ts(&response.next_cursor)
                .or(response.snapshot_timestamp),
            has_more: response.has_more,
            hints: None,
        })
    }

    async fn conversations_all_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationsAllSync,
    ) -> Result<ConversationsAllSyncRes, FlareError> {
        let limit = req
            .sync_options
            .as_ref()
            .map(|o| o.max_batch_size)
            .unwrap_or(100)
            .max(1);
        let snap_req = SyncSnapshotSync {
            conversation_ids: Vec::new(),
            messages_per_conversation: limit,
            include_deleted: req
                .sync_options
                .as_ref()
                .map(|o| o.include_deleted)
                .unwrap_or(false),
            include_conversations: true,
            snapshot_cursor: String::new(),
        };
        let outcome = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let response = outcome.res;
        let conversations = response
            .conversations
            .iter()
            .zip(outcome.routing.iter())
            .map(|(c, hint)| snapshot_row_to_summary(c, hint))
            .collect::<Vec<_>>();

        Ok(ConversationsAllSyncRes {
            conversations,
            server_conversation_cursor: response.snapshot_timestamp,
            server_max_seq: response.snapshot_seq as u64,
            metadata: HashMap::new(),
            hints: None,
        })
    }

    async fn conversation_detail_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationDetailSync,
    ) -> Result<ConversationDetailSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        let max = self
            .conversation_max_seq_sync(
                ctx,
                user_id,
                ConversationMaxSeqSync {
                    conversation_id: req.conversation_id.clone(),
                },
            )
            .await?;
        let outcome = self
            .get_sync_snapshot(
                ctx,
                user_id,
                SyncSnapshotSync {
                    conversation_ids: vec![req.conversation_id.clone()],
                    messages_per_conversation: 1,
                    include_deleted: false,
                    include_conversations: true,
                    snapshot_cursor: String::new(),
                },
            )
            .await?;
        let snap = outcome.res;
        let summary = snap
            .conversations
            .first()
            .zip(outcome.routing.first())
            .map(|(c, hint)| snapshot_row_to_summary(c, hint));
        let mut ext = summary.as_ref().map(|s| s.ext.clone()).unwrap_or_default();
        ext.insert("max_seq".to_string(), max.max_seq.to_string());
        ext.insert("last_message_id".to_string(), max.last_message_id.clone());

        Ok(ConversationDetailSyncRes {
            detail: Some(ConversationDetail {
                conversation_id: req.conversation_id,
                conversation_type: summary
                    .as_ref()
                    .map(|s| s.conversation_type.clone())
                    .unwrap_or_default(),
                business_type: summary
                    .as_ref()
                    .map(|s| s.business_type.clone())
                    .unwrap_or_default(),
                display_name: summary
                    .as_ref()
                    .map(|s| s.display_name.clone())
                    .unwrap_or_default(),
                avatar_url: summary
                    .as_ref()
                    .map(|s| s.avatar_url.clone())
                    .unwrap_or_default(),
                description: ext.get("description").cloned().unwrap_or_default(),
                announcement: ext.get("announcement").cloned().unwrap_or_default(),
                announcement_updated_at: None,
                announcement_updated_by: ext
                    .get("announcement_updated_by")
                    .cloned()
                    .unwrap_or_default(),
                visibility: 0,
                lifecycle_state: 0,
                policy: None,
                participants: Vec::new(),
                presence: None,
                created_at: summary.as_ref().and_then(|s| s.created_at.clone()),
                updated_at: summary
                    .as_ref()
                    .and_then(|s| s.updated_at.clone())
                    .or(max.last_timestamp.clone()),
                member_count: summary.as_ref().map(|s| s.member_count).unwrap_or(0),
                attributes: HashMap::new(),
                channel_id: summary
                    .as_ref()
                    .map(|s| s.channel_id.clone())
                    .unwrap_or_default(),
                ext,
            }),
            metadata: HashMap::new(),
            hints: None,
        })
    }

    async fn query_events_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        mut req: QueryEventsSync,
    ) -> Result<QueryEventsSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        normalize_query_event_types(&mut req.event_types);
        let limit = clamp_query_events_limit(req.limit);
        let max_seq_hint = None;
        let intent = SyncIntent::from_event_anchor(req.after_seq, max_seq_hint);
        debug!(
            user_id = %user_id,
            conversation_id = %req.conversation_id,
            after_seq = req.after_seq,
            before_seq = req.before_seq,
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
                req.after_seq,
                req.before_seq,
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

        Ok(QueryEventsSyncRes {
            envelope: Some(EventEnvelope {
                events,
                max_seq: last_seq.max(0) as u64,
                has_more: page.has_more,
                next_cursor: page.next_cursor,
                window_id: String::new(),
            }),
            hints: None,
            stale: None,
        })
    }

    async fn conversation_max_seq_sync(
        &self,
        ctx: &Ctx,
        _user_id: &str,
        req: ConversationMaxSeqSync,
    ) -> Result<ConversationMaxSeqSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        debug!(
            conversation_id = %req.conversation_id,
            "get_conversation_max_seq via storage reader (message head)"
        );
        let head = self
            .infra
            .get_conversation_message_head(ctx, &req.conversation_id)
            .await?;
        Ok(ConversationMaxSeqSyncRes {
            max_seq: head.max_seq,
            last_timestamp: head.last_timestamp,
            last_message_id: head.last_message_id,
        })
    }

    async fn get_sync_cursor_sync(
        &self,
        user_id: &str,
        req: GetSyncCursorSync,
    ) -> Result<GetSyncCursorSyncRes, FlareError> {
        require_nonempty_conversation_id(&req.conversation_id)?;
        if let Some(cursor) = self.cursor_cache.get(user_id, &req.conversation_id).await {
            debug!(
                user_id = %user_id,
                conversation_id = %req.conversation_id,
                "get sync cursor hit (L1)"
            );
            return Ok(GetSyncCursorSyncRes {
                cursor: Some(cursor),
                hints: None,
            });
        }
        debug!(
            user_id = %user_id,
            conversation_id = %req.conversation_id,
            "get sync cursor miss (L1)"
        );
        Ok(GetSyncCursorSyncRes {
            cursor: None,
            hints: None,
        })
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
            last_sync_seq = cursor.last_sync_seq,
            "update_sync_cursor (命令)"
        );

        let prev = self
            .cursor_cache
            .previous_last_seq(user_id, &cursor.conversation_id)
            .await;
        ensure_cursor_monotonic(prev, cursor.last_sync_seq as i64).map_err(FlareError::from)?;

        self.infra
            .update_read_cursor(
                ctx,
                UpdateCursorRequest {
                    conversation_id: cursor.conversation_id.clone(),
                    message_ts: cursor.last_sync_seq as i64,
                    device_id: cursor.device_id.clone(),
                },
            )
            .await?;

        let out = MultiDeviceCursor {
            device_id: cursor.device_id.clone(),
            conversation_id: cursor.conversation_id.clone(),
            last_sync_seq: cursor.last_sync_seq,
            last_sync_at: cursor.last_sync_at.clone(),
            last_read_seq: cursor.last_read_seq,
            last_critical_event_seq: cursor.last_critical_event_seq,
        };
        self.cursor_cache.put(user_id, out.clone()).await;

        Ok(UpdateSyncCursorSyncRes {
            cursor: Some(out),
            hints: None,
        })
    }
}

fn decode_sync_kind(raw: i32) -> SyncKind {
    match raw {
        1 => SyncKind::SingleConversation,
        2 => SyncKind::MultiConversation,
        3 => SyncKind::ConversationsIncremental,
        4 => SyncKind::ConversationsAll,
        5 => SyncKind::ConversationDetail,
        7 => SyncKind::QueryEvents,
        8 => SyncKind::GetSyncCursor,
        9 => SyncKind::UpdateSyncCursor,
        10 => SyncKind::EventStreamAck,
        11 => SyncKind::SyncSnapshot,
        12 => SyncKind::ConversationMaxSeq,
        _ => SyncKind::Unspecified,
    }
}

fn message_to_sync_item(message: &Message) -> Result<SyncSliceItem, FlareError> {
    let mut payload = Vec::new();
    message
        .encode(&mut payload)
        .map_err(|e| FlareError::system(format!("encode Message for SyncSliceItem: {e}")))?;
    Ok(SyncSliceItem {
        seq: message.seq as u64,
        created_at: message.timestamp.clone(),
        payload,
    })
}

fn build_snapshot_cursor_from_ts(ts: Option<&prost_types::Timestamp>) -> String {
    let ms = ts_millis(ts);
    if ms <= 0 {
        String::new()
    } else {
        format!("{ms}|")
    }
}

fn snapshot_cursor_to_ts(cursor: &str) -> Option<prost_types::Timestamp> {
    let ms_part = cursor.split('|').next().unwrap_or_default();
    let ms = ms_part.parse::<i64>().ok()?;
    Some(prost_types::Timestamp {
        seconds: ms / 1000,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    })
}

fn latest_message(item: &SnapshotConversationRow) -> Option<&Message> {
    item.messages.iter().max_by_key(|m| m.seq)
}

fn conversation_type_int(message: Option<&Message>) -> i32 {
    let Some(msg) = message else {
        return 0;
    };
    msg.conversation_type
}

/// 同步补丁摘要 `channel_id`：最新消息体优先，否则 Bootstrap 摘要
fn merge_sync_summary_channel_id(from_message: &str, hint: &ConversationSyncRoutingHint) -> String {
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

fn message_preview(
    message: Option<&Message>,
    sent_at: Option<prost_types::Timestamp>,
) -> Option<MessagePreview> {
    message.map(|m| MessagePreview {
        message_id: m.server_id.clone(),
        sender_id: m.sender_id.clone(),
        r#type: m.message_type,
        text: m.extra.get("text_preview").cloned().unwrap_or_default(),
        time: sent_at,
    })
}

fn snapshot_row_to_summary(
    item: &SnapshotConversationRow,
    hint: &ConversationSyncRoutingHint,
) -> ConversationSummary {
    let latest = latest_message(item);
    let mut ext = HashMap::new();
    if let Some(msg) = latest {
        ext.extend(msg.extra.clone());
    }
    let display_name = ext.get("display_name").cloned().unwrap_or_default();
    let avatar_url = ext.get("avatar_url").cloned().unwrap_or_default();
    ext.entry("peer_read_seq".to_string())
        .or_insert_with(|| hint.peer_read_seq.to_string());
    let channel_from_msg = latest.map(|m| m.channel_id.as_str()).unwrap_or_default();
    let channel_id = merge_sync_summary_channel_id(channel_from_msg, hint);
    let type_from_msg = conversation_type_int(latest);
    let conversation_type =
        merge_sync_summary_conversation_type(type_from_msg, hint.conversation_type);
    // SnapshotConversationRow 当前不包含 last_read_seq。
    // 若仅从 ext 取值（缺省为 0），会在会话增量同步后把客户端读位重置，
    // 导致重登后 unread 计算与 read_states 上报失真。
    let derived_last_read_seq = item
        .last_seq
        .saturating_sub((item.unread_count.max(0)) as i64)
        .max(0) as u64;
    let last_read_seq = ext
        .get("last_read_seq")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(derived_last_read_seq);
    ConversationSummary {
        conversation_id: item.conversation_id.clone(),
        conversation_type: conversation_type.to_string(),
        business_type: ext.get("business_type").cloned().unwrap_or_default(),
        display_name,
        avatar_url,
        last_message: message_preview(latest, item.last_timestamp.clone()),
        unread_count: item.unread_count as u32,
        max_seq: item.last_seq as u64,
        last_read_seq,
        is_muted: ext
            .get("is_muted")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        is_pinned: ext
            .get("is_pinned")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        mute_until: None,
        updated_at: item.last_timestamp.clone(),
        created_at: None,
        labels: Vec::new(),
        member_count: ext
            .get("member_count")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0),
        channel_id,
        ext,
    }
}

fn summary_to_light(summary: &ConversationSummary) -> ConversationLight {
    ConversationLight {
        conversation_id: summary.conversation_id.clone(),
        conversation_type: summary.conversation_type.clone(),
        unread_count: summary.unread_count,
        max_seq: summary.max_seq,
        last_read_seq: summary.last_read_seq,
        preview: summary.last_message.clone(),
        updated_at: summary.updated_at.clone(),
        is_muted: summary.is_muted,
        is_pinned: summary.is_pinned,
        mute_until: summary.mute_until.clone(),
        labels: summary.labels.clone(),
        channel_id: summary.channel_id.clone(),
        ext: summary.ext.clone(),
    }
}

fn filter_events_by_types(events: &mut Vec<flare_proto::common::Event>, allowed: &[i32]) {
    if allowed.is_empty() {
        return;
    }
    let set: HashSet<i32> = allowed.iter().copied().collect();
    events.retain(|e| set.contains(&e.r#type));
}
