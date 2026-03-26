//! 同步编排应用服务：组合 Conversation + Storage +（可选）事件读端口，落实初始化/离线/增量策略。
//!
//! 对外统一为 `flare.common.v1.Sync` / `SyncRes`（gRPC `ExecuteSync` 与 DATA 信道一致）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::{
    ConversationDetail, ConversationDetailSync, ConversationDetailSyncRes, ConversationLight,
    ConversationMaxSeqSync, ConversationMaxSeqSyncRes, ConversationPatch, ConversationPatchType,
    ConversationSummary, ConversationsAllSync, ConversationsAllSyncRes, ConversationsIncrementalSync,
    ConversationsIncrementalSyncRes, ErrorCode, EventEnvelope, EventStreamAckSyncRes,
    GetSyncCursorSync, GetSyncCursorSyncRes, MessagePreview, MultiConversationSync, MultiConversationSyncRes,
    MultiDeviceCursor, QueryEventsSync, QueryEventsSyncRes, RpcStatus, SingleConversationSync,
    SingleConversationSyncRes, SnapshotConversationRow, SyncKind, SyncRes, SyncSliceItem,
    SyncSnapshotSync, SyncSnapshotSyncRes, UpdateSyncCursorSync, UpdateSyncCursorSyncRes,
};
use flare_proto::conversation::{ConversationBootstrapRequest, UpdateCursorRequest};
use flare_proto::Message;
use flare_server_core::context::Ctx;
use flare_server_core::error::{proto::ok_status, proto::to_rpc_status, FlareError};
use prost::Message as ProstMessage;
use tracing::{debug, trace};

use crate::application::error::require_nonempty_conversation_id;
use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, MemorySyncCursorCache, StorageReadPort, SyncCursorCachePort,
};
use crate::domain::model::{
    clamp_messages_per_conversation, clamp_query_events_limit, normalize_query_event_types, SyncIntent,
};
use crate::domain::service::{
    build_snapshot_cursor, ensure_cursor_monotonic, max_seq_from_events, parse_snapshot_cursor, snapshot_global_seq,
    ts_millis,
};

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
        Self { infra, cursor_cache }
    }

    /// 统一入口：`Sync` → 编排逻辑 → `SyncRes`（业务错误写入 `RpcStatus`，便于网关直转）。
    pub async fn execute_sync(&self, ctx: &Ctx, user_id: &str, mut sync: flare_proto::common::Sync) -> SyncRes {
        fn reject(msg: &str) -> SyncRes {
            SyncRes {
                status: Some(RpcStatus {
                    code: ErrorCode::InvalidArgument as i32,
                    message: msg.to_string(),
                    details: Vec::new(),
                    context: None,
                    localization_key: String::new(),
                    localization_params: Default::default(),
                }),
                payload: None,
            }
        }

        fn ok_pay(p: SyncResPayload) -> SyncRes {
            SyncRes {
                status: Some(ok_status()),
                payload: Some(p),
            }
        }

        fn from_flare(e: FlareError) -> SyncRes {
            SyncRes {
                status: Some(to_rpc_status(&e)),
                payload: None,
            }
        }

        let kind = decode_sync_kind(sync.kind);
        if kind == SyncKind::Unspecified {
            return reject("sync kind must not be SYNC_KIND_UNSPECIFIED");
        }
        let Some(payload) = sync.payload.take() else {
            return reject("sync payload is required");
        };

        match (kind, payload) {
            (SyncKind::SingleConversation, SyncPayload::SingleConversation(req)) => {
                match self.single_conversation_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::SingleConversation(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::MultiConversation, SyncPayload::MultiConversation(req)) => {
                match self.multi_conversation_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::MultiConversation(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::ConversationsIncremental, SyncPayload::ConversationsIncremental(req)) => {
                match self.conversations_incremental_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::ConversationsIncremental(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::ConversationsAll, SyncPayload::ConversationsAll(req)) => {
                match self.conversations_all_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::ConversationsAll(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::ConversationDetail, SyncPayload::ConversationDetail(req)) => {
                match self.conversation_detail_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::ConversationDetail(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::QueryEvents, SyncPayload::QueryEvents(req)) => match self.query_events_sync(ctx, user_id, req).await {
                Ok(v) => ok_pay(SyncResPayload::QueryEvents(v)),
                Err(e) => from_flare(e),
            },
            (SyncKind::GetSyncCursor, SyncPayload::GetSyncCursor(req)) => match self.get_sync_cursor_sync(user_id, req).await {
                Ok(v) => ok_pay(SyncResPayload::GetSyncCursor(v)),
                Err(e) => from_flare(e),
            },
            (SyncKind::UpdateSyncCursor, SyncPayload::UpdateSyncCursor(req)) => {
                match self.update_sync_cursor_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::UpdateSyncCursor(v)),
                    Err(e) => from_flare(e),
                }
            }
            (SyncKind::EventStreamAck, SyncPayload::EventStreamAck(_)) => {
                ok_pay(SyncResPayload::EventStreamAckRes(EventStreamAckSyncRes {}))
            }
            (SyncKind::SyncSnapshot, SyncPayload::SyncSnapshot(req)) => match self.get_sync_snapshot(ctx, user_id, req).await {
                Ok(v) => ok_pay(SyncResPayload::SyncSnapshotRes(v)),
                Err(e) => from_flare(e),
            },
            (SyncKind::ConversationMaxSeq, SyncPayload::ConversationMaxSeq(req)) => {
                match self.conversation_max_seq_sync(ctx, user_id, req).await {
                    Ok(v) => ok_pay(SyncResPayload::ConversationMaxSeqRes(v)),
                    Err(e) => from_flare(e),
                }
            }
            _ => reject("sync kind does not match payload oneof"),
        }
    }

    pub async fn get_sync_snapshot(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: SyncSnapshotSync,
    ) -> Result<SyncSnapshotSyncRes, FlareError> {
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

        let mut merged: HashMap<String, SnapshotConversationRow> = HashMap::new();
        let mut filtered_out = 0usize;

        for summary in conv_resp.conversations {
            if let Some(set) = &filter_set {
                if !set.contains(summary.conversation_id.as_str()) {
                    filtered_out += 1;
                    continue;
                }
            }
            let conversation_id = summary.conversation_id;
            let max_seq = summary.max_seq as i64;
            let mut item = SnapshotConversationRow {
                conversation_id: conversation_id.clone(),
                messages: Vec::new(),
                last_seq: max_seq.max(0),
                last_timestamp: summary.updated_at,
                unread_count: (summary.unread_count as i32).max(0),
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
                    .query_messages_by_seq(ctx, &conversation_id, after_seq, 0, message_limit, user_id)
                    .await?;

                item.messages = messages;
                if last_seq > 0 {
                    item.last_seq = last_seq;
                }
                if item.last_timestamp.is_none() {
                    item.last_timestamp = item.messages.iter().filter_map(|m| m.timestamp.clone()).max_by_key(
                        |ts| (ts.seconds, ts.nanos),
                    );
                }
            }

            merged.insert(conversation_id, item);
        }

        let page_limit = req.messages_per_conversation.max(crate::domain::model::MIN_SNAPSHOT_PAGE_SIZE) as usize;
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
            .map(|item| {
                let patched_ms = ts_millis(item.last_timestamp.as_ref());
                (patched_ms, item.conversation_id.clone(), item)
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
        let conversations = page.into_iter().map(|(_, _, item)| item).collect::<Vec<_>>();

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

        Ok(SyncSnapshotSyncRes {
            conversations,
            snapshot_seq,
            snapshot_timestamp,
            next_cursor,
            has_more,
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
            .and_then(|m| m.extra.get("seq").map(|s| format!("seq:{}:{}", s, m.server_id)))
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
                .and_then(|m| m.extra.get("seq").map(|s| format!("seq:{}:{}", s, m.server_id)))
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
        let response = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let fallback_patched_at = response.snapshot_timestamp.clone();
        let patches = response
            .conversations
            .iter()
            .filter_map(|c| {
                let patched_at = c
                    .last_timestamp
                    .clone()
                    .or_else(|| fallback_patched_at.clone());
                let summary = snapshot_row_to_summary(c);
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
            server_conversation_cursor: snapshot_cursor_to_ts(&response.next_cursor).or(response.snapshot_timestamp),
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
            include_deleted: req.sync_options.as_ref().map(|o| o.include_deleted).unwrap_or(false),
            include_conversations: true,
            snapshot_cursor: String::new(),
        };
        let response = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let conversations = response
            .conversations
            .iter()
            .map(snapshot_row_to_summary)
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
        let snap = self
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
        let summary = snap.conversations.first().map(snapshot_row_to_summary);
        let mut ext = summary.as_ref().map(|s| s.ext.clone()).unwrap_or_default();
        ext.insert("max_seq".to_string(), max.max_seq.to_string());
        ext.insert("last_message_id".to_string(), max.last_message_id.clone());

        Ok(ConversationDetailSyncRes {
            detail: Some(ConversationDetail {
                conversation_id: req.conversation_id,
                conversation_type: summary.as_ref().map(|s| s.conversation_type.clone()).unwrap_or_default(),
                business_type: summary.as_ref().map(|s| s.business_type.clone()).unwrap_or_default(),
                display_name: summary.as_ref().map(|s| s.display_name.clone()).unwrap_or_default(),
                avatar_url: summary.as_ref().map(|s| s.avatar_url.clone()).unwrap_or_default(),
                description: ext.get("description").cloned().unwrap_or_default(),
                announcement: ext.get("announcement").cloned().unwrap_or_default(),
                announcement_updated_at: None,
                announcement_updated_by: ext.get("announcement_updated_by").cloned().unwrap_or_default(),
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

    async fn get_sync_cursor_sync(&self, user_id: &str, req: GetSyncCursorSync) -> Result<GetSyncCursorSyncRes, FlareError> {
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
            FlareError::localized(flare_server_core::error::ErrorCode::InvalidParameter, "cursor is required")
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

fn conversation_type_str(message: Option<&Message>) -> String {
    let Some(msg) = message else {
        return String::new();
    };
    match msg.conversation_type {
        1 => "single".to_string(),
        2 => "group".to_string(),
        3 => "channel".to_string(),
        _ => String::new(),
    }
}

fn message_preview(message: Option<&Message>, sent_at: Option<prost_types::Timestamp>) -> Option<MessagePreview> {
    message.map(|m| MessagePreview {
        message_id: m.server_id.clone(),
        sender_id: m.sender_id.clone(),
        r#type: m.message_type,
        text: m.extra.get("text_preview").cloned().unwrap_or_default(),
        time: sent_at,
    })
}

fn snapshot_row_to_summary(item: &SnapshotConversationRow) -> ConversationSummary {
    let latest = latest_message(item);
    let mut ext = HashMap::new();
    if let Some(msg) = latest {
        ext.extend(msg.extra.clone());
        if !msg.channel_id.is_empty() {
            ext.entry("channel_id".to_string()).or_insert_with(|| msg.channel_id.clone());
        }
    }
    let display_name = ext.get("display_name").cloned().unwrap_or_default();
    let avatar_url = ext.get("avatar_url").cloned().unwrap_or_default();
    ConversationSummary {
        conversation_id: item.conversation_id.clone(),
        conversation_type: conversation_type_str(latest),
        business_type: ext.get("business_type").cloned().unwrap_or_default(),
        display_name,
        avatar_url,
        last_message: message_preview(latest, item.last_timestamp.clone()),
        unread_count: item.unread_count as u32,
        max_seq: item.last_seq as u64,
        last_read_seq: ext.get("last_read_seq").and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
        is_muted: ext.get("is_muted").map(|v| v == "true" || v == "1").unwrap_or(false),
        is_pinned: ext.get("is_pinned").map(|v| v == "true" || v == "1").unwrap_or(false),
        mute_until: None,
        updated_at: item.last_timestamp.clone(),
        created_at: None,
        labels: Vec::new(),
        member_count: ext.get("member_count").and_then(|v| v.parse::<i32>().ok()).unwrap_or(0),
        channel_id: latest.map(|m| m.channel_id.clone()).unwrap_or_default(),
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
