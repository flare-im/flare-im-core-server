//! 同步编排应用服务：组合 Conversation + Storage +（可选）事件读端口，落实初始化/离线/增量策略。
//!
//! 对外统一为 `flare.common.v1.Sync` / `SyncRes`（gRPC `ExecuteSync` 与 DATA 信道一致；`SyncRes` 仅承载 `payload`，错误走 gRPC `Status`）。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ListConversationParticipantsRequest, UpdateCursorRequest,
};
use flare_im_core::Ctx;
use flare_im_core::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_proto::Message;
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::{
    ConversationDetailSync, ConversationDetailSyncRes, ConversationParticipant,
    ConversationParticipantsSync, ConversationParticipantsSyncRes, ConversationSummary,
    ConversationsSync, ConversationsSyncRes, EventEnvelope, EventStreamAckSyncRes,
    GetSyncCursorSync, GetSyncCursorSyncRes, MessagePreview, MultiConversationSync,
    MultiConversationSyncRes, MultiDeviceCursor, QueryEventsSync, QueryEventsSyncRes,
    SingleConversationSync, SingleConversationSyncRes, SnapshotConversationRow, SyncKind, SyncRes,
    SyncSliceItem, SyncSliceItemKind, SyncSnapshotSync, SyncSnapshotSyncRes, UpdateSyncCursorSync,
    UpdateSyncCursorSyncRes,
};
use prost::Message as ProstMessage;
use tracing::{debug, trace, warn};

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
    participant_version: u64,
    member_preview: Vec<ConversationParticipant>,
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
            (SyncKind::Conversations, SyncPayload::Conversations(req)) => {
                let v = self.conversations_sync(ctx, user_id, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::Conversations(v)),
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
                let v = self.get_sync_cursor_sync(ctx, user_id, req).await?;
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
            (SyncKind::ConversationParticipants, SyncPayload::ConversationParticipants(req)) => {
                let v = self.conversation_participants_sync(ctx, req).await?;
                Ok(SyncRes {
                    payload: Some(SyncResPayload::ConversationParticipants(v)),
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
                    participant_version: m.bootstrap.participant_version,
                    member_preview: m.bootstrap.member_preview.clone(),
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
        let conversation_id = req.conversation_id;
        require_nonempty_conversation_id(&conversation_id)?;
        let limit = req.limit.max(1).min(500);
        // `SingleConversationSync.max_seq`：客户端本地已知的最后消息 seq，语义同 `QueryMessagesBySeq.after_seq`（严格大于）。
        let (messages, _storage_last_seq) = self
            .infra
            .query_messages_by_seq(
                ctx,
                &conversation_id,
                req.max_seq as i64,
                0,
                limit + 1,
                user_id,
            )
            .await?;
        let head_max_seq = self
            .conversation_head_max_seq(ctx, &conversation_id, req.max_seq as i64)
            .await;
        let page = build_contiguous_sync_items(
            &conversation_id,
            req.max_seq,
            limit as usize,
            messages,
            head_max_seq as u64,
        )?;
        Ok(SingleConversationSyncRes {
            conversation_id,
            items: page.items,
            max_seq: page.max_seq,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
            hints: None,
            stale: None,
            ..Default::default()
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
        let limit = req.limit_per_conversation.max(1).min(500);
        let mut slices = Vec::new();
        let mut max_seq_per_conversation = HashMap::new();
        let mut has_more = false;

        for cid in &req.conversation_ids {
            if cid.trim().is_empty() {
                continue;
            }
            let after = req.last_seq_per_conversation.get(cid).copied().unwrap_or(0) as i64;
            let (messages, _storage_last_seq) = self
                .infra
                .query_messages_by_seq(ctx, cid, after, 0, limit + 1, user_id)
                .await?;
            let head_max_seq = self.conversation_head_max_seq(ctx, cid, after).await;
            let page = build_contiguous_sync_items(
                cid,
                after as u64,
                limit as usize,
                messages,
                head_max_seq as u64,
            )?;
            let slice_has_more = page.has_more;
            if page.has_more {
                has_more = true;
            }
            let max_seq = page.max_seq;
            max_seq_per_conversation.insert(cid.clone(), max_seq);
            slices.push(flare_proto::common::ConversationSyncSlice {
                conversation_id: cid.clone(),
                items: page.items,
                max_seq,
                next_cursor: page.next_cursor,
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

    async fn conversations_sync(
        &self,
        ctx: &Ctx,
        user_id: &str,
        req: ConversationsSync,
    ) -> Result<ConversationsSyncRes, FlareError> {
        let limit = req.limit.max(1);
        let client_cursor = req.client_conversation_cursor.clone();
        let is_cold_start = client_cursor.is_none();
        let snap_req = SyncSnapshotSync {
            conversation_ids: Vec::new(),
            messages_per_conversation: limit,
            include_deleted: req.include_deleted,
            include_conversations: true,
            snapshot_cursor: if is_cold_start {
                String::new()
            } else {
                build_snapshot_cursor_from_ts(client_cursor.as_ref())
            },
        };
        let outcome = self.get_sync_snapshot(ctx, user_id, snap_req).await?;
        let response = outcome.res;
        let conversations = response
            .conversations
            .iter()
            .zip(outcome.routing.iter())
            .map(|(c, hint)| snapshot_row_to_summary(c, hint))
            .collect::<Vec<_>>();
        let server_conversation_cursor = if is_cold_start {
            response.snapshot_timestamp
        } else {
            snapshot_cursor_to_ts(&response.next_cursor)
                .or(response.snapshot_timestamp)
                .or(client_cursor)
        };

        Ok(ConversationsSyncRes {
            conversations,
            server_conversation_cursor,
            has_more: response.has_more,
            hints: None,
            ..Default::default()
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
            ..Default::default()
        })
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

    async fn get_sync_cursor_sync(
        &self,
        ctx: &Ctx,
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
                },
            )
            .await?;
        if let Some(cursor_value) = conv_resp.server_cursor_map.get(&req.conversation_id) {
            let cursor = MultiDeviceCursor {
                device_id: req.device_id,
                conversation_id: req.conversation_id,
                last_sync_seq: (*cursor_value).max(0) as u64,
                last_sync_at: None,
                last_read_seq: 0,
                last_critical_event_seq: 0,
            };
            self.cursor_cache.put(user_id, cursor.clone()).await;
            debug!(
                user_id = %user_id,
                conversation_id = %cursor.conversation_id,
                last_sync_seq = cursor.last_sync_seq,
                "get sync cursor hit (persistent)"
            );
            return Ok(GetSyncCursorSyncRes {
                cursor: Some(cursor),
                hints: None,
            });
        }

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
        3 => SyncKind::Conversations,
        5 => SyncKind::ConversationDetail,
        7 => SyncKind::QueryEvents,
        8 => SyncKind::GetSyncCursor,
        9 => SyncKind::UpdateSyncCursor,
        10 => SyncKind::EventStreamAck,
        11 => SyncKind::SyncSnapshot,
        13 => SyncKind::ConversationParticipants,
        _ => SyncKind::Unspecified,
    }
}

fn message_to_sync_item(message: &Message) -> Result<SyncSliceItem, FlareError> {
    let skip_reason = message
        .extra
        .get("__sync_skip")
        .cloned()
        .unwrap_or_default();
    if !skip_reason.trim().is_empty() {
        return Ok(SyncSliceItem {
            seq: message.seq,
            created_at: message.timestamp.clone(),
            payload: Vec::new(),
            kind: SyncSliceItemKind::Skip as i32,
            skip_reason,
        });
    }

    let mut payload = Vec::new();
    message
        .encode(&mut payload)
        .map_err(|e| FlareError::system(format!("encode Message for SyncSliceItem: {e}")))?;
    Ok(SyncSliceItem {
        seq: message.seq,
        created_at: message.timestamp.clone(),
        payload,
        kind: SyncSliceItemKind::Message as i32,
        skip_reason: String::new(),
    })
}

struct ContiguousSyncPage {
    items: Vec<SyncSliceItem>,
    max_seq: u64,
    next_cursor: String,
    has_more: bool,
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
        .map(|message| message.seq)
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
        if message.seq <= after_seq || message.seq > page_end_seq {
            continue;
        }
        let seq = message.seq;
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
                seq,
                created_at: None,
                payload: Vec::new(),
                kind: SyncSliceItemKind::Tombstone as i32,
                skip_reason: "storage_missing_committed_seq".to_string(),
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
        participant_version: hint.participant_version,
        member_preview: if conversation_type == flare_proto::common::ConversationType::Single as i32
        {
            Vec::new()
        } else {
            hint.member_preview.clone()
        },
        ext,
    }
}

fn filter_events_by_types(events: &mut Vec<flare_proto::common::Event>, allowed: &[i32]) {
    if allowed.is_empty() {
        return;
    }
    let set: HashSet<i32> = allowed.iter().copied().collect();
    events.retain(|e| set.contains(&e.r#type));
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::conversation::ConversationBootstrapResponse;
    use flare_server_core::context::Context;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockInfra {
        bootstrap: ConversationBootstrapResponse,
        updates: Mutex<Vec<UpdateCursorRequest>>,
    }

    impl ConversationSyncPort for MockInfra {
        async fn conversation_bootstrap(
            &self,
            _ctx: &Ctx,
            _req: ConversationBootstrapRequest,
        ) -> Result<ConversationBootstrapResponse, FlareError> {
            Ok(self.bootstrap.clone())
        }

        async fn update_read_cursor(
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
    }

    impl StorageReadPort for MockInfra {
        async fn query_messages_by_seq(
            &self,
            _ctx: &Ctx,
            _conversation_id: &str,
            _after_seq: i64,
            _before_seq: i64,
            _limit: i32,
            _user_id: &str,
        ) -> Result<(Vec<Message>, i64), FlareError> {
            Ok((Vec::new(), 0))
        }

        async fn get_conversation_message_head(
            &self,
            _ctx: &Ctx,
            _conversation_id: &str,
        ) -> Result<crate::application::ports::StorageConversationMessageHead, FlareError> {
            Ok(Default::default())
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
            Ok(Default::default())
        }
    }

    fn ts(ms: i64) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: ms / 1000,
            nanos: ((ms % 1000) * 1_000_000) as i32,
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

    #[tokio::test]
    async fn conversations_sync_echoes_client_cursor_when_no_changes() {
        let infra = Arc::new(MockInfra {
            bootstrap: ConversationBootstrapResponse {
                conversations: vec![ConversationSummary {
                    conversation_id: "c1".to_string(),
                    max_seq: 10,
                    updated_at: Some(ts(1_000)),
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
                    client_conversation_cursor: Some(ts(2_000)),
                    limit: 100,
                    ..Default::default()
                },
            )
            .await
            .expect("conversations sync");

        assert!(response.conversations.is_empty());
        assert_eq!(response.server_conversation_cursor, Some(ts(2_000)));
        assert!(!response.has_more);
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
                },
            )
            .await
            .expect("get cursor");

        let cursor = response.cursor.expect("cursor");
        assert_eq!(cursor.conversation_id, "__conversations__");
        assert_eq!(cursor.last_sync_seq, 1_778_673_857_000);
    }
}
