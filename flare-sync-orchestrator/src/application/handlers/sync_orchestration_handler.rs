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
use flare_proto::common::sync::Payload as SyncPayload;
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::sync_slice_item::Payload as SyncSlicePayload;
use flare_proto::common::{
    ConversationDetailSync, ConversationDetailSyncRes, ConversationParticipant,
    ConversationParticipantsSync, ConversationParticipantsSyncRes, ConversationSummary,
    ConversationType as ProtoConversationType, ConversationUserSettingsSync,
    ConversationUserSettingsSyncRes, ConversationVersion, ConversationsSync, ConversationsSyncRes,
    EventEnvelope, EventEnvelopeDeliveryMode, EventStreamAckSyncRes, GetSyncCursorSync,
    GetSyncCursorSyncRes, MessagePreview, MultiConversationSync, MultiConversationSyncRes,
    MultiDeviceCursor, QueryEventsSync, QueryEventsSyncRes, SingleConversationSync,
    SingleConversationSyncRes, SnapshotConversationRow, SyncRes, SyncSessionHints, SyncSkipItem,
    SyncSliceItem, SyncSnapshotSync, SyncSnapshotSyncRes, SyncTombstoneItem, UpdateSyncCursorSync,
    UpdateSyncCursorSyncRes,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tracing::{debug, trace, warn};

use crate::application::error::require_nonempty_conversation_id;
use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, ConversationVersionIndexPort,
    MemorySyncCursorCache, StorageReadPort, SyncCursorCachePort,
};
use crate::domain::model::{
    SyncIntent, clamp_messages_per_conversation, clamp_query_events_limit,
    normalize_query_event_types,
};
use crate::domain::service::{
    build_snapshot_cursor, max_seq_from_events, parse_snapshot_cursor, snapshot_global_seq,
};

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
            let conversation_id = bootstrap.conversation_id.clone();
            let max_seq = bootstrap.max_conversation_seq as i64;
            let mut item = SnapshotConversationRow {
                conversation_id: conversation_id.clone(),
                messages: Vec::new(),
                last_conversation_seq: max_seq.max(0) as u64,
                last_message_at: bootstrap.updated_at,
                unread_count: (bootstrap.unread_count as i32).max(0),
                last_read_seq: bootstrap.last_read_seq,
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
                    item.last_conversation_seq = last_seq as u64;
                }
                if item.last_message_at <= 0 {
                    item.last_message_at = item
                        .messages
                        .iter()
                        .map(|m| m.created_at)
                        .max()
                        .unwrap_or_default();
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
                let patched_ms = m.row.last_message_at;
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
                });
                m.row
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
        // `after_conversation_seq`：客户端本地已应用的最后 conversation_seq。
        let (messages, _storage_last_seq) = self
            .infra
            .query_messages_by_seq(
                ctx,
                &conversation_id,
                req.after_conversation_seq as i64,
                0,
                limit + 1,
                user_id,
            )
            .await?;
        let head_max_seq = self
            .conversation_head_max_seq(ctx, &conversation_id, req.after_conversation_seq as i64)
            .await;
        let page = build_contiguous_sync_items(
            &conversation_id,
            req.after_conversation_seq,
            limit as usize,
            messages,
            head_max_seq as u64,
        )?;
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
        let mut slices = Vec::new();
        let mut max_seq_per_conversation = HashMap::new();
        let mut has_more = false;

        for cid in &req.conversation_ids {
            if cid.trim().is_empty() {
                continue;
            }
            let after = req
                .last_conversation_seq_per_conversation
                .get(cid)
                .copied()
                .unwrap_or(0) as i64;
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
            messages_per_conversation: limit,
            include_deleted: req.include_deleted,
            include_conversations: true,
            snapshot_cursor: if is_cold_start {
                String::new()
            } else {
                client_cursor.clone()
            },
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

fn message_preview(message: Option<&Message>, sent_at: i64) -> Option<MessagePreview> {
    message.map(|m| MessagePreview {
        message_id: m.server_id.clone(),
        sender_id: m.sender_id.clone(),
        r#type: m.message_type,
        text: m
            .attributes
            .get("text_preview")
            .cloned()
            .unwrap_or_default(),
        created_at: sent_at,
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
        updates: Mutex<Vec<UpdateCursorRequest>>,
        settings_updates: Mutex<Vec<UpdateConversationUserSettingsRequest>>,
        version_changes: Vec<crate::application::ports::ConversationVersionChange>,
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
}
