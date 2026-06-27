//! Proto 与领域模型互转（仅用于 interface 与 infrastructure 边界，application/domain 不依赖 proto）
//! 与 event_bus / topic_envelope.proto 对齐：MQ payload 解析入口统一在此。

use crate::domain::model::{
    BurnScheduledPayload, BurnedPayload, DeletePayload, EditPayload, Event, EventPayload,
    EventType, HardDeletedPayload, MarkPayload, PinPayload, ReactionPayload, ReadPayload,
    RecallPayload, RequestContext, TenantContext, UnmarkPayload, UnpinPayload,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::{millis_to_timestamp, normalize_tenant_id, timestamp_to_millis};
use flare_proto::common;
use flare_proto::common::message_content::Content;
use prost::Message as ProstMessage;

/// 统一从 flare-im-core 重导出，供本 crate 其他处使用
pub use flare_im_contracts::message::{message_from_proto, message_to_proto};

/// 从 proto Event 转为领域 Event（common::Event 无 tenant_id/operator_id，由 metadata 注入，此处填空）
pub fn event_from_proto(p: &flare_proto::common::Event) -> Event {
    let r#type = event_type_from_i32(p.r#type);
    let payload = p.payload.as_ref().and_then(event_payload_from_proto);
    Event {
        tenant_id: String::new(),
        conversation_id: p.conversation_id.clone(),
        seq: p.conversation_seq,
        r#type,
        created_at: if p.created_at > 0 {
            millis_to_timestamp(p.created_at)
        } else {
            None
        },
        operator_id: String::new(), // proto 无此字段，由调用方从 metadata 注入
        event_seq: None,
        request_id: p.request_id.clone(),
        payload,
    }
}

/// 为 JetStream 消费到的操作事件补全 `tenant_id` / `operator_id`（common::Event 无此字段；信封亦无 tenant）。
pub fn enrich_operation_event_from_ctx(event: &mut Event, ctx: &Ctx) {
    if event.tenant_id.is_empty() {
        event.tenant_id = ctx
            .tenant_id()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string());
    }
    if event.operator_id.is_empty() {
        event.operator_id = ctx
            .user_id()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_default();
    }
}

/// 从领域 Event 转为 proto Event（用于持久化；common::Event 无 tenant_id/operator_id，operator 由 metadata 注入）
pub fn event_to_proto(e: &Event) -> flare_proto::common::Event {
    let payload = e.payload.as_ref().and_then(event_payload_to_proto);
    flare_proto::common::Event {
        conversation_id: e.conversation_id.clone(),
        conversation_seq: e.seq,
        r#type: e.r#type as i32,
        created_at: e
            .created_at
            .as_ref()
            .and_then(timestamp_to_millis)
            .unwrap_or_default(),
        event_id: format!("{}:{}", e.conversation_id, e.seq),
        request_id: e.request_id.clone(),
        payload,
    }
}

fn event_type_from_i32(v: i32) -> EventType {
    match common::EventType::try_from(v) {
        Ok(common::EventType::EventMessage) => EventType::Message,
        Ok(common::EventType::EventMessageRecall) => EventType::MessageRecall,
        Ok(common::EventType::EventMessageEdit) => EventType::MessageEdit,
        Ok(common::EventType::EventMessageDelete) => EventType::MessageDelete,
        Ok(common::EventType::EventReadReceipt) => EventType::ReadReceipt,
        Ok(common::EventType::EventReaction) => EventType::Reaction,
        Ok(common::EventType::EventPin) => EventType::Pin,
        Ok(common::EventType::EventUnpin) => EventType::Unpin,
        Ok(common::EventType::EventMark) => EventType::Mark,
        Ok(common::EventType::EventUnmark) => EventType::Unmark,
        Ok(common::EventType::EventMessageRetentionScheduled) => EventType::MessageBurnScheduled,
        Ok(common::EventType::EventMessageRetentionExpired) => EventType::MessageBurned,
        Ok(common::EventType::EventMessageRetentionPurged) => EventType::MessageHardDeleted,
        _ => EventType::Unspecified,
    }
}

fn event_payload_from_proto(p: &flare_proto::common::event::Payload) -> Option<EventPayload> {
    use flare_proto::common::event::Payload as P;
    Some(match p {
        P::Recall(r) => EventPayload::Recall(RecallPayload {
            server_msg_id: r.server_msg_id.clone(),
            reason: r.reason.clone(),
            time_limit_seconds: r.time_limit_seconds,
            allow_admin_recall: r.allow_admin_recall,
        }),
        P::Edit(e) => EventPayload::Edit(EditPayload {
            server_msg_id: e.server_msg_id.clone(),
            new_content: e
                .new_content
                .as_ref()
                .and_then(|content| {
                    let mut bytes = Vec::new();
                    content.encode(&mut bytes).ok()?;
                    Some(bytes)
                })
                .unwrap_or_default(),
            edit_version: e.edit_version,
            reason: e.reason.clone(),
            show_edited_mark: e.show_edited_mark,
        }),
        P::Delete(d) => EventPayload::Delete(DeletePayload {
            server_msg_id: d.server_msg_id.clone(),
            delete_type: d.delete_type,
            scope: d.scope,
            target_user_id: d.target_user_id.clone(),
            reason: d.reason.clone(),
            notify_others: d.notify_others,
        }),
        P::Read(r) => EventPayload::Read(ReadPayload {
            conversation_id: r.conversation_id.clone(),
            read_seq: r.read_seq,
            user_id: r.user_id.clone(),
            message_ids: r.message_ids.clone(),
            read_at: r.read_at.and_then(millis_to_timestamp),
            burn_after_read: None,
        }),
        P::Reaction(r) => EventPayload::Reaction(ReactionPayload {
            server_msg_id: r.server_msg_id.clone(),
            user_id: r.user_id.clone(),
            emoji: r.emoji.clone(),
            action: r.action,
        }),
        P::Pin(p) => EventPayload::Pin(PinPayload {
            server_msg_id: p.server_msg_id.clone(),
            pinned_by: p.pinned_by.clone(),
            reason: p.reason.clone(),
            expire_at: p.expire_at.map(timestamp_seconds),
            scope: p.scope,
        }),
        P::Unpin(u) => EventPayload::Unpin(UnpinPayload {
            server_msg_id: u.server_msg_id.clone(),
            unpinned_by: u.unpinned_by.clone(),
            scope: u.scope,
        }),
        P::Mark(m) => EventPayload::Mark(MarkPayload {
            server_msg_id: m.server_msg_id.clone(),
            user_id: m.user_id.clone(),
            mark_type: m.mark_type,
            color: m.color.clone(),
        }),
        P::Unmark(u) => EventPayload::Unmark(UnmarkPayload {
            server_msg_id: u.server_msg_id.clone(),
            user_id: u.user_id.clone(),
            mark_type: u.mark_type,
        }),
        P::RetentionScheduled(b) => EventPayload::BurnScheduled(BurnScheduledPayload {
            tenant_id: String::new(),
            conversation_id: b.conversation_id.clone(),
            message_id: b.server_msg_id.clone(),
            server_id: b.server_msg_id.clone(),
            seq: None,
            reader_id: b.reader_id.clone(),
            burn_at: b
                .state
                .as_ref()
                .and_then(|state| state.expire_at)
                .unwrap_or_default(),
            event_time: b.scheduled_at,
        }),
        P::RetentionExpired(b) => EventPayload::Burned(BurnedPayload {
            tenant_id: String::new(),
            conversation_id: b.conversation_id.clone(),
            message_id: b.server_msg_id.clone(),
            server_id: b.server_msg_id.clone(),
            seq: None,
            reader_id: b.reader_id.clone(),
            burn_at: b
                .state
                .as_ref()
                .and_then(|state| state.expire_at)
                .unwrap_or_default(),
            burned_at: b.expired_at,
            event_time: b.expired_at,
        }),
        P::RetentionPurged(b) => EventPayload::HardDeleted(HardDeletedPayload {
            tenant_id: String::new(),
            conversation_id: b.conversation_id.clone(),
            message_id: b.server_msg_id.clone(),
            server_id: b.server_msg_id.clone(),
            seq: None,
            reader_id: b.reader_id.clone(),
            burn_at: b.state.as_ref().and_then(|state| state.expire_at),
            burned_at: b.state.as_ref().and_then(|state| state.expired_at),
            event_time: b.purged_at,
        }),
        _ => return None,
    })
}

fn event_payload_to_proto(p: &EventPayload) -> Option<flare_proto::common::event::Payload> {
    use flare_proto::common::event::Payload as P;
    Some(match p {
        EventPayload::Recall(r) => P::Recall(common::MessageRecallEvent {
            server_msg_id: r.server_msg_id.clone(),
            reason: r.reason.clone(),
            time_limit_seconds: r.time_limit_seconds,
            allow_admin_recall: r.allow_admin_recall,
        }),
        EventPayload::Edit(e) => P::Edit(common::MessageEditEvent {
            server_msg_id: e.server_msg_id.clone(),
            new_content: flare_proto::decode_message_content(&e.new_content).ok(),
            edit_version: e.edit_version,
            reason: e.reason.clone(),
            show_edited_mark: e.show_edited_mark,
        }),
        EventPayload::Delete(d) => P::Delete(common::MessageDeleteEvent {
            server_msg_id: d.server_msg_id.clone(),
            delete_type: d.delete_type,
            scope: d.scope,
            target_user_id: d.target_user_id.clone(),
            reason: d.reason.clone(),
            notify_others: d.notify_others,
        }),
        EventPayload::Read(r) => P::Read(common::ReadReceiptEvent {
            conversation_id: r.conversation_id.clone(),
            read_seq: r.read_seq,
            user_id: r.user_id.clone(),
            message_ids: r.message_ids.clone(),
            read_at: r.read_at.as_ref().and_then(timestamp_to_millis),
        }),
        EventPayload::Reaction(r) => P::Reaction(common::ReactionEvent {
            server_msg_id: r.server_msg_id.clone(),
            user_id: r.user_id.clone(),
            emoji: r.emoji.clone(),
            action: r.action,
        }),
        EventPayload::Pin(p) => P::Pin(common::PinEvent {
            server_msg_id: p.server_msg_id.clone(),
            pinned_by: p.pinned_by.clone(),
            reason: p.reason.clone(),
            expire_at: p.expire_at.as_ref().map(|ts| ts.seconds),
            scope: p.scope,
        }),
        EventPayload::Unpin(u) => P::Unpin(common::UnpinEvent {
            server_msg_id: u.server_msg_id.clone(),
            unpinned_by: u.unpinned_by.clone(),
            scope: u.scope,
        }),
        EventPayload::Mark(m) => P::Mark(common::MarkEvent {
            server_msg_id: m.server_msg_id.clone(),
            user_id: m.user_id.clone(),
            mark_type: m.mark_type,
            color: m.color.clone(),
        }),
        EventPayload::Unmark(u) => P::Unmark(common::UnmarkEvent {
            server_msg_id: u.server_msg_id.clone(),
            user_id: u.user_id.clone(),
            mark_type: u.mark_type,
        }),
        EventPayload::BurnScheduled(b) => {
            P::RetentionScheduled(common::MessageRetentionScheduledEvent {
                conversation_id: b.conversation_id.clone(),
                server_msg_id: b.message_id.clone(),
                reader_id: b.reader_id.clone(),
                policy: Some(retention_policy_at(b.burn_at)),
                state: Some(retention_state_scheduled(b.reader_id.clone(), b.burn_at)),
                scheduled_at: b.event_time,
            })
        }
        EventPayload::Burned(b) => P::RetentionExpired(common::MessageRetentionExpiredEvent {
            conversation_id: b.conversation_id.clone(),
            server_msg_id: b.message_id.clone(),
            reader_id: b.reader_id.clone(),
            state: Some(retention_state_expired(
                b.reader_id.clone(),
                b.burn_at,
                b.burned_at,
            )),
            expired_at: b.burned_at,
        }),
        EventPayload::HardDeleted(b) => P::RetentionPurged(common::MessageRetentionPurgedEvent {
            conversation_id: b.conversation_id.clone(),
            server_msg_id: b.message_id.clone(),
            reader_id: b.reader_id.clone(),
            state: Some(retention_state_purged(
                b.reader_id.clone(),
                b.burn_at,
                b.burned_at,
                b.event_time,
            )),
            purged_at: b.event_time,
        }),
        EventPayload::Message(msg) => P::Message(message_to_proto(msg)),
        _ => return None,
    })
}

fn retention_policy_at(expire_at: i64) -> common::MessageRetentionPolicy {
    common::MessageRetentionPolicy {
        mode: common::RetentionMode::AfterRead as i32,
        trigger: common::RetentionTrigger::AfterRead as i32,
        expire_after_seconds: None,
        expire_at: Some(expire_at),
        visibility_after_expiration: common::ContentVisibility::Redacted as i32,
        attributes: Default::default(),
    }
}

fn retention_state_scheduled(
    reader_id: Option<String>,
    expire_at: i64,
) -> common::MessageRetentionState {
    common::MessageRetentionState {
        lifecycle: common::MessageRetentionLifecycle::Scheduled as i32,
        content_visibility: common::ContentVisibility::Available as i32,
        first_triggered_at: None,
        expire_at: Some(expire_at),
        expired_at: None,
        purged_at: None,
        triggered_by_user_id: reader_id,
    }
}

fn retention_state_expired(
    reader_id: Option<String>,
    expire_at: i64,
    expired_at: i64,
) -> common::MessageRetentionState {
    common::MessageRetentionState {
        lifecycle: common::MessageRetentionLifecycle::Expired as i32,
        content_visibility: common::ContentVisibility::Redacted as i32,
        first_triggered_at: None,
        expire_at: Some(expire_at),
        expired_at: Some(expired_at),
        purged_at: None,
        triggered_by_user_id: reader_id,
    }
}

fn retention_state_purged(
    reader_id: Option<String>,
    expire_at: Option<i64>,
    expired_at: Option<i64>,
    purged_at: i64,
) -> common::MessageRetentionState {
    common::MessageRetentionState {
        lifecycle: common::MessageRetentionLifecycle::Purged as i32,
        content_visibility: common::ContentVisibility::Purged as i32,
        first_triggered_at: None,
        expire_at,
        expired_at,
        purged_at: Some(purged_at),
        triggered_by_user_id: reader_id,
    }
}

fn timestamp_seconds(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

/// 从 request_id 构建领域 RequestContext（与 flare_server_core Context 对齐，无 proto 依赖）
pub fn request_context_from_request_id(request_id: Option<&str>) -> RequestContext {
    RequestContext {
        request_id: request_id.unwrap_or("").to_string(),
        device_id: None,
        platform: None,
    }
}

/// 从 tenant_id 构建领域 TenantContext（与 flare_server_core Context 对齐，无 proto 依赖）
pub fn tenant_context_from_tenant_id(tenant_id: Option<&str>) -> TenantContext {
    TenantContext {
        tenant_id: tenant_id.map(normalize_tenant_id).unwrap_or_default(),
        user_id: None,
    }
}

/// 从编辑内容 bytes 中解析出纯文本（用于更新 extra.contentText），不依赖在 domain 暴露 proto
pub fn content_bytes_to_text(bytes: &[u8]) -> Option<String> {
    let content = flare_proto::decode_message_content(bytes).ok()?;
    match content.content? {
        Content::Text(t) => Some(t.text),
        _ => None,
    }
}

/// 从 common.Message 构建应用层命令（envelope 在 extra：__sync、__tags、metadata）
pub fn message_command_from_proto(
    msg: flare_proto::common::Message,
) -> crate::application::commands::ProcessStoreMessageCommand {
    use crate::application::commands::ProcessStoreMessageCommand;
    let payload =
        flare_im_contracts::abstractions::storage_payload::StorageMessagePayload::from_message(msg);
    let message = payload.message.as_ref().map(message_from_proto);
    let tenant = payload
        .metadata
        .get("x-tenant-id")
        .or_else(|| payload.metadata.get("tenant_id"))
        .map(|t| TenantContext {
            tenant_id: normalize_tenant_id(t),
            user_id: None,
        });
    ProcessStoreMessageCommand {
        conversation_id: payload.conversation_id,
        message,
        sync: payload.sync,
        context: None,
        tenant,
        tags: payload.tags,
        metadata: payload.metadata,
    }
}
