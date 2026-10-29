//! Proto 与领域模型互转（仅用于 interface 与 infrastructure 边界，application/domain 不依赖 proto）
//! 与 event_bus / topic_envelope.proto 对齐：MQ payload 解析入口统一在此。

use crate::domain::model::{
    BurnScheduledPayload, BurnedPayload, DeletePayload, EditPayload, Event, EventPayload,
    EventType, HardDeletedPayload, MarkPayload, PinPayload, PluginStatePayload, ReactionPayload,
    ReadPayload, RecallPayload, RequestContext, TenantContext, UnmarkPayload, UnpinPayload,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::{
    millis_to_timestamp, normalize_optional_tenant_id, normalize_tenant_id, timestamp_to_millis,
};
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

/// 操作事件的消息头里没有租户：拒绝落库，由消费者投死信（不再按租户 `"0"` 写）。
pub const OPERATION_EVENT_WITHOUT_TENANT: &str = "operation event without a tenant header";

/// 为 JetStream 消费到的操作事件补全 `tenant_id` / `operator_id`（common::Event 无此字段）。
///
/// 租户只取生产方按已验证上下文写的消息头（`ctx`）；取不到返回
/// [`OPERATION_EVENT_WITHOUT_TENANT`]。显式的租户 `"0"` 照常处理。
pub fn enrich_operation_event_from_ctx(event: &mut Event, ctx: &Ctx) -> Result<(), &'static str> {
    if event.tenant_id.trim().is_empty() {
        event.tenant_id =
            normalize_optional_tenant_id(ctx.tenant_id()).ok_or(OPERATION_EVENT_WITHOUT_TENANT)?;
    }
    if event.operator_id.is_empty() {
        event.operator_id = ctx
            .user_id()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_default();
    }
    Ok(())
}

/// 从领域 Event 转为 proto Event（用于持久化；common::Event 无 tenant_id/operator_id，operator 由 metadata 注入）
pub fn event_to_proto(e: &Event) -> flare_proto::common::Event {
    let payload = e.payload.as_ref().and_then(event_payload_to_proto);
    flare_proto::common::Event {
        conversation_id: e.conversation_id.clone(),
        conversation_seq: e.seq,
        r#type: event_type_to_proto_i32(e.r#type),
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

/// 领域事件类型 → `flare.common.v1.EventType` 的取值（`events.event_type` 列与 `Event.type` 存的都是它）。
///
/// 领域枚举的判别值与 proto 不同（领域多了 Typing/Presence，从 6 起错位），不能用
/// `as i32`：以前这里直接 `e.r#type as i32`，反应事件（领域 11）落成 `EVENT_MARK`（11），置顶（12）
/// 落成 `EVENT_UNMARK`，Reader 按 proto 类型解码内层载荷，反应解不开（W10-S1 F1 的
/// `decode Mark event payload`），置顶则被静默解成取消标记。proto 没有的领域类型记为未指定。
fn event_type_to_proto_i32(t: EventType) -> i32 {
    use common::EventType as P;
    let proto = match t {
        EventType::Message => P::EventMessage,
        EventType::MessageRecall => P::EventMessageRecall,
        EventType::MessageEdit => P::EventMessageEdit,
        EventType::MessageDelete => P::EventMessageDelete,
        EventType::ReadReceipt => P::EventReadReceipt,
        EventType::ConversationUpdate => P::EventConversationUpdate,
        EventType::ConversationDelete => P::EventConversationDelete,
        EventType::Reaction => P::EventReaction,
        EventType::Pin => P::EventPin,
        EventType::Unpin => P::EventUnpin,
        EventType::Mark => P::EventMark,
        EventType::Unmark => P::EventUnmark,
        EventType::MessageBurnScheduled => P::EventMessageRetentionScheduled,
        EventType::MessageBurned => P::EventMessageRetentionExpired,
        EventType::MessageHardDeleted => P::EventMessageRetentionPurged,
        EventType::PluginState => P::EventMessagePluginState,
        EventType::Custom => P::EventCustom,
        EventType::Typing | EventType::Presence | EventType::Unspecified => P::Unspecified,
    };
    proto as i32
}

fn event_type_from_i32(v: i32) -> EventType {
    match common::EventType::try_from(v) {
        Ok(common::EventType::EventMessage) => EventType::Message,
        Ok(common::EventType::EventMessageRecall) => EventType::MessageRecall,
        Ok(common::EventType::EventMessageEdit) => EventType::MessageEdit,
        Ok(common::EventType::EventMessageDelete) => EventType::MessageDelete,
        Ok(common::EventType::EventReadReceipt) => EventType::ReadReceipt,
        Ok(common::EventType::EventConversationUpdate) => EventType::ConversationUpdate,
        Ok(common::EventType::EventConversationDelete) => EventType::ConversationDelete,
        Ok(common::EventType::EventReaction) => EventType::Reaction,
        Ok(common::EventType::EventPin) => EventType::Pin,
        Ok(common::EventType::EventUnpin) => EventType::Unpin,
        Ok(common::EventType::EventMark) => EventType::Mark,
        Ok(common::EventType::EventUnmark) => EventType::Unmark,
        Ok(common::EventType::EventMessageRetentionScheduled) => EventType::MessageBurnScheduled,
        Ok(common::EventType::EventMessageRetentionExpired) => EventType::MessageBurned,
        Ok(common::EventType::EventMessageRetentionPurged) => EventType::MessageHardDeleted,
        Ok(common::EventType::EventCustom) => EventType::Custom,
        Ok(common::EventType::EventMessagePluginState) => EventType::PluginState,
        Ok(common::EventType::Unspecified) | Err(_) => EventType::Unspecified,
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
        P::PluginState(s) => EventPayload::PluginState(PluginStatePayload {
            server_msg_id: s.server_msg_id.clone(),
            plugin_id: s.plugin_id.clone(),
            entity: s.entity.clone(),
            version: s.version,
            values_json: s.values_json.clone(),
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
        EventPayload::PluginState(s) => P::PluginState(common::PluginStateEvent {
            server_msg_id: s.server_msg_id.clone(),
            plugin_id: s.plugin_id.clone(),
            entity: s.entity.clone(),
            version: s.version,
            values_json: s.values_json.clone(),
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
///
/// 不从消息体确定租户：`attributes` 是发送方提交、PreSend Hook 可整体改写的数据，其中的
/// `tenant_id`/`x-tenant-id` 不是租户来源。`tenant` 留空，由消费者按消息头填（见
/// `MessageCreatedHandler`）。
pub fn message_command_from_proto(
    msg: flare_proto::common::Message,
) -> crate::application::commands::ProcessStoreMessageCommand {
    use crate::application::commands::ProcessStoreMessageCommand;
    let payload =
        flare_im_contracts::abstractions::storage_payload::StorageMessagePayload::from_message(msg);
    let message = payload.message.as_ref().map(message_from_proto);
    ProcessStoreMessageCommand {
        conversation_id: payload.conversation_id,
        message,
        sync: payload.sync,
        context: None,
        tenant: None,
        tags: payload.tags,
        metadata: payload.metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::utils::Context;
    use std::sync::Arc;

    fn operation_event() -> Event {
        event_from_proto(&flare_proto::common::Event {
            conversation_id: "conv-a".to_string(),
            conversation_seq: 3,
            r#type: common::EventType::EventMessageRecall as i32,
            ..Default::default()
        })
    }

    /// 操作事件的租户只取消息头；缺失时拒绝，不再记作租户 "0"（W11-T）。
    #[test]
    fn operation_event_tenant_comes_from_the_header_context_only() {
        let mut event = operation_event();
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id("tenant-a"));
        enrich_operation_event_from_ctx(&mut event, &ctx).expect("tenant present");
        assert_eq!(event.tenant_id, "tenant-a");

        let mut zero = operation_event();
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id("0"));
        enrich_operation_event_from_ctx(&mut zero, &ctx).expect("explicit tenant 0");
        assert_eq!(zero.tenant_id, "0");

        for ctx in [
            Arc::new(Context::with_request_id("req")),
            Arc::new(Context::with_request_id("req").with_tenant_id(" ")),
        ] {
            let mut event = operation_event();
            assert_eq!(
                enrich_operation_event_from_ctx(&mut event, &ctx),
                Err(OPERATION_EVENT_WITHOUT_TENANT)
            );
            assert!(event.tenant_id.is_empty());
        }
    }

    #[test]
    fn message_attributes_never_set_the_command_tenant() {
        let msg = flare_proto::common::Message {
            server_id: "msg-a".to_string(),
            conversation_id: "conv-a".to_string(),
            attributes: [
                ("tenant_id".to_string(), "tenant-b".to_string()),
                ("x-tenant-id".to_string(), "tenant-c".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert!(message_command_from_proto(msg).tenant.is_none());
    }

    fn typed_event(r#type: EventType) -> Event {
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            seq: 9,
            r#type,
            created_at: None,
            operator_id: String::new(),
            event_seq: None,
            request_id: None,
            payload: None,
        }
    }

    /// 落库的 `Event.type` 是 proto `EventType` 的取值，不是领域枚举的判别值（W10-S1 F1：反应
    /// 曾落成 `EVENT_MARK`）。期望值照 `event.proto` 写死成数字，不用同一张映射表现算；proto 有的
    /// 类型反向映射回原领域类型。
    #[test]
    fn persisted_event_type_is_the_proto_enum_value() {
        let cases = [
            (EventType::Message, 1),
            (EventType::MessageRecall, 2),
            (EventType::MessageEdit, 3),
            (EventType::MessageDelete, 4),
            (EventType::ReadReceipt, 5),
            (EventType::ConversationUpdate, 6),
            (EventType::ConversationDelete, 7),
            (EventType::Reaction, 8),
            (EventType::Pin, 9),
            (EventType::Unpin, 10),
            (EventType::Mark, 11),
            (EventType::Unmark, 12),
            (EventType::MessageBurnScheduled, 13),
            (EventType::MessageBurned, 14),
            (EventType::MessageHardDeleted, 15),
            (EventType::Custom, 16),
            (EventType::PluginState, 17),
            (EventType::Typing, 0),
            (EventType::Presence, 0),
            (EventType::Unspecified, 0),
        ];
        for (domain, proto) in cases {
            assert_eq!(
                event_to_proto(&typed_event(domain)).r#type,
                proto,
                "{domain:?}"
            );
            if proto != 0 {
                assert_eq!(event_type_from_i32(proto), domain, "{proto}");
            }
        }
        assert_eq!(event_type_from_i32(0), EventType::Unspecified);
        assert_eq!(event_type_from_i32(99), EventType::Unspecified);
    }
}
