//! Proto 与领域模型互转（仅用于 interface 与 infrastructure 边界，application/domain 不依赖 proto）
//! 与 event_bus / topic_envelope.proto 对齐：MessageEnvelope、TopicEventEnvelope 解析入口统一在此。

use crate::application::commands::ProcessStoreMessageCommand;
use crate::domain::model::{
    DeletePayload, EditPayload, Event, EventPayload, EventType, MarkPayload, PinPayload,
    ReactionPayload, ReadPayload, RecallPayload, RequestContext, TenantContext, UnmarkPayload,
    UnpinPayload,
};
use flare_proto::common;
use flare_proto::common::message_content::Content;
use flare_im_core::Ctx;

/// 统一从 flare-im-core 重导出，供本 crate 其他处使用
pub use flare_im_core::message::{message_from_proto, message_to_proto};

/// 从 proto Event 转为领域 Event（common::Event 无 tenant_id/operator_id，由 metadata 注入，此处填空）
pub fn event_from_proto(p: &flare_proto::common::Event) -> Event {
    let r#type = event_type_from_i32(p.r#type);
    let payload = p
        .payload
        .as_ref()
        .and_then(|pl| event_payload_from_proto(pl));
    Event {
        tenant_id: String::new(),
        conversation_id: p.conversation_id.clone(),
        seq: p.seq,
        r#type,
        created_at: p.created_at.clone(),
        operator_id: String::new(), // proto 无此字段，由调用方从 metadata 注入
        event_seq: p.event_seq,
        request_id: p.request_id.clone(),
        payload,
    }
}

/// 为 Kafka 消费到的操作事件补全 `tenant_id` / `operator_id`（common::Event 无此字段；信封亦无 tenant）。
pub fn enrich_operation_event_from_ctx(event: &mut Event, ctx: &Ctx) {
    if event.tenant_id.is_empty() {
        event.tenant_id = ctx
            .tenant_id()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
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
        seq: e.seq,
        r#type: e.r#type as i32,
        created_at: e.created_at.clone(),
        event_id: format!("{}:{}", e.conversation_id, e.seq),
        event_seq: e.event_seq,
        request_id: e.request_id.clone(),
        payload,
        ..Default::default()
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
            new_content: e.new_content.clone(),
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
            read_at: r.read_at.clone(),
            burn_after_read: r.burn_after_read,
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
            expire_at: p.expire_at.clone(),
        }),
        P::Unpin(u) => EventPayload::Unpin(UnpinPayload {
            server_msg_id: u.server_msg_id.clone(),
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
            ..Default::default()
        }),
        EventPayload::Edit(e) => P::Edit(common::MessageEditEvent {
            server_msg_id: e.server_msg_id.clone(),
            new_content: e.new_content.clone(),
            edit_version: e.edit_version,
            reason: e.reason.clone(),
            show_edited_mark: e.show_edited_mark,
            ..Default::default()
        }),
        EventPayload::Delete(d) => P::Delete(common::MessageDeleteEvent {
            server_msg_id: d.server_msg_id.clone(),
            delete_type: d.delete_type,
            scope: d.scope,
            target_user_id: d.target_user_id.clone(),
            reason: d.reason.clone(),
            notify_others: d.notify_others,
            ..Default::default()
        }),
        EventPayload::Read(r) => P::Read(common::ReadReceiptEvent {
            conversation_id: r.conversation_id.clone(),
            read_seq: r.read_seq,
            user_id: r.user_id.clone(),
            message_ids: r.message_ids.clone(),
            read_at: r.read_at.clone(),
            burn_after_read: r.burn_after_read,
            ..Default::default()
        }),
        EventPayload::Reaction(r) => P::Reaction(common::ReactionEvent {
            server_msg_id: r.server_msg_id.clone(),
            user_id: r.user_id.clone(),
            emoji: r.emoji.clone(),
            action: r.action,
            ..Default::default()
        }),
        EventPayload::Pin(p) => P::Pin(common::PinEvent {
            server_msg_id: p.server_msg_id.clone(),
            pinned_by: p.pinned_by.clone(),
            reason: p.reason.clone(),
            expire_at: p.expire_at.clone(),
            ..Default::default()
        }),
        EventPayload::Unpin(u) => P::Unpin(common::UnpinEvent {
            server_msg_id: u.server_msg_id.clone(),
            ..Default::default()
        }),
        EventPayload::Mark(m) => P::Mark(common::MarkEvent {
            server_msg_id: m.server_msg_id.clone(),
            user_id: m.user_id.clone(),
            mark_type: m.mark_type,
            color: m.color.clone(),
            ..Default::default()
        }),
        EventPayload::Unmark(u) => P::Unmark(common::UnmarkEvent {
            server_msg_id: u.server_msg_id.clone(),
            user_id: u.user_id.clone(),
            mark_type: u.mark_type,
            ..Default::default()
        }),
        EventPayload::Message(msg) => P::Message(message_to_proto(msg)),
        _ => return None,
    })
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
        tenant_id: tenant_id.unwrap_or("").to_string(),
        user_id: None,
    }
}

/// 从编辑内容 bytes 中解析出纯文本（用于更新 extra.content_text），不依赖在 domain 暴露 proto
pub fn content_bytes_to_text(bytes: &[u8]) -> Option<String> {
    let content = flare_proto::decode_message_content(bytes).ok()?;
    match content.content? {
        Content::Text(t) => Some(t.text),
        _ => None,
    }
}

/// TopicEventEnvelope 分发结果（与 event_bus EVENT_TYPE_* 对应）
#[derive(Debug)]
pub enum TopicEventDispatch {
    /// message.created → 持久化
    MessageCreated(ProcessStoreMessageCommand),
    /// operation.* → 操作落库
    Operation(Event),
    /// 忽略
    Unsupported,
}

/// 从 TopicEventEnvelope 分发为持久化命令或领域事件（供 event_bus / operation 消费者共用）
pub fn dispatch_topic_event_envelope(
    env: &flare_proto::common::TopicEventEnvelope,
) -> TopicEventDispatch {
    use flare_im_core::event::EVENT_TYPE_MESSAGE_CREATED;
    let event = match &env.event {
        Some(ev) => ev,
        None => return TopicEventDispatch::Unsupported,
    };
    if env.event_type == EVENT_TYPE_MESSAGE_CREATED {
        if let Some(flare_proto::common::event::Payload::Message(mut m)) = event.payload.clone() {
            if !env.tenant_id.is_empty() {
                m.extra
                    .insert("x-tenant-id".to_string(), env.tenant_id.clone());
            }
            return TopicEventDispatch::MessageCreated(message_command_from_proto(m));
        }
    }
    if env.event_type.starts_with("operation.") {
        let mut domain_event = event_from_proto(event);
        domain_event.tenant_id = env.tenant_id.clone();
        return TopicEventDispatch::Operation(domain_event);
    }
    TopicEventDispatch::Unsupported
}

/// 从 proto MessageEnvelope 构建应用层命令（envelope 内为 common.Message）
pub fn command_from_message_envelope(
    envelope: &flare_proto::common::MessageEnvelope,
) -> crate::application::commands::ProcessStoreMessageCommand {
    let mut msg = envelope
        .message
        .clone()
        .unwrap_or_else(flare_proto::common::Message::default);
    if msg.conversation_id.is_empty() {
        msg.conversation_id = envelope.conversation_id.clone();
    }
    if !envelope.tenant_id.is_empty() {
        msg.extra
            .insert("x-tenant-id".to_string(), envelope.tenant_id.clone());
    }
    msg.extra.insert(
        flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC.to_string(),
        envelope.sync.to_string(),
    );
    if let Ok(tags_json) = serde_json::to_string(&envelope.tags) {
        msg.extra.insert(
            flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS.to_string(),
            tags_json,
        );
    }
    for (k, v) in &envelope.metadata {
        msg.extra.insert(k.clone(), v.clone());
    }
    message_command_from_proto(msg)
}

/// 从 common.Message 构建应用层命令（envelope 在 extra：__sync、__tags、metadata）
pub fn message_command_from_proto(
    msg: flare_proto::common::Message,
) -> crate::application::commands::ProcessStoreMessageCommand {
    use crate::application::commands::ProcessStoreMessageCommand;
    let payload =
        flare_im_core::abstractions::storage_payload::StorageMessagePayload::from_message(msg);
    let message = payload.message.as_ref().map(message_from_proto);
    let tenant = payload
        .metadata
        .get("x-tenant-id")
        .or_else(|| payload.metadata.get("tenant_id"))
        .map(|t| TenantContext {
            tenant_id: t.clone(),
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
