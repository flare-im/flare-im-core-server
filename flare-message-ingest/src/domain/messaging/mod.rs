//! 主消息队列 `TOPIC_MESSAGE_MAIN`：JetStream 外层为 [`flare_server_core::eventbus::EventEnvelope`]（JSON），
//! `payload` 为 [`flare_proto::common::MqEnvelope`]（含 `recipient_user_ids` 与 `Message`/`Event` 二选一）。
//! `tenant_id` / `trace_id` / `request_id` 由外层 [`flare_server_core::eventbus::EventEnvelope`]、MQ 头或 [`flare_server_core::context::Ctx`] 传递。
//! Push Server 从本 Topic 解析 `MqEnvelope` 后组装 `PushMessageRequest` / `PushEventRequest`；仅推送、不经过主队列的场景仍走 `TOPIC_PUSH_MESSAGES` 等。

pub use flare_im_contracts::constants::topics::TOPIC_MESSAGE_MAIN;
pub use flare_im_contracts::event::{
    MqEnvelopeDecodeError, decode_mq_envelope, mq_envelope_for_main_queue_event,
    mq_envelope_for_main_queue_event_with_headers, mq_envelope_for_main_queue_message,
    mq_envelope_for_main_queue_message_with_headers,
};
pub use flare_proto::common::mq_envelope::Payload as MqPayload;
pub use flare_proto::common::{MqEnvelope, MqPayloadKind};
