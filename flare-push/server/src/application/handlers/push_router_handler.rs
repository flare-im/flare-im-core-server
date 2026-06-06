use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::access_gateway;
use flare_proto::common::{
    Ack, AckPayload, CustomData, CustomPayload, NotificationKind, NotificationMessage,
    NotificationPayload, NotificationPriority, PushAck, PushEnvelope, PushTaskEnvelope,
    PushTaskPayloadKind, SystemPayload, ack, notification_message, push_envelope,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use prost::Message as _;

use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::online::online_status_service::OnlineStatusService;

pub struct PushRouterHandler {
    online_status: Arc<OnlineStatusService>,
    publisher: Arc<PushServerMqPublisher>,
}

impl PushRouterHandler {
    pub fn new(
        online_status: Arc<OnlineStatusService>,
        publisher: Arc<PushServerMqPublisher>,
    ) -> Self {
        Self {
            online_status,
            publisher,
        }
    }

    fn gateway_options(
        options: &Option<flare_proto::common::PushOptions>,
        device_ids: &[String],
    ) -> Option<access_gateway::PushOptions> {
        if options.is_none() && device_ids.is_empty() {
            return None;
        }

        let common = options.as_ref();
        Some(access_gateway::PushOptions {
            require_ack: common.map(|o| o.require_ack).unwrap_or(false),
            priority: common.map(|o| o.priority).unwrap_or(5),
            expire_at_ms: common.and_then(|o| o.expire_at).unwrap_or_default(),
            offline_push: false,
            device_ids: device_ids.to_vec(),
            platforms: Vec::new(),
            delivery_mode: 0,
            allow_duplicate: false,
            enforce_order: false,
            shard_key: String::new(),
        })
    }

    fn ack_from_payload(envelope_id: &str, payload: AckPayload) -> Ack {
        let mut metadata = HashMap::new();
        metadata.insert("conversation_id".to_string(), payload.conversation_id);
        metadata.insert("ack_type".to_string(), payload.ack_type);

        Ack {
            ack_id: Some(envelope_id.to_string()),
            ack_at: Some(payload.ack_at),
            payload: Some(ack::Payload::Push(PushAck {
                window_id: payload.message_id,
                ack_seq: payload.seq,
                ack_id: Some(envelope_id.to_string()),
                ack_at: Some(payload.ack_at),
                device_id: None,
                attributes: metadata,
            })),
        }
    }

    fn notification_from_payload(payload: NotificationPayload) -> NotificationMessage {
        let mut attributes = payload.attributes;
        if !payload.icon.is_empty() {
            attributes.insert("icon".to_string(), payload.icon);
        }
        if !payload.sound.is_empty() {
            attributes.insert("sound".to_string(), payload.sound);
        }
        if !payload.click_action.is_empty() {
            attributes.insert("click_action".to_string(), payload.click_action);
        }

        NotificationMessage {
            kind: NotificationKind::Custom as i32,
            notification_id: payload.notification_id,
            title: payload.title,
            body: payload.body,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes,
            action: None,
            payload: None,
        }
    }

    fn notification_from_system(payload: SystemPayload) -> NotificationMessage {
        NotificationMessage {
            kind: NotificationKind::System as i32,
            notification_id: format!("system-{}", uuid::Uuid::new_v4()),
            title: payload.title,
            body: payload.content,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes: payload.attributes,
            action: None,
            payload: Some(notification_message::Payload::System(Default::default())),
        }
    }

    fn custom_from_payload(payload: CustomPayload) -> CustomData {
        CustomData {
            r#type: payload.custom_type,
            payload: payload.custom_data,
            attributes: payload.attributes,
        }
    }

    fn push_task_payload(
        envelope: &PushEnvelope,
        target_user_ids: Vec<String>,
    ) -> Result<(PushTaskPayloadKind, Vec<u8>, String)> {
        let options = Self::gateway_options(&envelope.options, &envelope.target_device_ids);
        let payload = envelope.payload.clone().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "push envelope payload missing")
                .build_error()
        })?;

        match payload {
            push_envelope::Payload::Ack(ack) => {
                let req = access_gateway::PushAckRequest {
                    user_ids: target_user_ids,
                    ack: Some(Self::ack_from_payload(&envelope.envelope_id, ack)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Ack,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
            push_envelope::Payload::Notification(notification) => {
                let message_id = if notification.notification_id.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    notification.notification_id.clone()
                };
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_payload(notification)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    message_id,
                ))
            }
            push_envelope::Payload::Custom(custom) => {
                let message_id = if custom.custom_type.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    format!("{}:{}", envelope.envelope_id, custom.custom_type)
                };
                let req = access_gateway::PushCustomRequest {
                    user_ids: target_user_ids,
                    custom_data: Some(Self::custom_from_payload(custom)),
                    options,
                };
                Ok((PushTaskPayloadKind::Custom, req.encode_to_vec(), message_id))
            }
            push_envelope::Payload::System(system) => {
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_system(system)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
        }
    }

    pub async fn handle_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushMessageRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let message = req.messages.first().cloned().unwrap_or_default();
        let conversation_id = message.conversation_id.clone();
        let message_id = message.server_id.clone();
        let priority = 5;
        let expire_at = None;

        let push_payload = req.encode_to_vec();
        let metadata = HashMap::new();

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::ServiceUnavailable,
                        "Failed to query online status",
                    )
                })?;

            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at,
                push_payload: push_payload.clone(),
                headers: metadata.clone(),
                payload_kind: PushTaskPayloadKind::Message as i32,
            };

            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish online push task",
                        )
                    })?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish offline push task",
                        )
                    })?;
            }
        }

        Ok(())
    }

    pub async fn handle_event(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let conversation_id = req
            .events
            .first()
            .map(|e| e.conversation_id.clone())
            .unwrap_or_default();
        let message_id = req
            .events
            .first()
            .map(|e| {
                if e.event_id.is_empty() {
                    format!("event-{}", uuid::Uuid::new_v4())
                } else {
                    e.event_id.clone()
                }
            })
            .unwrap_or_else(|| format!("event-{}", uuid::Uuid::new_v4()));

        let push_payload = req.encode_to_vec();

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::ServiceUnavailable,
                        "Failed to query online status",
                    )
                })?;

            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at,
                push_payload: push_payload.clone(),
                headers: HashMap::new(),
                payload_kind: PushTaskPayloadKind::Event as i32,
            };

            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish online push task",
                        )
                    })?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish offline push task",
                        )
                    })?;
            }
        }

        Ok(())
    }

    /// 处理统一推送信封
    ///
    /// ## 设计
    /// - 统一处理 ACK、通知、CustomData、系统消息
    /// - 支持全量推送、用户列表推送、设备列表推送
    pub async fn handle_push_envelope(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: PushEnvelope,
    ) -> Result<()> {
        // 提取推送选项
        let priority = envelope.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = envelope.options.as_ref().and_then(|o| o.expire_at);

        // 根据目标类型处理
        let target_user_ids = match flare_proto::PushTargetType::try_from(envelope.target_type) {
            Ok(flare_proto::PushTargetType::All) => {
                // 全量推送：需要查询所有在线用户
                // TODO: 实现全量推送逻辑
                tracing::warn!("Full broadcast push not yet implemented");
                return Ok(());
            }
            Ok(flare_proto::PushTargetType::Users) => envelope.target_user_ids.clone(),
            Ok(flare_proto::PushTargetType::Devices) => {
                // 设备级推送：需要从设备ID反查用户ID
                // TODO: 实现设备级推送逻辑
                tracing::warn!("Device-level push not yet implemented");
                return Ok(());
            }
            _ => envelope.target_user_ids.clone(),
        };

        if target_user_ids.is_empty() {
            return Ok(());
        }

        let (payload_kind, push_payload, message_id) =
            Self::push_task_payload(&envelope, target_user_ids.clone())?;
        let conversation_id = String::new();

        // 为每个用户创建推送任务
        for user_id in &target_user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::ServiceUnavailable,
                        "Failed to query online status",
                    )
                })?;

            let task = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: envelope.tenant_id.clone(),
                priority,
                expire_at,
                push_payload: push_payload.clone(),
                headers: envelope.headers.clone(),
                payload_kind: payload_kind as i32,
            };

            let payload = task.encode_to_vec();

            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish online push task",
                        )
                    })?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish offline push task",
                        )
                    })?;
            }
        }

        Ok(())
    }
}
