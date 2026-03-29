use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use flare_proto::access_gateway;
use flare_proto::common::{NotificationMessage, PushTaskEnvelope, PushTaskPayloadKind};
use flare_proto::push::{PushCustomRequest, PushMessageRequest, PushNotificationRequest};
use prost::Message as _;

use crate::domain::merge_envelope_metadata;
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

    pub async fn handle_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: PushMessageRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let message = req.message.clone().unwrap_or_default();
        let conversation_id = message.conversation_id.clone();
        let message_id = message.server_id.clone();
        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at_ms = 0;

        let ag_req = access_gateway::PushMessageRequest {
            user_ids: req.user_ids.clone(),
            messages: vec![message],
            options: None,
        };
        let push_payload = ag_req.encode_to_vec();
        let metadata = merge_envelope_metadata(&req.options, &req.metadata);

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .unwrap_or(false);

            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: metadata.clone(),
                payload_kind: PushTaskPayloadKind::Message as i32,
            };

            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
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
        let expire_at_ms = req.options.as_ref().map(|o| o.expire_at_ms).unwrap_or(0);
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
                .unwrap_or(false);

            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: HashMap::new(),
                payload_kind: PushTaskPayloadKind::Event as i32,
            };

            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn handle_notification(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: PushNotificationRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at_ms = 0;
        let message_id = format!("notif-{}", uuid::Uuid::new_v4());
        let conversation_id = String::new();

        let common_notification = req.notification.map(|n| NotificationMessage {
            kind: 6,
            notification_id: uuid::Uuid::new_v4().to_string(),
            title: n.title,
            body: n.body,
            priority: 2,
            expire_at: None,
            created_at: None,
            action: if n.click_action.is_empty() {
                None
            } else {
                Some(flare_proto::common::notification_message::Action::Deeplink(
                    n.click_action,
                ))
            },
            payload: None,
            extra: n.metadata,
        });

        let ag_req = access_gateway::PushNotificationRequest {
            user_ids: req.user_ids.clone(),
            notification: common_notification,
            options: None,
        };
        let push_payload = ag_req.encode_to_vec();
        let metadata = merge_envelope_metadata(&req.options, &req.metadata);

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .unwrap_or(false);

            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: metadata.clone(),
                payload_kind: PushTaskPayloadKind::Notification as i32,
            };

            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn handle_custom(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: PushCustomRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at_ms = 0;
        let message_id = format!("custom-{}", uuid::Uuid::new_v4());
        let conversation_id = String::new();

        let ag_req = access_gateway::PushCustomRequest {
            user_ids: req.user_ids.clone(),
            custom_data: req.custom_data,
            options: None,
        };
        let push_payload = ag_req.encode_to_vec();
        let metadata = merge_envelope_metadata(&req.options, &req.metadata);

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .unwrap_or(false);
            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: metadata.clone(),
                payload_kind: PushTaskPayloadKind::Custom as i32,
            };
            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn handle_ack(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushAckRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at_ms = req.options.as_ref().map(|o| o.expire_at_ms).unwrap_or(0);
        let conversation_id = req
            .ack
            .as_ref()
            .and_then(|a| a.payload.as_ref())
            .and_then(|p| match p {
                flare_proto::common::ack::Payload::Conversation(c) => {
                    Some(c.conversation_id.clone())
                }
                _ => None,
            })
            .unwrap_or_default();
        let message_id = req
            .ack
            .as_ref()
            .and_then(|a| a.ack_id.clone())
            .filter(|ack_id| !ack_id.is_empty())
            .unwrap_or_else(|| format!("ack-{}", uuid::Uuid::new_v4()));
        let push_payload = req.encode_to_vec();

        for user_id in &req.user_ids {
            let online = self
                .online_status
                .is_online(ctx, user_id)
                .await
                .unwrap_or(false);
            let env = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: message_id.clone(),
                conversation_id: conversation_id.clone(),
                tenant_id: self.online_status.default_tenant_id().to_string(),
                priority,
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: HashMap::new(),
                payload_kind: PushTaskPayloadKind::Ack as i32,
            };
            let payload = env.encode_to_vec();
            if online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            } else {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await?;
            }
        }

        Ok(())
    }
}
