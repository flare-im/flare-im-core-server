use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::access_gateway;
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_proto::common::{PushEnvelope, PushPayloadKind, PushTaskEnvelope, PushTaskPayloadKind};
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
        let expire_at_ms = 0;

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
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: metadata.clone(),
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
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: HashMap::new(),
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
        let expire_at_ms = envelope
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .unwrap_or(0);

        // 根据目标类型处理
        let target_user_ids =
            match flare_proto::common::PushTargetType::try_from(envelope.target_type) {
                Ok(flare_proto::common::PushTargetType::All) => {
                    // 全量推送：需要查询所有在线用户
                    // TODO: 实现全量推送逻辑
                    tracing::warn!("Full broadcast push not yet implemented");
                    return Ok(());
                }
                Ok(flare_proto::common::PushTargetType::Users) => envelope.target_user_ids.clone(),
                Ok(flare_proto::common::PushTargetType::Devices) => {
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

        // 序列化 PushEnvelope 作为推送载荷
        let push_payload = envelope.encode_to_vec();
        let message_id = envelope.envelope_id.clone();
        let conversation_id = String::new();

        // 转换 payload_kind
        let payload_kind = match PushPayloadKind::try_from(envelope.payload_kind) {
            Ok(PushPayloadKind::Ack) => PushTaskPayloadKind::Ack as i32,
            Ok(PushPayloadKind::Notification) => PushTaskPayloadKind::Notification as i32,
            Ok(PushPayloadKind::Custom) => PushTaskPayloadKind::Custom as i32,
            Ok(PushPayloadKind::System) => PushTaskPayloadKind::Custom as i32, // 系统消息映射为 Custom
            _ => PushTaskPayloadKind::Unspecified as i32,
        };

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
                expire_at_ms,
                push_payload: push_payload.clone(),
                metadata: envelope.headers.clone(),
                payload_kind,
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
