//! 推送应用层处理器
//!
//! 职责：接收 proto 请求 → 转领域命令/参数 → 调用 PushDomainService（业务下沉领域层）→ 组装 proto 响应。
//! 业务逻辑在 PushDomainService 中实现，此处仅做编排与 DTO 转换。

use std::sync::Arc;

use chrono::Utc;
use flare_grpc_proto::access_gateway::{
    PushAckResponse, PushNotificationResponse, PushResponse, PushResult, UserPushResult,
};
use flare_im_contracts::Ctx;
use flare_proto::common::Message;
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use tracing::instrument;

use crate::application::commands::{
    PushAckCommand, PushCustomDataCommand, PushEventCommand, PushMessageCommand,
    PushNotificationCommand,
};
use crate::domain::service::{EventEnvelopePushRequest, PushDomainService};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

fn push_result_at_now(
    pushed_device_count: i32,
    offline_pending_count: i32,
    window_id: String,
) -> PushResult {
    PushResult {
        pushed_device_count,
        offline_pending_count,
        window_id,
        at: Some(Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        }),
    }
}

/// 推送处理器：门面，委托领域服务完成推送业务
pub struct PushHandler {
    push_domain_service: Arc<PushDomainService>,
}

impl PushHandler {
    pub fn new(push_domain_service: Arc<PushDomainService>) -> Self {
        Self {
            push_domain_service,
        }
    }

    /// PushMessage：将 `Message` 列表封装为 `MessagePush` 并下行至各用户在线连接。
    #[instrument(skip(self, req), fields(user_count = req.user_ids.len(), message_count = req.messages.len()))]
    pub async fn handle_push_message(
        &self,
        ctx: &Ctx,
        req: PushMessageCommand,
    ) -> Result<PushResponse> {
        if req.user_ids.is_empty() {
            return Err(ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "PushMessage: user_ids is empty",
            )
            .build_error());
        }
        if req.messages.is_empty() {
            return Err(ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "PushMessage: messages is empty",
            )
            .build_error());
        }

        let options = req.options.unwrap_or_default();
        let per_user = self
            .push_domain_service
            .push_message_push_to_users(ctx, &req.user_ids, req.messages, &options)
            .await?;

        let window_id = uuid::Uuid::new_v4().to_string();
        let at = Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        };

        let mut total_pushed: i32 = 0;
        let mut total_failed: i32 = 0;
        let mut total_offline: i32 = 0;
        let mut user_results: Vec<UserPushResult> = Vec::with_capacity(per_user.len());

        for (user_id, pushed, fail, offline) in per_user {
            total_pushed = total_pushed.saturating_add(pushed);
            total_failed = total_failed.saturating_add(fail);
            total_offline = total_offline.saturating_add(offline);
            user_results.push(UserPushResult {
                user_id,
                result: Some(PushResult {
                    pushed_device_count: pushed,
                    offline_pending_count: offline,
                    window_id: window_id.clone(),
                    at: Some(at),
                }),
            });
        }
        if total_failed > 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "PushMessage: failed to write to one or more online connections",
            )
            .details(format!(
                "pushed_device_count={total_pushed}, failed_connection_count={total_failed}, offline_pending_count={total_offline}"
            ))
            .build_error());
        }

        Ok(PushResponse {
            result: Some(PushResult {
                pushed_device_count: total_pushed,
                offline_pending_count: total_offline,
                window_id: window_id.clone(),
                at: Some(at),
            }),
            user_results,
        })
    }

    /// DeliverToConversation（统一读扩散）：把消息扇给"本网关节点订阅该会话的在线连接"。
    /// 上游无需解析收件人；无本地订阅者时为空操作（成功 0）。
    #[instrument(skip(self, messages), fields(message_count = messages.len()))]
    pub async fn handle_deliver_to_conversation(
        &self,
        ctx: &Ctx,
        conversation_id: String,
        messages: Vec<Message>,
    ) -> Result<PushResponse> {
        if conversation_id.trim().is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "DeliverToConversation: conversation_id is empty",
            )
            .build_error());
        }
        if messages.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "DeliverToConversation: messages is empty",
            )
            .build_error());
        }
        let (pushed, failed) = self
            .push_domain_service
            .push_message_to_conversation(ctx, &conversation_id, messages)
            .await?;
        if failed > 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "DeliverToConversation: failed to write to one or more connections",
            )
            .details(format!("pushed={pushed}, failed={failed}"))
            .build_error());
        }
        Ok(PushResponse {
            result: Some(push_result_at_now(
                pushed,
                0,
                uuid::Uuid::new_v4().to_string(),
            )),
            user_results: vec![],
        })
    }

    /// PushEvent：将 `Event` 列表封装为 `EventEnvelope` 下行（与 PushMessage 对称，客户端走同一 `PayloadCommand::Message` + 内层 proto 解码）。
    #[instrument(skip(self, req), fields(user_count = req.user_ids.len(), event_count = req.events.len()))]
    pub async fn handle_push_event(
        &self,
        ctx: &Ctx,
        req: PushEventCommand,
    ) -> Result<PushResponse> {
        if req.user_ids.is_empty() {
            return Err(ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "PushEvent: user_ids is empty",
            )
            .build_error());
        }
        if req.events.is_empty()
            && (req.conversation_id.trim().is_empty() || req.max_conversation_seq == 0)
        {
            return Err(ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "PushEvent: events is empty and ping fields are incomplete",
            )
            .build_error());
        }

        let options = req.options.unwrap_or_default();
        let window_id = uuid::Uuid::new_v4().to_string();
        let per_user = self
            .push_domain_service
            .push_event_envelope_to_users(
                ctx,
                EventEnvelopePushRequest {
                    user_ids: &req.user_ids,
                    events: req.events,
                    options: &options,
                    window_id: &window_id,
                    conversation_id: &req.conversation_id,
                    max_conversation_seq: req.max_conversation_seq,
                    delivery_mode: req.delivery_mode,
                    inline_events_truncated: req.inline_events_truncated,
                },
            )
            .await?;

        let at = Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        };

        let mut total_pushed: i32 = 0;
        let mut total_failed: i32 = 0;
        let mut total_offline: i32 = 0;
        let mut user_results: Vec<UserPushResult> = Vec::with_capacity(per_user.len());

        for (user_id, pushed, fail, offline) in per_user {
            total_pushed = total_pushed.saturating_add(pushed);
            total_failed = total_failed.saturating_add(fail);
            total_offline = total_offline.saturating_add(offline);
            user_results.push(UserPushResult {
                user_id,
                result: Some(PushResult {
                    pushed_device_count: pushed,
                    offline_pending_count: offline,
                    window_id: window_id.clone(),
                    at: Some(at),
                }),
            });
        }
        if total_failed > 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "PushEvent: failed to write to one or more online connections",
            )
            .details(format!(
                "pushed_device_count={total_pushed}, failed_connection_count={total_failed}, offline_pending_count={total_offline}"
            ))
            .build_error());
        }

        Ok(PushResponse {
            result: Some(PushResult {
                pushed_device_count: total_pushed,
                offline_pending_count: total_offline,
                window_id: window_id.clone(),
                at: Some(at),
            }),
            user_results,
        })
    }

    /// PushNotification：编排领域层
    #[instrument(skip(self))]
    pub async fn handle_push_notification(
        &self,
        ctx: &Ctx,
        req: PushNotificationCommand,
    ) -> Result<PushNotificationResponse> {
        let _ = req;
        let window_id = uuid::Uuid::new_v4().to_string();
        Ok(PushNotificationResponse {
            result: Some(push_result_at_now(0, 0, window_id)),
            user_results: vec![],
            notification_id: None,
        })
    }

    /// PushAck：编排领域层（可委托 push_domain_service.push_ack_to_user）
    #[instrument(skip(self))]
    pub async fn handle_push_ack(&self, ctx: &Ctx, req: PushAckCommand) -> Result<PushAckResponse> {
        let window_id = uuid::Uuid::new_v4().to_string();
        if req.user_ids.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "PushAck: user_ids is empty",
            )
            .build_error());
        }

        let options = req.options.unwrap_or_default();
        let mut ack_payload = Vec::new();
        req.ack.encode(&mut ack_payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode PushAck payload failed")
                .details(e.to_string())
                .build_error()
        })?;
        let per_user = self
            .push_domain_service
            .push_ack_to_users(ctx, &req.user_ids, ack_payload, &options)
            .await?;
        let at = Timestamp {
            seconds: Utc::now().timestamp(),
            nanos: 0,
        };

        let mut total_pushed: i32 = 0;
        let mut total_offline: i32 = 0;
        let mut user_results: Vec<UserPushResult> = Vec::with_capacity(per_user.len());
        for (user_id, pushed, _fail, offline) in per_user {
            total_pushed = total_pushed.saturating_add(pushed);
            total_offline = total_offline.saturating_add(offline);
            user_results.push(UserPushResult {
                user_id,
                result: Some(PushResult {
                    pushed_device_count: pushed,
                    offline_pending_count: offline,
                    window_id: window_id.clone(),
                    at: Some(at),
                }),
            });
        }

        Ok(PushAckResponse {
            result: Some(PushResult {
                pushed_device_count: total_pushed,
                offline_pending_count: total_offline,
                window_id,
                at: Some(at),
            }),
            user_results,
        })
    }

    /// PushCustom：编排领域层
    #[instrument(skip(self))]
    pub async fn handle_push_custom(
        &self,
        ctx: &Ctx,
        req: PushCustomDataCommand,
    ) -> Result<PushResponse> {
        let _ = req;
        let window_id = uuid::Uuid::new_v4().to_string();
        Ok(PushResponse {
            result: Some(push_result_at_now(0, 0, window_id)),
            user_results: vec![],
        })
    }

    /// 供 gRPC 层将 proto 请求转为 Command 时使用（可选）
    pub fn push_domain_service(&self) -> &Arc<PushDomainService> {
        &self.push_domain_service
    }
}
