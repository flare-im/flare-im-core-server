//! 推送领域服务
//!
//! 包含推送相关的核心业务逻辑，仅依赖 ConnectionQuery（读连接）与 IPushPort（写推送）。

use std::sync::Arc;

use flare_grpc_proto::access_gateway::PushOptions;
use flare_im_contracts::Ctx;
use flare_proto::common::{Event, EventEnvelope, EventEnvelopeDeliveryMode, Message, MessagePush};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use prost::Message as ProstMessage;
use tracing::{info, instrument};

use crate::domain::model::{ConnectionInfo, DomainPushResult};
use crate::domain::ports::{ConnectionQuery, IPushPort};

/// 推送领域服务
pub struct PushDomainService {
    push_port: Arc<dyn IPushPort>,
    connection_query: Arc<dyn ConnectionQuery>,
}

pub struct EventEnvelopePushRequest<'a> {
    pub user_ids: &'a [String],
    pub events: Vec<Event>,
    pub options: &'a PushOptions,
    pub window_id: &'a str,
    pub conversation_id: &'a str,
    pub max_conversation_seq: u64,
    pub delivery_mode: i32,
    pub inline_events_truncated: bool,
}

impl PushDomainService {
    pub fn new(push_port: Arc<dyn IPushPort>, connection_query: Arc<dyn ConnectionQuery>) -> Self {
        Self {
            push_port,
            connection_query,
        }
    }

    fn build_event_envelope(
        events: Vec<Event>,
        window_id: &str,
        conversation_id_hint: &str,
        max_conversation_seq_hint: u64,
        delivery_mode_hint: i32,
        inline_events_truncated: bool,
    ) -> Result<EventEnvelope> {
        let conversation_id = events
            .first()
            .map(|event| event.conversation_id.trim().to_string())
            .filter(|id| !id.is_empty())
            .or_else(|| {
                let trimmed = conversation_id_hint.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            })
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "EventEnvelope: conversation_id is empty",
                )
                .build_error()
            })?;
        if conversation_id.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: conversation_id is empty",
            )
            .build_error());
        }
        if events
            .iter()
            .any(|event| event.conversation_id.trim() != conversation_id)
        {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: mixed conversation_id values are not supported",
            )
            .build_error());
        }

        let min_conversation_seq = events.iter().map(|e| e.conversation_seq).min().unwrap_or(0);
        let max_conversation_seq = events
            .iter()
            .map(|e| e.conversation_seq)
            .max()
            .unwrap_or(0)
            .max(max_conversation_seq_hint);
        if events.is_empty() && max_conversation_seq == 0 {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: pure ping requires max_conversation_seq",
            )
            .build_error());
        }
        let delivery_mode =
            Self::resolve_event_envelope_delivery_mode(delivery_mode_hint, events.is_empty())?;
        Ok(EventEnvelope {
            events,
            max_conversation_seq,
            has_more: false,
            next_cursor: if max_conversation_seq > 0 {
                format!("evt:{max_conversation_seq}")
            } else {
                String::new()
            },
            window_id: window_id.to_string(),
            delivery_mode,
            conversation_id,
            min_conversation_seq,
            inline_events_truncated,
            attributes: Default::default(),
        })
    }

    fn resolve_event_envelope_delivery_mode(delivery_mode: i32, events_empty: bool) -> Result<i32> {
        let mode = EventEnvelopeDeliveryMode::try_from(delivery_mode)
            .unwrap_or(EventEnvelopeDeliveryMode::Unspecified);
        let resolved = match mode {
            EventEnvelopeDeliveryMode::Unspecified if events_empty => {
                EventEnvelopeDeliveryMode::Ping
            }
            EventEnvelopeDeliveryMode::Unspecified => EventEnvelopeDeliveryMode::PingWithInline,
            other => other,
        };
        if events_empty && resolved != EventEnvelopeDeliveryMode::Ping {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "EventEnvelope: pure ping must use PING delivery mode",
            )
            .build_error());
        }
        Ok(resolved as i32)
    }

    /// 检查用户是否在线
    ///
    /// Gateway 直接查询本地连接状态，不维护缓存
    /// 在线状态由 Signaling Online 服务统一管理
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn check_user_online(&self, tx: &Ctx, user_id: &str) -> Result<bool> {
        // 直接查询本地连接状态
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(!connections.is_empty())
    }

    /// 过滤连接（根据设备ID和平台）
    pub fn filter_connections(
        &self,
        _tx: &Ctx,
        connections: &[ConnectionInfo],
        options: &PushOptions,
    ) -> Vec<ConnectionInfo> {
        connections
            .iter()
            .filter(|conn| {
                if !options.device_ids.is_empty() && !options.device_ids.contains(&conn.device_id) {
                    return false;
                }
                let platform = conn.platform.as_deref().unwrap_or("");
                if !options.platforms.is_empty() && !options.platforms.iter().any(|p| p == platform)
                {
                    return false;
                }
                true
            })
            .cloned()
            .collect()
    }

    /// 推送消息到连接（委托仓储按 connection_id 批量推送）
    #[instrument(skip(self, tx, message_bytes), fields(user_id = %user_id, connection_count = connections.len()))]
    pub async fn push_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        message_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        let payload_type = flare_core::common::protocol::payload_command::Type::Message as i32;
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, message_bytes.to_vec())
            .await
    }

    /// 向**单连接**下行已编码载荷（gRPC/领域编排共用，统一走 [`IPushPort`]）
    #[instrument(skip(self, tx, payload), fields(connection_id = %connection_id, payload_type = %payload_type))]
    pub async fn deliver_payload_to_connection(
        &self,
        tx: &Ctx,
        connection_id: &str,
        payload_type: i32,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.push_port
            .push_payload_to_connection(tx, connection_id, payload_type, payload)
            .await
    }

    /// 按 Payload 类型推送字节到指定连接列表（2=Event 3=Ack 4=Data）
    #[instrument(skip(self, tx, payload_bytes), fields(user_id = %user_id, payload_type = %payload_type))]
    pub async fn push_payload_to_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        connections: &[ConnectionInfo],
        payload_type: i32,
        payload_bytes: &[u8],
    ) -> Result<(i32, i32)> {
        let _ = user_id;
        let connection_ids: Vec<String> = connections
            .iter()
            .map(|c| c.connection_id.clone())
            .collect();
        self.push_port
            .push_payload_to_connections(tx, &connection_ids, payload_type, payload_bytes.to_vec())
            .await
    }

    /// 获取用户连接并过滤
    #[instrument(skip(self, tx), fields(user_id = %user_id))]
    pub async fn get_filtered_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
        options: &PushOptions,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self
            .connection_query
            .query_user_connections(tx, user_id)
            .await?;

        Ok(self.filter_connections(tx, &connections, options))
    }

    /// 将业务消息下行给多个用户：载荷为 `MessagePush` 编码字节（与客户端 `chatroom_client` / SDK 解码一致）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 按用户一行。
    #[instrument(skip(self, tx, messages), fields(user_count = user_ids.len(), message_count = messages.len()))]
    pub async fn push_message_push_to_users(
        &self,
        tx: &Ctx,
        user_ids: &[String],
        messages: Vec<Message>,
        options: &PushOptions,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let push = MessagePush {
            messages,
            notifications: vec![],
        };
        let mut payload = Vec::new();
        push.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode MessagePush failed")
                .details(e.to_string())
                .build_error()
        })?;

        let mut out = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            let connections = self.get_filtered_connections(tx, user_id, options).await?;
            if connections.is_empty() {
                info!(user_id = %user_id, "push_message: user has no matching online connection");
                out.push((user_id.clone(), 0, 0, 1));
                continue;
            }
            let (ok, fail) = self
                .push_to_connections(tx, user_id, &connections, &payload)
                .await?;
            info!(
                user_id = %user_id,
                pushed = ok,
                failed = fail,
                "push_message: MessagePush delivered"
            );
            out.push((user_id.clone(), ok, fail, 0));
        }
        Ok(out)
    }

    /// 将领域事件批下行给多个用户：载荷为 `EventEnvelope` 编码字节（与客户端 SDK `ProtobufCodec::decode_server` 一致，走 `PayloadCommand::Message` 内层解码）。
    ///
    /// 返回 `(user_id, pushed_device_count, failed_count, offline_pending_count)` 与 [`Self::push_message_push_to_users`] 对齐。
    #[instrument(skip(self, tx, request), fields(user_count = request.user_ids.len(), event_count = request.events.len()))]
    pub async fn push_event_envelope_to_users(
        &self,
        tx: &Ctx,
        request: EventEnvelopePushRequest<'_>,
    ) -> Result<Vec<(String, i32, i32, i32)>> {
        let envelope = Self::build_event_envelope(
            request.events,
            request.window_id,
            request.conversation_id,
            request.max_conversation_seq,
            request.delivery_mode,
            request.inline_events_truncated,
        )?;
        let mut payload = Vec::new();
        envelope.encode(&mut payload).map_err(|e| {
            ErrorBuilder::new(ErrorCode::InternalError, "encode EventEnvelope failed")
                .details(e.to_string())
                .build_error()
        })?;

        let mut out = Vec::with_capacity(request.user_ids.len());
        for user_id in request.user_ids {
            let connections = self
                .get_filtered_connections(tx, user_id, request.options)
                .await?;
            if connections.is_empty() {
                info!(user_id = %user_id, "push_event: user has no matching online connection");
                out.push((user_id.clone(), 0, 0, 1));
                continue;
            }
            let (ok, fail) = self
                .push_to_connections(tx, user_id, &connections, &payload)
                .await?;
            info!(
                user_id = %user_id,
                pushed = ok,
                failed = fail,
                "push_event: EventEnvelope delivered"
            );
            out.push((user_id.clone(), ok, fail, 0));
        }
        Ok(out)
    }

    /// 推送 ACK 字节给用户（payload 为 common::Ack encode_to_vec）
    #[instrument(skip(self, tx, ack_payload), fields(user_id = %user_id))]
    pub async fn push_ack_to_user(
        &self,
        tx: &Ctx,
        user_id: &str,
        ack_payload: Vec<u8>,
    ) -> Result<()> {
        let payload_type = flare_core::common::protocol::payload_command::Type::Ack as i32;
        self.push_port
            .push_payload_to_user(tx, user_id, payload_type, ack_payload)
            .await
    }

    /// 构建推送结果
    pub fn build_push_result(
        _tx: &Ctx,
        user_id: String,
        success_count: i32,
        failure_count: i32,
    ) -> DomainPushResult {
        DomainPushResult {
            user_id,
            success_count,
            failure_count,
            error_message: if failure_count > 0 {
                format!("Failed to push to {} connections", failure_count)
            } else {
                String::new()
            },
        }
    }

    /// 查询用户连接列表（供 GetUserConnections 使用）：按平台过滤并限制条数
    #[instrument(skip(self, _tx), fields(user_id = %user_id))]
    pub async fn list_user_connections(
        &self,
        _tx: &Ctx,
        user_id: &str,
        platforms: &[String],
        limit: i32,
    ) -> Result<Vec<ConnectionInfo>> {
        let connections = self.connection_query.list_user_connections(user_id).await?;
        let limit = limit.clamp(0, 500) as usize;
        let filtered: Vec<ConnectionInfo> = if platforms.is_empty() {
            connections
        } else {
            connections
                .into_iter()
                .filter(|c| {
                    c.platform
                        .as_ref()
                        .map(|p| platforms.iter().any(|f| f == p))
                        .unwrap_or(false)
                })
                .collect()
        };
        Ok(filtered.into_iter().take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CapturingPushPort {
        payloads: Mutex<Vec<(i32, Vec<u8>)>>,
    }

    #[async_trait]
    impl IPushPort for CapturingPushPort {
        async fn push_message_to_user(
            &self,
            _tx: &Ctx,
            _user_id: &str,
            _message: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_message_to_connection(
            &self,
            _tx: &Ctx,
            _connection_id: &str,
            _message: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn push_payload_to_connection(
            &self,
            _tx: &Ctx,
            _connection_id: &str,
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<()> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok(())
        }

        async fn push_payload_to_user(
            &self,
            _tx: &Ctx,
            _user_id: &str,
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<()> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok(())
        }

        async fn push_payload_to_connections(
            &self,
            _tx: &Ctx,
            connection_ids: &[String],
            payload_type: i32,
            payload: Vec<u8>,
        ) -> Result<(i32, i32)> {
            self.payloads
                .lock()
                .expect("payload mutex poisoned")
                .push((payload_type, payload));
            Ok((connection_ids.len() as i32, 0))
        }
    }

    struct StaticConnectionQuery {
        connections: Vec<ConnectionInfo>,
    }

    #[async_trait]
    impl ConnectionQuery for StaticConnectionQuery {
        async fn query_user_connections(
            &self,
            _tx: &Ctx,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(self.connections.clone())
        }

        async fn list_user_connections(&self, _user_id: &str) -> Result<Vec<ConnectionInfo>> {
            Ok(self.connections.clone())
        }
    }

    #[tokio::test]
    async fn push_event_envelope_sets_window_and_delivery_contract() {
        let push_port = Arc::new(CapturingPushPort::default());
        let connection_query = Arc::new(StaticConnectionQuery {
            connections: vec![
                ConnectionInfo::new(
                    "conn-1".to_string(),
                    "u1".to_string(),
                    "t1".to_string(),
                    "dev-1".to_string(),
                )
                .with_platform("ios".to_string()),
            ],
        });
        let service = PushDomainService::new(push_port.clone(), connection_query);
        let ctx: Ctx = Arc::new(flare_server_core::Context::root());
        let user_ids = vec!["u1".to_string()];
        let event = Event {
            conversation_id: "c1".to_string(),
            conversation_seq: 42,
            ..Default::default()
        };

        let result = service
            .push_event_envelope_to_users(
                &ctx,
                EventEnvelopePushRequest {
                    user_ids: &user_ids,
                    events: vec![event],
                    options: &PushOptions::default(),
                    window_id: "window-1",
                    conversation_id: "",
                    max_conversation_seq: 0,
                    delivery_mode: 0,
                    inline_events_truncated: false,
                },
            )
            .await
            .expect("push should succeed");

        assert_eq!(result, vec![("u1".to_string(), 1, 0, 0)]);
        let payloads = push_port.payloads.lock().expect("payload mutex poisoned");
        assert_eq!(payloads.len(), 1);
        assert_eq!(
            payloads[0].0,
            flare_core::common::protocol::payload_command::Type::Message as i32
        );
        let envelope =
            EventEnvelope::decode(payloads[0].1.as_slice()).expect("decode EventEnvelope");
        assert_eq!(envelope.window_id, "window-1");
        assert_eq!(envelope.conversation_id, "c1");
        assert_eq!(envelope.min_conversation_seq, 42);
        assert_eq!(envelope.max_conversation_seq, 42);
        assert_eq!(
            envelope.delivery_mode,
            EventEnvelopeDeliveryMode::PingWithInline as i32
        );
        assert!(!envelope.inline_events_truncated);
    }

    #[test]
    fn build_event_envelope_rejects_mixed_conversation_ids() {
        let events = vec![
            Event {
                conversation_id: "c1".to_string(),
                conversation_seq: 1,
                ..Default::default()
            },
            Event {
                conversation_id: "c2".to_string(),
                conversation_seq: 2,
                ..Default::default()
            },
        ];

        let error = PushDomainService::build_event_envelope(events, "window-1", "", 0, 0, false)
            .expect_err("mixed conversations must be rejected");

        assert!(error.to_string().contains("mixed conversation_id"));
    }

    #[test]
    fn build_event_envelope_supports_pure_ping() {
        let envelope = PushDomainService::build_event_envelope(
            vec![],
            "window-1",
            "c1",
            99,
            EventEnvelopeDeliveryMode::Ping as i32,
            true,
        )
        .expect("pure ping should be valid");

        assert!(envelope.events.is_empty());
        assert_eq!(envelope.conversation_id, "c1");
        assert_eq!(envelope.max_conversation_seq, 99);
        assert_eq!(
            envelope.delivery_mode,
            EventEnvelopeDeliveryMode::Ping as i32
        );
        assert!(envelope.inline_events_truncated);
    }
}
